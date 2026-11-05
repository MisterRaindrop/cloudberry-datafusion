// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! RX1: regexp_replace(text, text, text) as PostgreSQL computes it, for the
//! regular expressions src/vx_regex.c lets through.  The C side cuts the
//! expression into segments at its top-level capturing groups; the match is
//! the leftmost (it ends at the end of the value: the expression ends in $),
//! and each segment in turn ends as late as the rest still allows
//! (PostgreSQL's ccondissect).  Lazy DFAs of the segments find that split:
//! a reverse search anchored at the end lists where the rest of the
//! expression can start, a forward search anchored at the segment's start
//! where it can end, and the segment ends at the last position in both.

use std::fmt::Write;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use regex_automata::hybrid::dfa::{Cache, OverlappingState, DFA};
use regex_automata::nfa::thompson;
use regex_automata::{Anchored, Input, MatchKind};

use crate::pgstr::utf8;

fn internal(message: String) -> DataFusionError {
    DataFusionError::Internal(format!("regexp_replace: {message}"))
}

/// A part of the expression, as vx_regex.c sends it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Segment {
    pub re: String,
    pub capture: bool,
}

/// `c` quoted for regex-syntax where it would mean something else, in or
/// out of a class.
fn push_literal(out: &mut String, c: char) {
    if "\\.+*?()|[]{}^$#&-~".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

/// A segment in the syntax of regex-syntax: the subset vx_regex.c takes
/// reads the same there but for the quoting of literals; '.' matches a
/// newline as in PostgreSQL (the caller sets the s flag).
fn translate(re: &str) -> Result<String> {
    let bad = || internal(format!("unexpected expression {re:?}"));
    let mut out = String::with_capacity(re.len() + 8);
    let mut it = re.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '\\' => push_literal(&mut out, it.next().ok_or_else(bad)?),
            '[' => {
                out.push('[');
                if it.peek() == Some(&'^') {
                    out.push(it.next().unwrap());
                }
                loop {
                    match it.next().ok_or_else(bad)? {
                        ']' => break,
                        '\\' => push_literal(&mut out, it.next().ok_or_else(bad)?),
                        lo if lo.is_ascii_alphanumeric() && it.peek() == Some(&'-') => {
                            it.next();
                            let hi = it.next().ok_or_else(bad)?;
                            if !hi.is_ascii_alphanumeric() {
                                return Err(bad());
                            }
                            out.push(lo);
                            out.push('-');
                            out.push(hi);
                        }
                        l => push_literal(&mut out, l),
                    }
                }
                out.push(']');
            }
            '(' => {
                if it.next() != Some('?') || it.next() != Some(':') {
                    return Err(bad());
                }
                out.push_str("(?:");
            }
            '{' => {
                out.push('{');
                loop {
                    let d = it.next().ok_or_else(bad)?;
                    out.push(d);
                    if d == '}' {
                        break;
                    }
                    if !d.is_ascii_digit() && d != ',' {
                        return Err(bad());
                    }
                }
            }
            ')' | '.' | '^' | '$' | '?' | '*' | '+' => out.push(c),
            l => push_literal(&mut out, l),
        }
    }
    Ok(out)
}

fn dfa(segments: &[Segment], reverse: bool) -> Result<DFA> {
    let mut re = String::from("(?s)");
    for s in segments {
        write!(re, "(?:{})", translate(&s.re)?).unwrap();
    }
    DFA::builder()
        .configure(DFA::config().match_kind(MatchKind::All))
        .thompson(thompson::Config::new().reverse(reverse))
        .build(&re)
        .map_err(|e| internal(format!("{re}: {e}")))
}

/// The replacement, its \1 to \9 and \& read as PostgreSQL does
/// (appendStringInfoRegexpSubstr).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Piece {
    Text(String),
    Group(usize), // 0: the whole match
}

fn pieces(replacement: &str) -> Vec<Piece> {
    let mut out = Vec::new();
    let mut text = String::new();
    let mut it = replacement.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\\' {
            text.push(c);
            continue;
        }
        match it.peek().copied() {
            Some(d @ '1'..='9') => {
                it.next();
                out.push(Piece::Text(std::mem::take(&mut text)));
                out.push(Piece::Group(d as usize - '0' as usize));
            }
            Some('&') => {
                it.next();
                out.push(Piece::Text(std::mem::take(&mut text)));
                out.push(Piece::Group(0));
            }
            Some('\\') => {
                it.next();
                text.push('\\');
            }
            // any other character (or none) after it: the \ itself
            _ => text.push('\\'),
        }
    }
    out.push(Piece::Text(text));
    out.retain(|p| !matches!(p, Piece::Text(t) if t.is_empty()));
    out
}

/// The automata of one call's expression.
struct Matcher {
    /// the whole expression, reversed: the leftmost start of the match
    whole: DFA,
    /// per segment up to the last capturing group needed: the segment
    /// forward, and the rest of the expression after it reversed
    splits: Vec<(DFA, DFA)>,
    /// byte spans of the groups, by segment index
    groups: Vec<usize>,
}

struct Caches {
    whole: Cache,
    splits: Vec<(Cache, Cache)>,
}

impl Matcher {
    fn new(segments: &[Segment], pieces: &[Piece]) -> Result<Matcher> {
        let groups: Vec<usize> = (0..segments.len())
            .filter(|&i| segments[i].capture)
            .collect();
        // only the groups the replacement refers to need a split
        let used = pieces
            .iter()
            .filter_map(|p| match p {
                Piece::Group(g) if *g >= 1 && *g <= groups.len() => Some(groups[*g - 1]),
                _ => None,
            })
            .max();
        let mut splits = Vec::new();
        if let Some(last) = used {
            for k in 0..=last.min(segments.len() - 2) {
                splits.push((
                    dfa(&segments[k..=k], false)?,
                    dfa(&segments[k + 1..], true)?,
                ));
            }
        }
        Ok(Matcher {
            whole: dfa(segments, true)?,
            splits,
            groups,
        })
    }

    fn caches(&self) -> Caches {
        Caches {
            whole: self.whole.create_cache(),
            splits: self
                .splits
                .iter()
                .map(|(f, r)| (f.create_cache(), r.create_cache()))
                .collect(),
        }
    }

    /// Where matches of reversed `dfa` anchored at the end of `s` start, from
    /// `from` on, as flags by offset from `from`.
    fn starts(dfa: &DFA, cache: &mut Cache, s: &str, from: usize, flags: &mut Vec<bool>) -> Result<()> {
        flags.clear();
        flags.resize(s.len() - from + 1, false);
        let input = Input::new(s).range(from..).anchored(Anchored::Yes);
        let mut state = OverlappingState::start();
        loop {
            dfa.try_search_overlapping_rev(cache, &input, &mut state)
                .map_err(|e| internal(e.to_string()))?;
            match state.get_match() {
                Some(m) => flags[m.offset() - from] = true,
                None => return Ok(()),
            }
        }
    }

    /// The byte spans of the match (0) and of the segments up to the last
    /// split, or None if nothing matches.
    fn find(&self, c: &mut Caches, s: &str, flags: &mut Vec<bool>) -> Result<Option<Vec<usize>>> {
        Self::starts(&self.whole, &mut c.whole, s, 0, flags)?;
        let Some(start) = flags.iter().position(|&f| f) else {
            return Ok(None);
        };
        // bounds[k] is where segment k starts
        let mut bounds = vec![start];
        for (k, (fwd, rev)) in self.splits.iter().enumerate() {
            let cur = bounds[k];
            let (fc, rc) = &mut c.splits[k];
            Self::starts(rev, rc, s, cur, flags)?;
            let input = Input::new(s).range(cur..).anchored(Anchored::Yes);
            let mut state = OverlappingState::start();
            let mut end = None;
            loop {
                fwd.try_search_overlapping_fwd(fc, &input, &mut state)
                    .map_err(|e| internal(e.to_string()))?;
                match state.get_match() {
                    Some(m) if flags[m.offset() - cur] => end = Some(m.offset()),
                    Some(_) => {}
                    None => break,
                }
            }
            bounds.push(end.ok_or_else(|| internal(format!("no split of {s:?}")))?);
        }
        Ok(Some(bounds))
    }
}

/// regexp_replace(text, constant, constant) of the segments' expression.
pub struct PgRegexpReplace {
    segments: Vec<Segment>,
    replacement: String,
    pieces: Vec<Piece>,
    matcher: Arc<Matcher>,
    signature: Signature,
}

impl std::fmt::Debug for PgRegexpReplace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgRegexpReplace")
            .field("segments", &self.segments)
            .field("replacement", &self.replacement)
            .finish()
    }
}

impl PartialEq for PgRegexpReplace {
    fn eq(&self, other: &Self) -> bool {
        self.segments == other.segments && self.replacement == other.replacement
    }
}

impl Eq for PgRegexpReplace {}

impl Hash for PgRegexpReplace {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.segments.hash(state);
        self.replacement.hash(state);
    }
}

impl PgRegexpReplace {
    pub fn new(segments: Vec<Segment>, replacement: String) -> Result<PgRegexpReplace> {
        if segments.is_empty() {
            return Err(internal("no segments".into()));
        }
        let pieces = pieces(&replacement);
        let matcher = Arc::new(Matcher::new(&segments, &pieces)?);
        Ok(PgRegexpReplace {
            segments,
            replacement,
            pieces,
            matcher,
            signature: Signature::any(1, Volatility::Immutable),
        })
    }

    pub fn udf(segments: Vec<Segment>, replacement: String) -> Result<ScalarUDF> {
        Ok(ScalarUDF::new_from_impl(Self::new(segments, replacement)?))
    }

    /// `s` with its match replaced, into `out`; false if nothing matches.
    fn replace(&self, c: &mut Caches, s: &str, flags: &mut Vec<bool>, out: &mut String) -> Result<bool> {
        let Some(bounds) = self.matcher.find(c, s, flags)? else {
            return Ok(false);
        };
        out.clear();
        out.push_str(&s[..bounds[0]]);
        for p in &self.pieces {
            match p {
                Piece::Text(t) => out.push_str(t),
                Piece::Group(0) => out.push_str(&s[bounds[0]..]),
                Piece::Group(g) => {
                    // a group that does not exist is empty
                    if let Some(&seg) = self.matcher.groups.get(*g - 1) {
                        out.push_str(&s[bounds[seg]..bounds[seg + 1]]);
                    }
                }
            }
        }
        Ok(true)
    }

    /// The function on plain strings (tests).
    pub fn eval(&self, s: &str) -> Result<String> {
        let mut c = self.matcher.caches();
        let (mut flags, mut out) = (Vec::new(), String::new());
        Ok(if self.replace(&mut c, s, &mut flags, &mut out)? {
            out
        } else {
            s.to_string()
        })
    }
}

impl ScalarUDFImpl for PgRegexpReplace {
    fn name(&self) -> &str {
        "pg_regexp_replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let a = utf8(&args.args[0].to_array(n)?)?;
        let a = a.as_string::<i32>();
        let mut c = self.matcher.caches();
        let (mut flags, mut buf) = (Vec::new(), String::new());
        let mut b = StringBuilder::with_capacity(n, a.value_data().len());
        for r in 0..n {
            if a.is_null(r) {
                b.append_null();
                continue;
            }
            let s = a.value(r);
            if self.replace(&mut c, s, &mut flags, &mut buf)? {
                b.append_value(&buf);
            } else {
                b.append_value(s);
            }
        }
        let out: ArrayRef = Arc::new(b.finish());
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(re: &str, capture: bool) -> Segment {
        Segment {
            re: re.into(),
            capture,
        }
    }

    fn rr(segs: &[(&str, bool)], repl: &str, s: &str) -> String {
        let segs = segs.iter().map(|&(r, c)| seg(r, c)).collect();
        PgRegexpReplace::new(segs, repl.into()).unwrap().eval(s).unwrap()
    }

    // Results of PostgreSQL 16 (regexp_replace(s, pattern, repl)).
    #[test]
    fn splits_as_postgres() {
        // '^a?(?:ab)?(.*)$': the part before the group as long as it can be
        assert_eq!(rr(&[("^a?(?:ab)?", false), (".*", true), ("$", false)], "[\\1]", "ab"), "[]");
        // 'a?(?:ab)?(.*)$': the leftmost match
        assert_eq!(rr(&[("a?(?:ab)?", false), (".*", true), ("$", false)], "[\\1]", "xab"), "[xab]");
        // '^(a*)(a*)$'
        assert_eq!(rr(&[("^", false), ("a*", true), ("a*", true), ("$", false)], "[\\1|\\2]", "aaa"), "[aaa|]");
        // '^(?:ab)?(a?)(.*)$'
        assert_eq!(
            rr(&[("^(?:ab)?", false), ("a?", true), (".*", true), ("$", false)], "[\\1|\\2]", "abab"),
            "[a|b]"
        );
        // ClickBench's Q29
        let q29 = [("^https?://(?:www\\.)?", false), ("[^/]+", true), ("/.*$", false)];
        assert_eq!(rr(&q29, "\\1", "http://www.x.com/p"), "x.com");
        assert_eq!(rr(&q29, "\\1", "https://www.x.com/"), "x.com");
        assert_eq!(rr(&q29, "\\1", "https://www./"), "www.");
        assert_eq!(rr(&q29, "\\1", "ftp://x.com/p"), "ftp://x.com/p");
        assert_eq!(rr(&q29, "\\1", "http://x.com"), "http://x.com");
        assert_eq!(rr(&q29, "\\1", "http://wwwa.ру/путь\n/x"), "wwwa.ру");
    }

    #[test]
    fn replacements_as_postgres() {
        let segs = [("b", true), ("c$", false)];
        assert_eq!(rr(&segs, "<\\&>", "abc"), "a<bc>");
        assert_eq!(rr(&segs, "\\\\1\\x\\", "abc"), "a\\1\\x\\");
        assert_eq!(rr(&segs, "[\\2]", "abc"), "a[]");
        assert_eq!(rr(&segs, "", "abd"), "abd");
        // '.' and [^x] match a newline; $ only at the end
        assert_eq!(rr(&[(".", true), ("$", false)], "<\\1>", "a\n"), "a<\n>");
        assert_eq!(rr(&[("a$", false)], "", "a\nb"), "a\nb");
    }

    #[test]
    fn literals_are_quoted() {
        assert_eq!(translate("a\\.b[\\]&-]#x{2,3}(?:y)").unwrap(), "a\\.b[\\]\\&\\-]\\#x{2,3}(?:y)");
        assert_eq!(rr(&[("[a-c&]+", true), ("$", false)], "<\\1>", "zz&b"), "zz<&b>");
    }
}
