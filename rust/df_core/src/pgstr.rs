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

//! PostgreSQL's string operators and functions over UTF-8 text, transcribed
//! from the backend so that results and errors agree with it: LIKE
//! (like_match.c, UTF8_MatchText).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::pgfunc::PgError;

fn pg_error(sqlstate: &'static str, message: &str) -> DataFusionError {
    DataFusionError::External(Box::new(PgError::new(sqlstate, message)))
}

/// `a` as a Utf8 array (DataFusion may hand over another string layout).
fn utf8(a: &ArrayRef) -> Result<ArrayRef> {
    if a.data_type() == &DataType::Utf8 {
        Ok(a.clone())
    } else {
        cast(a, &DataType::Utf8).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// A scalar text argument: Some(None) for NULL, None if it is not a scalar.
fn scalar_text(v: &ColumnarValue) -> Option<Option<String>> {
    match v {
        ColumnarValue::Scalar(ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s)) => {
            Some(s.clone())
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// LIKE
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum Like {
    True,
    False,
    Abort,
}

/// Patterns recurse once per % group; PostgreSQL stops at max_stack_depth.
const LIKE_MAX_DEPTH: usize = 10_000;

/// NextChar for UTF-8: skip the byte and any continuation bytes.
#[inline]
fn next_char(t: &[u8], mut i: usize) -> usize {
    i += 1;
    while i < t.len() && t[i] & 0xC0 == 0x80 {
        i += 1;
    }
    i
}

fn escape_at_end() -> DataFusionError {
    pg_error("22025", "LIKE pattern must not end with escape character")
}

/// MatchText of like_match.c for UTF-8 with the default escape (backslash).
fn match_text(t: &[u8], p: &[u8], depth: usize) -> Result<Like> {
    if p == b"%" {
        return Ok(Like::True);
    }
    if depth > LIKE_MAX_DEPTH {
        return Err(pg_error("54001", "stack depth limit exceeded"));
    }
    let (mut ti, mut pi) = (0usize, 0usize);
    while ti < t.len() && pi < p.len() {
        if p[pi] == b'\\' {
            // The next pattern byte must match literally, and there must be one.
            pi += 1;
            if pi >= p.len() {
                return Err(escape_at_end());
            }
            if p[pi] != t[ti] {
                return Ok(Like::False);
            }
        } else if p[pi] == b'%' {
            // Skip further wildcards: N _'s and any %'s match at least N chars.
            pi += 1;
            while pi < p.len() {
                if p[pi] == b'%' {
                    pi += 1;
                } else if p[pi] == b'_' {
                    if ti >= t.len() {
                        return Ok(Like::Abort);
                    }
                    ti = next_char(t, ti);
                    pi += 1;
                } else {
                    break;
                }
            }
            if pi >= p.len() {
                return Ok(Like::True);
            }
            // Try each text position where the next literal can start.
            let first = if p[pi] == b'\\' {
                if pi + 1 >= p.len() {
                    return Err(escape_at_end());
                }
                p[pi + 1]
            } else {
                p[pi]
            };
            while ti < t.len() {
                if t[ti] == first {
                    let m = match_text(&t[ti..], &p[pi..], depth + 1)?;
                    if m != Like::False {
                        return Ok(m);
                    }
                }
                ti = next_char(t, ti);
            }
            return Ok(Like::Abort);
        } else if p[pi] == b'_' {
            ti = next_char(t, ti);
            pi += 1;
            continue;
        } else if p[pi] != t[ti] {
            return Ok(Like::False);
        }
        // In lockstep: advancing by byte keeps text and pattern in sync.
        ti += 1;
        pi += 1;
    }
    if ti < t.len() {
        return Ok(Like::False);
    }
    while pi < p.len() && p[pi] == b'%' {
        pi += 1;
    }
    Ok(if pi >= p.len() { Like::True } else { Like::Abort })
}

/// A pattern prepared once: literal pieces between %'s when it has no `_`
/// (and no dangling escape), else PostgreSQL's general matcher.
#[derive(Debug)]
enum Pattern {
    /// The text equals the only piece, or with %'s: starts with the first
    /// piece, ends with the last and holds the middle ones in order.
    Pieces(Vec<Vec<u8>>),
    General(Vec<u8>),
}

impl Pattern {
    fn new(p: &[u8]) -> Pattern {
        let mut pieces = vec![Vec::new()];
        let mut i = 0;
        while i < p.len() {
            match p[i] {
                b'\\' if i + 1 < p.len() => {
                    pieces.last_mut().unwrap().push(p[i + 1]);
                    i += 2;
                    continue;
                }
                b'\\' | b'_' => return Pattern::General(p.to_vec()),
                b'%' => pieces.push(Vec::new()),
                c => pieces.last_mut().unwrap().push(c),
            }
            i += 1;
        }
        Pattern::Pieces(pieces)
    }

    fn matches(&self, t: &[u8]) -> Result<bool> {
        match self {
            Pattern::General(p) => Ok(match_text(t, p, 0)? == Like::True),
            Pattern::Pieces(pieces) if pieces.len() == 1 => Ok(t == pieces[0].as_slice()),
            Pattern::Pieces(pieces) => {
                let (first, last) = (&pieces[0], &pieces[pieces.len() - 1]);
                if t.len() < first.len() + last.len() || !t.starts_with(first) || !t.ends_with(last) {
                    return Ok(false);
                }
                // UTF-8 pieces can only match at character boundaries.
                let mut rest = &t[first.len()..t.len() - last.len()];
                for piece in &pieces[1..pieces.len() - 1] {
                    match find(rest, piece) {
                        Some(at) => rest = &rest[at + piece.len()..],
                        None => return Ok(false),
                    }
                }
                Ok(true)
            }
        }
    }
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    // Both are UTF-8 (whole characters), so str's searcher applies.
    let (h, n) = unsafe { (std::str::from_utf8_unchecked(hay), std::str::from_utf8_unchecked(needle)) };
    h.find(n)
}

/// `text LIKE pattern` (`~~`) and `NOT LIKE` (`!~~`).
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgLike {
    negated: bool,
    signature: Signature,
}

impl PgLike {
    pub fn udf(negated: bool) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgLike { negated, signature: Signature::any(2, Volatility::Immutable) })
    }
}

impl ScalarUDFImpl for PgLike {
    fn name(&self) -> &str {
        if self.negated {
            "pg_textnlike"
        } else {
            "pg_textlike"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let text = utf8(&args.args[0].to_array(n)?)?;
        let text = text.as_string::<i32>();
        let out: BooleanArray = match scalar_text(&args.args[1]) {
            Some(None) => BooleanArray::new_null(n),
            Some(Some(p)) => {
                let pattern = Pattern::new(p.as_bytes());
                let mut v = Vec::with_capacity(n);
                for r in 0..n {
                    v.push(if text.is_null(r) {
                        None
                    } else {
                        Some(pattern.matches(text.value(r).as_bytes())? != self.negated)
                    });
                }
                BooleanArray::from(v)
            }
            None => {
                let pats = utf8(&args.args[1].to_array(n)?)?;
                let pats = pats.as_string::<i32>();
                let mut v = Vec::with_capacity(n);
                for r in 0..n {
                    v.push(if text.is_null(r) || pats.is_null(r) {
                        None
                    } else {
                        let m = match_text(text.value(r).as_bytes(), pats.value(r).as_bytes(), 0)? == Like::True;
                        Some(m != self.negated)
                    });
                }
                BooleanArray::from(v)
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn like(t: &str, p: &str) -> Result<bool> {
        let general = match_text(t.as_bytes(), p.as_bytes(), 0)? == Like::True;
        let prepared = Pattern::new(p.as_bytes()).matches(t.as_bytes())?;
        assert_eq!(general, prepared, "{t:?} LIKE {p:?}");
        Ok(general)
    }

    #[test]
    fn like_agrees_with_postgresql() {
        // Expected values from PostgreSQL 16.
        let cases: &[(&str, &str, bool)] = &[
            ("abc", "abc", true),
            ("abc", "a%", true),
            ("abc", "%c", true),
            ("abc", "%b%", true),
            ("abc", "a_c", true),
            ("abc", "_", false),
            ("", "%", true),
            ("", "", true),
            ("", "_", false),
            ("a%c", "a\\%c", true),
            ("abc", "a\\%c", false),
            ("a_c", "a\\_c", true),
            ("abc", "a\\bc", true),
            ("a\\c", "a\\\\c", true),
            ("line1\nline2", "line1%line2", true),
            ("中文字符", "中_字%", true),
            ("中文字符", "__字符", true),
            ("中文字符", "___", false),
            ("ab", "a%%%b", true),
            ("abcabc", "%abc", true),
            ("abcab", "%a%b%c%", true),
            ("abab", "a%a%b", true),
            ("aXbXc", "a_b_c", true),
            ("abc", "%_%_%_%", true),
            ("ab", "%_%_%_%", false),
            ("aaa", "%aa", true),
            ("aa", "%aaa", false),
        ];
        for (t, p, want) in cases {
            assert_eq!(like(t, p).unwrap(), *want, "{t:?} LIKE {p:?}");
        }
    }

    #[test]
    fn dangling_escape_errors_only_when_reached() {
        // 'xbc' LIKE 'a\' fails on 'x' before reaching the escape.
        assert_eq!(like("xbc", "a\\").unwrap(), false);
        assert!(like("abc", "a\\").is_err());
        assert!(like("abc", "%\\").is_err());
    }
}
