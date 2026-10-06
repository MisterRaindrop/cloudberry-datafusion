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
//! (like_match.c, UTF8_MatchText) and the functions of varlena.c and
//! oracle_compat.c the planner hook lets through.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, Int32Array, Int32Builder, StringArray,
    StringBuilder,
};
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
        ColumnarValue::Scalar(
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) | ScalarValue::Utf8View(s),
        ) => Some(s.clone()),
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
    Ok(if pi >= p.len() {
        Like::True
    } else {
        Like::Abort
    })
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
                if t.len() < first.len() + last.len() || !t.starts_with(first) || !t.ends_with(last)
                {
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
    let (h, n) = unsafe {
        (
            std::str::from_utf8_unchecked(hay),
            std::str::from_utf8_unchecked(needle),
        )
    };
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
        ScalarUDF::new_from_impl(PgLike {
            negated,
            signature: Signature::any(2, Volatility::Immutable),
        })
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
                        let m = match_text(text.value(r).as_bytes(), pats.value(r).as_bytes(), 0)?
                            == Like::True;
                        Some(m != self.negated)
                    });
                }
                BooleanArray::from(v)
            }
        };
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrFn {
    CharLength,
    OctetLength,
    Substr,
    Textcat,
    Concat,
    Btrim,
    Ltrim,
    Rtrim,
    Left,
    Right,
    Reverse,
    Repeat,
    Lpad,
    Rpad,
    Strpos,
    Replace,
    SplitPart,
    StartsWith,
    Lower,
    Upper,
}

impl StrFn {
    pub fn from_pg(name: &str) -> Option<StrFn> {
        Some(match name {
            "char_length" => StrFn::CharLength,
            "octet_length" => StrFn::OctetLength,
            "substr" => StrFn::Substr,
            "textcat" => StrFn::Textcat,
            "concat" => StrFn::Concat,
            "btrim" => StrFn::Btrim,
            "ltrim" => StrFn::Ltrim,
            "rtrim" => StrFn::Rtrim,
            "left" => StrFn::Left,
            "right" => StrFn::Right,
            "reverse" => StrFn::Reverse,
            "repeat" => StrFn::Repeat,
            "lpad" => StrFn::Lpad,
            "rpad" => StrFn::Rpad,
            "strpos" => StrFn::Strpos,
            "replace" => StrFn::Replace,
            "split_part" => StrFn::SplitPart,
            "starts_with" => StrFn::StartsWith,
            "lower" => StrFn::Lower,
            "upper" => StrFn::Upper,
            _ => return None,
        })
    }

    /// Can a call raise an error (and so need the AND/OR guards)?
    pub fn may_fail(self) -> bool {
        matches!(
            self,
            StrFn::Substr | StrFn::Repeat | StrFn::Lpad | StrFn::Rpad | StrFn::SplitPart
        )
    }

    fn name(self) -> &'static str {
        match self {
            StrFn::CharLength => "pg_char_length",
            StrFn::OctetLength => "pg_octet_length",
            StrFn::Substr => "pg_substr",
            StrFn::Textcat => "pg_textcat",
            StrFn::Concat => "pg_concat",
            StrFn::Btrim => "pg_btrim",
            StrFn::Ltrim => "pg_ltrim",
            StrFn::Rtrim => "pg_rtrim",
            StrFn::Left => "pg_left",
            StrFn::Right => "pg_right",
            StrFn::Reverse => "pg_reverse",
            StrFn::Repeat => "pg_repeat",
            StrFn::Lpad => "pg_lpad",
            StrFn::Rpad => "pg_rpad",
            StrFn::Strpos => "pg_strpos",
            StrFn::Replace => "pg_replace",
            StrFn::SplitPart => "pg_split_part",
            StrFn::StartsWith => "pg_starts_with",
            StrFn::Lower => "pg_lower",
            StrFn::Upper => "pg_upper",
        }
    }

    fn return_type(self) -> DataType {
        match self {
            StrFn::CharLength | StrFn::OctetLength | StrFn::Strpos => DataType::Int32,
            StrFn::StartsWith => DataType::Boolean,
            _ => DataType::Utf8,
        }
    }
}

/// MaxAllocSize: the largest value PostgreSQL builds.
const MAX_ALLOC: i64 = 0x3fff_ffff;

fn too_large() -> DataFusionError {
    pg_error("54000", "requested length too large")
}

fn nchars(s: &str) -> usize {
    s.chars().count()
}

/// The first `n` characters of `s` (all of it if it has fewer).
fn first_chars(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((at, _)) => &s[..at],
        None => s,
    }
}

/// `s` without its first `n` characters.
fn skip_chars(s: &str, n: usize) -> &str {
    &s[first_chars(s, n).len()..]
}

/// text_substring: characters from `start` (1-based), `len` of them or to
/// the end.
fn substr(s: &str, start: i32, len: Option<i32>) -> Result<&str> {
    let s1 = start.max(1);
    let take = match len {
        None => None,
        Some(l) if l < 0 => return Err(pg_error("22011", "negative substring length not allowed")),
        Some(l) => match start.checked_add(l) {
            None => None, // to the end
            Some(e) if e < 1 => return Ok(""),
            Some(e) => Some((e - s1) as usize),
        },
    };
    let rest = skip_chars(s, (s1 - 1) as usize);
    Ok(match take {
        Some(n) => first_chars(rest, n),
        None => rest,
    })
}

/// lpad/rpad: `s` cut or padded to `len` characters with `fill` repeated.
fn pad(s: &str, len: i32, fill: &str, left: bool) -> Result<String> {
    let mut len = len.max(0) as usize;
    let s1len = nchars(s).min(len);
    if fill.is_empty() {
        len = s1len;
    }
    // PostgreSQL sizes the result for 4-byte characters first.
    if 4 * len as i64 + 4 > MAX_ALLOC {
        return Err(too_large());
    }
    let mut out = String::new();
    let head = first_chars(s, s1len);
    if !left {
        out.push_str(head);
    }
    let mut filler = fill.chars().cycle();
    for _ in 0..len - s1len {
        out.push(filler.next().unwrap());
    }
    if left {
        out.push_str(head);
    }
    Ok(out)
}

/// An argument column: text or int4.
enum Arg<'a> {
    Text(&'a StringArray),
    Int(&'a Int32Array),
}

impl<'a> Arg<'a> {
    fn is_null(&self, r: usize) -> bool {
        match self {
            Arg::Text(a) => a.is_null(r),
            Arg::Int(a) => a.is_null(r),
        }
    }

    fn text(&self, r: usize) -> Result<&'a str> {
        match self {
            Arg::Text(a) => Ok(a.value(r)),
            Arg::Int(_) => Err(DataFusionError::Internal("expected a text argument".into())),
        }
    }

    fn int(&self, r: usize) -> Result<i32> {
        match self {
            Arg::Int(a) => Ok(a.value(r)),
            Arg::Text(_) => Err(DataFusionError::Internal(
                "expected an integer argument".into(),
            )),
        }
    }
}

/// An argument as text or int4 (the C side passes int2 and int8 arguments
/// of these functions nowhere, but casting costs nothing to allow).
fn prepare(a: ArrayRef) -> Result<ArrayRef> {
    match a.data_type() {
        DataType::Int32 => Ok(a),
        DataType::Int16 | DataType::Int64 => {
            cast(&a, &DataType::Int32).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
        _ => utf8(&a),
    }
}

/// A text result: a piece of an argument where possible.
enum Text<'a> {
    Slice(&'a str),
    Owned(String),
}

impl Text<'_> {
    fn as_str(&self) -> &str {
        match self {
            Text::Slice(s) => s,
            Text::Owned(s) => s,
        }
    }
}

/// Row `r` of a text-valued function other than textcat and concat; None
/// for NULL.
fn call_text<'a>(f: StrFn, args: &[Arg<'a>], r: usize) -> Result<Option<Text<'a>>> {
    if args.iter().any(|a| a.is_null(r)) {
        return Ok(None); // strict
    }
    let t = |i: usize| args[i].text(r);
    let n = |i: usize| args[i].int(r);
    let set = || -> Result<&'a str> {
        if args.len() > 1 {
            t(1)
        } else {
            Ok(" ")
        }
    };
    Ok(Some(match f {
        StrFn::Substr => Text::Slice(substr(
            t(0)?,
            n(1)?,
            if args.len() > 2 { Some(n(2)?) } else { None },
        )?),
        StrFn::Btrim => {
            let set = set()?;
            Text::Slice(t(0)?.trim_matches(|c| set.contains(c)))
        }
        StrFn::Ltrim => {
            let set = set()?;
            Text::Slice(t(0)?.trim_start_matches(|c| set.contains(c)))
        }
        StrFn::Rtrim => {
            let set = set()?;
            Text::Slice(t(0)?.trim_end_matches(|c| set.contains(c)))
        }
        StrFn::Left => {
            let (s, k) = (t(0)?, n(1)? as i64);
            let keep = if k >= 0 { k } else { nchars(s) as i64 + k };
            Text::Slice(if keep <= 0 {
                ""
            } else {
                first_chars(s, keep as usize)
            })
        }
        StrFn::Right => {
            let (s, k) = (t(0)?, n(1)?);
            // -INT_MIN wraps (Cloudberry builds with -fwrapv): nothing skipped.
            let skip = if k == i32::MIN {
                0
            } else if k < 0 {
                -k as i64
            } else {
                nchars(s) as i64 - k as i64
            };
            Text::Slice(if skip <= 0 {
                s
            } else {
                skip_chars(s, skip as usize)
            })
        }
        StrFn::Reverse => Text::Owned(t(0)?.chars().rev().collect()),
        StrFn::Repeat => {
            let (s, count) = (t(0)?, n(1)?.max(0) as i64);
            let total = count * s.len() as i64;
            if total > i32::MAX as i64 || total + 4 > MAX_ALLOC {
                return Err(too_large());
            }
            Text::Owned(s.repeat(count as usize))
        }
        StrFn::Lpad => Text::Owned(pad(t(0)?, n(1)?, t(2)?, true)?),
        StrFn::Rpad => Text::Owned(pad(t(0)?, n(1)?, t(2)?, false)?),
        StrFn::Replace => {
            let (s, from, to) = (t(0)?, t(1)?, t(2)?);
            if s.is_empty() || from.is_empty() || !s.contains(from) {
                Text::Slice(s)
            } else {
                Text::Owned(s.replace(from, to))
            }
        }
        StrFn::SplitPart => {
            let (s, sep, field) = (t(0)?, t(1)?, n(2)?);
            if field == 0 {
                return Err(pg_error("22023", "field position must not be zero"));
            }
            Text::Slice(if s.is_empty() {
                ""
            } else if sep.is_empty() {
                if field == 1 || field == -1 {
                    s
                } else {
                    ""
                }
            } else if field > 0 {
                s.split(sep).nth(field as usize - 1).unwrap_or("")
            } else {
                // Fields as PostgreSQL finds them, left to right (not rsplit:
                // with a separator like "aa" the matches differ).
                let fields: Vec<&str> = s.split(sep).collect();
                let i = fields.len() as i64 + field as i64;
                if i < 0 {
                    ""
                } else {
                    fields[i as usize]
                }
            })
        }
        // Under the C collation PostgreSQL maps ASCII letters only.
        StrFn::Lower => {
            let s = t(0)?;
            if s.bytes().any(|b| b.is_ascii_uppercase()) {
                Text::Owned(s.to_ascii_lowercase())
            } else {
                Text::Slice(s)
            }
        }
        StrFn::Upper => {
            let s = t(0)?;
            if s.bytes().any(|b| b.is_ascii_lowercase()) {
                Text::Owned(s.to_ascii_uppercase())
            } else {
                Text::Slice(s)
            }
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{f:?} is not a text function"
            )))
        }
    }))
}

/// Row `r` of an int4-valued function; None for NULL.
fn call_int(f: StrFn, args: &[Arg], r: usize) -> Result<Option<i32>> {
    if args.iter().any(|a| a.is_null(r)) {
        return Ok(None);
    }
    let s = args[0].text(r)?;
    Ok(Some(match f {
        StrFn::CharLength => nchars(s) as i32,
        StrFn::OctetLength => s.len() as i32,
        StrFn::Strpos => {
            let sub = args[1].text(r)?;
            if sub.is_empty() {
                1
            } else {
                match s.find(sub) {
                    Some(at) => nchars(&s[..at]) as i32 + 1,
                    None => 0,
                }
            }
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "{f:?} is not an integer function"
            )))
        }
    }))
}

/// One of PostgreSQL's string functions (StrFn) over text and int4 values.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgStrFn {
    f: StrFn,
    signature: Signature,
}

impl PgStrFn {
    pub fn udf(f: StrFn) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgStrFn {
            f,
            signature: Signature::variadic_any(Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgStrFn {
    fn name(&self) -> &str {
        self.f.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.f.return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        use std::fmt::Write;

        let n = args.number_rows;
        let owned = args
            .args
            .iter()
            .map(|a| prepare(a.to_array(n)?))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let cols: Vec<Arg> = owned
            .iter()
            .map(|a| match a.data_type() {
                DataType::Int32 => Arg::Int(a.as_primitive()),
                _ => Arg::Text(a.as_string::<i32>()),
            })
            .collect();
        let out: ArrayRef = match self.f {
            StrFn::CharLength | StrFn::OctetLength | StrFn::Strpos => {
                let mut b = Int32Builder::with_capacity(n);
                for r in 0..n {
                    b.append_option(call_int(self.f, &cols, r)?);
                }
                Arc::new(b.finish())
            }
            StrFn::StartsWith => {
                let mut b = BooleanBuilder::with_capacity(n);
                for r in 0..n {
                    if cols[0].is_null(r) || cols[1].is_null(r) {
                        b.append_null();
                    } else {
                        b.append_value(cols[0].text(r)?.starts_with(cols[1].text(r)?));
                    }
                }
                Arc::new(b.finish())
            }
            StrFn::Textcat | StrFn::Concat => {
                // Written straight into the result buffer.
                let mut b = StringBuilder::with_capacity(n, 0);
                for r in 0..n {
                    if self.f == StrFn::Textcat && cols.iter().any(|a| a.is_null(r)) {
                        b.append_null();
                        continue;
                    }
                    // concat skips NULLs and is never NULL.
                    for a in &cols {
                        if !a.is_null(r) {
                            b.write_str(a.text(r)?)
                                .map_err(|e| DataFusionError::Internal(e.to_string()))?;
                        }
                    }
                    b.append_value("");
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = StringBuilder::with_capacity(n, 0);
                for r in 0..n {
                    match call_text(self.f, &cols, r)? {
                        Some(v) => b.append_value(v.as_str()),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        Ok(ColumnarValue::Array(out))
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

    /// One row of `f` over text (`T`) and int4 (`I`) arguments.
    enum A<'a> {
        T(&'a str),
        I(i32),
    }

    fn text_fn(f: StrFn, args: &[A]) -> String {
        let owned: Vec<ArrayRef> = args
            .iter()
            .map(|a| -> ArrayRef {
                match a {
                    A::T(s) => Arc::new(StringArray::from(vec![*s])),
                    A::I(i) => Arc::new(Int32Array::from(vec![*i])),
                }
            })
            .collect();
        let cols: Vec<Arg> = owned
            .iter()
            .map(|a| match a.data_type() {
                DataType::Int32 => Arg::Int(a.as_primitive()),
                _ => Arg::Text(a.as_string::<i32>()),
            })
            .collect();
        if f == StrFn::Strpos {
            return call_int(f, &cols, 0).unwrap().unwrap().to_string();
        }
        call_text(f, &cols, 0)
            .unwrap()
            .unwrap()
            .as_str()
            .to_string()
    }

    #[test]
    fn functions_agree_with_postgresql() {
        use A::{I, T};
        // Expected values from PostgreSQL 16.
        let cases: Vec<(StrFn, Vec<A>, &str)> = vec![
            (StrFn::SplitPart, vec![T("aaa"), T("aa"), I(1)], ""),
            (StrFn::SplitPart, vec![T("aaa"), T("aa"), I(2)], "a"),
            (StrFn::SplitPart, vec![T("aaa"), T("aa"), I(-1)], "a"),
            (StrFn::SplitPart, vec![T("aaa"), T("aa"), I(-2)], ""),
            (StrFn::SplitPart, vec![T("a,b,,c"), T(","), I(-2)], ""),
            (StrFn::SplitPart, vec![T("a,b"), T(","), I(5)], ""),
            (StrFn::Substr, vec![T("中文字符串"), I(0), I(3)], "中文"),
            (StrFn::Substr, vec![T("中文字符串"), I(-2), I(5)], "中文"),
            (StrFn::Substr, vec![T("中文字符串"), I(4)], "符串"),
            (StrFn::Substr, vec![T("abc"), I(i32::MAX), I(10)], ""),
            (StrFn::Substr, vec![T("abc"), I(2), I(i32::MAX)], "bc"),
            (StrFn::Left, vec![T("中文字符串"), I(-2)], "中文字"),
            (StrFn::Right, vec![T("中文字符串"), I(-2)], "字符串"),
            (StrFn::Right, vec![T("abc"), I(i32::MIN)], "abc"),
            (StrFn::Left, vec![T("abc"), I(i32::MIN)], ""),
            (StrFn::Right, vec![T("abc"), I(5)], "abc"),
            (StrFn::Lpad, vec![T("中文"), I(5), T("xy")], "xyx中文"),
            (StrFn::Rpad, vec![T("中文字符串"), I(3), T("x")], "中文字"),
            (StrFn::Lpad, vec![T("ab"), I(-1), T("x")], ""),
            (StrFn::Rpad, vec![T("ab"), I(4), T("")], "ab"),
            (StrFn::Btrim, vec![T("xxa中xx"), T("x中")], "a"),
            (StrFn::Ltrim, vec![T("  a  ")], "a  "),
            (StrFn::Rtrim, vec![T("😀a😀"), T("😀")], "😀a"),
            (StrFn::Strpos, vec![T("中文字符串"), T("字")], "3"),
            (StrFn::Strpos, vec![T("abc"), T("")], "1"),
            (StrFn::Replace, vec![T("aaaa"), T("aa"), T("b")], "bb"),
        ];
        for (f, args, want) in &cases {
            assert_eq!(text_fn(*f, args), *want, "{f:?}");
        }
    }

    #[test]
    fn dangling_escape_errors_only_when_reached() {
        // 'xbc' LIKE 'a\' fails on 'x' before reaching the escape.
        assert!(!like("xbc", "a\\").unwrap());
        assert!(like("abc", "a\\").is_err());
        assert!(like("abc", "%\\").is_err());
    }
}
