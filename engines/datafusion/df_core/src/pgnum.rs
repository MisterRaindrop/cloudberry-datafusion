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

//! numeric values of a fixed scale as Decimal256(76, scale) (N2, N3).
//! Columns and constants are read with up to 38 digits; arithmetic results
//! have up to 76, sums of up to 66.  NaN, which a numeric column may hold,
//! is `NUMERIC_NAN`, above every 76-digit value, as the C side writes it
//! (vx_numeric.c): PostgreSQL sorts NaN above all numbers and NaN equals
//! NaN, so comparisons, min/max, grouping and joins work on the integers.
//! What does not: rescaling and arithmetic (NaN in, NaN out, here) and
//! sums, which a NaN makes NaN (`sum_decimal` in query.rs adds the others
//! and marks the group with these functions).
//!
//! N6: values known to have at most 38 digits (columns of numeric(p, s) of
//! p <= 38, constants, and what + - * and sums of such make within 38) are
//! Decimal128(digits, scale) instead, half the bytes: the type's precision
//! is the most digits a value may have, which the functions here carry
//! through.  NaN and the infinities are i128::MAX, MAX - 1 and MIN there,
//! as a batch Motion carries them (NW1).  Where the two kinds meet, as
//! operands of one comparison, branches of one CASE or inputs of one
//! UNION, `NumericWidth` (query.rs) makes them one type with `NumericFn::As`.

use std::borrow::Cow;
use std::cmp::Ordering;

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Decimal256Array, PrimitiveArray,
};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{
    i256, DataType, Decimal128Type, Decimal256Type, Int32Type, Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, EmitTo, GroupsAccumulator,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use num_bigint::BigUint;

/// NaN of a numeric value (i256::MAX, beyond 10^76).
pub const NUMERIC_NAN: i256 = i256::MAX;

/// The infinities of a numeric value (DT2), also beyond 10^76: below every
/// value, and above every one but NaN, as PostgreSQL sorts them.  Only
/// extract(year) of an infinite date makes them, and the planner hook keeps
/// them out of arithmetic, rescaling, casts and sums.
pub const NUMERIC_PINF: i256 = i256::from_parts(u128::MAX - 1, i128::MAX);
pub const NUMERIC_NINF: i256 = i256::MIN;

/// Digits a numeric value of this representation may have.
pub const NUMERIC_PRECISION: u8 = 76;

pub fn numeric_type(scale: i8) -> DataType {
    DataType::Decimal256(NUMERIC_PRECISION, scale)
}

/// N6: NaN and the infinities of a narrow numeric value (Decimal128).
pub const NARROW_NAN: i128 = i128::MAX;
pub const NARROW_PINF: i128 = i128::MAX - 1;
pub const NARROW_NINF: i128 = i128::MIN;

/// The most digits a narrow numeric value has.
pub const NARROW_PRECISION: u8 = 38;

/// The type of numeric values of at most `digits` digits at `scale`:
/// narrow up to 38 digits, Decimal256 beyond.  Arrow wants the digits at
/// least the scale (0.05 has one digit at scale 2).
pub fn numeric_type_of(digits: i64, scale: i8) -> DataType {
    let digits = digits.max(scale as i64).max(1);
    if digits <= NARROW_PRECISION as i64 {
        DataType::Decimal128(digits as u8, scale)
    } else {
        numeric_type(scale)
    }
}

/// The digits and scale of numeric type `t` (76 for Decimal256), if it is
/// one.
pub fn numeric_digits(t: &DataType) -> Option<(u8, i8)> {
    match t {
        DataType::Decimal128(p, s) => Some((*p, *s)),
        DataType::Decimal256(_, s) => Some((NUMERIC_PRECISION, *s)),
        _ => None,
    }
}

/// A narrow numeric value as a wide one.
#[inline]
pub fn widen_value(v: i128) -> i256 {
    match v {
        NARROW_NAN => NUMERIC_NAN,
        NARROW_PINF => NUMERIC_PINF,
        NARROW_NINF => NUMERIC_NINF,
        v => i256::from_i128(v),
    }
}

/// A wide numeric value as a narrow one, if it has at most 38 digits.
#[inline]
pub fn narrow_value(v: i256) -> Option<i128> {
    match v {
        NUMERIC_NAN => Some(NARROW_NAN),
        NUMERIC_PINF => Some(NARROW_PINF),
        NUMERIC_NINF => Some(NARROW_NINF),
        v => v.to_i128().filter(|v| *v < NARROW_PINF && *v > NARROW_NINF),
    }
}

/// Numeric value `r` of array `a` of either kind, as a wide one.
#[inline]
pub fn numeric_value(a: &dyn Array, r: usize) -> i256 {
    match a.data_type() {
        DataType::Decimal128(..) => widen_value(a.as_primitive::<Decimal128Type>().value(r)),
        _ => a.as_primitive::<Decimal256Type>().value(r),
    }
}

/// Numeric array `a` of either kind as Decimal256(76, its scale).
pub fn widen(a: &ArrayRef) -> Result<ArrayRef> {
    Ok(match decimal(a)? {
        Cow::Borrowed(_) => a.clone(),
        Cow::Owned(w) => Arc::new(w),
    })
}

/// Numeric array `a` of either kind, a narrow one widened.
fn decimal(a: &ArrayRef) -> Result<Cow<'_, Decimal256Array>> {
    if let Some(d) = a.as_primitive_opt::<Decimal256Type>() {
        return Ok(Cow::Borrowed(d));
    }
    match a.data_type() {
        DataType::Decimal128(_, s) => Ok(Cow::Owned(
            a.as_primitive::<Decimal128Type>()
                .unary::<_, Decimal256Type>(widen_value)
                .with_data_type(numeric_type(*s)),
        )),
        t => Err(DataFusionError::Internal(format!(
            "expected a numeric column, got {t}"
        ))),
    }
}

fn narrow_array(a: &ArrayRef) -> Option<&Decimal128Array> {
    a.as_primitive_opt::<Decimal128Type>()
}

/// NW1: how a numeric column of at most 38 digits goes through a batch
/// Motion: its integers narrowed to 8 bytes (up to 18 digits) or 16 (up to
/// 38) instead of Decimal256's 32.  In TPC-DS Q64 under the Postgres
/// planner, numerics were 46% of the bytes the Motions carried.  NaN and
/// the infinities, beyond any value of those digits, go as the narrow
/// type's largest, next largest and smallest integers.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum MotionNumeric {
    Int64,
    Decimal128,
}

impl MotionNumeric {
    /// The narrow form of values of `digits` digits, if any.
    pub fn of_digits(digits: i64) -> Option<MotionNumeric> {
        match digits {
            1..=18 => Some(MotionNumeric::Int64),
            19..=38 => Some(MotionNumeric::Decimal128),
            _ => None,
        }
    }

    /// The Arrow type the column goes as, for scale `scale`.
    pub fn wire_type(self, scale: i8) -> DataType {
        match self {
            MotionNumeric::Int64 => DataType::Int64,
            MotionNumeric::Decimal128 => DataType::Decimal128(38, scale),
        }
    }

    /// Numeric column `a` (of either kind) in this form.  Err for a value
    /// it cannot hold, which the planner hook's digits rule out.
    pub fn narrow(self, a: &ArrayRef) -> Result<ArrayRef> {
        let lost = || ArrowError::ComputeError("numeric value beyond its digits".into());
        if let Some(d) = narrow_array(a) {
            // N6: as it is, or 8 bytes of it
            let scale = d.scale();
            return Ok(match self {
                MotionNumeric::Int64 => Arc::new(d.try_unary::<_, Int64Type, _>(|v| {
                    Ok::<_, ArrowError>(match v {
                        NARROW_NAN => i64::MAX,
                        NARROW_PINF => i64::MAX - 1,
                        NARROW_NINF => i64::MIN,
                        v => i64::try_from(v)
                            .ok()
                            .filter(|v| *v < i64::MAX - 1 && *v > i64::MIN)
                            .ok_or_else(lost)?,
                    })
                })?),
                MotionNumeric::Decimal128 => {
                    Arc::new(d.clone().with_data_type(DataType::Decimal128(38, scale)))
                }
            });
        }
        let d = decimal(a)?;
        let scale = d.scale();
        Ok(match self {
            MotionNumeric::Int64 => Arc::new(d.try_unary::<_, Int64Type, _>(|v| {
                Ok::<_, ArrowError>(if v == NUMERIC_NAN {
                    i64::MAX
                } else if v == NUMERIC_PINF {
                    i64::MAX - 1
                } else if v == NUMERIC_NINF {
                    i64::MIN
                } else {
                    v.to_i128()
                        .and_then(|v| i64::try_from(v).ok())
                        .filter(|v| *v < i64::MAX - 1 && *v > i64::MIN)
                        .ok_or_else(lost)?
                })
            })?),
            MotionNumeric::Decimal128 => Arc::new(
                d.try_unary::<_, Decimal128Type, _>(|v| {
                    Ok::<_, ArrowError>(if v == NUMERIC_NAN {
                        i128::MAX
                    } else if v == NUMERIC_PINF {
                        i128::MAX - 1
                    } else if v == NUMERIC_NINF {
                        i128::MIN
                    } else {
                        v.to_i128()
                            .filter(|v| *v < i128::MAX - 1 && *v > i128::MIN)
                            .ok_or_else(lost)?
                    })
                })?
                .with_precision_and_scale(38, scale)?,
            ),
        })
    }

    /// Column `a` in this form back as numeric of `digits` digits at scale
    /// `scale`: narrow (N6), as its form is.
    pub fn widen(self, a: &ArrayRef, digits: u8, scale: i8) -> Result<ArrayRef> {
        let bad =
            || DataFusionError::Internal(format!("unexpected Motion column {}", a.data_type()));
        let ty = DataType::Decimal128(digits, scale);
        Ok(match self {
            MotionNumeric::Int64 => Arc::new(
                a.as_primitive_opt::<Int64Type>()
                    .ok_or_else(bad)?
                    .unary::<_, Decimal128Type>(|v| match v {
                        i64::MAX => NARROW_NAN,
                        v if v == i64::MAX - 1 => NARROW_PINF,
                        i64::MIN => NARROW_NINF,
                        v => v as i128,
                    })
                    .with_data_type(ty),
            ),
            MotionNumeric::Decimal128 => Arc::new(
                a.as_primitive_opt::<Decimal128Type>()
                    .ok_or_else(bad)?
                    .clone()
                    .with_data_type(ty),
            ),
        })
    }
}

fn pow10(k: u32) -> i256 {
    i256::from_i128(10)
        .checked_pow(k)
        .expect("10^k within 76 digits")
}

/// The NBASE (10000) digits of |v| times 10^-`scale`, least significant
/// first, aligned on the decimal point as PostgreSQL's numeric stores them,
/// and how many of them are fractional.  May end with zero digits.
pub fn nbase_digits(v: i256, scale: i8) -> (Vec<u16>, i32) {
    let scale = scale as u32;
    let pad = (4 - scale % 4) % 4;
    let nfrac = ((scale + pad) / 4) as i32;
    // the lowest digit takes the last 4 - pad decimal digits, times
    // 10^pad, so that nothing overflows
    let mut digits: Vec<u16> = Vec::with_capacity(20);
    let mut x = v.wrapping_abs();
    if pad > 0 {
        let low = i256::from_i128(10i128.pow(4 - pad));
        digits.push(((x % low).as_i128() * 10i128.pow(pad)) as u16);
        x /= low;
    }
    match x.to_i128() {
        Some(x) => {
            let mut x = x as u128;
            while x > 0 {
                let mut chunk = (x % 10_000_000_000_000_000) as u64;
                x /= 10_000_000_000_000_000;
                for _ in 0..4 {
                    digits.push((chunk % 10000) as u16);
                    chunk /= 10000;
                }
            }
        }
        None => {
            let base = i256::from_i128(10000);
            while x > i256::ZERO {
                digits.push((x % base).as_i128() as u16);
                x /= base;
            }
        }
    }
    (digits, nfrac)
}

/// The weight and first digit of nonzero `v` times 10^-`scale`, as
/// select_div_scale reads them; (0, 0) for zero.
fn weight_first(v: i256, scale: i8) -> (i32, u16) {
    let (digits, nfrac) = nbase_digits(v, scale);
    match digits.iter().rposition(|&d| d != 0) {
        Some(i) => (i as i32 - nfrac, digits[i]),
        None => (0, 0),
    }
}

/// select_div_scale of `sum` (an integer of scale `from`) and `count`:
/// at least 16 significant digits of the quotient, not below `from`.
pub fn pg_avg_rscale(sum: i256, from: i8, count: i64) -> i32 {
    let (w1, d1) = weight_first(sum, from);
    let (w2, d2) = weight_first(i256::from_i128(count as i128), 0);
    let qweight = w1 - w2 - i32::from(d1 <= d2);
    (16 - 4 * qweight).max(from as i32).clamp(0, 1000)
}

/// DN1: select_div_scale of `x` (an integer of scale `sx`, display scale
/// `dx`) by `y` (of scale `sy`, display scale `dy`): at least 16
/// significant digits of the quotient, not below either display scale.
pub fn pg_div_rscale(x: i256, sx: i8, dx: i32, y: i256, sy: i8, dy: i32) -> i32 {
    let (w1, d1) = weight_first(x, sx);
    let (w2, d2) = weight_first(y, sy);
    let qweight = w1 - w2 - i32::from(d1 <= d2);
    (16 - 4 * qweight).max(dx).max(dy).max(0).min(1000)
}

/// DN1: numeric_div of `x` (an integer of scale `sx`) by nonzero `y` (of
/// scale `sy`) as div_var computes it: the quotient rounded half away from
/// zero to `rscale`, given at scale `to` (not below `rscale`).  None beyond
/// Decimal256, which the planner hook's bounds rule out for the quotient;
/// the dividend or divisor scaled on the way may be beyond it.
pub fn pg_div(x: i256, sx: i8, y: i256, sy: i8, rscale: i32, to: i8) -> Option<i256> {
    if rscale > to as i32 || y == i256::ZERO {
        return None;
    }
    // |x| / |y| * 10^(rscale - sx + sy), exactly, then rounded
    let e = rscale - sx as i32 + sy as i32;
    let q = match div_scaled(x, y, e) {
        Some(q) => q,
        None => div_scaled_exact(x, y, e)?,
    };
    let q = q.checked_mul(pow10((to as i32 - rscale) as u32))?;
    Some(if (x < i256::ZERO) != (y < i256::ZERO) {
        q.wrapping_neg()
    } else {
        q
    })
}

/// |x| / |y| * 10^e rounded half away from zero, where |x| or |y| times
/// 10^|e| is within Decimal256.
fn div_scaled(x: i256, y: i256, e: i32) -> Option<i256> {
    let mut num = x.checked_abs()?;
    let mut den = y.checked_abs()?;
    if e >= 0 {
        num = num.checked_mul(i256::from_i128(10).checked_pow(e as u32)?)?;
    } else {
        den = den.checked_mul(i256::from_i128(10).checked_pow((-e) as u32)?)?;
    }
    let mut q = num / den;
    if (num % den).checked_mul(i256::from_i128(2))? >= den {
        q = q.checked_add(i256::ONE)?;
    }
    Some(q)
}

/// div_scaled in arbitrary precision (QM1: the dividend of a quotient
/// whose bound comes from the divisor's smallest value may scale beyond
/// Decimal256); None if the quotient is beyond it.
fn div_scaled_exact(x: i256, y: i256, e: i32) -> Option<i256> {
    let (mut num, mut den) = (magnitude(x), magnitude(y));
    if e >= 0 {
        num *= ten(e);
    } else {
        den *= ten(-e);
    }
    let mut q = &num / &den;
    if (num % &den) * 2u32 >= den {
        q += 1u32;
    }
    let b = q.to_bytes_be();
    if b.len() > 31 {
        return None;
    }
    let mut a = [0u8; 32];
    a[32 - b.len()..].copy_from_slice(&b);
    Some(i256::from_be_bytes(a))
}

/// AVG1: numeric_div(sum, count) as numeric_avg computes it, `sum` an
/// integer of scale `from` and `count` > 0: the quotient rounded half away
/// from zero to select_div_scale's scale, at least 16 significant digits
/// and not below `from`, and given at scale `to`.  None if that scale is
/// beyond `to` or a value beyond Decimal256.
pub fn pg_avg(sum: i256, from: i8, count: i64, to: i8) -> Option<i256> {
    if count <= 0 {
        return None;
    }
    let rscale = pg_avg_rscale(sum, from, count);
    if rscale > to as i32 {
        return None;
    }
    let num = sum
        .wrapping_abs()
        .checked_mul(pow10((rscale - from as i32) as u32))?;
    let c = i256::from_i128(count as i128);
    let mut q = num / c;
    if (num % c).checked_mul(i256::from_i128(2))? >= c {
        q = q.checked_add(i256::ONE)?;
    }
    let q = q.checked_mul(pow10((to as i32 - rscale) as u32))?;
    Some(if sum < i256::ZERO {
        q.wrapping_neg()
    } else {
        q
    })
}

/// STD1: the largest display scale pg_stddev gives values of int2 and
/// int4: select_div_scale of a variance of at least 1/n, n < 2^63.
pub const STDDEV_MAX_SCALE: i8 = 44;

/// STD1: numeric_stddev_internal over integers: `n` values, their sum `sx`
/// and sum of squares `sxx` (integers).  Its variance is (n * sxx - sx^2)
/// / (n * (n - 1)), or / n^2 for a population, rounded half away from
/// zero at select_div_scale's scale; the standard deviation is sqrt_var's
/// root of that at the same scale: the integer square root of the variance
/// carried to its NBASE digits, then rounded.  0 (of scale 0) where the
/// numerator is not above 0.  The value at scale `to` and its display
/// scale; Ok(None) where PostgreSQL's is NULL (no row, or one for a
/// sample); Err beyond Decimal256 or a scale above `to`.
pub fn pg_stddev(
    n: i64,
    sx: i256,
    sxx: i256,
    sample: bool,
    variance: bool,
    to: i8,
) -> std::result::Result<Option<(i256, i32)>, ArrowError> {
    if n == 0 || (sample && n <= 1) {
        return Ok(None);
    }
    let nn = i256::from_i128(n as i128);
    let num = nn
        .checked_mul(sxx)
        .and_then(|a| sx.checked_mul(sx).and_then(|b| a.checked_sub(b)))
        .ok_or_else(beyond)?;
    if num <= i256::ZERO {
        return Ok(Some((i256::ZERO, 0)));
    }
    let den = nn
        .checked_mul(if sample { nn - i256::ONE } else { nn })
        .ok_or_else(beyond)?;
    let rscale = pg_div_rscale(num, 0, 0, den, 0, 0);
    if rscale > to as i32 {
        return Err(beyond());
    }
    let var = pg_div(num, 0, den, 0, rscale, rscale as i8).ok_or_else(beyond)?;
    let at_to = |v: i256| v.checked_mul(pow10((to as i32 - rscale) as u32)).ok_or_else(beyond);
    if variance {
        return Ok(Some((at_to(var)?, rscale)));
    }
    // sqrt_var: weight of the result, NBASE digits computed, input digits read
    let (w, _) = weight_first(var, rscale as i8);
    let res_weight = if w >= 0 { w / 2 } else { -((-w - 1) / 2 + 1) };
    let res_ndigits = (res_weight + 1 + (rscale + 4) / 4).max(1);
    let m = res_ndigits - res_weight - 1;
    let src_ndigits = (w + 1 + 2 * m).max(1);
    let k = src_ndigits - w - 1;
    if k != 2 * m || m < 0 || 4 * m <= rscale {
        return Err(beyond());
    }
    // the variance truncated to k NBASE digits after the point, as an integer
    let shift = 4 * k - rscale;
    let input = if shift >= 0 {
        magnitude(var) * ten(shift)
    } else {
        magnitude(var) / ten(-shift)
    };
    let root = input.sqrt();
    // root / NBASE^m, rounded half away from zero at rscale
    let t = ten(4 * m - rscale);
    let mut q = &root / &t;
    if (&root % &t) * 2u32 >= t {
        q += 1u32;
    }
    let q = i256::from_be_bytes({
        let b = q.to_bytes_be();
        if b.len() > 31 {
            return Err(beyond());
        }
        let mut a = [0u8; 32];
        a[32 - b.len()..].copy_from_slice(&b);
        a
    });
    Ok(Some((at_to(q)?, rscale)))
}

/// RD1: numeric_round of `v`, of scale `from`, at scale `to`: round_var's
/// rounding half away from zero; NaN stays NaN.  None beyond Decimal256,
/// which the planner hook's bounds rule out.
pub fn pg_round(v: i256, from: i8, to: i8) -> Option<i256> {
    if v == NUMERIC_NAN {
        return Some(v);
    }
    if to >= from {
        return v.checked_mul(pow10((to - from) as u32));
    }
    let d = pow10((from - to) as u32);
    let (q, r) = (v / d, v % d);
    if r.wrapping_abs().checked_mul(i256::from_i128(2))? >= d {
        q.checked_add(if v < i256::ZERO { i256::MINUS_ONE } else { i256::ONE })
    } else {
        Some(q)
    }
}

fn beyond() -> ArrowError {
    // The planner hook bounds every result within 76 digits.
    ArrowError::ComputeError("numeric result beyond 76 digits".into())
}

// N1: numeric + - * of two values, NaN in, NaN out.  Operands within 38
// digits (most columns and what they make) take i128 arithmetic, which
// cannot be NaN or infinite (both beyond i128); the rest i256's.

#[inline]
fn num_add(a: i256, b: i256) -> Result<i256, ArrowError> {
    if let (Some(x), Some(y)) = (a.to_i128(), b.to_i128()) {
        if let Some(v) = x.checked_add(y) {
            return Ok(i256::from_i128(v));
        }
    }
    if a == NUMERIC_NAN || b == NUMERIC_NAN {
        return Ok(NUMERIC_NAN);
    }
    a.checked_add(b).ok_or_else(beyond)
}

#[inline]
fn num_sub(a: i256, b: i256) -> Result<i256, ArrowError> {
    if let (Some(x), Some(y)) = (a.to_i128(), b.to_i128()) {
        if let Some(v) = x.checked_sub(y) {
            return Ok(i256::from_i128(v));
        }
    }
    if a == NUMERIC_NAN || b == NUMERIC_NAN {
        return Ok(NUMERIC_NAN);
    }
    a.checked_sub(b).ok_or_else(beyond)
}

// N4: arrays whose every value fits an i64 (nearly all of a numeric(p, s)
// column of p <= 18 and what + - * of such make), whose + - * fit an i128
// without checks, go through plain loops: NaN and the infinities never fit.
// Null slots are looked at too; a value there that does not fit only sends
// the array the general way.

#[inline]
fn fits_i64(x: i256) -> bool {
    let (lo, hi) = x.to_parts();
    let l = lo as i128;
    hi == l >> 127 && l == l as i64 as i128
}

fn all_i64(v: &[i256]) -> bool {
    v.iter().fold(true, |all, &x| all & fits_i64(x))
}

/// `$body` once per operator, `$f` that operator of values fitting an
/// i64: inlined in the loops (a fn pointer cost a call per row, 2% of Q1)
/// and * one widening multiply.
macro_rules! with_op {
    ($op:expr, $f:ident => $body:expr) => {
        match $op {
            NumOp::Add => {
                let $f = |x: i128, y: i128| (x as i64 as i128) + (y as i64 as i128);
                $body
            }
            NumOp::Sub => {
                let $f = |x: i128, y: i128| (x as i64 as i128) - (y as i64 as i128);
                $body
            }
            NumOp::Mul => {
                let $f = |x: i128, y: i128| (x as i64 as i128) * (y as i64 as i128);
                $body
            }
        }
    };
}

/// The second operand of `narrow_arith`.
enum Operand<'a> {
    One(i256),
    Each(&'a Decimal256Array),
}

/// `op` of `xs` and `b` when all fit an i64.
fn narrow_arith(op: NumOp, xs: &Decimal256Array, b: Operand) -> Option<Decimal256Array> {
    if !all_i64(xs.values()) {
        return None;
    }
    let lo = |v: i256| v.as_i128();
    let values: Vec<i256> = with_op!(op, f => match b {
        Operand::One(b) => {
            if !fits_i64(b) {
                return None;
            }
            let b = lo(b);
            xs.values().iter().map(|&a| i256::from_i128(f(lo(a), b))).collect()
        }
        Operand::Each(ys) => {
            if !all_i64(ys.values()) {
                return None;
            }
            xs.values()
                .iter()
                .zip(ys.values().iter())
                .map(|(&a, &b)| i256::from_i128(f(lo(a), lo(b))))
                .collect()
        }
    });
    let nulls = match b {
        Operand::One(_) => xs.nulls().cloned(),
        Operand::Each(ys) => NullBuffer::union(xs.nulls(), ys.nulls()),
    };
    Some(Decimal256Array::new(ScalarBuffer::from(values), nulls))
}

/// N6: an operand of + - * of narrow values: one value, or one per row.
#[derive(Clone, Copy)]
enum Narrow<'a> {
    One(i128),
    Each(&'a Decimal128Array),
}

impl Narrow<'_> {
    /// Does every value (null slots too) fit an i64?
    fn all_i64(self) -> bool {
        let fits = |v: i128| v == v as i64 as i128;
        match self {
            Narrow::One(v) => fits(v),
            Narrow::Each(a) => a.values().iter().fold(true, |all, &v| all & fits(v)),
        }
    }

    #[inline]
    fn get(self, r: usize) -> i128 {
        match self {
            Narrow::One(v) => v,
            Narrow::Each(a) => a.values()[r],
        }
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        match self {
            Narrow::One(_) => None,
            Narrow::Each(a) => a.nulls(),
        }
    }
}

/// `f` of the values of `a` and `b`, row by row, `n` rows.
#[inline]
fn map_narrow<T: Clone>(n: usize, a: Narrow, b: Narrow, f: impl Fn(i128, i128) -> T) -> Vec<T> {
    match (a, b) {
        (Narrow::Each(x), Narrow::Each(y)) => x
            .values()
            .iter()
            .zip(y.values().iter())
            .map(|(&x, &y)| f(x, y))
            .collect(),
        (Narrow::Each(x), Narrow::One(y)) => x.values().iter().map(|&x| f(x, y)).collect(),
        (Narrow::One(x), Narrow::Each(y)) => y.values().iter().map(|&y| f(x, y)).collect(),
        (Narrow::One(x), Narrow::One(y)) => vec![f(x, y); n],
    }
}

fn beyond_digits() -> ArrowError {
    // The planner hook's digits rule it out.
    ArrowError::ComputeError("numeric value beyond its digits".into())
}

/// N6: `op` of narrow `a` and `b`, of type `out`.  A narrow result has at
/// most 38 digits by the operands' (`PgNumeric::return_type`), within an
/// i128 and short of its NaN.  Values that all fit an i64 cannot be NaN
/// and their + - * fit an i128: plain loops (N4's); the others are checked.
fn arith_narrow(op: NumOp, a: Narrow, b: Narrow, n: usize, out: &DataType) -> Result<ArrayRef> {
    let nulls = NullBuffer::union(a.nulls(), b.nulls());
    let fast = a.all_i64() && b.all_i64();
    let checked = |x: i128, y: i128| match op {
        NumOp::Add => x.checked_add(y),
        NumOp::Sub => x.checked_sub(y),
        NumOp::Mul => x.checked_mul(y),
    };
    let null = |r: usize| nulls.as_ref().is_some_and(|nb| nb.is_null(r));
    let arrow = |e: ArrowError| DataFusionError::ArrowError(Box::new(e), None);
    if let DataType::Decimal128(..) = out {
        let values: Vec<i128> = if fast {
            with_op!(op, f => map_narrow(n, a, b, f))
        } else {
            let mut v = Vec::with_capacity(n);
            for r in 0..n {
                let (x, y) = (a.get(r), b.get(r));
                v.push(if null(r) {
                    0
                } else if x == NARROW_NAN || y == NARROW_NAN {
                    NARROW_NAN
                } else {
                    checked(x, y)
                        .filter(|v| *v < NARROW_PINF && *v > NARROW_NINF)
                        .ok_or_else(beyond_digits)
                        .map_err(arrow)?
                });
            }
            v
        };
        return Ok(Arc::new(
            Decimal128Array::new(ScalarBuffer::from(values), nulls).with_data_type(out.clone()),
        ));
    }
    let values: Vec<i256> = if fast {
        with_op!(op, f => map_narrow(n, a, b, |x, y| i256::from_i128(f(x, y))))
    } else {
        let mut v = Vec::with_capacity(n);
        for r in 0..n {
            let (x, y) = (a.get(r), b.get(r));
            v.push(if null(r) {
                i256::ZERO
            } else if x == NARROW_NAN || y == NARROW_NAN {
                NUMERIC_NAN
            } else {
                match checked(x, y) {
                    Some(v) => i256::from_i128(v),
                    None => {
                        let (x, y) = (i256::from_i128(x), i256::from_i128(y));
                        match op {
                            NumOp::Add => x.checked_add(y),
                            NumOp::Sub => x.checked_sub(y),
                            NumOp::Mul => x.checked_mul(y),
                        }
                        .ok_or_else(beyond)
                        .map_err(arrow)?
                    }
                }
            });
        }
        v
    };
    Ok(Arc::new(
        Decimal256Array::new(ScalarBuffer::from(values), nulls).with_data_type(out.clone()),
    ))
}

/// `op` of arguments `args` if both are narrow (N6), else None.
fn arith_narrow_args(
    op: NumOp,
    args: &[ColumnarValue],
    n: usize,
    out: &DataType,
) -> Result<Option<ArrayRef>> {
    enum Side {
        One(Option<i128>),
        Each(ArrayRef),
    }
    let side = |c: &ColumnarValue| match c {
        ColumnarValue::Scalar(ScalarValue::Decimal128(v, ..)) => Some(Side::One(*v)),
        ColumnarValue::Array(a) if narrow_array(a).is_some() => Some(Side::Each(a.clone())),
        _ => None,
    };
    let (Some(a), Some(b)) = (side(&args[0]), side(&args[1])) else {
        return Ok(None);
    };
    fn view(s: &Side) -> Option<Narrow<'_>> {
        match s {
            Side::One(v) => v.map(Narrow::One),
            Side::Each(a) => narrow_array(a).map(Narrow::Each),
        }
    }
    match (view(&a), view(&b)) {
        (Some(x), Some(y)) => arith_narrow(op, x, y, n, out).map(Some),
        _ => Ok(Some(datafusion::arrow::array::new_null_array(out, n))),
    }
}

/// The type of `op` of numeric operands of types `l` and `r` at scale
/// `to`: + and - add a digit to the larger part before the point, * adds
/// up the digits (numeric_mul is exact).
fn arith_type(op: NumOp, to: i8, l: &DataType, r: &DataType) -> DataType {
    match (numeric_digits(l), numeric_digits(r)) {
        (Some((pl, sl)), Some((pr, sr))) => {
            let (pl, sl, pr, sr) = (pl as i64, sl as i64, pr as i64, sr as i64);
            let digits = match op {
                NumOp::Add | NumOp::Sub => (pl - sl).max(pr - sr) + to as i64 + 1,
                NumOp::Mul => pl + pr,
            };
            numeric_type_of(digits, to)
        }
        _ => numeric_type(to),
    }
}

#[inline]
fn num_mul(a: i256, b: i256) -> Result<i256, ArrowError> {
    if let (Some(x), Some(y)) = (a.to_i128(), b.to_i128()) {
        if let Some(v) = x.checked_mul(y) {
            return Ok(i256::from_i128(v));
        }
    }
    if a == NUMERIC_NAN || b == NUMERIC_NAN {
        return Ok(NUMERIC_NAN);
    }
    a.checked_mul(b).ok_or_else(beyond)
}

/// N1: sum_decimal, the sum of numeric values as PostgreSQL's numeric sum
/// gives it: NaN if any value is NaN, NULL without values.  One pass,
/// where `sum` of the values with NaN as 0, `bool_or` of "is NaN" and a
/// projection marking NaN sums took four.  Its state is that value, which
/// merges as the values do.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgSumDecimal {
    signature: Signature,
}

impl PgSumDecimal {
    pub fn udaf() -> AggregateUDF {
        AggregateUDF::new_from_impl(PgSumDecimal {
            signature: Signature::any(1, Volatility::Immutable),
        })
    }
}

impl AggregateUDFImpl for PgSumDecimal {
    fn name(&self) -> &str {
        "pg_sum_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(t @ DataType::Decimal256(..)) => Ok(t.clone()),
            // N6: fewer than 2^63 values, under 10^19, add up to 19 digits
            Some(DataType::Decimal128(p, s)) => Ok(numeric_type_of(*p as i64 + 19, *s)),
            t => Err(DataFusionError::Internal(format!(
                "pg_sum_decimal of {t:?}"
            ))),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumDecimal {
            data_type: args.return_field.data_type().clone(),
            sum: i256::ZERO,
            values: 0,
            nans: 0,
            pinfs: 0,
            ninfs: 0,
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumDecimalGroups::new(
            args.return_field.data_type().clone(),
        )))
    }
}

/// NI1: the special values a sum saw, as bits: numeric_add makes NaN of a
/// NaN or of both infinities, an infinity of one of them (adding them to
/// the sum as numbers made +Infinity + 1 NaN's encoding and -Infinity + 1
/// a number).  No plan gives a sum infinities yet: a column of numeric(p, s)
/// cannot hold them, and a sum of numeric of unknown precision stays in
/// PostgreSQL (vx_plan_check.c); the values still reach the readers'
/// conversion, which keeps them.
const SAW_NAN: u8 = 1;
const SAW_PINF: u8 = 2;
const SAW_NINF: u8 = 4;

/// The special value `v` is, as a SAW_ bit; 0 for a number.
#[inline]
fn special_bit(v: i256) -> u8 {
    match v {
        NUMERIC_NAN => SAW_NAN,
        NUMERIC_PINF => SAW_PINF,
        NUMERIC_NINF => SAW_NINF,
        _ => 0,
    }
}

/// The sum of values among which were the specials `saw`, if any.
#[inline]
fn special_sum(saw: u8) -> Option<i256> {
    if saw & SAW_NAN != 0 || saw & (SAW_PINF | SAW_NINF) == SAW_PINF | SAW_NINF {
        Some(NUMERIC_NAN)
    } else if saw & SAW_PINF != 0 {
        Some(NUMERIC_PINF)
    } else if saw & SAW_NINF != 0 {
        Some(NUMERIC_NINF)
    } else {
        None
    }
}

/// pg_sum_decimal of one group, or of a window frame: the values it saw
/// and the NaNs and infinities among them are counted so that a frame can
/// drop them.
#[derive(Debug)]
struct SumDecimal {
    data_type: DataType,
    sum: i256,
    values: i64,
    nans: i64,
    pinfs: i64,
    ninfs: i64,
}

impl SumDecimal {
    fn add(&mut self, a: &ArrayRef, sign: i64) -> Result<()> {
        for v in decimal(a)?.iter().flatten() {
            if v == NUMERIC_NAN {
                self.nans += sign;
            } else if v == NUMERIC_PINF {
                self.pinfs += sign;
            } else if v == NUMERIC_NINF {
                self.ninfs += sign;
            } else if sign > 0 {
                self.sum = self.sum.wrapping_add(v);
            } else {
                self.sum = self.sum.wrapping_sub(v);
            }
            self.values += sign;
        }
        Ok(())
    }
}

impl Accumulator for SumDecimal {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add(&values[0], 1)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.add(&values[0], -1)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.add(&states[0], 1)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let flag = |n: i64, bit: u8| if n > 0 { bit } else { 0 };
        let saw = flag(self.nans, SAW_NAN) | flag(self.pinfs, SAW_PINF) | flag(self.ninfs, SAW_NINF);
        let v = (self.values > 0).then(|| special_sum(saw).unwrap_or(self.sum));
        if let DataType::Decimal128(..) = self.data_type {
            let v = v
                .map(|v| narrow_value(v).ok_or_else(beyond_digits))
                .transpose()
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            return ScalarValue::new_primitive::<Decimal128Type>(v, &self.data_type);
        }
        ScalarValue::new_primitive::<Decimal256Type>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// pg_sum_decimal of many groups.
struct SumDecimalGroups {
    data_type: DataType,
    /// N4: each sum's part from batches of values that all fit an i64,
    /// added in i128: within 2^63 each, 2^63 of them cannot overflow
    narrow: Vec<i128>,
    /// values added to `narrow` since it was last folded into `wide`
    narrow_rows: u64,
    /// the rest of each sum, and whether it is NaN: empty until a value
    /// needs them (`spread`), so a group takes 17 bytes, not 50; at 50
    /// Q18's 50M groups a segment spilled
    wide: Vec<i256>,
    /// NI1: the special values each group saw (SAW_ bits)
    special: Vec<u8>,
    seen: Vec<bool>,
}

impl SumDecimalGroups {
    fn new(data_type: DataType) -> Self {
        SumDecimalGroups {
            data_type,
            narrow: Vec::new(),
            narrow_rows: 0,
            wide: Vec::new(),
            special: Vec::new(),
            seen: Vec::new(),
        }
    }

    /// Gives every group a `wide` part and `special` bits.
    fn spread(&mut self) {
        self.wide.resize(self.narrow.len(), i256::ZERO);
        self.special.resize(self.narrow.len(), 0);
    }

    fn add(
        &mut self,
        a: &ArrayRef,
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total: usize,
    ) -> Result<()> {
        self.narrow.resize(total, 0);
        self.seen.resize(total, false);
        if !self.wide.is_empty() {
            self.spread();
        }
        if let Some(xs) = narrow_array(a) {
            return self.add_narrow(xs, groups, filter);
        }
        let xs = decimal(a)?;
        let values = xs.values();
        if all_i64(values) {
            if self.narrow_rows + groups.len() as u64 > 1 << 62 {
                self.fold_narrow();
            }
            self.narrow_rows += groups.len() as u64;
            if xs.null_count() == 0 && filter.is_none() {
                for (&g, &v) in groups.iter().zip(values.iter()) {
                    self.narrow[g] += v.as_i128();
                    self.seen[g] = true;
                }
            } else {
                for (r, &g) in groups.iter().enumerate() {
                    if xs.is_valid(r) && filter.is_none_or(|f| f.is_valid(r) && f.value(r)) {
                        self.narrow[g] += values[r].as_i128();
                        self.seen[g] = true;
                    }
                }
            }
            return Ok(());
        }
        self.spread();
        let mut one = |g: usize, v: i256| {
            match special_bit(v) {
                0 => self.wide[g] = self.wide[g].wrapping_add(v),
                bit => self.special[g] |= bit,
            }
            self.seen[g] = true;
        };
        if xs.null_count() == 0 && filter.is_none() {
            for (&g, &v) in groups.iter().zip(values.iter()) {
                one(g, v);
            }
        } else {
            for (r, &g) in groups.iter().enumerate() {
                if xs.is_valid(r) && filter.is_none_or(|f| f.is_valid(r) && f.value(r)) {
                    one(g, values[r]);
                }
            }
        }
        Ok(())
    }

    /// N6: `add` of narrow values: those of a batch that all fit an i64
    /// into `narrow`, as wide ones fitting it do; the others one by one.
    fn add_narrow(
        &mut self,
        xs: &Decimal128Array,
        groups: &[usize],
        filter: Option<&BooleanArray>,
    ) -> Result<()> {
        let values = xs.values();
        let take = |r: usize| xs.is_valid(r) && filter.is_none_or(|f| f.is_valid(r) && f.value(r));
        let dense = xs.null_count() == 0 && filter.is_none();
        if values.iter().fold(true, |all, &v| all & (v == v as i64 as i128)) {
            if self.narrow_rows + groups.len() as u64 > 1 << 62 {
                self.fold_narrow();
            }
            self.narrow_rows += groups.len() as u64;
            if dense {
                for (&g, &v) in groups.iter().zip(values.iter()) {
                    self.narrow[g] += v;
                    self.seen[g] = true;
                }
            } else {
                for (r, &g) in groups.iter().enumerate() {
                    if take(r) {
                        self.narrow[g] += values[r];
                        self.seen[g] = true;
                    }
                }
            }
            return Ok(());
        }
        self.spread();
        for (r, &g) in groups.iter().enumerate() {
            if dense || take(r) {
                match values[r] {
                    NARROW_NAN => self.special[g] |= SAW_NAN,
                    NARROW_PINF => self.special[g] |= SAW_PINF,
                    NARROW_NINF => self.special[g] |= SAW_NINF,
                    v => self.wide[g] = self.wide[g].wrapping_add(i256::from_i128(v)),
                }
                self.seen[g] = true;
            }
        }
        Ok(())
    }

    fn fold_narrow(&mut self) {
        self.spread();
        for (s, n) in self.wide.iter_mut().zip(self.narrow.iter_mut()) {
            *s = s.wrapping_add(i256::from_i128(std::mem::take(n)));
        }
        self.narrow_rows = 0;
    }
}

/// `emit_to.take_needed` of a vector that may be empty (none of its groups
/// needed it yet).
fn take_spread<T>(emit_to: EmitTo, v: &mut Vec<T>) -> Vec<T> {
    if v.is_empty() {
        Vec::new()
    } else {
        emit_to.take_needed(v)
    }
}

impl GroupsAccumulator for SumDecimalGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(&values[0], group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.add(&values[0], group_indices, None, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let narrow = emit_to.take_needed(&mut self.narrow);
        let wide = take_spread(emit_to, &mut self.wide);
        let special = take_spread(emit_to, &mut self.special);
        let seen = emit_to.take_needed(&mut self.seen);
        let wide_sum = |g: usize| wide[g].wrapping_add(i256::from_i128(narrow[g]));
        if let DataType::Decimal128(..) = self.data_type {
            // N6: within its digits (`return_type`)
            let values = (0..narrow.len())
                .map(|g| match (special.is_empty(), wide.is_empty()) {
                    (false, _) if special[g] != 0 => special_sum(special[g]).and_then(narrow_value),
                    (_, true) => Some(narrow[g]).filter(|v| *v < NARROW_PINF && *v > NARROW_NINF),
                    _ => narrow_value(wide_sum(g)),
                })
                .map(|v| v.ok_or_else(beyond_digits))
                .collect::<Result<Vec<i128>, ArrowError>>()
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            return Ok(Arc::new(
                Decimal128Array::new(ScalarBuffer::from(values), Some(NullBuffer::from(seen)))
                    .with_data_type(self.data_type.clone()),
            ));
        }
        let values: Vec<i256> = (0..narrow.len())
            .map(|g| match (special.is_empty(), wide.is_empty()) {
                (false, _) if special[g] != 0 => special_sum(special[g]).unwrap(),
                (_, true) => i256::from_i128(narrow[g]),
                _ => wide_sum(g),
            })
            .collect();
        Ok(Arc::new(
            Decimal256Array::new(ScalarBuffer::from(values), Some(NullBuffer::from(seen)))
                .with_data_type(self.data_type.clone()),
        ))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit_to)?])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // the values as the state's type: a narrow one relabeled or widened
        let a = &values[0];
        let a = match (&self.data_type, a.data_type()) {
            (DataType::Decimal256(..), DataType::Decimal128(..)) => widen(a)?,
            (DataType::Decimal128(..), DataType::Decimal128(..)) => Arc::new(
                a.as_primitive::<Decimal128Type>()
                    .clone()
                    .with_data_type(self.data_type.clone()),
            ),
            _ => a.clone(),
        };
        let Some(f) = opt_filter else {
            return Ok(vec![a]);
        };
        let keep: Vec<bool> = (0..a.len())
            .map(|r| a.is_valid(r) && f.is_valid(r) && f.value(r))
            .collect();
        let nulls = Some(NullBuffer::from(keep));
        Ok(vec![match narrow_array(&a) {
            Some(d) => Arc::new(Decimal128Array::new(d.values().clone(), nulls).with_data_type(self.data_type.clone())),
            None => Arc::new(
                Decimal256Array::new(decimal(&a)?.values().clone(), nulls)
                    .with_data_type(self.data_type.clone()),
            ),
        }])
    }

    fn size(&self) -> usize {
        self.wide.capacity() * std::mem::size_of::<i256>()
            + self.narrow.capacity() * std::mem::size_of::<i128>()
            + self.special.capacity()
            + self.seen.capacity()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumOp {
    Add,
    Sub,
    Mul,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumericFn {
    /// N6: x, numeric of scale `scale`, as numeric of `digits` digits:
    /// Decimal256 for 76, else narrow (x within those digits); NaN stays
    /// NaN.  An x of another type (an integer) is cast.
    As { digits: u8, scale: i8 },
    /// x * 10^by, to scale `to`; NaN stays NaN.
    Rescale { by: i8, to: i8 },
    /// abs(x), as numeric_abs; NaN stays NaN.
    Abs,
    /// RD1: round(x, to), as numeric_round: x of scale `from`, rounded half
    /// away from zero at scale `to` (not below 0); NaN stays NaN.
    Round { from: i8, to: i8 },
    /// x op y, of scale `to`: + and - take operands already of that scale,
    /// * operands whose scales add up to it (numeric_mul's rscale).
    Arith { op: NumOp, to: i8 },
    /// AVG1: avg of a sum x of scale `from` and a count y (pg_avg), at
    /// scale `to`; NULL for no rows, NaN for a NaN sum.
    Avg { from: i8, to: i8 },
    /// AV2: the display scale of that avg (pg_avg_rscale), int4; 0 for a
    /// NaN sum, NULL for no rows.
    AvgScale { from: i8 },
    /// DN1: x / y (pg_div), of arguments x, its display scale, y, its
    /// display scale; x of scale `x`, y of scale `y`, the result at `to`.
    /// NaN if either is; 22012 for a zero y.
    Div { x: i8, y: i8, to: i8 },
    /// DN1: the display scale of that quotient (pg_div_rscale), int4.
    DivScale { x: i8, y: i8 },
    /// STD1: stddev or variance (pg_stddev) of a count, a sum and a sum of
    /// squares of integers, at scale `to`; NULL as PostgreSQL's.
    Stddev { sample: bool, variance: bool, to: i8 },
    /// STD1: its display scale, int4.
    StddevScale { sample: bool, variance: bool },
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgNumeric {
    f: NumericFn,
    signature: Signature,
}

impl PgNumeric {
    pub fn udf(f: NumericFn) -> ScalarUDF {
        let n = match f {
            NumericFn::Arith { .. } | NumericFn::Avg { .. } | NumericFn::AvgScale { .. } => 2,
            NumericFn::Div { .. } | NumericFn::DivScale { .. } => 4,
            NumericFn::Stddev { .. } | NumericFn::StddevScale { .. } => 3,
            _ => 1,
        };
        ScalarUDF::new_from_impl(PgNumeric {
            f,
            signature: Signature::any(n, Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgNumeric {
    fn name(&self) -> &str {
        match self.f {
            NumericFn::As { .. } => "pg_numeric_as",
            NumericFn::Rescale { .. } => "pg_numeric_rescale",
            NumericFn::Abs => "pg_numeric_abs",
            NumericFn::Round { .. } => "pg_numeric_round",
            NumericFn::Arith { op: NumOp::Add, .. } => "pg_numeric_add",
            NumericFn::Arith { op: NumOp::Sub, .. } => "pg_numeric_sub",
            NumericFn::Arith { op: NumOp::Mul, .. } => "pg_numeric_mul",
            NumericFn::Avg { .. } => "pg_numeric_avg",
            NumericFn::AvgScale { .. } => "pg_numeric_avg_scale",
            NumericFn::Div { .. } => "pg_numeric_div",
            NumericFn::DivScale { .. } => "pg_numeric_div_scale",
            NumericFn::Stddev { .. } => "pg_numeric_stddev",
            NumericFn::StddevScale { .. } => "pg_numeric_stddev_scale",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.f {
            NumericFn::As { digits, scale } => {
                if digits >= NUMERIC_PRECISION {
                    numeric_type(scale)
                } else {
                    numeric_type_of(digits as i64, scale)
                }
            }
            // N6: narrow values stay narrow within 38 digits
            NumericFn::Rescale { by, to } => match &arg_types[0] {
                DataType::Decimal128(p, _) => numeric_type_of(*p as i64 + by as i64, to),
                _ => numeric_type(to),
            },
            NumericFn::Arith { op, to } => arith_type(op, to, &arg_types[0], &arg_types[1]),
            NumericFn::Round { to, .. }
            | NumericFn::Avg { to, .. }
            | NumericFn::Div { to, .. }
            | NumericFn::Stddev { to, .. } => numeric_type(to),
            NumericFn::DivScale { .. }
            | NumericFn::AvgScale { .. }
            | NumericFn::StddevScale { .. } => DataType::Int32,
            _ => arg_types[0].clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args
            .args
            .iter()
            .all(|a| matches!(a, ColumnarValue::Scalar(_)))
            && args.number_rows != 1
        {
            // N1: constant arguments (a literal rescaled, 1 - x's 1) make one
            // value, not one per row
            let one = ScalarFunctionArgs {
                number_rows: 1,
                ..args
            };
            return match self.invoke_with_args(one)? {
                ColumnarValue::Array(a) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&a, 0)?))
                }
                s => Ok(s),
            };
        }
        let n = args.number_rows;
        let arrow = |e: ArrowError| DataFusionError::ArrowError(Box::new(e), None);
        let stddev = match self.f {
            NumericFn::Stddev { sample, variance, to } => Some((sample, variance, to, false)),
            NumericFn::StddevScale { sample, variance } => {
                Some((sample, variance, STDDEV_MAX_SCALE, true))
            }
            _ => None,
        };
        if let Some((sample, variance, to, scale_only)) = stddev {
            // STD1: count (int8), sum and sum of squares (numeric of scale 0)
            let c = args.args[0].to_array(n)?;
            let counts = c.as_primitive_opt::<Int64Type>().ok_or_else(|| {
                DataFusionError::Internal(format!("stddev: count of type {}", c.data_type()))
            })?;
            let (a, b) = (args.args[1].to_array(n)?, args.args[2].to_array(n)?);
            let (sums, squares) = (decimal(&a)?, decimal(&b)?);
            let mut values = Vec::with_capacity(n);
            let mut scales = Vec::with_capacity(n);
            for r in 0..n {
                let got = if counts.is_null(r) || sums.is_null(r) || squares.is_null(r) {
                    None
                } else {
                    pg_stddev(counts.value(r), sums.value(r), squares.value(r), sample, variance, to)
                        .map_err(arrow)?
                };
                values.push(got.map(|(v, _)| v));
                scales.push(got.map(|(_, d)| d));
            }
            let out: ArrayRef = if scale_only {
                Arc::new(PrimitiveArray::<Int32Type>::from(scales))
            } else {
                Arc::new(Decimal256Array::from(values).with_data_type(numeric_type(to)))
            };
            return Ok(ColumnarValue::Array(out));
        }
        let out_type = args.return_field.data_type().clone();
        if let NumericFn::Arith { op, .. } = self.f {
            if let Some(a) = arith_narrow_args(op, &args.args, n, &out_type)? {
                return Ok(ColumnarValue::Array(a));
            }
        }
        let x = args.args[0].to_array(n)?;
        if let NumericFn::As { scale, digits } = self.f {
            if numeric_digits(x.data_type()).is_some_and(|(_, s)| s != scale) {
                if digits < NUMERIC_PRECISION {
                    return Err(DataFusionError::Internal(format!(
                        "pg_numeric_as: {} at scale {scale}",
                        x.data_type()
                    )));
                }
                // an output at another scale: rescaled as a cast does
                return Ok(ColumnarValue::Array(
                    datafusion::arrow::compute::cast(&widen(&x)?, &out_type).map_err(arrow)?,
                ));
            }
        }
        if matches!(self.f, NumericFn::As { .. }) && numeric_digits(x.data_type()).is_none() {
            // an output of numeric type computed in another (an integer)
            return Ok(ColumnarValue::Array(
                datafusion::arrow::compute::cast(&x, &out_type).map_err(arrow)?,
            ));
        }
        if let Some(d) = narrow_array(&x) {
            // N6: narrow in, narrow out
            match (self.f, &out_type) {
                (NumericFn::As { .. }, DataType::Decimal128(..)) | (NumericFn::Abs, _) => {
                    let a = if matches!(self.f, NumericFn::Abs) {
                        d.unary::<_, Decimal128Type>(|v| match v {
                            NARROW_NAN | NARROW_PINF | NARROW_NINF => v,
                            v => v.abs(),
                        })
                    } else {
                        d.clone()
                    };
                    return Ok(ColumnarValue::Array(Arc::new(a.with_data_type(out_type))));
                }
                (NumericFn::Rescale { by, .. }, DataType::Decimal128(..)) => {
                    let k = 10i128.pow(by as u32);
                    let a: Decimal128Array = d
                        .try_unary(|v| match v {
                            NARROW_NAN | NARROW_PINF | NARROW_NINF => Ok(v),
                            v => v
                                .checked_mul(k)
                                .filter(|v| *v < NARROW_PINF && *v > NARROW_NINF)
                                .ok_or_else(beyond_digits),
                        })
                        .map_err(arrow)?;
                    return Ok(ColumnarValue::Array(Arc::new(a.with_data_type(out_type))));
                }
                _ => {}
            }
        }
        let xs = decimal(&x)?;
        let out: ArrayRef = match self.f {
            NumericFn::Stddev { .. } | NumericFn::StddevScale { .. } => {
                return Err(DataFusionError::Internal("stddev computed above".into()))
            }
            NumericFn::As { .. } => match &out_type {
                DataType::Decimal128(..) => {
                    // a wide x within the digits
                    let a: Decimal128Array = xs
                        .try_unary(|v| narrow_value(v).ok_or_else(beyond_digits))
                        .map_err(arrow)?;
                    Arc::new(a.with_data_type(out_type.clone()))
                }
                _ => Arc::new(xs.into_owned()),
            },
            NumericFn::Rescale { by, to } => {
                let k = pow10(by as u32);
                let a: Decimal256Array = xs
                    .try_unary(|v| {
                        if v == NUMERIC_NAN {
                            Ok(v)
                        } else {
                            v.checked_mul(k).ok_or_else(beyond)
                        }
                    })
                    .map_err(arrow)?;
                Arc::new(a.with_data_type(numeric_type(to)))
            }
            NumericFn::Abs => {
                // within 76 digits, far from i256::MIN
                let a: Decimal256Array =
                    xs.unary(|v| if v == NUMERIC_NAN { v } else { v.wrapping_abs() });
                Arc::new(a.with_data_type(x.data_type().clone()))
            }
            NumericFn::Round { from, to } => {
                let a: Decimal256Array = xs
                    .try_unary(|v| pg_round(v, from, to).ok_or_else(beyond))
                    .map_err(arrow)?;
                Arc::new(a.with_data_type(numeric_type(to)))
            }
            NumericFn::Arith { op, to } => {
                let f = match op {
                    NumOp::Add => num_add,
                    NumOp::Sub => num_sub,
                    NumOp::Mul => num_mul,
                };
                let a: Decimal256Array = match &args.args[1] {
                    // a constant operand (1 - x): one pass over x
                    ColumnarValue::Scalar(ScalarValue::Decimal256(Some(b), ..)) => {
                        let b = *b;
                        match narrow_arith(op, &xs, Operand::One(b)) {
                            Some(a) => a,
                            None => xs.try_unary(|a| f(a, b)).map_err(arrow)?,
                        }
                    }
                    ColumnarValue::Scalar(ScalarValue::Decimal256(None, ..)) => {
                        Decimal256Array::new_null(n)
                    }
                    y => {
                        let y = y.to_array(n)?;
                        let ys = decimal(&y)?;
                        match narrow_arith(op, &xs, Operand::Each(&ys)) {
                            Some(a) => a,
                            None => datafusion::arrow::compute::try_binary(&*xs, &*ys, f)
                                .map_err(arrow)?,
                        }
                    }
                };
                Arc::new(a.with_data_type(numeric_type(to)))
            }
            NumericFn::AvgScale { from } => {
                let y = args.args[1].to_array(n)?;
                let counts = y.as_primitive_opt::<Int64Type>().ok_or_else(|| {
                    DataFusionError::Internal(format!("avg: count of type {}", y.data_type()))
                })?;
                let scales: PrimitiveArray<Int32Type> = (0..n)
                    .map(|r| {
                        if xs.is_null(r) || counts.is_null(r) || counts.value(r) == 0 {
                            None
                        } else if xs.value(r) == NUMERIC_NAN {
                            Some(0)
                        } else {
                            Some(pg_avg_rscale(xs.value(r), from, counts.value(r)))
                        }
                    })
                    .collect();
                Arc::new(scales)
            }
            NumericFn::Avg { from, to } => {
                let y = args.args[1].to_array(n)?;
                let counts = y.as_primitive_opt::<Int64Type>().ok_or_else(|| {
                    DataFusionError::Internal(format!("avg: count of type {}", y.data_type()))
                })?;
                let mut values = Vec::with_capacity(n);
                for r in 0..n {
                    if xs.is_null(r) || counts.is_null(r) || counts.value(r) == 0 {
                        values.push(None);
                        continue;
                    }
                    let sum = xs.value(r);
                    values.push(Some(if sum == NUMERIC_NAN {
                        NUMERIC_NAN
                    } else {
                        pg_avg(sum, from, counts.value(r), to)
                            .ok_or_else(beyond)
                            .map_err(arrow)?
                    }));
                }
                Arc::new(Decimal256Array::from(values).with_data_type(numeric_type(to)))
            }
            NumericFn::Div { x: sx, y: sy, .. } | NumericFn::DivScale { x: sx, y: sy } => {
                let dx = args.args[1].to_array(n)?;
                let y = args.args[2].to_array(n)?;
                let dy = args.args[3].to_array(n)?;
                let ys = decimal(&y)?;
                let int = |a: &ArrayRef| {
                    a.as_primitive_opt::<Int32Type>().cloned().ok_or_else(|| {
                        DataFusionError::Internal(format!("div: display scale of type {}", a.data_type()))
                    })
                };
                let (dx, dy) = (int(&dx)?, int(&dy)?);
                let scale_only = matches!(self.f, NumericFn::DivScale { .. });
                let mut values = Vec::with_capacity(n);
                let mut scales = Vec::with_capacity(n);
                for r in 0..n {
                    if xs.is_null(r) || ys.is_null(r) {
                        values.push(None);
                        scales.push(None);
                        continue;
                    }
                    let (a, b) = (xs.value(r), ys.value(r));
                    if a == NUMERIC_NAN || b == NUMERIC_NAN {
                        values.push(Some(NUMERIC_NAN));
                        scales.push(Some(0));
                        continue;
                    }
                    if b == i256::ZERO {
                        return Err(DataFusionError::ArrowError(
                            Box::new(crate::pgfunc::division_by_zero()),
                            None,
                        ));
                    }
                    let d = |s: &PrimitiveArray<Int32Type>| if s.is_null(r) { 0 } else { s.value(r) };
                    let rscale = pg_div_rscale(a, sx, d(&dx), b, sy, d(&dy));
                    scales.push(Some(rscale));
                    if !scale_only {
                        let to = match self.f {
                            NumericFn::Div { to, .. } => to,
                            _ => unreachable!(),
                        };
                        values.push(Some(pg_div(a, sx, b, sy, rscale, to).ok_or_else(beyond).map_err(arrow)?));
                    }
                }
                match self.f {
                    NumericFn::Div { to, .. } => {
                        Arc::new(Decimal256Array::from(values).with_data_type(numeric_type(to)))
                    }
                    _ => Arc::new(PrimitiveArray::<Int32Type>::from(scales)),
                }
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

/// A side of an exact comparison (PgNumCmp).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpSide {
    /// a value of scale `scale`
    Value { scale: i8 },
    /// numeric_div of x, of scale `x`, by y, of scale `y`, as pg_div
    /// computes it, of arguments x, its display scale, y, its display scale
    Div { x: i8, y: i8 },
}

impl CmpSide {
    fn nargs(self) -> usize {
        match self {
            CmpSide::Value { .. } => 1,
            CmpSide::Div { .. } => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Lt,
    Le,
    Eq,
    Ne,
    Ge,
    Gt,
}

impl CmpOp {
    pub fn from_pg(op: &str) -> Option<CmpOp> {
        Some(match op {
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            "=" => CmpOp::Eq,
            "<>" => CmpOp::Ne,
            ">=" => CmpOp::Ge,
            ">" => CmpOp::Gt,
            _ => return None,
        })
    }

    fn holds(self, o: Ordering) -> bool {
        match self {
            CmpOp::Lt => o == Ordering::Less,
            CmpOp::Le => o != Ordering::Greater,
            CmpOp::Eq => o == Ordering::Equal,
            CmpOp::Ne => o != Ordering::Equal,
            CmpOp::Ge => o != Ordering::Less,
            CmpOp::Gt => o == Ordering::Greater,
        }
    }
}

/// A side's value exactly: NaN, or a sign, magnitude and scale.
#[derive(Debug, PartialEq)]
enum Exact {
    Nan,
    Num { neg: bool, mag: BigUint, scale: i32 },
}

fn magnitude(v: i256) -> BigUint {
    BigUint::from_bytes_be(&v.wrapping_abs().to_be_bytes())
}

fn ten(k: i32) -> BigUint {
    BigUint::from(10u32).pow(k as u32)
}

impl Exact {
    fn value(v: i256, scale: i8) -> Exact {
        if v == NUMERIC_NAN {
            return Exact::Nan;
        }
        Exact::Num { neg: v < i256::ZERO, mag: magnitude(v), scale: scale as i32 }
    }

    /// pg_div's quotient, without its bound on digits: rounded half away
    /// from zero at pg_div_rscale's scale.  None for a zero divisor.
    fn quotient(x: i256, sx: i8, dx: i32, y: i256, sy: i8, dy: i32) -> Option<Exact> {
        if x == NUMERIC_NAN || y == NUMERIC_NAN {
            return Some(Exact::Nan);
        }
        if y == i256::ZERO {
            return None;
        }
        let rscale = pg_div_rscale(x, sx, dx, y, sy, dy);
        let e = rscale - sx as i32 + sy as i32;
        let (mut num, mut den) = (magnitude(x), magnitude(y));
        if e >= 0 {
            num *= ten(e);
        } else {
            den *= ten(-e);
        }
        let mut q = &num / &den;
        if (num % &den) * 2u32 >= den {
            q += 1u32;
        }
        Some(Exact::Num { neg: (x < i256::ZERO) != (y < i256::ZERO), mag: q, scale: rscale })
    }

    /// As PostgreSQL orders numeric: NaN above every number, equal to NaN.
    fn compare(&self, other: &Exact) -> Ordering {
        match (self, other) {
            (Exact::Nan, Exact::Nan) => Ordering::Equal,
            (Exact::Nan, _) => Ordering::Greater,
            (_, Exact::Nan) => Ordering::Less,
            (
                Exact::Num { neg: na, mag: ma, scale: sa },
                Exact::Num { neg: nb, mag: mb, scale: sb },
            ) => {
                // -0 is 0
                let na = *na && ma.bits() > 0;
                let nb = *nb && mb.bits() > 0;
                if na != nb {
                    return if na { Ordering::Less } else { Ordering::Greater };
                }
                let s = (*sa).max(*sb);
                let o = (ma * ten(s - sa)).cmp(&(mb * ten(s - sb)));
                if na {
                    o.reverse()
                } else {
                    o
                }
            }
        }
    }
}

/// An exact comparison of two numeric values, either of which may be a
/// quotient whose digits no fixed scale holds (DN1's bound beyond 76): the
/// sides are computed as PostgreSQL computes them, in arbitrary precision,
/// and compared.  NULL if any value is; 22012 for a zero divisor.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgNumCmp {
    op: CmpOp,
    l: CmpSide,
    r: CmpSide,
    signature: Signature,
}

impl PgNumCmp {
    pub fn udf(op: CmpOp, l: CmpSide, r: CmpSide) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgNumCmp {
            op,
            l,
            r,
            signature: Signature::any(l.nargs() + r.nargs(), Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgNumCmp {
    fn name(&self) -> &str {
        "pg_numeric_cmp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let arrays = args
            .args
            .iter()
            .map(|a| a.to_array(n))
            .collect::<Result<Vec<_>>>()?;
        let side = |kind: CmpSide, a: &[ArrayRef], r: usize| -> Result<Option<Exact>> {
            match kind {
                CmpSide::Value { scale } => {
                    let v = decimal(&a[0])?;
                    Ok((!v.is_null(r)).then(|| Exact::value(v.value(r), scale)))
                }
                CmpSide::Div { x: sx, y: sy } => {
                    let (x, y) = (decimal(&a[0])?, decimal(&a[2])?);
                    if x.is_null(r) || y.is_null(r) {
                        return Ok(None);
                    }
                    let d = |a: &ArrayRef| -> Result<i32> {
                        let s = a.as_primitive_opt::<Int32Type>().ok_or_else(|| {
                            DataFusionError::Internal(format!("cmp: display scale of type {}", a.data_type()))
                        })?;
                        Ok(if s.is_null(r) { 0 } else { s.value(r) })
                    };
                    Exact::quotient(x.value(r), sx, d(&a[1])?, y.value(r), sy, d(&a[3])?)
                        .map(Some)
                        .ok_or_else(|| {
                            DataFusionError::ArrowError(Box::new(crate::pgfunc::division_by_zero()), None)
                        })
                }
            }
        };
        let (la, ra) = arrays.split_at(self.l.nargs());
        let mut out = Vec::with_capacity(n);
        for r in 0..n {
            out.push(match (side(self.l, la, r)?, side(self.r, ra, r)?) {
                (Some(a), Some(b)) => Some(self.op.holds(a.compare(&b))),
                _ => None,
            });
        }
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(out))))
    }
}

/// A numeric value in PostgreSQL's representation (utils/numeric.h), the
/// bytes after its varlena header, at `scale`: as vx_numeric_value_wide
/// reads it on the C side, for the direct PAX reader (C2).  NaN and the
/// infinities become their sentinels; a value with more fractional digits
/// than `scale`, or more than 76 digits at it, is an error.
#[inline]
pub fn from_pg_numeric(bytes: &[u8], scale: i8) -> std::result::Result<i256, String> {
    // N1: the short form (values of display scale up to 63) of at most 38
    // digits at `scale`, nearly every value of a numeric(p, s) column,
    // without the general path's checks: every NBASE digit goes into an
    // i128, then the trailing fractional digits come off.
    if let [h0, h1, digits @ ..] = bytes {
        let h = u16::from_ne_bytes([*h0, *h1]);
        if h & 0xC000 == 0x8000 {
            let weight = (h & 0x003F) as i32 - if h & 0x0040 != 0 { 0x40 } else { 0 };
            let dscale = ((h & 0x1F80) >> 7) as i32;
            let s = scale as i32;
            // the integer of all the digits has at most 4 (weight + 1)
            // digits before the point and dscale rounded up to 4 after
            if dscale <= s && 4 * (weight + 1) + s + 3 <= 38 {
                let mut acc: i128 = 0;
                let mut n = 0i32;
                for d in digits.chunks_exact(2) {
                    acc = acc * 10000 + u16::from_ne_bytes([d[0], d[1]]) as i128;
                    n += 1;
                }
                // acc is the value times 10^(4 (n - 1 - weight)); digits past
                // dscale are zeros, so the division is exact
                let shift = s - 4 * (n - 1 - weight);
                acc = if shift >= 0 {
                    acc * POW10_I128[shift as usize]
                } else {
                    acc / POW10_I128[(-shift) as usize]
                };
                return Ok(i256::from_i128(if h & 0x2000 != 0 { -acc } else { acc }));
            }
        }
    }
    const NUMERIC_SIGN_MASK: u16 = 0xC000;
    const NUMERIC_NEG: u16 = 0x4000;
    const NUMERIC_SHORT: u16 = 0x8000;
    const NUMERIC_SPECIAL: u16 = 0xC000;
    const NUMERIC_NAN_HDR: u16 = 0xC000;
    const NUMERIC_NINF_HDR: u16 = 0xF000;
    let word = |at: usize| -> std::result::Result<u16, String> {
        bytes
            .get(at..at + 2)
            .map(|b| u16::from_ne_bytes([b[0], b[1]]))
            .ok_or_else(|| "truncated numeric value".to_string())
    };
    let h = word(0)?;
    if h & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
        return Ok(match h {
            NUMERIC_NAN_HDR => NUMERIC_NAN,
            NUMERIC_NINF_HDR => NUMERIC_NINF,
            _ => NUMERIC_PINF,
        });
    }
    let (neg, dscale, weight, first) = if h & NUMERIC_SHORT != 0 {
        // short form: sign, display scale and weight in the header word
        let weight = (h & 0x003F) as i32 - if h & 0x0040 != 0 { 0x40 } else { 0 };
        (h & 0x2000 != 0, ((h & 0x1F80) >> 7) as i32, weight, 2)
    } else {
        let weight = word(2)? as i16 as i32;
        (
            h & NUMERIC_SIGN_MASK == NUMERIC_NEG,
            (h & 0x3FFF) as i32,
            weight,
            4,
        )
    };
    // NBASE digits, read in place: this runs once per value of a scan
    let digits = &bytes[first..];
    let ndigits = digits.len() / 2;
    let digit = |i: usize| u16::from_ne_bytes([digits[2 * i], digits[2 * i + 1]]) as i32;
    let scale = scale as i32;
    // decimal digits before the point
    let int_digits = if ndigits > 0 && weight >= 0 {
        let d = digit(0);
        weight * 4 + if d == 0 { 0 } else { d.ilog10() as i32 + 1 }
    } else {
        0
    };
    if dscale > scale || int_digits + scale > NUMERIC_PRECISION as i32 {
        return Err(format!(
            "numeric value beyond numeric({NUMERIC_PRECISION}, {scale})"
        ));
    }
    // Horner over the digits worth at least 1 at `scale`; the next one may
    // reach past it, with zeros there since dscale <= scale.  Within 38
    // digits (any numeric(p, s) column) in i128, beyond that in i256.
    if int_digits + scale <= 38 {
        let mut acc: i128 = 0;
        let mut exp10 = 0i32;
        for i in 0..ndigits {
            let e = (weight - i as i32) * 4 + scale;
            if e >= 0 {
                acc = acc * 10000 + digit(i) as i128;
                exp10 = e;
            } else {
                acc *= POW10_I128[exp10 as usize];
                exp10 = 0;
                if e > -4 {
                    acc += (digit(i) / 10i32.pow((-e) as u32)) as i128;
                }
                break;
            }
        }
        acc *= POW10_I128[exp10 as usize];
        return Ok(i256::from_i128(if neg { -acc } else { acc }));
    }
    let nbase = i256::from_i128(10000);
    let mut acc = i256::ZERO;
    let mut exp10 = 0i32;
    for i in 0..ndigits {
        let e = (weight - i as i32) * 4 + scale;
        if e >= 0 {
            acc = acc * nbase + i256::from_i128(digit(i) as i128);
            exp10 = e;
        } else {
            acc *= pow10(exp10 as u32);
            exp10 = 0;
            if e > -4 {
                acc += i256::from_i128((digit(i) / 10i32.pow((-e) as u32)) as i128);
            }
            break;
        }
    }
    acc *= pow10(exp10 as u32);
    Ok(if neg { acc.wrapping_neg() } else { acc })
}

/// 10^0 .. 10^38
const POW10_I128: [i128; 39] = {
    let mut t = [1i128; 39];
    let mut k = 1;
    while k < 39 {
        t[k] = t[k - 1] * 10;
        k += 1;
    }
    t
};

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    /// PostgreSQL's representation of a numeric, after the varlena header.
    fn pg_bytes(header: u16, weight: Option<i16>, digits: &[u16]) -> Vec<u8> {
        let mut b = header.to_ne_bytes().to_vec();
        if let Some(w) = weight {
            b.extend(w.to_ne_bytes());
        }
        for d in digits {
            b.extend(d.to_ne_bytes());
        }
        b
    }

    #[test]
    fn narrows_motion_numerics() {
        let values = vec![
            Some(i256::from_i128(-999_999_999_999_999_999)),
            None,
            Some(i256::from_i128(12345)),
            Some(NUMERIC_NAN),
            Some(NUMERIC_PINF),
            Some(NUMERIC_NINF),
        ];
        let a: ArrayRef = Arc::new(
            Decimal256Array::from(values.clone())
                .with_precision_and_scale(NUMERIC_PRECISION, 2)
                .unwrap(),
        );
        for nw in [MotionNumeric::Int64, MotionNumeric::Decimal128] {
            let n = nw.narrow(&a).unwrap();
            assert_eq!(n.data_type(), &nw.wire_type(2));
            let w = nw.widen(&n, 38, 2).unwrap();
            assert_eq!(w.data_type(), &DataType::Decimal128(38, 2));
            assert_eq!(decimal(&w).unwrap().iter().collect::<Vec<_>>(), values);
        }
        // beyond int8, within 16 bytes
        let big: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(10i128.pow(30)))])
                .with_precision_and_scale(NUMERIC_PRECISION, 4)
                .unwrap(),
        );
        assert!(MotionNumeric::Int64.narrow(&big).is_err());
        let n = MotionNumeric::Decimal128.narrow(&big).unwrap();
        assert_eq!(
            widen(&MotionNumeric::Decimal128.widen(&n, 38, 4).unwrap()).unwrap().as_ref(),
            big.as_ref()
        );
        assert_eq!(MotionNumeric::of_digits(18), Some(MotionNumeric::Int64));
        assert_eq!(
            MotionNumeric::of_digits(19),
            Some(MotionNumeric::Decimal128)
        );
        assert_eq!(MotionNumeric::of_digits(39), None);
    }

    #[test]
    fn reads_pg_numerics() {
        let v = |b: &[u8], s: i8| from_pg_numeric(b, s).unwrap();
        // 1.50, short form: dscale 2 in bits 7-12, weight 0
        let x = pg_bytes(0x8000 | (2 << 7), None, &[1, 5000]);
        assert_eq!(v(&x, 2), i256::from_i128(150));
        assert_eq!(v(&x, 4), i256::from_i128(15000));
        assert!(from_pg_numeric(&x, 1).is_err());
        // -1234.5678, long form: NUMERIC_NEG | dscale 4, weight 0
        let x = pg_bytes(0x4000 | 4, Some(0), &[1234, 5678]);
        assert_eq!(v(&x, 4), i256::from_i128(-12345678));
        // 0.001: weight -1 in the short form's 7-bit field, digit 0010
        let x = pg_bytes(0x8000 | (3 << 7) | 0x0040 | 0x3F, None, &[10]);
        assert_eq!(v(&x, 3), i256::from_i128(1));
        assert_eq!(v(&x, 5), i256::from_i128(100));
        // 12.3: its last digit 3000 reaches past scale 1, with zeros there
        let x = pg_bytes(0x8000 | (1 << 7), None, &[12, 3000]);
        assert_eq!(v(&x, 1), i256::from_i128(123));
        // 0 has no digits
        assert_eq!(v(&pg_bytes(0x8000, None, &[]), 2), i256::ZERO);
        // 10^36 at scale 2 (38 digits) and the largest 76-digit integer
        let x = pg_bytes(0x8000 | 9, None, &[1]);
        assert_eq!(v(&x, 2), pow10(38));
        let x = pg_bytes(0, Some(18), &[9999; 19]);
        assert_eq!(v(&x, 0), pow10(76) - i256::ONE);
        // one more digit is too many
        let x = pg_bytes(0, Some(19), &[1]);
        assert!(from_pg_numeric(&x, 0).is_err());
        // NaN and the infinities
        assert_eq!(v(&pg_bytes(0xC000, None, &[]), 2), NUMERIC_NAN);
        assert_eq!(v(&pg_bytes(0xD000, None, &[]), 2), NUMERIC_PINF);
        assert_eq!(v(&pg_bytes(0xF000, None, &[]), 2), NUMERIC_NINF);
    }

    #[test]
    fn short_form_fast_path_reads_as_the_long_form() {
        // values of up to 38 digits at several display scales, each in the
        // short form (N1's fast path when it fits) and the long form (the
        // general path), read at scales from dscale up
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20000 {
            let ndig = (next() % 39) as u32;
            let dscale = (next() % 13) as i8;
            let mag = ((next() as u128) << 64 | next() as u128) % 10u128.pow(ndig.min(38)).max(1);
            let v = if next() % 2 == 0 {
                mag as i128
            } else {
                -(mag as i128)
            };
            let (le, nfrac) = nbase_digits(i256::from_i128(v), dscale);
            let (be, weight): (Vec<u16>, i32) = match le.iter().rposition(|&d| d != 0) {
                Some(top) => (
                    le[..=top].iter().rev().copied().collect(),
                    top as i32 - nfrac,
                ),
                None => (vec![], 0),
            };
            let neg = v < 0;
            let short = 0x8000
                | if neg { 0x2000 } else { 0 }
                | ((dscale as u16) << 7)
                | (weight as u16 & 0x7F);
            let long = if neg { 0x4000 } else { 0 } | dscale as u16;
            let sb = pg_bytes(short, None, &be);
            let lb = pg_bytes(long, Some(weight as i16), &be);
            for s in dscale..=dscale + 3 {
                let want = i256::from_i128(v)
                    .checked_mul(pow10((s - dscale) as u32))
                    .unwrap();
                assert_eq!(
                    from_pg_numeric(&sb, s).unwrap(),
                    want,
                    "{v} dscale {dscale} at {s}"
                );
                assert_eq!(from_pg_numeric(&lb, s).unwrap(), want, "{v} long at {s}");
            }
            if dscale > 0 && v % 10 != 0 {
                assert!(from_pg_numeric(&sb, dscale - 1).is_err());
            }
        }
    }

    /// numeric_div(s::numeric(38, 2), c), as numeric_avg computes avg:
    /// SELECT (s::numeric(38,2) / c::numeric)::text.
    /// "12.50" as (1250, 2)
    fn dec(t: &str) -> (i256, i8) {
        let neg = t.starts_with('-');
        let t = t.trim_start_matches('-');
        let (int, frac) = t.split_once('.').unwrap_or((t, ""));
        let mut v = i256::ZERO;
        for c in int.chars().chain(frac.chars()) {
            v = v * i256::from_i128(10) + i256::from_i128(c.to_digit(10).unwrap() as i128);
        }
        (if neg { v.wrapping_neg() } else { v }, frac.len() as i8)
    }

    fn show(v: i256, scale: i32) -> String {
        let neg = v < i256::ZERO;
        let digits = v.wrapping_abs().to_string();
        let digits = format!("{:0>width$}", digits, width = scale as usize + 1);
        let (int, frac) = digits.split_at(digits.len() - scale as usize);
        format!("{}{}{}{}", if neg { "-" } else { "" }, int, if scale > 0 { "." } else { "" }, frac)
    }

    /// numeric_div's values and display scales, from PostgreSQL itself;
    /// the scale the operands are carried at does not matter.
    #[test]
    fn div_as_postgresql() {
        let cases = [
            ("1.00", "3.00", "0.33333333333333333333"),
            ("10.00", "4.0", "2.5000000000000000"),
            ("-7.5", "2", "-3.7500000000000000"),
            ("2", "-3", "-0.66666666666666666667"),
            ("1", "3", "0.33333333333333333333"),
            ("0.00", "7.00", "0.00000000000000000000"),
            ("123456789.12", "0.01", "12345678912.00000000"),
            ("0.01", "123456789.12", "0.0000000000810000006583680054"),
            ("99999.99", "0.01", "9999999.000000000000"),
            ("-0.005", "3", "-0.00166666666666666667"),
            ("5", "2", "2.5000000000000000"),
            ("12345678901234567890.12", "3.3", "3741114818555929663.67"),
            ("0.0001", "99999999.9999", "0.0000000000010000000000010000"),
            ("1.5", "1.5", "1.00000000000000000000"),
            ("9999", "10000", "0.99990000000000000000"),
            ("1", "10000", "0.000100000000000000000000"),
            ("-2.5", "-0.5", "5.0000000000000000"),
            ("0.00000001", "3", "0.0000000033333333333333333333"),
            ("100", "7", "14.2857142857142857"),
            ("1.000000", "3", "0.33333333333333333333"),
        ];
        for (x, y, want) in cases {
            let ((xv, xs), (yv, ys)) = (dec(x), dec(y));
            for pad in [0i8, 3, 9] {
                let k = pow10(pad as u32);
                let (xc, yc) = (xv * k, yv * k);
                let rscale = pg_div_rscale(xc, xs + pad, xs as i32, yc, ys + pad, ys as i32);
                let q = pg_div(xc, xs + pad, yc, ys + pad, rscale, 40).unwrap();
                let q = q / pow10((40 - rscale) as u32);
                assert_eq!(show(q, rscale), want, "{x} / {y} carried {pad} digits finer");
            }
        }
    }

    #[test]
    fn avg_as_postgresql() {
        let cases = [
            ("-0.15", 1, "-0.15000000000000000000"),
            ("-0.15", 2, "-0.07500000000000000000"),
            ("-0.15", 3, "-0.05000000000000000000"),
            ("-0.15", 7, "-0.02142857142857142857"),
            ("-0.15", 9999, "-0.000015001500150015001500"),
            ("-0.15", 10000, "-0.000015000000000000000000"),
            ("-0.15", 123456789, "-0.0000000012150000110565001006"),
            (
                "-0.15",
                9000000000000000000,
                "-0.000000000000000000016666666666666667",
            ),
            ("-5.55", 1, "-5.5500000000000000"),
            ("-5.55", 2, "-2.7750000000000000"),
            ("-5.55", 3, "-1.8500000000000000"),
            ("-5.55", 7, "-0.79285714285714285714"),
            ("-5.55", 9999, "-0.00055505550555055506"),
            ("-5.55", 10000, "-0.00055500000000000000"),
            ("-5.55", 123456789, "-0.000000044955000409090504"),
            (
                "-5.55",
                9000000000000000000,
                "-0.000000000000000000616666666666666667",
            ),
            ("0.00", 1, "0.00000000000000000000"),
            ("0.00", 2, "0.00000000000000000000"),
            ("0.00", 3, "0.00000000000000000000"),
            ("0.00", 7, "0.00000000000000000000"),
            ("0.00", 9999, "0.00000000000000000000"),
            ("0.00", 10000, "0.000000000000000000000000"),
            ("0.00", 123456789, "0.0000000000000000000000000000"),
            (
                "0.00",
                9000000000000000000,
                "0.000000000000000000000000000000000000",
            ),
            ("0.01", 1, "0.01000000000000000000"),
            ("0.01", 2, "0.00500000000000000000"),
            ("0.01", 3, "0.00333333333333333333"),
            ("0.01", 7, "0.00142857142857142857"),
            ("0.01", 9999, "0.000001000100010001000100"),
            ("0.01", 10000, "0.000001000000000000000000"),
            ("0.01", 123456789, "0.0000000000810000007371000067"),
            (
                "0.01",
                9000000000000000000,
                "0.0000000000000000000011111111111111111111",
            ),
            ("0.05", 1, "0.05000000000000000000"),
            ("0.05", 2, "0.02500000000000000000"),
            ("0.05", 3, "0.01666666666666666667"),
            ("0.05", 7, "0.00714285714285714286"),
            ("0.05", 9999, "0.000005000500050005000500"),
            ("0.05", 10000, "0.000005000000000000000000"),
            ("0.05", 123456789, "0.0000000004050000036855000335"),
            (
                "0.05",
                9000000000000000000,
                "0.0000000000000000000055555555555555555556",
            ),
            ("1.00", 1, "1.00000000000000000000"),
            ("1.00", 2, "0.50000000000000000000"),
            ("1.00", 3, "0.33333333333333333333"),
            ("1.00", 7, "0.14285714285714285714"),
            ("1.00", 9999, "0.00010001000100010001"),
            ("1.00", 10000, "0.000100000000000000000000"),
            ("1.00", 123456789, "0.0000000081000000737100006708"),
            (
                "1.00",
                9000000000000000000,
                "0.000000000000000000111111111111111111",
            ),
            ("12345.67", 1, "12345.6700000000000000"),
            ("12345.67", 2, "6172.8350000000000000"),
            ("12345.67", 3, "4115.2233333333333333"),
            ("12345.67", 7, "1763.6671428571428571"),
            ("12345.67", 9999, "1.2346904690469047"),
            ("12345.67", 10000, "1.23456700000000000000"),
            ("12345.67", 123456789, "0.000099999927909999343981"),
            (
                "12345.67",
                9000000000000000000,
                "0.00000000000000137174111111111111",
            ),
            (
                "1234567890123456789012345678901234.56",
                1,
                "1234567890123456789012345678901234.56",
            ),
            (
                "1234567890123456789012345678901234.56",
                2,
                "617283945061728394506172839450617.28",
            ),
            (
                "1234567890123456789012345678901234.56",
                3,
                "411522630041152263004115226300411.52",
            ),
            (
                "1234567890123456789012345678901234.56",
                7,
                "176366841446208112716049382700176.37",
            ),
            (
                "1234567890123456789012345678901234.56",
                9999,
                "123469135925938272728507418631.99",
            ),
            (
                "1234567890123456789012345678901234.56",
                10000,
                "123456789012345678901234567890.12",
            ),
            (
                "1234567890123456789012345678901234.56",
                123456789,
                "10000000001000000000100000.00",
            ),
            (
                "1234567890123456789012345678901234.56",
                9000000000000000000,
                "137174210013717.4210",
            ),
            ("9999.99", 1, "9999.9900000000000000"),
            ("9999.99", 2, "4999.9950000000000000"),
            ("9999.99", 3, "3333.3300000000000000"),
            ("9999.99", 7, "1428.5700000000000000"),
            ("9999.99", 9999, "1.00009900990099009901"),
            ("9999.99", 10000, "0.99999900000000000000"),
            ("9999.99", 123456789, "0.000080999919737099269608"),
            (
                "9999.99",
                9000000000000000000,
                "0.00000000000000111111000000000000",
            ),
            ("99999999999.99", 1, "99999999999.99000000"),
            ("99999999999.99", 2, "49999999999.99500000"),
            ("99999999999.99", 3, "33333333333.33000000"),
            ("99999999999.99", 7, "14285714285.71285714"),
            ("99999999999.99", 9999, "10001000.100009000900"),
            ("99999999999.99", 10000, "9999999.999999000000"),
            ("99999999999.99", 123456789, "810.0000073709190671"),
            (
                "99999999999.99",
                9000000000000000000,
                "0.000000011111111111110000",
            ),
        ];
        let int = |s: &str| i256::from_string(&s.replace(['.', '-'], "")).unwrap();
        for (sum, count, want) in cases {
            let neg = sum.starts_with('-');
            let s = if neg {
                int(sum).wrapping_neg()
            } else {
                int(sum)
            };
            let scale = want.split('.').nth(1).map_or(0, str::len) as i32;
            assert_eq!(pg_avg_rscale(s, 2, count), scale, "{sum} / {count}");
            let mut w = int(want) * pow10((40 - scale) as u32);
            if want.starts_with('-') {
                w = w.wrapping_neg();
            }
            assert_eq!(pg_avg(s, 2, count, 40), Some(w), "{sum} / {count}");
        }
        // a scale beyond the one asked for
        assert_eq!(pg_avg(i256::ONE, 2, 9_000_000_000_000_000_000, 20), None);
    }

    fn call(f: NumericFn, args: Vec<ArrayRef>) -> ArrayRef {
        let n = args[0].len();
        let arg_fields = args
            .iter()
            .map(|a| Arc::new(Field::new("a", a.data_type().clone(), true)))
            .collect();
        let udf = PgNumeric {
            f,
            signature: Signature::any(args.len(), Volatility::Immutable),
        };
        let ret = udf
            .return_type(
                &args
                    .iter()
                    .map(|a| a.data_type().clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let out = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: args.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: n,
                return_field: Arc::new(Field::new("r", ret, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap();
        match out {
            ColumnarValue::Array(a) => a,
            _ => unreachable!(),
        }
    }

    fn dec256(v: Vec<Option<i256>>, scale: i8) -> ArrayRef {
        Arc::new(Decimal256Array::from(v).with_data_type(numeric_type(scale)))
    }

    #[test]
    fn arithmetic_past_i128_and_with_constants() {
        let i = |v: i128| i256::from_i128(v);
        let big = i(i128::MAX / 2);
        let x = dec256(vec![Some(big), Some(i(-3)), None, Some(NUMERIC_NAN)], 0);
        let y = dec256(vec![Some(i(4)), Some(big), Some(i(1)), Some(i(2))], 0);
        // beyond i128, then i256's arithmetic
        let p = call(
            NumericFn::Arith {
                op: NumOp::Mul,
                to: 0,
            },
            vec![x.clone(), y.clone()],
        );
        let p = decimal(&p).unwrap();
        assert_eq!(p.value(0), big.checked_mul(i(4)).unwrap());
        assert_eq!(p.value(1), big.checked_mul(i(-3)).unwrap());
        assert!(p.is_null(2));
        assert_eq!(p.value(3), NUMERIC_NAN);
        let a = call(
            NumericFn::Arith {
                op: NumOp::Add,
                to: 0,
            },
            vec![x.clone(), x.clone()],
        );
        let a = decimal(&a).unwrap();
        assert_eq!(
            (a.value(0), a.value(1), a.value(3)),
            (big + big, i(-6), NUMERIC_NAN)
        );
        let d = call(
            NumericFn::Arith {
                op: NumOp::Sub,
                to: 0,
            },
            vec![y, x.clone()],
        );
        let d = decimal(&d).unwrap();
        assert_eq!((d.value(0), d.value(1)), (i(4) - big, big + i(3)));
        // a constant operand: 10 - x; a NULL one: NULL
        let udf = PgNumeric::udf(NumericFn::Arith {
            op: NumOp::Sub,
            to: 0,
        });
        let run = |l: ColumnarValue, r: ColumnarValue, n: usize| {
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![l, r],
                arg_fields: vec![Arc::new(Field::new("a", numeric_type(0), true)); 2],
                number_rows: n,
                return_field: Arc::new(Field::new("r", numeric_type(0), true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
        };
        let ten = || ColumnarValue::Scalar(ScalarValue::Decimal256(Some(i(10)), 76, 0));
        let r = run(ColumnarValue::Array(x.clone()), ten(), 4)
            .to_array(4)
            .unwrap();
        let r = decimal(&r).unwrap();
        assert_eq!(
            (r.value(1), r.is_null(2), r.value(3)),
            (i(-13), true, NUMERIC_NAN)
        );
        let none = ColumnarValue::Scalar(ScalarValue::Decimal256(None, 76, 0));
        let r = run(ColumnarValue::Array(x), none, 4).to_array(4).unwrap();
        assert_eq!(r.null_count(), 4);
        // constants only: one value
        match run(ten(), ten(), 4) {
            ColumnarValue::Scalar(ScalarValue::Decimal256(Some(v), 76, 0)) => assert_eq!(v, i(0)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn narrow_paths_match_the_general_ones() {
        // N4: arrays all within i64 (boundaries included) take the plain
        // loops; one value beyond, or NaN, sends them the general way; the
        // results, nulls and sums are the same either way
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let edges = [i64::MAX, i64::MIN, i64::MAX - 1, i64::MIN + 1, 0, -1, 1];
        for round in 0..200 {
            let n = 1 + (next() % 300) as usize;
            let mut gen = |k: usize| -> Option<i256> {
                match next() % 10 {
                    0 => None,
                    1 => Some(i256::from_i128(edges[k % edges.len()] as i128)),
                    _ => Some(i256::from_i128((next() as i64 >> (next() % 60)) as i128)),
                }
            };
            let mut a: Vec<Option<i256>> = (0..n).map(&mut gen).collect();
            let b: Vec<Option<i256>> = (0..n).map(&mut gen).collect();
            match round % 4 {
                1 => a[n / 2] = Some(NUMERIC_NAN),
                2 => a[n / 2] = Some(i256::from_i128(i64::MAX as i128 + 1)),
                _ => {}
            }
            let xa = dec256(a.clone(), 2);
            let xb = dec256(b.clone(), 2);
            let (xs, ys) = (decimal(&xa).unwrap(), decimal(&xb).unwrap());
            for op in [NumOp::Add, NumOp::Sub, NumOp::Mul] {
                let f = match op {
                    NumOp::Add => num_add,
                    NumOp::Sub => num_sub,
                    NumOp::Mul => num_mul,
                };
                let general: Decimal256Array =
                    datafusion::arrow::compute::try_binary(&*xs, &*ys, f).unwrap();
                match narrow_arith(op, &xs, Operand::Each(&ys)) {
                    Some(fast) => {
                        assert!(round % 4 == 0 || round % 4 == 3);
                        assert_eq!(fast.nulls(), general.nulls());
                        for r in 0..n {
                            if general.is_valid(r) {
                                assert_eq!(fast.value(r), general.value(r), "{op:?} row {r}");
                            }
                        }
                    }
                    None => assert!(round % 4 == 1 || round % 4 == 2),
                }
                let c = i256::from_i128(edges[round % edges.len()] as i128);
                let general = xs.try_unary::<_, Decimal256Type, _>(|v| f(v, c)).unwrap();
                if let Some(fast) = narrow_arith(op, &xs, Operand::One(c)) {
                    for r in 0..n {
                        if general.is_valid(r) {
                            assert_eq!(fast.value(r), general.value(r), "{op:?} const row {r}");
                        }
                    }
                }
            }
            // sums: narrow and general batches into the same groups
            let groups: Vec<usize> = (0..n).map(|_| (next() % 5) as usize).collect();
            let mut g = SumDecimalGroups::new(numeric_type(2));
            g.update_batch(&[xa.clone()], &groups, None, 5).unwrap();
            g.update_batch(&[xb.clone()], &groups, None, 5).unwrap();
            let out = g.evaluate(EmitTo::All).unwrap();
            let out = decimal(&out).unwrap();
            for k in 0..5 {
                let mut sum = i256::ZERO;
                let (mut seen, mut nan) = (false, false);
                for (r, &gr) in groups.iter().enumerate() {
                    for v in [a[r], b[r]] {
                        if gr == k {
                            if let Some(v) = v {
                                seen = true;
                                if v == NUMERIC_NAN {
                                    nan = true;
                                } else {
                                    sum = sum.wrapping_add(v);
                                }
                            }
                        }
                    }
                }
                assert_eq!(out.is_valid(k), seen);
                if seen {
                    assert_eq!(out.value(k), if nan { NUMERIC_NAN } else { sum });
                }
            }
        }
    }

    #[test]
    fn sum_decimal_groups_spread_when_a_value_needs_it() {
        let i = |v: i128| i256::from_i128(v);
        let big = 1i128 << 70;
        // wide: values within an i64 leave groups 17 bytes; one beyond it
        // or a NaN after groups were emitted spreads to all groups left
        let mut g = SumDecimalGroups::new(numeric_type(2));
        let small = dec256(vec![Some(i(1)), Some(i(2)), Some(i(3))], 2);
        g.update_batch(&[small], &[0, 1, 2], None, 3).unwrap();
        assert!(g.wide.is_empty() && g.special.is_empty());
        let out = g.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(decimal(&out).unwrap().value(0), i(1));
        let more = dec256(vec![Some(i(big)), Some(NUMERIC_NAN), Some(i(5))], 2);
        g.update_batch(&[more], &[0, 2, 1], None, 3).unwrap();
        assert_eq!(g.wide.len(), 3);
        // and keeps up with groups that small values add afterwards
        g.update_batch(&[dec256(vec![Some(i(7))], 2)], &[3], None, 4).unwrap();
        let out = g.evaluate(EmitTo::All).unwrap();
        let out = decimal(&out).unwrap();
        assert_eq!(
            (out.value(0), out.value(1), out.value(2), out.value(3)),
            (i(big + 2), i(8), NUMERIC_NAN, i(7))
        );
        // narrow: the same as Decimal128 values and sums
        let t = DataType::Decimal128(38, 2);
        let d128 = |v: Vec<Option<i128>>| -> ArrayRef {
            Arc::new(Decimal128Array::from(v).with_data_type(DataType::Decimal128(18, 2)))
        };
        let mut g = SumDecimalGroups::new(t.clone());
        g.update_batch(&[d128(vec![Some(1), None, Some(3)])], &[0, 1, 2], None, 3)
            .unwrap();
        assert!(g.wide.is_empty());
        let out = g.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(out.as_primitive::<Decimal128Type>().value(0), 1);
        g.update_batch(&[d128(vec![Some(big), Some(NARROW_NAN), Some(5)])], &[0, 2, 1], None, 3)
            .unwrap();
        let out = g.evaluate(EmitTo::All).unwrap();
        let out = out.as_primitive::<Decimal128Type>();
        assert_eq!(out.data_type(), &t);
        assert_eq!((out.value(0), out.value(1), out.value(2)), (big, 3 + 5, NARROW_NAN));
        assert!(out.is_valid(0) && out.is_valid(2));
    }

    #[test]
    fn sum_decimal_of_infinities() {
        // NI1: numeric_add's infinities: +Inf + 1 = +Inf, -Inf + 1 = -Inf,
        // +Inf + -Inf = NaN, NaN + Inf = NaN; wide and narrow, one group
        // and many, and a frame dropping them
        let i = |v: i128| i256::from_i128(v);
        let cases = [
            (vec![NUMERIC_PINF, i(1)], NUMERIC_PINF),
            (vec![i(1), NUMERIC_NINF], NUMERIC_NINF),
            (vec![NUMERIC_PINF, NUMERIC_NINF], NUMERIC_NAN),
            (vec![NUMERIC_NAN, NUMERIC_PINF], NUMERIC_NAN),
            (vec![NUMERIC_PINF, NUMERIC_PINF], NUMERIC_PINF),
        ];
        for (vals, want) in cases {
            let n = vals.len();
            let mut g = SumDecimalGroups::new(numeric_type(2));
            g.update_batch(&[dec256(vals.iter().copied().map(Some).collect(), 2)], &vec![0; n], None, 1)
                .unwrap();
            assert_eq!(decimal(&g.evaluate(EmitTo::All).unwrap()).unwrap().value(0), want, "{vals:?}");
            // narrow: Decimal128 values and sum
            let t = DataType::Decimal128(38, 2);
            let narrow: Vec<Option<i128>> = vals.iter().map(|&v| narrow_value(v)).collect();
            let a: ArrayRef = Arc::new(Decimal128Array::from(narrow).with_data_type(DataType::Decimal128(18, 2)));
            let mut g = SumDecimalGroups::new(t);
            g.update_batch(&[a], &vec![0; n], None, 1).unwrap();
            let out = g.evaluate(EmitTo::All).unwrap();
            assert_eq!(Some(out.as_primitive::<Decimal128Type>().value(0)), narrow_value(want), "{vals:?}");
            let mut a = SumDecimal {
                data_type: numeric_type(2),
                sum: i256::ZERO,
                values: 0,
                nans: 0,
                pinfs: 0,
                ninfs: 0,
            };
            a.update_batch(&[dec256(vals.iter().copied().map(Some).collect(), 2)]).unwrap();
            assert_eq!(a.evaluate().unwrap(), ScalarValue::Decimal256(Some(want), 76, 2), "{vals:?}");
        }
        // a frame: +Inf in it, then out of it
        let mut a = SumDecimal {
            data_type: numeric_type(2),
            sum: i256::ZERO,
            values: 0,
            nans: 0,
            pinfs: 0,
            ninfs: 0,
        };
        a.update_batch(&[dec256(vec![Some(NUMERIC_PINF), Some(i(3))], 2)]).unwrap();
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Decimal256(Some(NUMERIC_PINF), 76, 2));
        a.retract_batch(&[dec256(vec![Some(NUMERIC_PINF)], 2)]).unwrap();
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Decimal256(Some(i(3)), 76, 2));
    }

    #[test]
    fn sum_decimal_is_numeric_sum() {
        let i = |v: i128| i256::from_i128(v);
        let t = numeric_type(2);
        // groups: 0 sums 1.50 + 2.25, 1 sees a NaN, 2 only NULLs, 3 a filtered-out value
        let x = dec256(
            vec![
                Some(i(150)),
                Some(i(-7)),
                Some(NUMERIC_NAN),
                None,
                Some(i(225)),
                Some(i(9)),
            ],
            2,
        );
        let groups = [0, 1, 1, 2, 0, 3];
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            None,
        ]);
        let mut g = SumDecimalGroups::new(t.clone());
        g.update_batch(&[x.clone()], &groups, Some(&filter), 4)
            .unwrap();
        let out = g.evaluate(EmitTo::All).unwrap();
        let out = decimal(&out).unwrap();
        assert_eq!(out.data_type(), &t);
        assert_eq!((out.value(0), out.value(1)), (i(375), NUMERIC_NAN));
        assert!(out.is_null(2) && out.is_null(3));
        // states merge as values; a NaN state keeps the group NaN; First(n) emits the first n
        g.update_batch(&[x.clone()], &groups, None, 4).unwrap();
        let st = g.state(EmitTo::All).unwrap();
        let mut m = SumDecimalGroups::new(t.clone());
        m.merge_batch(&st, &[1, 1, 0, 0], 2).unwrap();
        m.merge_batch(&[dec256(vec![Some(i(5))], 2)], &[0], 2)
            .unwrap();
        let out = m.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!((out.len(), decimal(&out).unwrap().value(0)), (1, i(9 + 5)));
        let out = m.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            (out.len(), decimal(&out).unwrap().value(0)),
            (1, NUMERIC_NAN)
        );
        // a frame: values in and out, NaN only while one is in it
        let mut a = SumDecimal {
            data_type: t,
            sum: i256::ZERO,
            values: 0,
            nans: 0,
            pinfs: 0,
            ninfs: 0,
        };
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Decimal256(None, 76, 2));
        a.update_batch(&[dec256(vec![Some(i(1)), Some(NUMERIC_NAN), Some(i(2))], 2)])
            .unwrap();
        assert_eq!(
            a.evaluate().unwrap(),
            ScalarValue::Decimal256(Some(NUMERIC_NAN), 76, 2)
        );
        a.retract_batch(&[dec256(vec![Some(i(1)), Some(NUMERIC_NAN)], 2)])
            .unwrap();
        assert_eq!(
            a.evaluate().unwrap(),
            ScalarValue::Decimal256(Some(i(2)), 76, 2)
        );
        a.retract_batch(&[dec256(vec![Some(i(2)), None], 2)])
            .unwrap();
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Decimal256(None, 76, 2));
    }

    #[test]
    fn round_is_numeric_round() {
        let i = |v: i128| i256::from_i128(v);
        // 12.345 -> 12.35, -12.345 -> -12.35, 12.344 -> 12.34, 1.5 -> 2, 7 -> 7.00
        assert_eq!(pg_round(i(12345), 3, 2), Some(i(1235)));
        assert_eq!(pg_round(i(-12345), 3, 2), Some(i(-1235)));
        assert_eq!(pg_round(i(12344), 3, 2), Some(i(1234)));
        assert_eq!(pg_round(i(15), 1, 0), Some(i(2)));
        assert_eq!(pg_round(i(-25), 1, 0), Some(i(-3)));
        assert_eq!(pg_round(i(7), 0, 2), Some(i(700)));
        assert_eq!(pg_round(NUMERIC_NAN, 3, 1), Some(NUMERIC_NAN));
    }

    #[test]
    fn exact_quotients_are_pg_divs_without_a_bound() {
        let i = |v: i128| i256::from_i128(v);
        // where pg_div fits, the same quotient at the same scale
        for (x, sx, dx, y, sy, dy) in [
            (150, 2, 2, 7, 0, 0),
            (-1, 0, 0, 3, 0, 0),
            (123456789, 4, 4, -98765, 2, 2),
            (5, 3, 3, 2, 0, 0),
            (-25, 1, 1, -10, 1, 1),
        ] {
            let r = pg_div_rscale(i(x), sx, dx, i(y), sy, dy);
            let q = pg_div(i(x), sx, i(y), sy, r, r as i8).unwrap();
            assert_eq!(
                Exact::quotient(i(x), sx, dx, i(y), sy, dy).unwrap(),
                Exact::value(q, r as i8),
                "{x}e-{sx} / {y}e-{sy}"
            );
        }
        // QM1: a dividend scaled beyond Decimal256 on the way to a
        // quotient within it: a stddev's 3037000499.97... at scale 44 over
        // an avg's 1.08e-19 (1 / (2^63 - 1)) at scale 36, rounded at 44
        let x = pow10(53).wrapping_sub(i(7));
        let y = i(108) * pow10(15);
        assert!(x.checked_mul(pow10(36)).is_none());
        let r = pg_div_rscale(x, 44, 44, y, 36, 36);
        assert_eq!(r, 44);
        let q = pg_div(x, 44, y, 36, r, 44).unwrap();
        assert_eq!(Exact::quotient(x, 44, 44, y, 36, 36).unwrap(), Exact::value(q, 44));
        let q2 = pg_div(i(-1), 44, y, 36, r, 44).unwrap();
        assert_eq!(Exact::quotient(i(-1), 44, 44, y, 36, 36).unwrap(), Exact::value(q2, 44));
        // still None where the quotient itself is beyond it
        assert_eq!(pg_div(pow10(75), 0, i(1), 40, 40, 40), None);
        // a quotient beyond 76 digits: 10^60 / 10^-40 at scale 40 (the
        // larger display scale), and one more digit than its neighbour
        let big = pow10(60);
        let tiny = i(1);
        let q = Exact::quotient(big, 0, 0, tiny, 40, 40).unwrap();
        let below = Exact::value(i256::MAX.wrapping_sub(i(1)), 0);
        assert_eq!(q.compare(&below), Ordering::Greater);
        assert_eq!(Exact::quotient(i(1), 0, 0, i256::ZERO, 0, 0), None);
        // NaN above every number and equal to NaN; -0 is 0
        let nan = Exact::value(NUMERIC_NAN, 2);
        assert_eq!(nan.compare(&q), Ordering::Greater);
        assert_eq!(nan.compare(&Exact::quotient(NUMERIC_NAN, 0, 0, i(0), 0, 0).unwrap()), Ordering::Equal);
        let neg_zero = Exact::Num { neg: true, mag: BigUint::from(0u32), scale: 3 };
        assert_eq!(neg_zero.compare(&Exact::value(i256::ZERO, 0)), Ordering::Equal);
        // values of different scales: 1.50 = 1.5, -2 < -1.99
        assert_eq!(Exact::value(i(150), 2).compare(&Exact::value(i(15), 1)), Ordering::Equal);
        assert_eq!(Exact::value(i(-2), 0).compare(&Exact::value(i(-199), 2)), Ordering::Less);
    }

    #[test]
    fn nan_survives_rescaling_and_arithmetic() {
        let x: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(150)),
                Some(NUMERIC_NAN),
                None,
                Some(i256::from_i128(-1)),
            ])
            .with_data_type(numeric_type(2)),
        );
        let r = call(NumericFn::Rescale { by: 2, to: 4 }, vec![x.clone()]);
        let r = decimal(&r).unwrap();
        assert_eq!(r.data_type(), &numeric_type(4));
        assert_eq!(
            (r.value(0), r.value(1), r.is_null(2), r.value(3)),
            (
                i256::from_i128(15000),
                NUMERIC_NAN,
                true,
                i256::from_i128(-100)
            )
        );
        // 1.50 * -0.01 = -0.0150 (scales add up); NaN in, NaN out
        let y: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(-1)); 4])
                .with_data_type(numeric_type(2)),
        );
        let x2: ArrayRef = Arc::new(
            Decimal256Array::from(vec![
                Some(i256::from_i128(150)),
                Some(NUMERIC_NAN),
                None,
                Some(i256::from_i128(-1)),
            ])
            .with_data_type(numeric_type(2)),
        );
        let p = call(
            NumericFn::Arith {
                op: NumOp::Mul,
                to: 4,
            },
            vec![x2, y],
        );
        let p = decimal(&p).unwrap();
        assert_eq!(
            (p.value(0), p.value(1), p.is_null(2), p.value(3)),
            (i256::from_i128(-150), NUMERIC_NAN, true, i256::from_i128(1))
        );
    }

    #[test]
    fn stddev_as_postgresql() {
        // PostgreSQL's values (scale-free integers at their display scales)
        let v = |x: i128| i256::from_i128(x);
        let at = |n, sx, sxx, sample, variance, to| {
            pg_stddev(n, v(sx), v(sxx), sample, variance, to).unwrap()
        };
        // 1, 2, 3, 4: 1.2909944487358056, 1.6666666666666667,
        // 1.1180339887498948, 1.2500000000000000
        assert_eq!(at(4, 10, 30, true, false, 16), Some((v(12909944487358056), 16)));
        assert_eq!(at(4, 10, 30, true, true, 16), Some((v(16666666666666667), 16)));
        assert_eq!(at(4, 10, 30, false, false, 16), Some((v(11180339887498948), 16)));
        assert_eq!(at(4, 10, 30, false, true, 16), Some((v(12500000000000000), 16)));
        // 7, 7, 7: 0; 5 alone: NULL for a sample, 0 for a population
        assert_eq!(at(3, 21, 147, true, false, 16), Some((v(0), 0)));
        assert_eq!(at(1, 5, 25, true, false, 16), None);
        assert_eq!(at(1, 5, 25, false, false, 16), Some((v(0), 0)));
        // 0, 1000000: 707106.78118655, 500000000000.00000000
        assert_eq!(at(2, 1_000_000, 1_000_000_000_000, true, false, 8), Some((v(70710678118655), 8)));
        assert_eq!(at(2, 1_000_000, 1_000_000_000_000, true, true, 8), Some((v(50000000000000000000), 8)));
        // 2147483647, -2147483648, 0: 2147483647, 3074457344186602838
        let sxx = 2147483647i128 * 2147483647 + 2147483648i128 * 2147483648;
        assert_eq!(at(3, -1, sxx, true, false, 0), Some((v(2147483647), 0)));
        assert_eq!(at(3, -1, sxx, false, true, 0), Some((v(3074457344186602838), 0)));
    }
}
