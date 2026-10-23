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
//! (df_numeric.c): PostgreSQL sorts NaN above all numbers and NaN equals
//! NaN, so comparisons, min/max, grouping and joins work on the integers.
//! What does not: rescaling and arithmetic (NaN in, NaN out, here) and
//! sums, which a NaN makes NaN (`sum_decimal` in query.rs adds the others
//! and marks the group with these functions).

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal256Array, PrimitiveArray,
};
use datafusion::arrow::datatypes::{i256, DataType, Decimal256Type, Int32Type, Int64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
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

fn decimal(a: &ArrayRef) -> Result<&Decimal256Array> {
    a.as_primitive_opt::<Decimal256Type>().ok_or_else(|| {
        DataFusionError::Internal(format!("expected a numeric column, got {}", a.data_type()))
    })
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
/// Decimal256, which the planner hook's bounds rule out.
pub fn pg_div(x: i256, sx: i8, y: i256, sy: i8, rscale: i32, to: i8) -> Option<i256> {
    if rscale > to as i32 || y == i256::ZERO {
        return None;
    }
    // |x| / |y| * 10^(rscale - sx + sy), exactly, then rounded
    let e = rscale - sx as i32 + sy as i32;
    let mut num = x.checked_abs()?;
    let mut den = y.checked_abs()?;
    if e >= 0 {
        num = num.checked_mul(pow10(e as u32))?;
    } else {
        den = den.checked_mul(pow10((-e) as u32))?;
    }
    let mut q = num / den;
    if (num % den).checked_mul(i256::from_i128(2))? >= den {
        q = q.checked_add(i256::ONE)?;
    }
    let q = q.checked_mul(pow10((to as i32 - rscale) as u32))?;
    Some(if (x < i256::ZERO) != (y < i256::ZERO) {
        q.wrapping_neg()
    } else {
        q
    })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumOp {
    Add,
    Sub,
    Mul,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumericFn {
    /// x * 10^by, to scale `to`; NaN stays NaN.
    Rescale { by: i8, to: i8 },
    /// x, with NaN as 0.
    NanToZero,
    /// abs(x), as numeric_abs; NaN stays NaN.
    Abs,
    /// RD1: round(x, to), as numeric_round: x of scale `from`, rounded half
    /// away from zero at scale `to` (not below 0); NaN stays NaN.
    Round { from: i8, to: i8 },
    /// x is NaN.
    IsNan,
    /// NaN where the second argument is true, else x.
    NanIf,
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
            NumericFn::NanIf
            | NumericFn::Arith { .. }
            | NumericFn::Avg { .. }
            | NumericFn::AvgScale { .. } => 2,
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
            NumericFn::Rescale { .. } => "pg_numeric_rescale",
            NumericFn::NanToZero => "pg_numeric_nan_to_zero",
            NumericFn::Abs => "pg_numeric_abs",
            NumericFn::Round { .. } => "pg_numeric_round",
            NumericFn::IsNan => "pg_numeric_is_nan",
            NumericFn::NanIf => "pg_numeric_nan_if",
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
            NumericFn::Rescale { to, .. }
            | NumericFn::Round { to, .. }
            | NumericFn::Arith { to, .. }
            | NumericFn::Avg { to, .. }
            | NumericFn::Div { to, .. }
            | NumericFn::Stddev { to, .. } => numeric_type(to),
            NumericFn::IsNan => DataType::Boolean,
            NumericFn::DivScale { .. }
            | NumericFn::AvgScale { .. }
            | NumericFn::StddevScale { .. } => DataType::Int32,
            _ => arg_types[0].clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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
        let x = args.args[0].to_array(n)?;
        let xs = decimal(&x)?;
        let out: ArrayRef = match self.f {
            NumericFn::Stddev { .. } | NumericFn::StddevScale { .. } => {
                return Err(DataFusionError::Internal("stddev computed above".into()))
            }
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
            NumericFn::NanToZero => {
                let a: Decimal256Array =
                    xs.unary(|v| if v == NUMERIC_NAN { i256::ZERO } else { v });
                Arc::new(a.with_data_type(x.data_type().clone()))
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
            NumericFn::IsNan => Arc::new(BooleanArray::from_unary(xs, |v| v == NUMERIC_NAN)),
            NumericFn::NanIf => {
                let flag = args.args[1].to_array(n)?;
                let flag = flag.as_boolean();
                let a: Decimal256Array = (0..n)
                    .map(|r| {
                        if xs.is_null(r) {
                            None
                        } else if !flag.is_null(r) && flag.value(r) {
                            Some(NUMERIC_NAN)
                        } else {
                            Some(xs.value(r))
                        }
                    })
                    .collect();
                Arc::new(a.with_data_type(x.data_type().clone()))
            }
            NumericFn::Arith { op, to } => {
                let y = args.args[1].to_array(n)?;
                let ys = decimal(&y)?;
                let mut values = Vec::with_capacity(n);
                for r in 0..n {
                    if xs.is_null(r) || ys.is_null(r) {
                        values.push(None);
                        continue;
                    }
                    let (a, b) = (xs.value(r), ys.value(r));
                    values.push(Some(if a == NUMERIC_NAN || b == NUMERIC_NAN {
                        NUMERIC_NAN
                    } else {
                        match op {
                            NumOp::Add => a.checked_add(b),
                            NumOp::Sub => a.checked_sub(b),
                            NumOp::Mul => a.checked_mul(b),
                        }
                        .ok_or_else(beyond)
                        .map_err(arrow)?
                    }));
                }
                Arc::new(Decimal256Array::from(values).with_data_type(numeric_type(to)))
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
/// bytes after its varlena header, at `scale`: as df_numeric_value_wide
/// reads it on the C side, for the direct PAX reader (C2).  NaN and the
/// infinities become their sentinels; a value with more fractional digits
/// than `scale`, or more than 76 digits at it, is an error.
pub fn from_pg_numeric(bytes: &[u8], scale: i8) -> std::result::Result<i256, String> {
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
    fn nan_survives_rescaling_and_marks_sums() {
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
        let z = call(NumericFn::NanToZero, vec![x.clone()]);
        assert_eq!(decimal(&z).unwrap().value(1), i256::ZERO);
        let isnan = call(NumericFn::IsNan, vec![x.clone()]);
        assert!(isnan.as_boolean().value(1) && !isnan.as_boolean().value(0));
        let flag: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(true),
            None,
        ]));
        let m = call(NumericFn::NanIf, vec![x, flag]);
        let m = decimal(&m).unwrap();
        assert_eq!(
            (m.value(0), m.value(1), m.is_null(2), m.value(3)),
            (NUMERIC_NAN, NUMERIC_NAN, true, i256::from_i128(-1))
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
