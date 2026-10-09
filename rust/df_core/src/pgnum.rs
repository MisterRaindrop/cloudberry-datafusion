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

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal256Array};
use datafusion::arrow::datatypes::{i256, DataType, Decimal256Type, Int64Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

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
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgNumeric {
    f: NumericFn,
    signature: Signature,
}

impl PgNumeric {
    pub fn udf(f: NumericFn) -> ScalarUDF {
        let n = match f {
            NumericFn::NanIf | NumericFn::Arith { .. } | NumericFn::Avg { .. } => 2,
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
            NumericFn::IsNan => "pg_numeric_is_nan",
            NumericFn::NanIf => "pg_numeric_nan_if",
            NumericFn::Arith { op: NumOp::Add, .. } => "pg_numeric_add",
            NumericFn::Arith { op: NumOp::Sub, .. } => "pg_numeric_sub",
            NumericFn::Arith { op: NumOp::Mul, .. } => "pg_numeric_mul",
            NumericFn::Avg { .. } => "pg_numeric_avg",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.f {
            NumericFn::Rescale { to, .. }
            | NumericFn::Arith { to, .. }
            | NumericFn::Avg { to, .. } => numeric_type(to),
            NumericFn::IsNan => DataType::Boolean,
            _ => arg_types[0].clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let x = args.args[0].to_array(n)?;
        let xs = decimal(&x)?;
        let arrow = |e: ArrowError| DataFusionError::ArrowError(Box::new(e), None);
        let out: ArrayRef = match self.f {
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
        };
        Ok(ColumnarValue::Array(out))
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
}
