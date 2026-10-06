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

//! Casts whose rounding or errors DataFusion's cast does not share (E2),
//! transcribed from int.c, int8.c, float.c, date.c and numeric.c: integers
//! narrowed (range errors), floats to integers (rint, then the range),
//! float8 to float4 (overflow and underflow errors), date to timestamp
//! (infinities, range), timestamp to date (floor), numeric to integers
//! (half away from zero), to float8 (correctly rounded, as float8in) and
//! to numeric(p, s) (apply_typmod).

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal256Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array,
};
use datafusion::arrow::datatypes::{
    i256, DataType, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::pgfunc::PgError;
use crate::pgnum::{numeric_type, NUMERIC_NAN};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CastKind {
    /// int8/int4 to a narrower integer.
    Int,
    /// float4/float8 to an integer.
    FloatInt,
    Float8Float4,
    DateTimestamp,
    TimestampDate,
    /// numeric of scale `from_scale` to an integer.
    NumericInt,
    NumericFloat8,
    /// numeric of scale `from_scale` to numeric(precision, scale).
    NumericTypmod,
}

impl CastKind {
    pub fn parse(s: &str) -> Option<CastKind> {
        Some(match s {
            "int" => CastKind::Int,
            "float_int" => CastKind::FloatInt,
            "float8_float4" => CastKind::Float8Float4,
            "date_timestamp" => CastKind::DateTimestamp,
            "timestamp_date" => CastKind::TimestampDate,
            "numeric_int" => CastKind::NumericInt,
            "numeric_float8" => CastKind::NumericFloat8,
            "numeric_typmod" => CastKind::NumericTypmod,
            _ => return None,
        })
    }
}

fn err(sqlstate: &'static str, message: &str) -> DataFusionError {
    DataFusionError::External(Box::new(PgError::new(sqlstate, message)))
}

/// The out-of-range message of each integer type.
fn range_message(to: &DataType) -> &'static str {
    match to {
        DataType::Int16 => "smallint out of range",
        DataType::Int32 => "integer out of range",
        _ => "bigint out of range",
    }
}

fn int_name(to: &DataType) -> &'static str {
    match to {
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        _ => "bigint",
    }
}

/// [min, max] of each integer type.
fn int_range(to: &DataType) -> (i64, i64) {
    match to {
        DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
        DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    }
}

/// Integers of type `to` from i64 values already checked to fit.
fn int_array(to: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
    match to {
        DataType::Int16 => Arc::new(
            values
                .into_iter()
                .map(|v| v.map(|v| v as i16))
                .collect::<Int16Array>(),
        ),
        DataType::Int32 => Arc::new(
            values
                .into_iter()
                .map(|v| v.map(|v| v as i32))
                .collect::<Int32Array>(),
        ),
        _ => Arc::new(values.into_iter().collect::<Int64Array>()),
    }
}

fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    match a.data_type() {
        DataType::Int16 => a
            .as_primitive::<Int16Type>()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Int32 => a
            .as_primitive::<Int32Type>()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        _ => a.as_primitive::<Int64Type>().iter().collect(),
    }
}

fn floats(a: &ArrayRef) -> Vec<Option<f64>> {
    match a.data_type() {
        DataType::Float32 => a
            .as_primitive::<Float32Type>()
            .iter()
            .map(|v| v.map(f64::from))
            .collect(),
        _ => a.as_primitive::<Float64Type>().iter().collect(),
    }
}

fn pow10(k: u32) -> i256 {
    i256::from_i128(10)
        .checked_pow(k)
        .expect("10^k within 76 digits")
}

/// `v` * 10^-from rounded to scale `to`, half away from zero (round_var).
fn round_to(v: i256, from: i8, to: i8) -> i256 {
    if to >= from {
        return v
            .checked_mul(pow10((to - from) as u32))
            .expect("within 76 digits");
    }
    let k = pow10((from - to) as u32);
    let half = k.checked_div(i256::from_i128(2)).unwrap();
    let a = if v.is_negative() {
        v.checked_neg().unwrap()
    } else {
        v
    };
    let r = a.checked_add(half).unwrap().checked_div(k).unwrap();
    if v.is_negative() {
        r.checked_neg().unwrap()
    } else {
        r
    }
}

/// The decimal string of `v` * 10^-scale, as numeric_out writes it.
fn decimal_string(v: i256, scale: i8) -> String {
    let neg = v.is_negative();
    let digits = if neg { v.checked_neg().unwrap() } else { v }.to_string();
    let scale = scale as usize;
    let digits = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int, frac) = digits.split_at(digits.len() - scale);
    let mut s = String::with_capacity(digits.len() + 2);
    if neg {
        s.push('-');
    }
    s.push_str(int);
    if scale > 0 {
        s.push('.');
        s.push_str(frac);
    }
    s
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCast {
    kind: CastKind,
    from_scale: i8,
    to: DataType,
    precision: u8,
    signature: Signature,
}

impl PgCast {
    pub fn udf(kind: CastKind, from_scale: i8, to: DataType, precision: u8) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgCast {
            kind,
            from_scale,
            to,
            precision,
            signature: Signature::any(1, Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgCast {
    fn name(&self) -> &str {
        "pg_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.to.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let a = args.args[0].to_array(n)?;
        let to = &self.to;
        let out: ArrayRef = match self.kind {
            CastKind::Int => {
                let (lo, hi) = int_range(to);
                let v = ints(&a)
                    .into_iter()
                    .map(|v| match v {
                        Some(x) if x < lo || x > hi => Err(err("22003", range_message(to))),
                        v => Ok(v),
                    })
                    .collect::<Result<Vec<_>>>()?;
                int_array(to, v)
            }
            CastKind::FloatInt => {
                // rint, then [min, -min): the range checks of float.c and int8.c
                let (lo, _) = int_range(to);
                let (lo, hi) = (lo as f64, -(lo as f64));
                let v = floats(&a)
                    .into_iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(x) => {
                            let r = x.round_ties_even();
                            if r.is_nan() || r < lo || r >= hi {
                                Err(err("22003", range_message(to)))
                            } else {
                                Ok(Some(r as i64))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                int_array(to, v)
            }
            CastKind::Float8Float4 => {
                let v = a
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(x) => {
                            let r = x as f32;
                            if r.is_infinite() && !x.is_infinite() {
                                Err(err("22003", "value out of range: overflow"))
                            } else if r == 0.0 && x != 0.0 {
                                Err(err("22003", "value out of range: underflow"))
                            } else {
                                Ok(Some(r))
                            }
                        }
                    })
                    .collect::<Result<Float32Array>>()?;
                Arc::new(v)
            }
            CastKind::DateTimestamp => {
                // days to microseconds; infinities to infinities, finite
                // dates from 294277-01-01 on out of range (date2timestamp)
                const END: i32 = 109_203_528 - 2_451_545;
                let v = a
                    .as_primitive::<Int32Type>()
                    .iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(i32::MIN) => Ok(Some(i64::MIN)),
                        Some(i32::MAX) => Ok(Some(i64::MAX)),
                        Some(d) if d >= END => Err(err("22008", "date out of range for timestamp")),
                        Some(d) => Ok(Some(d as i64 * 86_400_000_000)),
                    })
                    .collect::<Result<Int64Array>>()?;
                Arc::new(v)
            }
            CastKind::TimestampDate => {
                let v: Int32Array = a
                    .as_primitive::<Int64Type>()
                    .iter()
                    .map(|v| {
                        v.map(|t| match t {
                            i64::MIN => i32::MIN,
                            i64::MAX => i32::MAX,
                            t => t.div_euclid(86_400_000_000) as i32,
                        })
                    })
                    .collect();
                Arc::new(v)
            }
            CastKind::NumericInt => {
                let (lo, hi) = int_range(to);
                let d = a.as_primitive::<Decimal256Type>();
                let v = d
                    .iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(x) if x == NUMERIC_NAN => Err(err(
                            "0A000",
                            &format!("cannot convert NaN to {}", int_name(to)),
                        )),
                        Some(x) => {
                            let r = round_to(x, self.from_scale, 0);
                            match r.to_i128() {
                                Some(r) if r >= lo as i128 && r <= hi as i128 => Ok(Some(r as i64)),
                                _ => Err(err("22003", range_message(to))),
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                int_array(to, v)
            }
            CastKind::NumericFloat8 => {
                // numeric_out, then float8in (strtod): correctly rounded
                let d = a.as_primitive::<Decimal256Type>();
                let v: Float64Array = d
                    .iter()
                    .map(|v| {
                        v.map(|x| {
                            if x == NUMERIC_NAN {
                                f64::NAN
                            } else {
                                decimal_string(x, self.from_scale)
                                    .parse::<f64>()
                                    .unwrap_or(f64::NAN)
                            }
                        })
                    })
                    .collect();
                Arc::new(v)
            }
            CastKind::NumericTypmod => {
                // apply_typmod: round to the scale, then fewer than p - s
                // digits before the point
                let scale = match to {
                    DataType::Decimal256(_, s) => *s,
                    _ => {
                        return Err(DataFusionError::Internal(
                            "numeric typmod to a non-numeric type".into(),
                        ))
                    }
                };
                let p = self.precision as i8;
                let limit = pow10(p as u32);
                let d = a.as_primitive::<Decimal256Type>();
                let v = d
                    .iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(x) if x == NUMERIC_NAN => Ok(Some(x)),
                        Some(x) => {
                            let r = round_to(x, self.from_scale, scale);
                            let abs = if r.is_negative() { r.checked_neg().unwrap() } else { r };
                            if abs >= limit {
                                let maxdigits = p - scale;
                                Err(DataFusionError::External(Box::new(
                                    PgError::new("22003", "numeric field overflow").with_detail(format!(
                                        "A field with precision {p}, scale {scale} must round to an absolute value less than {}{}.",
                                        if maxdigits > 0 { "10^" } else { "" },
                                        if maxdigits > 0 { maxdigits } else { 1 }
                                    )),
                                )))
                            } else {
                                Ok(Some(r))
                            }
                        }
                    })
                    .collect::<Result<Decimal256Array>>()?;
                Arc::new(v.with_data_type(numeric_type(scale)))
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounding_agrees_with_postgresql() {
        let v = |x: i128| i256::from_i128(x);
        // 2.5 -> 3, -2.5 -> -3, 2.49 -> 2 (round_var, half away from zero)
        assert_eq!(round_to(v(250), 2, 0), v(3));
        assert_eq!(round_to(v(-250), 2, 0), v(-3));
        assert_eq!(round_to(v(249), 2, 0), v(2));
        assert_eq!(round_to(v(1234), 2, 3), v(12340));
        assert_eq!(decimal_string(v(-5), 2), "-0.05");
        assert_eq!(decimal_string(v(12345), 0), "12345");
        assert_eq!(decimal_string(v(7), 3), "0.007");
        // rint: half to even
        assert_eq!(2.5f64.round_ties_even(), 2.0);
        assert_eq!((-3.5f64).round_ties_even(), -4.0);
    }
}
