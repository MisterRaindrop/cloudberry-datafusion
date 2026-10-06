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
use datafusion::arrow::datatypes::{i256, DataType, Decimal256Type};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// NaN of a numeric value (i256::MAX, beyond 10^76).
pub const NUMERIC_NAN: i256 = i256::MAX;

/// Digits a numeric value of this representation may have.
pub const NUMERIC_PRECISION: u8 = 76;

pub fn numeric_type(scale: i8) -> DataType {
    DataType::Decimal256(NUMERIC_PRECISION, scale)
}

fn decimal(a: &ArrayRef) -> Result<&Decimal256Array> {
    a.as_primitive_opt::<Decimal256Type>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected a numeric column, got {}", a.data_type())))
}

fn pow10(k: u32) -> i256 {
    i256::from_i128(10).checked_pow(k).expect("10^k within 76 digits")
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
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgNumeric {
    f: NumericFn,
    signature: Signature,
}

impl PgNumeric {
    pub fn udf(f: NumericFn) -> ScalarUDF {
        let n = match f {
            NumericFn::NanIf | NumericFn::Arith { .. } => 2,
            _ => 1,
        };
        ScalarUDF::new_from_impl(PgNumeric { f, signature: Signature::any(n, Volatility::Immutable) })
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
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.f {
            NumericFn::Rescale { to, .. } | NumericFn::Arith { to, .. } => numeric_type(to),
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
                    .try_unary(|v| if v == NUMERIC_NAN { Ok(v) } else { v.checked_mul(k).ok_or_else(beyond) })
                    .map_err(arrow)?;
                Arc::new(a.with_data_type(numeric_type(to)))
            }
            NumericFn::NanToZero => {
                let a: Decimal256Array = xs.unary(|v| if v == NUMERIC_NAN { i256::ZERO } else { v });
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
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn call(f: NumericFn, args: Vec<ArrayRef>) -> ArrayRef {
        let n = args[0].len();
        let arg_fields = args.iter().map(|a| Arc::new(Field::new("a", a.data_type().clone(), true))).collect();
        let udf = PgNumeric { f, signature: Signature::any(args.len(), Volatility::Immutable) };
        let ret = udf.return_type(&args.iter().map(|a| a.data_type().clone()).collect::<Vec<_>>()).unwrap();
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
            Decimal256Array::from(vec![Some(i256::from_i128(150)), Some(NUMERIC_NAN), None, Some(i256::from_i128(-1))])
                .with_data_type(numeric_type(2)),
        );
        let r = call(NumericFn::Rescale { by: 2, to: 4 }, vec![x.clone()]);
        let r = decimal(&r).unwrap();
        assert_eq!(r.data_type(), &numeric_type(4));
        assert_eq!((r.value(0), r.value(1), r.is_null(2), r.value(3)), (i256::from_i128(15000), NUMERIC_NAN, true, i256::from_i128(-100)));
        let z = call(NumericFn::NanToZero, vec![x.clone()]);
        assert_eq!(decimal(&z).unwrap().value(1), i256::ZERO);
        let isnan = call(NumericFn::IsNan, vec![x.clone()]);
        assert!(isnan.as_boolean().value(1) && !isnan.as_boolean().value(0));
        let flag: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(true), None]));
        let m = call(NumericFn::NanIf, vec![x, flag]);
        let m = decimal(&m).unwrap();
        assert_eq!((m.value(0), m.value(1), m.is_null(2), m.value(3)), (NUMERIC_NAN, NUMERIC_NAN, true, i256::from_i128(-1)));
        // 1.50 * -0.01 = -0.0150 (scales add up); NaN in, NaN out
        let y: ArrayRef = Arc::new(Decimal256Array::from(vec![Some(i256::from_i128(-1)); 4]).with_data_type(numeric_type(2)));
        let x2: ArrayRef = Arc::new(
            Decimal256Array::from(vec![Some(i256::from_i128(150)), Some(NUMERIC_NAN), None, Some(i256::from_i128(-1))])
                .with_data_type(numeric_type(2)),
        );
        let p = call(NumericFn::Arith { op: NumOp::Mul, to: 4 }, vec![x2, y]);
        let p = decimal(&p).unwrap();
        assert_eq!((p.value(0), p.value(1), p.is_null(2), p.value(3)), (i256::from_i128(-150), NUMERIC_NAN, true, i256::from_i128(1)));
    }
}
