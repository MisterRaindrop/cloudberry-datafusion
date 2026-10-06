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

//! numeric(p, s) values as Decimal128(38, s) (N2).  NaN, which such a
//! column may hold, is `NUMERIC_NAN`, above every 38-digit value, as the C
//! side reads it (df_numeric.c): PostgreSQL sorts NaN above all numbers and
//! NaN equals NaN, so comparisons, min/max, grouping and joins work on the
//! integers.  What does not: rescaling (kept NaN here) and sums, which a
//! NaN makes NaN (`sum_decimal` in query.rs adds the others and marks the
//! group with these functions).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Decimal128Array};
use datafusion::arrow::datatypes::{DataType, Decimal128Type};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// NaN of a numeric column (i128::MAX, beyond 10^38).
pub const NUMERIC_NAN: i128 = i128::MAX;

fn decimal(a: &ArrayRef) -> Result<&Decimal128Array> {
    a.as_primitive_opt::<Decimal128Type>()
        .ok_or_else(|| DataFusionError::Internal(format!("expected a numeric column, got {}", a.data_type())))
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
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgNumeric {
    f: NumericFn,
    signature: Signature,
}

impl PgNumeric {
    pub fn udf(f: NumericFn) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgNumeric { f, signature: Signature::any(if f == NumericFn::NanIf { 2 } else { 1 }, Volatility::Immutable) })
    }
}

impl ScalarUDFImpl for PgNumeric {
    fn name(&self) -> &str {
        match self.f {
            NumericFn::Rescale { .. } => "pg_numeric_rescale",
            NumericFn::NanToZero => "pg_numeric_nan_to_zero",
            NumericFn::IsNan => "pg_numeric_is_nan",
            NumericFn::NanIf => "pg_numeric_nan_if",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.f {
            NumericFn::Rescale { to, .. } => DataType::Decimal128(38, to),
            NumericFn::IsNan => DataType::Boolean,
            _ => arg_types[0].clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let x = args.args[0].to_array(n)?;
        let xs = decimal(&x)?;
        let out: ArrayRef = match self.f {
            NumericFn::Rescale { by, to } => {
                let k = 10i128.pow(by as u32);
                // The planner hook keeps both sides within 38 digits.
                let a: Decimal128Array = xs
                    .try_unary(|v| {
                        if v == NUMERIC_NAN {
                            Ok(v)
                        } else {
                            v.checked_mul(k).ok_or_else(|| {
                                datafusion::arrow::error::ArrowError::ComputeError(
                                    "numeric rescaled beyond 38 digits".into(),
                                )
                            })
                        }
                    })
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(a.with_data_type(DataType::Decimal128(38, to)))
            }
            NumericFn::NanToZero => {
                let a: Decimal128Array = xs.unary(|v| if v == NUMERIC_NAN { 0 } else { v });
                Arc::new(a.with_data_type(x.data_type().clone()))
            }
            NumericFn::IsNan => {
                Arc::new(BooleanArray::from_unary(xs, |v| v == NUMERIC_NAN))
            }
            NumericFn::NanIf => {
                let flag = args.args[1].to_array(n)?;
                let flag = flag.as_boolean();
                let a: Decimal128Array = (0..n)
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
            Decimal128Array::from(vec![Some(150), Some(NUMERIC_NAN), None, Some(-1)])
                .with_data_type(DataType::Decimal128(38, 2)),
        );
        let r = call(NumericFn::Rescale { by: 2, to: 4 }, vec![x.clone()]);
        let r = decimal(&r).unwrap();
        assert_eq!(r.data_type(), &DataType::Decimal128(38, 4));
        assert_eq!((r.value(0), r.value(1), r.is_null(2), r.value(3)), (15000, NUMERIC_NAN, true, -100));
        let z = call(NumericFn::NanToZero, vec![x.clone()]);
        assert_eq!(decimal(&z).unwrap().value(1), 0);
        let isnan = call(NumericFn::IsNan, vec![x.clone()]);
        assert!(isnan.as_boolean().value(1) && !isnan.as_boolean().value(0));
        let flag: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(true), None]));
        let m = call(NumericFn::NanIf, vec![x, flag]);
        let m = decimal(&m).unwrap();
        assert_eq!((m.value(0), m.value(1), m.is_null(2), m.value(3)), (NUMERIC_NAN, NUMERIC_NAN, true, -1));
    }
}
