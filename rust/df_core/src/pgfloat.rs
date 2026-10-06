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

//! PostgreSQL's order of float4 and float8 values (float.c): -0 equals 0,
//! every NaN equals every other, and NaN sorts above all numbers.  Arrow
//! compares, hashes and sorts floats in IEEE total order instead, where
//! -0 < 0 and a NaN with its sign bit set is below -Infinity.  On values
//! with -0 made 0 and every NaN made the positive quiet NaN, total order
//! is PostgreSQL's order; `FloatFn::Key` does that to the operands of
//! comparisons, join and grouping keys and sort keys.  Values themselves
//! keep their bits: -0 prints as "-0".  min and max return one of their
//! inputs, so for them `FloatFn::Nan` only makes NaN positive: of -0 and 0,
//! which PostgreSQL considers equal, either is a correct answer.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FloatFn {
    /// -0 as 0, NaN as the positive quiet NaN
    Key,
    /// NaN as the positive quiet NaN
    Nan,
}

impl FloatFn {
    fn f64(self, v: f64) -> f64 {
        if v.is_nan() {
            f64::NAN
        } else if self == FloatFn::Key && v == 0.0 {
            0.0
        } else {
            v
        }
    }

    fn f32(self, v: f32) -> f32 {
        if v.is_nan() {
            f32::NAN
        } else if self == FloatFn::Key && v == 0.0 {
            0.0
        } else {
            v
        }
    }

    /// `e` as this function's result; a constant is converted here.
    pub fn apply(self, e: Expr) -> Expr {
        match e {
            Expr::Literal(ScalarValue::Float64(v), m) => {
                Expr::Literal(ScalarValue::Float64(v.map(|v| self.f64(v))), m)
            }
            Expr::Literal(ScalarValue::Float32(v), m) => {
                Expr::Literal(ScalarValue::Float32(v.map(|v| self.f32(v))), m)
            }
            e => PgFloat::udf(self).call(vec![e]),
        }
    }
}

/// Is `t` a type whose values `FloatFn` converts?
pub fn is_float(t: &DataType) -> bool {
    matches!(t, DataType::Float32 | DataType::Float64)
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgFloat {
    f: FloatFn,
    signature: Signature,
}

impl PgFloat {
    pub fn udf(f: FloatFn) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgFloat {
            f,
            signature: Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
        })
    }
}

impl ScalarUDFImpl for PgFloat {
    fn name(&self) -> &str {
        match self.f {
            FloatFn::Key => "pg_float_key",
            FloatFn::Nan => "pg_float_nan",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let f = self.f;
        let x = args.args[0].to_array(args.number_rows)?;
        let out: ArrayRef = match x.data_type() {
            DataType::Float64 => {
                let a: PrimitiveArray<Float64Type> =
                    x.as_primitive::<Float64Type>().unary(|v| f.f64(v));
                Arc::new(a)
            }
            DataType::Float32 => {
                let a: PrimitiveArray<Float32Type> =
                    x.as_primitive::<Float32Type>().unary(|v| f.f32(v));
                Arc::new(a)
            }
            t => return Err(DataFusionError::Internal(format!("{} of {t}", self.name()))),
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::compute::kernels::cmp;

    fn key(v: Vec<f64>) -> Float64Array {
        let a: Float64Array = Float64Array::from(v).unary(|v| FloatFn::Key.f64(v));
        a
    }

    #[test]
    fn keys_compare_as_postgresql() {
        let neg_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
        let a = key(vec![-0.0, neg_nan, neg_nan, neg_nan]);
        let b = key(vec![0.0, f64::NAN, f64::INFINITY, 0.0]);
        let eq = cmp::eq(&a, &b).unwrap();
        let gt = cmp::gt(&a, &b).unwrap();
        assert_eq!(
            (0..4).map(|i| eq.value(i)).collect::<Vec<_>>(),
            [true, true, false, false]
        );
        assert_eq!(
            (0..4).map(|i| gt.value(i)).collect::<Vec<_>>(),
            [false, false, true, true]
        );
    }

    #[test]
    fn nan_keeps_negative_zero() {
        let a: Float64Array = Float64Array::from(vec![-0.0]).unary(|v| FloatFn::Nan.f64(v));
        assert!(a.value(0).is_sign_negative());
        assert!(!a.is_null(0));
    }
}
