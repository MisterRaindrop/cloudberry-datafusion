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
//! keep their bits: -0 prints as "-0".
//!
//! min and max return one of their inputs.  DataFusion's grouped min and
//! max of floats start from f64::MAX / f64::MIN, so a group of +Infinity
//! alone came out as 1.797...e308, and they let a NaN replace or be
//! replaced depending on row order.  They therefore run on integers:
//! `FloatFn::OrderKey` maps a float (NaN made positive, -0 kept) to an
//! integer of its width whose order is the floats' total order, and
//! `FloatFn::FromOrderKey` maps the result back.  Of -0 and 0, which
//! PostgreSQL considers equal, min returns -0 and max 0, both correct.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{DataType, Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FloatFn {
    /// -0 as 0, NaN as the positive quiet NaN
    Key,
    /// float4/float8 to an int4/int8 in the same order, NaN as the positive
    /// quiet NaN
    OrderKey,
    /// the inverse of OrderKey
    FromOrderKey,
}

/// `v`, -0 as 0 if `key`, and any NaN as the positive quiet one.
fn canonical64(v: f64, key: bool) -> f64 {
    if v.is_nan() {
        f64::NAN
    } else if key && v == 0.0 {
        0.0
    } else {
        v
    }
}

fn canonical32(v: f32, key: bool) -> f32 {
    if v.is_nan() {
        f32::NAN
    } else if key && v == 0.0 {
        0.0
    } else {
        v
    }
}

/// Bits of a float, negative ones with all but the sign flipped: integers
/// in total order (as f64::total_cmp).  The map is its own inverse.
fn order64(bits: i64) -> i64 {
    bits ^ ((((bits >> 63) as u64) >> 1) as i64)
}

fn order32(bits: i32) -> i32 {
    bits ^ ((((bits >> 31) as u32) >> 1) as i32)
}

impl FloatFn {
    fn f64(self, v: f64) -> f64 {
        canonical64(v, self == FloatFn::Key)
    }

    fn f32(self, v: f32) -> f32 {
        canonical32(v, self == FloatFn::Key)
    }

    /// `e` as this function's result; a constant is converted here.
    pub fn apply(self, e: Expr) -> Expr {
        match e {
            Expr::Literal(ScalarValue::Float64(v), m) if self == FloatFn::Key => {
                Expr::Literal(ScalarValue::Float64(v.map(|v| self.f64(v))), m)
            }
            Expr::Literal(ScalarValue::Float32(v), m) if self == FloatFn::Key => {
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
        let types = match f {
            FloatFn::FromOrderKey => vec![DataType::Int32, DataType::Int64],
            _ => vec![DataType::Float32, DataType::Float64],
        };
        ScalarUDF::new_from_impl(PgFloat {
            f,
            signature: Signature::uniform(1, types, Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgFloat {
    fn name(&self) -> &str {
        match self.f {
            FloatFn::Key => "pg_float_key",
            FloatFn::OrderKey => "pg_float_order_key",
            FloatFn::FromOrderKey => "pg_float_from_order_key",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match (self.f, &arg_types[0]) {
            (FloatFn::OrderKey, DataType::Float32) => DataType::Int32,
            (FloatFn::OrderKey, _) => DataType::Int64,
            (FloatFn::FromOrderKey, DataType::Int32) => DataType::Float32,
            (FloatFn::FromOrderKey, _) => DataType::Float64,
            (_, t) => t.clone(),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let f = self.f;
        let x = args.args[0].to_array(args.number_rows)?;
        let out: ArrayRef = match (f, x.data_type()) {
            (FloatFn::Key, DataType::Float64) => {
                let a: PrimitiveArray<Float64Type> =
                    x.as_primitive::<Float64Type>().unary(|v| f.f64(v));
                Arc::new(a)
            }
            (FloatFn::Key, DataType::Float32) => {
                let a: PrimitiveArray<Float32Type> =
                    x.as_primitive::<Float32Type>().unary(|v| f.f32(v));
                Arc::new(a)
            }
            (FloatFn::OrderKey, DataType::Float64) => {
                let a: PrimitiveArray<Int64Type> = x
                    .as_primitive::<Float64Type>()
                    .unary(|v| order64(canonical64(v, false).to_bits() as i64));
                Arc::new(a)
            }
            (FloatFn::OrderKey, DataType::Float32) => {
                let a: PrimitiveArray<Int32Type> = x
                    .as_primitive::<Float32Type>()
                    .unary(|v| order32(canonical32(v, false).to_bits() as i32));
                Arc::new(a)
            }
            (FloatFn::FromOrderKey, DataType::Int64) => {
                let a: PrimitiveArray<Float64Type> = x
                    .as_primitive::<Int64Type>()
                    .unary(|k| f64::from_bits(order64(k) as u64));
                Arc::new(a)
            }
            (FloatFn::FromOrderKey, DataType::Int32) => {
                let a: PrimitiveArray<Float32Type> = x
                    .as_primitive::<Int32Type>()
                    .unary(|k| f32::from_bits(order32(k) as u32));
                Arc::new(a)
            }
            (_, t) => return Err(DataFusionError::Internal(format!("{} of {t}", self.name()))),
        };
        Ok(ColumnarValue::Array(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Float64Array;
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
    fn order_keys_sort_as_postgresql_and_map_back() {
        let neg_nan = f64::from_bits(f64::NAN.to_bits() | (1 << 63));
        let values = [
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
            neg_nan,
        ];
        let keys: Vec<i64> = values
            .iter()
            .map(|v| order64(canonical64(*v, false).to_bits() as i64))
            .collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]), "{keys:?}");
        for (v, k) in values.iter().zip(&keys) {
            let back = f64::from_bits(order64(*k) as u64);
            assert!(back.to_bits() == v.to_bits() || (v.is_nan() && back.is_nan()));
        }
        let keys32: Vec<i32> = [f32::NEG_INFINITY, -0.0, 0.0, f32::INFINITY, f32::NAN]
            .iter()
            .map(|v| order32(canonical32(*v, false).to_bits() as i32))
            .collect();
        assert!(keys32.windows(2).all(|w| w[0] < w[1]), "{keys32:?}");
        assert!(f32::from_bits(order32(keys32[1]) as u32).is_sign_negative());
    }
}
