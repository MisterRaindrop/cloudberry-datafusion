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

//! PostgreSQL semantics that DataFusion does not provide by itself.
//!
//! Arithmetic on integer and floating-point values must fail exactly where
//! PostgreSQL fails, with the same SQLSTATE and message, or a query would
//! return a wrapped-around number where PostgreSQL reports an error.  The
//! operators here follow int.c, int8.c and float.h of PostgreSQL 16:
//!
//! - integer + - * / overflow: 22003 "integer out of range" (smallint and
//!   bigint for int2 and int8); division or modulo by zero: 22012;
//!   INT_MIN % -1 is 0, not an error;
//! - float overflow to infinity from finite inputs: 22003 "value out of
//!   range: overflow"; a zero product or quotient of non-zero inputs:
//!   "value out of range: underflow"; x / 0 is 22012 unless x is NaN.
//!
//! The caller casts both operands to the operator's result type first, as
//! PostgreSQL's cross-type operators (e.g. int48pl) compute in that type.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// An error PostgreSQL would raise, with its SQLSTATE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgError {
    pub sqlstate: &'static str,
    pub message: String,
}

impl PgError {
    pub fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        PgError { sqlstate, message: message.into() }
    }
    pub fn internal(message: impl Into<String>) -> Self {
        PgError::new("XX000", message)
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PgError {}

const DIVISION_BY_ZERO: &str = "22012";
const OUT_OF_RANGE: &str = "22003";

fn division_by_zero() -> ArrowError {
    ArrowError::ExternalError(Box::new(PgError::new(DIVISION_BY_ZERO, "division by zero")))
}

fn out_of_range(message: &str) -> ArrowError {
    ArrowError::ExternalError(Box::new(PgError::new(OUT_OF_RANGE, message)))
}

/// Find the PostgreSQL error inside a DataFusion error, or describe it as an
/// internal error.
pub fn to_pg_error(e: &DataFusionError) -> PgError {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(pg) = err.downcast_ref::<PgError>() {
            return pg.clone();
        }
        if let Some(ArrowError::DivideByZero) = err.downcast_ref::<ArrowError>() {
            return PgError::new(DIVISION_BY_ZERO, "division by zero");
        }
        cur = err.source();
    }
    PgError::internal(format!("datafusion error: {e}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    pub fn from_pg(name: &str) -> Option<ArithOp> {
        Some(match name {
            "+" => ArithOp::Add,
            "-" => ArithOp::Sub,
            "*" => ArithOp::Mul,
            "/" => ArithOp::Div,
            "%" => ArithOp::Rem,
            _ => return None,
        })
    }
}

macro_rules! int_op {
    ($a:expr, $b:expr, $op:expr, $t:ty, $range:expr) => {{
        let a = $a.as_primitive::<$t>();
        let b = $b.as_primitive::<$t>();
        let r: PrimitiveArray<$t> = match $op {
            ArithOp::Add => try_binary(a, b, |x, y| x.checked_add(y).ok_or_else(|| out_of_range($range))),
            ArithOp::Sub => try_binary(a, b, |x, y| x.checked_sub(y).ok_or_else(|| out_of_range($range))),
            ArithOp::Mul => try_binary(a, b, |x, y| x.checked_mul(y).ok_or_else(|| out_of_range($range))),
            ArithOp::Div => try_binary(a, b, |x, y| {
                if y == 0 {
                    Err(division_by_zero())
                } else {
                    x.checked_div(y).ok_or_else(|| out_of_range($range))
                }
            }),
            ArithOp::Rem => try_binary(a, b, |x, y| {
                if y == 0 {
                    Err(division_by_zero())
                } else if y == -1 {
                    Ok(0)
                } else {
                    Ok(x % y)
                }
            }),
        }?;
        Arc::new(r) as ArrayRef
    }};
}

fn float_op<T>(a: &ArrayRef, b: &ArrayRef, op: ArithOp) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType,
    T::Native: num_like::Float,
{
    use num_like::Float;
    let a = a.as_primitive::<T>();
    let b = b.as_primitive::<T>();
    let r: PrimitiveArray<T> = try_binary(a, b, |x: T::Native, y: T::Native| {
        let overflow = || out_of_range("value out of range: overflow");
        let underflow = || out_of_range("value out of range: underflow");
        match op {
            ArithOp::Add | ArithOp::Sub => {
                let r = if op == ArithOp::Add { x.add(y) } else { x.sub(y) };
                if r.is_inf() && !x.is_inf() && !y.is_inf() {
                    return Err(overflow());
                }
                Ok(r)
            }
            ArithOp::Mul => {
                let r = x.mul(y);
                if r.is_inf() && !x.is_inf() && !y.is_inf() {
                    return Err(overflow());
                }
                if r.is_zero() && !x.is_zero() && !y.is_zero() {
                    return Err(underflow());
                }
                Ok(r)
            }
            ArithOp::Div => {
                if y.is_zero() && !x.is_nan() {
                    return Err(division_by_zero());
                }
                let r = x.div(y);
                if r.is_inf() && !x.is_inf() {
                    return Err(overflow());
                }
                if r.is_zero() && !x.is_zero() && !y.is_inf() {
                    return Err(underflow());
                }
                Ok(r)
            }
            ArithOp::Rem => Err(ArrowError::ComputeError(
                "PostgreSQL has no % operator for floating-point types".into(),
            )),
        }
    })?;
    Ok(Arc::new(r))
}

/// Minimal float abstraction so one function covers float4 and float8.
mod num_like {
    pub trait Float: Copy {
        fn add(self, o: Self) -> Self;
        fn sub(self, o: Self) -> Self;
        fn mul(self, o: Self) -> Self;
        fn div(self, o: Self) -> Self;
        fn is_inf(self) -> bool;
        fn is_nan(self) -> bool;
        fn is_zero(self) -> bool;
    }
    macro_rules! imp {
        ($t:ty) => {
            impl Float for $t {
                fn add(self, o: Self) -> Self { self + o }
                fn sub(self, o: Self) -> Self { self - o }
                fn mul(self, o: Self) -> Self { self * o }
                fn div(self, o: Self) -> Self { self / o }
                fn is_inf(self) -> bool { self.is_infinite() }
                fn is_nan(self) -> bool { <$t>::is_nan(self) }
                fn is_zero(self) -> bool { self == 0.0 }
            }
        };
    }
    imp!(f32);
    imp!(f64);
}

/// Apply `op` to two arrays of the same type with PostgreSQL semantics.
pub fn apply(op: ArithOp, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(match a.data_type() {
        DataType::Int16 => int_op!(a, b, op, Int16Type, "smallint out of range"),
        DataType::Int32 => int_op!(a, b, op, Int32Type, "integer out of range"),
        DataType::Int64 => int_op!(a, b, op, Int64Type, "bigint out of range"),
        DataType::Float32 => float_op::<Float32Type>(a, b, op)?,
        DataType::Float64 => float_op::<Float64Type>(a, b, op)?,
        other => {
            return Err(ArrowError::ComputeError(format!(
                "no PostgreSQL arithmetic for {other}"
            )))
        }
    })
}

/// DataFusion scalar function wrapping `apply`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgArith {
    op: ArithOp,
    signature: Signature,
}

impl PgArith {
    pub fn udf(op: ArithOp) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgArith {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgArith {
    fn name(&self) -> &str {
        match self.op {
            ArithOp::Add => "pg_add",
            ArithOp::Sub => "pg_sub",
            ArithOp::Mul => "pg_mul",
            ArithOp::Div => "pg_div",
            ArithOp::Rem => "pg_mod",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let r = apply(self.op, &arrays[0], &arrays[1]).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(ColumnarValue::Array(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int16Array, Int32Array};

    fn err_of(r: Result<ArrayRef, ArrowError>) -> PgError {
        let e = DataFusionError::ArrowError(Box::new(r.unwrap_err()), None);
        to_pg_error(&e)
    }

    #[test]
    fn integer_overflow_and_division() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![i32::MAX]));
        let one: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert_eq!(err_of(apply(ArithOp::Add, &a, &one)),
                   PgError::new("22003", "integer out of range"));
        let zero: ArrayRef = Arc::new(Int32Array::from(vec![0]));
        assert_eq!(err_of(apply(ArithOp::Div, &a, &zero)),
                   PgError::new("22012", "division by zero"));
        let min: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN]));
        let m1: ArrayRef = Arc::new(Int32Array::from(vec![-1]));
        assert_eq!(err_of(apply(ArithOp::Div, &min, &m1)),
                   PgError::new("22003", "integer out of range"));
        let r = apply(ArithOp::Rem, &min, &m1).unwrap();
        assert_eq!(r.as_primitive::<Int32Type>().value(0), 0);
        let s: ArrayRef = Arc::new(Int16Array::from(vec![i16::MAX]));
        let s1: ArrayRef = Arc::new(Int16Array::from(vec![1]));
        assert_eq!(err_of(apply(ArithOp::Add, &s, &s1)),
                   PgError::new("22003", "smallint out of range"));
    }

    #[test]
    fn integer_division_truncates_and_skips_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(-7), None]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(0)]));
        let r = apply(ArithOp::Div, &a, &b).unwrap();
        let r = r.as_primitive::<Int32Type>();
        assert_eq!(r.value(0), -3);
        assert!(r.is_null(1));
    }

    #[test]
    fn float_rules() {
        let big: ArrayRef = Arc::new(Float64Array::from(vec![f64::MAX]));
        assert_eq!(err_of(apply(ArithOp::Mul, &big, &big)),
                   PgError::new("22003", "value out of range: overflow"));
        let tiny: ArrayRef = Arc::new(Float64Array::from(vec![1e-300]));
        assert_eq!(err_of(apply(ArithOp::Mul, &tiny, &tiny)),
                   PgError::new("22003", "value out of range: underflow"));
        let zero: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let one: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        assert_eq!(err_of(apply(ArithOp::Div, &one, &zero)),
                   PgError::new("22012", "division by zero"));
        let nan: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        assert!(apply(ArithOp::Div, &nan, &zero).unwrap().as_primitive::<Float64Type>().value(0).is_nan());
        let inf: ArrayRef = Arc::new(Float64Array::from(vec![f64::INFINITY]));
        assert!(apply(ArithOp::Add, &inf, &one).is_ok());
    }
}
