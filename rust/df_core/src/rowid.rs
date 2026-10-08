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

//! Cloudberry's RowIdExpr: a number unique to each row within the query,
//! which the planner puts on one side of a semi join (JOIN_DEDUP_SEMI)
//! before the rows are multiplied, and groups by afterwards to drop the
//! copies.  PostgreSQL counts up from the segment's dbid shifted left by
//! 48 bits (execExpr.c); only uniqueness matters, so this numbers each
//! batch from a counter shared by the partitions of the slice.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct RowId {
    next: Arc<AtomicI64>,
    signature: Signature,
}

impl PartialEq for RowId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.next, &other.next)
    }
}

impl Eq for RowId {}

impl std::hash::Hash for RowId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.next).hash(state);
    }
}

impl RowId {
    /// Row ids from `base` + 1 on.  Volatile: each row gets its own.
    pub fn udf(base: i64) -> ScalarUDF {
        ScalarUDF::new_from_impl(RowId {
            next: Arc::new(AtomicI64::new(base + 1)),
            signature: Signature::exact(vec![], Volatility::Volatile),
        })
    }
}

impl ScalarUDFImpl for RowId {
    fn name(&self) -> &str {
        "pg_rowid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows as i64;
        let first = self.next.fetch_add(n, Ordering::Relaxed);
        Ok(ColumnarValue::Array(Arc::new(
            Int64Array::from_iter_values(first..first + n),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{Field, Int64Type};
    use datafusion::config::ConfigOptions;

    #[test]
    fn numbers_rows_across_batches() {
        let udf = RowId::udf(7 << 48);
        let call = |n: usize| {
            let args = ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: n,
                return_field: Arc::new(Field::new("r", DataType::Int64, false)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            match udf.invoke_with_args(args).unwrap() {
                ColumnarValue::Array(a) => a.as_primitive::<Int64Type>().values().to_vec(),
                _ => panic!("expected an array"),
            }
        };
        assert_eq!(call(3), vec![(7 << 48) + 1, (7 << 48) + 2, (7 << 48) + 3]);
        assert_eq!(call(2), vec![(7 << 48) + 4, (7 << 48) + 5]);
    }
}
