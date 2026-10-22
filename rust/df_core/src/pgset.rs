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

//! SP1: `x = ANY (rows of a subquery)` as PostgreSQL's hashed SubPlan
//! computes it (ExecHashSubPlan): the values of the subquery's rows make a
//! set, collected from an input of their own before the plan runs (the
//! main thread feeds that input first); each x is then looked up in it.
//! TRUE where x is in the set; where it is not, NULL if the set holds a
//! NULL, else FALSE; a NULL x is NULL against a set of rows, FALSE against
//! none.  Values compare as PostgreSQL's = does for the types the planner
//! hook lets through: integers, dates and booleans as integers, floats with
//! -0 as 0 and every NaN alike, strings byte by byte (a deterministic
//! collation), character(n) without its trailing blanks.

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// How the values compare: as they are, as floats, as character(n).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SetKey {
    Plain,
    Float,
    Bpchar,
}

impl SetKey {
    pub fn parse(s: &str) -> Option<SetKey> {
        match s {
            "plain" => Some(SetKey::Plain),
            "float" => Some(SetKey::Float),
            "bpchar" => Some(SetKey::Bpchar),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Val {
    Int(i64),
    Bits(u64),
    Str(String),
}

fn float_bits(f: f64) -> u64 {
    if f.is_nan() {
        f64::NAN.to_bits()
    } else if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    }
}

/// Row `i` of `a` as a set value, None where it is NULL.
fn value(a: &ArrayRef, i: usize, key: SetKey) -> Result<Option<Val>> {
    if a.is_null(i) {
        return Ok(None);
    }
    Ok(Some(match a.data_type() {
        DataType::Int16 => Val::Int(a.as_primitive::<Int16Type>().value(i) as i64),
        DataType::Int32 => Val::Int(a.as_primitive::<Int32Type>().value(i) as i64),
        DataType::Int64 => Val::Int(a.as_primitive::<Int64Type>().value(i)),
        DataType::Date32 => Val::Int(a.as_primitive::<Date32Type>().value(i) as i64),
        DataType::Boolean => Val::Int(a.as_boolean().value(i) as i64),
        DataType::Float32 => Val::Bits(float_bits(a.as_primitive::<Float32Type>().value(i) as f64)),
        DataType::Float64 => Val::Bits(float_bits(a.as_primitive::<Float64Type>().value(i))),
        DataType::Utf8 => {
            let s = a.as_string::<i32>().value(i);
            Val::Str(match key {
                SetKey::Bpchar => s.trim_end_matches(' ').to_string(),
                _ => s.to_string(),
            })
        }
        t => {
            return Err(DataFusionError::Internal(format!(
                "a subquery's set of {t} values"
            )))
        }
    }))
}

#[derive(Debug, Default)]
struct SetData {
    vals: HashSet<Val>,
    has_null: bool,
    rows: usize,
}

/// The set of one subquery's rows, collected once, read by every partition.
pub struct SetHolder {
    key: Mutex<SetKey>,
    building: Mutex<SetData>,
    data: OnceLock<SetData>,
}

impl fmt::Debug for SetHolder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SetHolder")
    }
}

impl Default for SetHolder {
    fn default() -> Self {
        SetHolder {
            key: Mutex::new(SetKey::Plain),
            building: Mutex::new(SetData::default()),
            data: OnceLock::new(),
        }
    }
}

impl SetHolder {
    pub fn set_key(&self, key: SetKey) {
        if let Ok(mut k) = self.key.lock() {
            *k = key;
        }
    }

    fn key(&self) -> SetKey {
        self.key.lock().map(|k| *k).unwrap_or(SetKey::Plain)
    }

    /// Add the values of the first column of `batch`.
    pub fn add(&self, batch: &RecordBatch) -> Result<()> {
        let key = self.key();
        let col = batch.column(0);
        let mut d = self
            .building
            .lock()
            .map_err(|_| DataFusionError::Internal("set lock poisoned".into()))?;
        for i in 0..batch.num_rows() {
            d.rows += 1;
            match value(col, i, key)? {
                Some(v) => {
                    d.vals.insert(v);
                }
                None => d.has_null = true,
            }
        }
        Ok(())
    }

    /// All rows are in: the set is ready for the plan.
    pub fn finish(&self) {
        let d = self
            .building
            .lock()
            .map(|mut d| std::mem::take(&mut *d))
            .unwrap_or_default();
        let _ = self.data.set(d);
    }

    /// x = ANY (the set) for each row of `x`.
    pub fn lookup(&self, x: &ArrayRef) -> Result<ArrayRef> {
        let d = self.data.get().ok_or_else(|| {
            DataFusionError::Internal("a subquery's set was read before it was complete".into())
        })?;
        let key = self.key();
        let mut out = BooleanBuilder::with_capacity(x.len());
        for i in 0..x.len() {
            out.append_option(match value(x, i, key)? {
                None if d.rows == 0 => Some(false),
                None => None,
                Some(v) if d.vals.contains(&v) => Some(true),
                Some(_) if d.has_null => None,
                Some(_) => Some(false),
            });
        }
        Ok(Arc::new(out.finish()))
    }
}

/// `in_set(x)`: x = ANY (the set of `holder`).
#[derive(Debug)]
pub struct InSet {
    holder: Arc<SetHolder>,
    signature: Signature,
}

impl PartialEq for InSet {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.holder, &other.holder)
    }
}

impl Eq for InSet {}

impl Hash for InSet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (Arc::as_ptr(&self.holder) as usize).hash(state);
    }
}

impl InSet {
    pub fn udf(holder: Arc<SetHolder>) -> ScalarUDF {
        ScalarUDF::new_from_impl(InSet {
            holder,
            // its value depends on the set, not on the argument alone
            signature: Signature::any(1, Volatility::Volatile),
        })
    }
}

impl ScalarUDFImpl for InSet {
    fn name(&self) -> &str {
        "pg_in_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let x = args.args[0].to_array(args.number_rows)?;
        Ok(ColumnarValue::Array(self.holder.lookup(&x)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use datafusion::arrow::datatypes::{Field, Schema};

    fn holder(key: SetKey, col: ArrayRef) -> SetHolder {
        let h = SetHolder::default();
        h.set_key(key);
        let schema = Arc::new(Schema::new(vec![Field::new("c", col.data_type().clone(), true)]));
        h.add(&RecordBatch::try_new(schema, vec![col]).unwrap()).unwrap();
        h.finish();
        h
    }

    fn look(h: &SetHolder, x: ArrayRef) -> Vec<Option<bool>> {
        let r = h.lookup(&x).unwrap();
        let r = r.as_any().downcast_ref::<BooleanArray>().unwrap();
        (0..r.len()).map(|i| (!r.is_null(i)).then(|| r.value(i))).collect()
    }

    #[test]
    fn nulls_as_postgresql_has_them() {
        let x: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(5), None]));
        let plain = holder(SetKey::Plain, Arc::new(Int32Array::from(vec![1, 2, 3])));
        assert_eq!(look(&plain, x.clone()), vec![Some(true), Some(false), None]);
        let with_null = holder(SetKey::Plain, Arc::new(Int32Array::from(vec![Some(1), None])));
        assert_eq!(look(&with_null, x.clone()), vec![Some(true), None, None]);
        let empty = holder(SetKey::Plain, Arc::new(Int32Array::from(Vec::<i32>::new())));
        assert_eq!(look(&empty, x), vec![Some(false), Some(false), Some(false)]);
    }

    #[test]
    fn floats_and_characters_compare_as_postgresql() {
        let f = holder(SetKey::Float, Arc::new(Float64Array::from(vec![0.0, f64::NAN])));
        let x: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, -f64::NAN, 1.0]));
        assert_eq!(look(&f, x), vec![Some(true), Some(true), Some(false)]);
        let b = holder(SetKey::Bpchar, Arc::new(StringArray::from(vec!["ab  ", "c"])));
        let x: ArrayRef = Arc::new(StringArray::from(vec!["ab", "c   ", "ab c"]));
        assert_eq!(look(&b, x), vec![Some(true), Some(true), Some(false)]);
        let t = holder(SetKey::Plain, Arc::new(StringArray::from(vec!["ab  "])));
        let x: ArrayRef = Arc::new(StringArray::from(vec!["ab", "ab  "]));
        assert_eq!(look(&t, x), vec![Some(false), Some(true)]);
    }

    #[test]
    fn unfinished_set_is_an_error() {
        let h = SetHolder::default();
        let x: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert!(h.lookup(&x).is_err());
    }
}
