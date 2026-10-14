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

//! Strings ordered by a libc collation of a language (CU2): `<`, `<=`,
//! `>`, `>=` and min/max of text under, say, en_US.utf8, as varstr_cmp
//! orders them: strcoll_l, and where it finds two strings equal, their
//! bytes (a deterministic collation).  glibc's strxfrm keys do not order
//! as strcoll does ('04z' and '0_4z'), so every comparison calls strcoll_l.
//! The C side names the locale: the database's LC_COLLATE on the node
//! running the slice, or a collation's collcollate.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CString};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

extern "C" {
    fn strcoll_l(a: *const c_char, b: *const c_char, locale: libc::locale_t) -> c_int;
}

/// A locale opened once per name and kept for the process, as an address
/// (locale_t is read-only once made, so threads share it).
fn locale(name: &str) -> Result<usize> {
    static LOCALES: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    let mut map = LOCALES.get_or_init(Default::default).lock().unwrap();
    if let Some(l) = map.get(name) {
        return Ok(*l);
    }
    let c = CString::new(name).map_err(|_| DataFusionError::Plan(format!("locale {name:?}")))?;
    // SAFETY: a NUL-terminated name and no base locale
    let l = unsafe { libc::newlocale(libc::LC_COLLATE_MASK, c.as_ptr(), std::ptr::null_mut()) };
    if l.is_null() {
        return Err(DataFusionError::Execution(format!("cannot open locale {name:?}")));
    }
    map.insert(name.to_string(), l as usize);
    Ok(l as usize)
}

/// varstr_cmp under libc locale `loc`: strcoll_l, then the bytes.  Text
/// holds no NUL, so the strings are passed NUL-terminated as they are.
pub fn compare(loc: usize, a: &[u8], b: &[u8], buf: &mut Vec<u8>) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    buf.clear();
    buf.extend_from_slice(a);
    buf.push(0);
    let split = buf.len();
    buf.extend_from_slice(b);
    buf.push(0);
    // SAFETY: both halves are NUL-terminated; the locale outlives the process's queries
    let r = unsafe {
        strcoll_l(
            buf.as_ptr() as *const c_char,
            buf[split..].as_ptr() as *const c_char,
            loc as libc::locale_t,
        )
    };
    match r.cmp(&0) {
        Ordering::Equal => a.cmp(b),
        o => o,
    }
}

fn utf8(a: &ArrayRef) -> Result<ArrayRef> {
    if a.data_type() == &DataType::Utf8 {
        Ok(a.clone())
    } else {
        cast(a, &DataType::Utf8).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// `<`, `<=`, `>`, `>=` of two strings under a locale.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCollCmp {
    op: String,
    locale: String,
    signature: Signature,
}

impl PgCollCmp {
    pub fn udf(op: &str, locale: &str) -> Result<ScalarUDF> {
        if !matches!(op, "<" | "<=" | ">" | ">=") {
            return Err(DataFusionError::Plan(format!("collated operator {op}")));
        }
        Ok(ScalarUDF::new_from_impl(PgCollCmp {
            op: op.to_string(),
            locale: locale.to_string(),
            signature: Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        }))
    }

    fn holds(&self, o: Ordering) -> bool {
        match self.op.as_str() {
            "<" => o == Ordering::Less,
            "<=" => o != Ordering::Greater,
            ">" => o == Ordering::Greater,
            _ => o != Ordering::Less,
        }
    }
}

impl ScalarUDFImpl for PgCollCmp {
    fn name(&self) -> &str {
        "pg_collated_cmp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let loc = locale(&self.locale)?;
        let a = utf8(&args.args[0].to_array(n)?)?;
        let b = utf8(&args.args[1].to_array(n)?)?;
        let (a, b) = (a.as_string::<i32>(), b.as_string::<i32>());
        let mut buf = Vec::new();
        let out: BooleanArray = (0..n)
            .map(|r| {
                (!a.is_null(r) && !b.is_null(r)).then(|| {
                    self.holds(compare(loc, a.value(r).as_bytes(), b.value(r).as_bytes(), &mut buf))
                })
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// min or max of strings under a locale; its state is the value so far,
/// which merges as the values do.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCollMinMax {
    max: bool,
    locale: String,
    signature: Signature,
}

impl PgCollMinMax {
    pub fn udaf(max: bool, locale: &str) -> AggregateUDF {
        AggregateUDF::new_from_impl(PgCollMinMax {
            max,
            locale: locale.to_string(),
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        })
    }
}

impl AggregateUDFImpl for PgCollMinMax {
    fn name(&self) -> &str {
        if self.max {
            "pg_collated_max"
        } else {
            "pg_collated_min"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CollMinMax {
            max: self.max,
            loc: locale(&self.locale)?,
            best: None,
            buf: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct CollMinMax {
    max: bool,
    loc: usize,
    best: Option<String>,
    buf: Vec<u8>,
}

impl CollMinMax {
    fn take(&mut self, values: &ArrayRef) -> Result<()> {
        let values = utf8(values)?;
        let values = values.as_string::<i32>();
        for r in 0..values.len() {
            if values.is_null(r) {
                continue;
            }
            let v = values.value(r);
            let better = match &self.best {
                None => true,
                Some(b) => {
                    let o = compare(self.loc, v.as_bytes(), b.as_bytes(), &mut self.buf);
                    if self.max {
                        o == Ordering::Greater
                    } else {
                        o == Ordering::Less
                    }
                }
            };
            if better {
                self.best = Some(v.to_string());
            }
        }
        Ok(())
    }
}

impl Accumulator for CollMinMax {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.take(&values[0])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.take(&states[0])
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(self.best.clone()))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.best.as_ref().map_or(0, String::capacity) + self.buf.capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_as_strcoll_then_bytes() {
        let c = locale("C").unwrap();
        let mut buf = Vec::new();
        assert_eq!(compare(c, b"B", b"a", &mut buf), Ordering::Less);
        assert_eq!(compare(c, b"", b"a", &mut buf), Ordering::Less);
        assert_eq!(compare(c, b"ab", b"ab", &mut buf), Ordering::Equal);
        // where installed, a language orders case and punctuation otherwise
        if let Ok(en) = locale("en_US.utf8") {
            assert_eq!(compare(en, b"a", b"B", &mut buf), Ordering::Less);
            assert_eq!(compare(en, b"04z", b"0_4z", &mut buf), Ordering::Less);
        }
    }
}
