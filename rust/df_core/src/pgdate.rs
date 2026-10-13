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

//! Fields of a date as PostgreSQL's extract(field from date) gives them
//! (extract_date in date.c), as numeric of scale 0 (DT2).  A date is the
//! integer PostgreSQL stores: days from 2000-01-01, its extremes the
//! infinities.  The year of an infinite date is ±Infinity
//! (`NUMERIC_PINF`, `NUMERIC_NINF`), the other fields NULL.

use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Decimal256Builder};
use datafusion::arrow::datatypes::{i256, DataType, Int32Type};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use crate::pgnum::{numeric_type, NUMERIC_NINF, NUMERIC_PINF};

/// == date2j(2000, 1, 1)
const POSTGRES_EPOCH_JDATE: i32 = 2451545;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DateField {
    Year,
    Quarter,
    Month,
    Day,
}

impl DateField {
    pub fn parse(s: &str) -> Option<DateField> {
        Some(match s {
            "year" => DateField::Year,
            "quarter" => DateField::Quarter,
            "month" => DateField::Month,
            "day" => DateField::Day,
            _ => return None,
        })
    }
}

/// j2date of datetime.c: year (1 BC is 0), month and day of a Julian day.
pub fn j2date(jd: i32) -> (i32, i32, i32) {
    let mut julian = (jd as u32).wrapping_add(32044);
    let mut quad = julian / 146097;
    let extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    let mut y = (julian * 4 / 1461) as i32;
    julian = if y != 0 {
        (julian + 305) % 365
    } else {
        (julian + 306) % 366
    } + 123;
    y += (quad * 4) as i32;
    let quad = julian * 2141 / 65536;
    let day = julian as i32 - (7834 * quad / 256) as i32;
    let month = ((quad + 10) % 12 + 1) as i32;
    (y - 4800, month, day)
}

/// extract(field from d) of a stored date; None for NULL.
pub fn extract(field: DateField, d: i32) -> Option<i256> {
    if d == i32::MIN || d == i32::MAX {
        return match field {
            DateField::Year => Some(if d == i32::MIN {
                NUMERIC_NINF
            } else {
                NUMERIC_PINF
            }),
            _ => None,
        };
    }
    let (year, month, day) = j2date(d + POSTGRES_EPOCH_JDATE);
    let v = match field {
        // there is no year 0, just 1 BC and 1 AD
        DateField::Year => {
            if year > 0 {
                year
            } else {
                year - 1
            }
        }
        DateField::Quarter => (month - 1) / 3 + 1,
        DateField::Month => month,
        DateField::Day => day,
    };
    Some(i256::from_i128(v as i128))
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgExtractDate {
    field: DateField,
    signature: Signature,
}

impl PgExtractDate {
    pub fn udf(field: DateField) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgExtractDate {
            field,
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgExtractDate {
    fn name(&self) -> &str {
        match self.field {
            DateField::Year => "pg_extract_year",
            DateField::Quarter => "pg_extract_quarter",
            DateField::Month => "pg_extract_month",
            DateField::Day => "pg_extract_day",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(numeric_type(0))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let x = args.args[0].to_array(n)?;
        let xs = x.as_primitive_opt::<Int32Type>().ok_or_else(|| {
            DataFusionError::Internal(format!("expected a date column, got {}", x.data_type()))
        })?;
        let mut out = Decimal256Builder::with_capacity(xs.len());
        for i in 0..xs.len() {
            out.append_option(if xs.is_null(i) {
                None
            } else {
                extract(self.field, xs.value(i))
            });
        }
        let out = out.finish().with_data_type(numeric_type(0));
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd(d: i32) -> (i128, i128, i128) {
        let f = |f| extract(f, d).unwrap().as_i128();
        (f(DateField::Year), f(DateField::Month), f(DateField::Day))
    }

    #[test]
    fn fields_of_dates() {
        assert_eq!(ymd(0), (2000, 1, 1));
        assert_eq!(ymd(-1), (1999, 12, 31));
        assert_eq!(ymd(59), (2000, 2, 29));
        // 4714-11-24 BC, the first date: Julian day 0
        assert_eq!(ymd(-POSTGRES_EPOCH_JDATE), (-4714, 11, 24));
        // 0001-12-31 BC and 0001-01-01 AD
        assert_eq!(ymd(-730120), (-1, 12, 31));
        assert_eq!(ymd(-730119), (1, 1, 1));
        // 5874897-12-31, the last finite date
        assert_eq!(ymd(2147483493 - POSTGRES_EPOCH_JDATE), (5874897, 12, 31));
        assert_eq!(extract(DateField::Quarter, 100).unwrap().as_i128(), 2);
        assert_eq!(extract(DateField::Year, i32::MAX), Some(NUMERIC_PINF));
        assert_eq!(extract(DateField::Year, i32::MIN), Some(NUMERIC_NINF));
        assert_eq!(extract(DateField::Month, i32::MAX), None);
    }
}

/// DT3: a date plus or minus a number of days, as date_pli and date_mii
/// (date.c): an infinite date stays as it is; a result outside the dates
/// PostgreSQL holds is 22008 "date out of range".
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgDateAddDays {
    minus: bool,
    signature: Signature,
}

/// IS_VALID_DATE: from Julian day 0 up to JULIAN_MAXYEAR, from 2000-01-01.
fn valid_date(d: i64) -> bool {
    (-2_451_545..2_147_483_494 - 2_451_545).contains(&d)
}

/// date_pli / date_mii of `date` and `days`, or None for "date out of range".
pub fn add_days(date: i32, days: i32, minus: bool) -> Option<i32> {
    if date == i32::MIN || date == i32::MAX {
        return Some(date);
    }
    let r = if minus { date as i64 - days as i64 } else { date as i64 + days as i64 };
    // beyond int32 is date.c's overflow; within it, its range check
    (r >= i32::MIN as i64 && r <= i32::MAX as i64 && valid_date(r)).then_some(r as i32)
}

impl PgDateAddDays {
    pub fn udf(minus: bool) -> ScalarUDF {
        ScalarUDF::new_from_impl(PgDateAddDays {
            minus,
            signature: Signature::exact(vec![DataType::Int32, DataType::Int32], Volatility::Immutable),
        })
    }
}

impl ScalarUDFImpl for PgDateAddDays {
    fn name(&self) -> &str {
        if self.minus {
            "pg_date_mii"
        } else {
            "pg_date_pli"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let dates = args.args[0].to_array(n)?;
        let days = args.args[1].to_array(n)?;
        let (dates, days) = (dates.as_primitive::<Int32Type>(), days.as_primitive::<Int32Type>());
        let mut out = Vec::with_capacity(n);
        for r in 0..n {
            if dates.is_null(r) || days.is_null(r) {
                out.push(None);
                continue;
            }
            match add_days(dates.value(r), days.value(r), self.minus) {
                Some(d) => out.push(Some(d)),
                None => {
                    return Err(DataFusionError::External(Box::new(
                        crate::pgfunc::PgError::new("22008", "date out of range"),
                    )))
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(
            datafusion::arrow::array::Int32Array::from(out),
        )))
    }
}

#[cfg(test)]
mod add_days_tests {
    use super::*;

    #[test]
    fn as_date_pli_and_date_mii() {
        assert_eq!(add_days(0, 31, false), Some(31));
        assert_eq!(add_days(100, 30, true), Some(70));
        assert_eq!(add_days(i32::MAX, 5, false), Some(i32::MAX));
        assert_eq!(add_days(i32::MIN, 5, true), Some(i32::MIN));
        // the last date PostgreSQL holds, 5874897-12-31, and one past it
        let last = 2_147_483_494 - 2_451_545 - 1;
        assert_eq!(add_days(last - 1, 1, false), Some(last));
        assert_eq!(add_days(last, 1, false), None);
        // 4714-11-24 BC is the first; int32 overflow is an error too
        assert_eq!(add_days(-2_451_545, 1, true), None);
        assert_eq!(add_days(2_000_000_000, i32::MAX, false), None);
    }
}
