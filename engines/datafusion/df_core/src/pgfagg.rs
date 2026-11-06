//! FO1: sum and avg of float4 and float8 as PostgreSQL computes them.
//!
//! PostgreSQL adds float values with float8_pl (float8_accum for avg),
//! which raises "value out of range: overflow" (22003) when two finite
//! values add up to an infinity.  DataFusion's sum and avg return the
//! infinity.  These aggregates add as DataFusion does, in double
//! precision, and raise PostgreSQL's error at the first such addition, in
//! a group's rows and when partial sums are merged alike.
//!
//! float8_accum also keeps a sum of squares for avg, and raises the same
//! error when that sum overflows (values beyond about 1e154); avg here does
//! not keep it, so that case is not raised (README, known differences).

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type, Int64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature,
    Volatility,
};

use crate::pgfunc::PgError;

fn overflow() -> DataFusionError {
    DataFusionError::External(Box::new(PgError::new("22003", "value out of range: overflow")))
}

/// float8_pl: `s + v`, or PostgreSQL's error when finite values make an
/// infinity.
#[inline]
fn add(s: f64, v: f64) -> Result<f64> {
    let r = s + v;
    if r.is_infinite() && !s.is_infinite() && !v.is_infinite() {
        return Err(overflow());
    }
    Ok(r)
}

/// float8_mi, for a window frame dropping `v`.
#[inline]
fn sub(s: f64, v: f64) -> Result<f64> {
    let r = s - v;
    if r.is_infinite() && !s.is_infinite() && !v.is_infinite() {
        return Err(overflow());
    }
    Ok(r)
}

fn f64s(a: &ArrayRef) -> Result<&Float64Array> {
    a.as_primitive_opt::<Float64Type>()
        .ok_or_else(|| DataFusionError::Internal(format!("pg float aggregate of {}", a.data_type())))
}

fn i64s(a: &ArrayRef) -> Result<&Int64Array> {
    a.as_primitive_opt::<Int64Type>()
        .ok_or_else(|| DataFusionError::Internal(format!("pg float aggregate count of {}", a.data_type())))
}

/// Is row `r` of `values` counted: not NULL, and the filter true there?
#[inline]
fn counted(values: &Float64Array, filter: Option<&BooleanArray>, r: usize) -> bool {
    values.is_valid(r) && filter.is_none_or(|f| f.is_valid(r) && f.value(r))
}

/// Which of the two aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FloatAgg {
    /// sum: state the sum (NULL without values)
    Sum,
    /// avg: state the sum and the count of values
    Avg,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgFloatAgg {
    kind: FloatAgg,
    signature: Signature,
}

impl PgFloatAgg {
    pub fn udaf(kind: FloatAgg) -> AggregateUDF {
        AggregateUDF::new_from_impl(PgFloatAgg {
            kind,
            // float4 arguments are added as float8, as DataFusion's sum does
            signature: Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
        })
    }
}

impl AggregateUDFImpl for PgFloatAgg {
    fn name(&self) -> &str {
        match self.kind {
            FloatAgg::Sum => "pg_float_sum",
            FloatAgg::Avg => "pg_float_avg",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let sum = Arc::new(Field::new(format!("{}[sum]", args.name), DataType::Float64, true));
        Ok(match self.kind {
            FloatAgg::Sum => vec![sum],
            FloatAgg::Avg => vec![
                sum,
                Arc::new(Field::new(format!("{}[count]", args.name), DataType::Int64, true)),
            ],
        })
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FloatAcc {
            kind: self.kind,
            sum: 0.0,
            count: 0,
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(FloatGroups {
            kind: self.kind,
            sums: Vec::new(),
            counts: Vec::new(),
        }))
    }
}

/// One group, or a window frame.
#[derive(Debug)]
struct FloatAcc {
    kind: FloatAgg,
    sum: f64,
    count: i64,
}

impl FloatAcc {
    fn result(&self) -> ScalarValue {
        match (self.count, self.kind) {
            (0, _) => ScalarValue::Float64(None),
            (_, FloatAgg::Sum) => ScalarValue::Float64(Some(self.sum)),
            (n, FloatAgg::Avg) => ScalarValue::Float64(Some(self.sum / n as f64)),
        }
    }
}

impl Accumulator for FloatAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let v = f64s(&values[0])?;
        for r in 0..v.len() {
            if v.is_valid(r) {
                self.sum = add(self.sum, v.value(r))?;
                self.count += 1;
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let v = f64s(&values[0])?;
        for r in 0..v.len() {
            if v.is_valid(r) {
                self.sum = sub(self.sum, v.value(r))?;
                self.count -= 1;
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let s = f64s(&states[0])?;
        let n = if self.kind == FloatAgg::Avg {
            Some(i64s(&states[1])?)
        } else {
            None
        };
        for r in 0..s.len() {
            if s.is_valid(r) {
                self.sum = add(self.sum, s.value(r))?;
                self.count += n.map_or(1, |n| n.value(r));
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum = ScalarValue::Float64((self.count > 0).then_some(self.sum));
        Ok(match self.kind {
            FloatAgg::Sum => vec![sum],
            FloatAgg::Avg => vec![sum, ScalarValue::Int64(Some(self.count))],
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.result())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Many groups: their sums and counts of values (0: no value yet, NULL).
#[derive(Debug)]
struct FloatGroups {
    kind: FloatAgg,
    sums: Vec<f64>,
    counts: Vec<i64>,
}

impl FloatGroups {
    fn grow(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0.0);
        self.counts.resize(total_num_groups, 0);
    }
}

impl GroupsAccumulator for FloatGroups {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let v = f64s(&values[0])?;
        for (r, &g) in group_indices.iter().enumerate() {
            if counted(v, opt_filter, r) {
                self.sums[g] = add(self.sums[g], v.value(r))?;
                self.counts[g] += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.grow(total_num_groups);
        let s = f64s(&values[0])?;
        let n = if self.kind == FloatAgg::Avg {
            Some(i64s(&values[1])?)
        } else {
            None
        };
        for (r, &g) in group_indices.iter().enumerate() {
            if s.is_valid(r) {
                self.sums[g] = add(self.sums[g], s.value(r))?;
                self.counts[g] += n.map_or(1, |n| n.value(r));
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let nulls = NullBuffer::from(counts.iter().map(|&n| n > 0).collect::<Vec<_>>());
        let values: Vec<f64> = match self.kind {
            FloatAgg::Sum => sums,
            FloatAgg::Avg => sums
                .iter()
                .zip(&counts)
                .map(|(&s, &n)| if n > 0 { s / n as f64 } else { 0.0 })
                .collect(),
        };
        Ok(Arc::new(Float64Array::new(values.into(), Some(nulls))))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let nulls = NullBuffer::from(counts.iter().map(|&n| n > 0).collect::<Vec<_>>());
        let sum: ArrayRef = Arc::new(Float64Array::new(sums.into(), Some(nulls)));
        Ok(match self.kind {
            FloatAgg::Sum => vec![sum],
            FloatAgg::Avg => vec![sum, Arc::new(Int64Array::from(counts))],
        })
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let v = f64s(&values[0])?;
        let keep: Vec<bool> = (0..v.len()).map(|r| counted(v, opt_filter, r)).collect();
        let counts: Vec<i64> = keep.iter().map(|&k| k as i64).collect();
        let sum: ArrayRef = Arc::new(Float64Array::new(
            v.values().clone(),
            Some(NullBuffer::from(keep)),
        ));
        Ok(match self.kind {
            FloatAgg::Sum => vec![sum],
            FloatAgg::Avg => vec![sum, Arc::new(Int64Array::from(counts))],
        })
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<f64>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgfunc::to_pg_error;

    fn arr(v: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
    }

    #[test]
    fn sum_overflow_is_an_error() {
        let mut g = FloatGroups { kind: FloatAgg::Sum, sums: vec![], counts: vec![] };
        let e = g
            .update_batch(&[arr(vec![Some(1e308), Some(1e308)])], &[0, 0], None, 1)
            .unwrap_err();
        let pg = to_pg_error(&e);
        assert_eq!((pg.sqlstate, pg.message.as_str()), ("22003", "value out of range: overflow"));
    }

    #[test]
    fn infinite_inputs_are_not_an_overflow() {
        let mut g = FloatGroups { kind: FloatAgg::Sum, sums: vec![], counts: vec![] };
        g.update_batch(&[arr(vec![Some(f64::INFINITY), Some(1e308), None])], &[0, 0, 1], None, 2)
            .unwrap();
        let r = g.evaluate(EmitTo::All).unwrap();
        let r = r.as_primitive::<Float64Type>();
        assert!(r.value(0).is_infinite());
        assert!(r.is_null(1));
    }

    #[test]
    fn merging_partial_sums_checks_too() {
        let mut g = FloatGroups { kind: FloatAgg::Avg, sums: vec![], counts: vec![] };
        let counts: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        assert!(g
            .merge_batch(&[arr(vec![Some(1e308), Some(1e308)]), counts], &[0, 0], 1)
            .is_err());
    }

    #[test]
    fn avg_and_negative_zero() {
        let mut g = FloatGroups { kind: FloatAgg::Avg, sums: vec![], counts: vec![] };
        g.update_batch(&[arr(vec![Some(1.0), Some(2.0), None])], &[0, 0, 0], None, 1)
            .unwrap();
        let r = g.evaluate(EmitTo::All).unwrap();
        assert_eq!(r.as_primitive::<Float64Type>().value(0), 1.5);
        let mut a = FloatAcc { kind: FloatAgg::Sum, sum: 0.0, count: 0 };
        a.update_batch(&[arr(vec![Some(-0.0)])]).unwrap();
        // 0.0 + -0.0 is 0.0, as DataFusion's sum gives (README)
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Float64(Some(0.0)));
    }
}
