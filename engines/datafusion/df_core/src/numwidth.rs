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

//! N6: numeric values are Decimal128 of their digits up to 38, Decimal256
//! beyond (pgnum).  Where DataFusion's own operators pair values up (the
//! operands of a comparison, BETWEEN and IN, the branches of a CASE, the
//! arguments of COALESCE and NULLIF, the keys of a join, the columns of a
//! UNION), its type coercion would cast one kind to the other, and an
//! Arrow cast moves NaN and the infinities off their sentinels (or fails
//! on them, beyond a narrow type's digits).  `NumericWidth` runs before
//! it and makes such values one type with `NumericFn::As`: Decimal256 if
//! any is, else Decimal128 of the most digits.  Constants are converted
//! in place, so that a narrow column compared with one stays narrow.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion::logical_expr::expr::{Case, InList, ScalarFunction};
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, LogicalPlan, Operator, Projection, Union,
};
use datafusion::optimizer::AnalyzerRule;

use crate::pgnum::{
    narrow_value, numeric_digits, widen_value, NumericFn, PgNumeric, NUMERIC_PRECISION,
};

#[derive(Debug, Default)]
pub struct NumericWidth;

impl AnalyzerRule for NumericWidth {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "numeric_width"
    }
}

/// The type `values` (an expression and its type each) are made, if they
/// are numeric values of one scale (or NULL constants) of different types.
fn common_type(values: &[(&Expr, &DataType)]) -> Option<DataType> {
    let mut scale = None;
    let mut digits = 0u8;
    let mut wide = false;
    let mut types: Vec<&DataType> = Vec::new();
    for (e, t) in values {
        if **t == DataType::Null {
            continue;
        }
        let (p, s) = numeric_digits(t)?;
        if *scale.get_or_insert(s) != s {
            return None;
        }
        types.push(t);
        match e {
            // a constant goes narrow if it fits
            Expr::Literal(sv, _) => {
                if let Some(v) = literal_digits(sv) {
                    digits = digits.max(v);
                } else {
                    wide = true;
                }
            }
            _ if p >= NUMERIC_PRECISION => wide = true,
            _ => digits = digits.max(p),
        }
    }
    let scale = scale?;
    if types.windows(2).all(|w| w[0] == w[1]) && values.iter().all(|(_, t)| **t != DataType::Null) {
        return None;
    }
    Some(if wide {
        crate::pgnum::numeric_type(scale)
    } else {
        crate::pgnum::numeric_type_of(digits as i64, scale)
    })
}

/// The digits a numeric constant takes narrow, if it can be.
fn literal_digits(sv: &ScalarValue) -> Option<u8> {
    match sv {
        ScalarValue::Decimal128(_, p, _) => Some(*p),
        ScalarValue::Decimal256(None, _, s) => Some((*s).max(1) as u8),
        ScalarValue::Decimal256(Some(v), _, s) => {
            let n = narrow_value(*v)?;
            let d = match n {
                crate::pgnum::NARROW_NAN | crate::pgnum::NARROW_PINF | crate::pgnum::NARROW_NINF => 1,
                n => n.unsigned_abs().checked_ilog10().map_or(1, |d| d as u8 + 1),
            };
            Some(d.max((*s).max(0) as u8)).filter(|d| *d <= 38)
        }
        _ => None,
    }
}

/// `e` of type `from` as type `to` (numeric of the same scale).
fn as_type(e: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to {
        return e;
    }
    let (p, s) = numeric_digits(to).expect("a numeric type");
    if let Expr::Literal(sv, meta) = &e {
        let v = match sv {
            ScalarValue::Decimal128(v, ..) => v.map(widen_value),
            ScalarValue::Decimal256(v, ..) => *v,
            _ => None,
        };
        let sv = match to {
            DataType::Decimal128(..) => {
                ScalarValue::Decimal128(v.map(|v| narrow_value(v).expect("checked")), p, s)
            }
            _ => ScalarValue::Decimal256(v, NUMERIC_PRECISION, s),
        };
        return Expr::Literal(sv, meta.clone());
    }
    if *from == DataType::Null {
        return e;
    }
    PgNumeric::udf(NumericFn::As { digits: p, scale: s }).call(vec![e])
}

/// `exprs` (typed over `schema`) made one type, if they need it.
fn unify(exprs: Vec<Expr>, schema: &DFSchema) -> Result<(Vec<Expr>, bool)> {
    let types: Vec<DataType> = match exprs.iter().map(|e| e.get_type(schema)).collect() {
        Ok(t) => t,
        Err(_) => return Ok((exprs, false)),
    };
    unify_typed(exprs, &types)
}

fn unify_typed(exprs: Vec<Expr>, types: &[DataType]) -> Result<(Vec<Expr>, bool)> {
    let pairs: Vec<(&Expr, &DataType)> = exprs.iter().zip(types).collect();
    let Some(to) = common_type(&pairs) else {
        return Ok((exprs, false));
    };
    Ok((
        exprs
            .into_iter()
            .zip(types)
            .map(|(e, t)| as_type(e, t, &to))
            .collect(),
        true,
    ))
}

fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    )
}

/// Expression `e` over `schema` with the values its operators pair up made
/// one type.
fn rewrite_expr(e: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    e.transform_up(|e| {
        Ok(match e {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if is_comparison(op) => {
                let (v, changed) = unify(vec![*left, *right], schema)?;
                let mut v = v.into_iter();
                let (l, r) = (v.next().unwrap(), v.next().unwrap());
                Transformed::new(
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r))),
                    changed,
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            }
            Expr::Between(Between { expr, negated, low, high }) => {
                let (v, changed) = unify(vec![*expr, *low, *high], schema)?;
                let mut v = v.into_iter();
                let (x, l, h) = (v.next().unwrap(), v.next().unwrap(), v.next().unwrap());
                Transformed::new(
                    Expr::Between(Between::new(Box::new(x), negated, Box::new(l), Box::new(h))),
                    changed,
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            }
            Expr::InList(InList { expr, list, negated }) => {
                let mut all = vec![*expr];
                all.extend(list);
                let (mut v, changed) = unify(all, schema)?;
                let x = v.remove(0);
                Transformed::new(
                    Expr::InList(InList::new(Box::new(x), v, negated)),
                    changed,
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            }
            Expr::Case(Case { expr, when_then_expr, else_expr }) => {
                let mut changed = false;
                let (whens, thens): (Vec<Expr>, Vec<Expr>) =
                    when_then_expr.into_iter().map(|(w, t)| (*w, *t)).unzip();
                // CASE x WHEN v: x and the v's are compared
                let (expr, whens) = match expr {
                    Some(x) => {
                        let mut all = vec![*x];
                        all.extend(whens);
                        let (mut v, c) = unify(all, schema)?;
                        changed |= c;
                        let x = v.remove(0);
                        (Some(Box::new(x)), v)
                    }
                    None => (None, whens),
                };
                // the results
                let n = thens.len();
                let mut results = thens;
                if let Some(e) = else_expr {
                    results.push(*e);
                }
                let (mut results, c) = unify(results, schema)?;
                changed |= c;
                let else_expr = (results.len() > n).then(|| Box::new(results.pop().unwrap()));
                let when_then_expr = whens
                    .into_iter()
                    .zip(results)
                    .map(|(w, t)| (Box::new(w), Box::new(t)))
                    .collect();
                Transformed::new(
                    Expr::Case(Case { expr, when_then_expr, else_expr }),
                    changed,
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            }
            Expr::ScalarFunction(ScalarFunction { func, args })
                if matches!(func.name(), "coalesce" | "nullif" | "greatest" | "least") =>
            {
                let (args, changed) = unify(args, schema)?;
                Transformed::new(
                    Expr::ScalarFunction(ScalarFunction { func, args }),
                    changed,
                    datafusion::common::tree_node::TreeNodeRecursion::Continue,
                )
            }
            Expr::Cast(c) => {
                // a narrow numeric cast to numeric of its scale
                let from = c.expr.get_type(schema).ok();
                match (from.as_ref().and_then(numeric_digits), numeric_digits(c.field.data_type())) {
                    (Some((p, s)), Some((_, ts))) if p < NUMERIC_PRECISION && s == ts => {
                        let to = c.field.data_type().clone();
                        let from = from.unwrap();
                        Transformed::yes(as_type(*c.expr, &from, &to))
                    }
                    (Some((p, s)), Some(_)) if p < NUMERIC_PRECISION => {
                        // to another scale: widened first
                        let from = from.unwrap();
                        let wide = as_type(*c.expr, &from, &crate::pgnum::numeric_type(s));
                        Transformed::yes(Expr::Cast(datafusion::logical_expr::Cast {
                            expr: Box::new(wide),
                            field: c.field,
                        }))
                    }
                    _ => Transformed::no(Expr::Cast(c)),
                }
            }
            e => Transformed::no(e),
        })
    })
}

/// `e` over `schema` as `NumericWidth` leaves it, for an expression planned
/// without the analyzer (LM1's conditions).
pub fn unify_numerics(e: Expr, schema: &DFSchema) -> Result<Expr> {
    rewrite_expr(e, schema).map(|t| t.data)
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Union(u) => rewrite_union(u),
        LogicalPlan::Join(mut j) => {
            let (ls, rs) = (j.left.schema().clone(), j.right.schema().clone());
            let mut changed = false;
            let mut on = Vec::with_capacity(j.on.len());
            for (l, r) in std::mem::take(&mut j.on) {
                let types = match (l.get_type(&ls), r.get_type(&rs)) {
                    (Ok(a), Ok(b)) => vec![a, b],
                    _ => {
                        on.push((l, r));
                        continue;
                    }
                };
                let (v, c) = unify_typed(vec![l, r], &types)?;
                changed |= c;
                let mut v = v.into_iter();
                on.push((v.next().unwrap(), v.next().unwrap()));
            }
            j.on = on;
            let plan = LogicalPlan::Join(j);
            let t = rewrite_exprs(plan)?;
            let changed = changed || t.transformed;
            let plan = if changed { t.data.recompute_schema()? } else { t.data };
            Ok(Transformed::new_transformed(plan, changed))
        }
        plan => {
            let t = rewrite_exprs(plan)?;
            if t.transformed {
                Ok(Transformed::yes(t.data.recompute_schema()?))
            } else {
                Ok(t)
            }
        }
    }
}

/// The expressions of `plan` rewritten over its inputs' columns.
fn rewrite_exprs(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if plan.inputs().is_empty() {
        return Ok(Transformed::no(plan));
    }
    let schema = merge_schema(&plan.inputs());
    plan.map_expressions(|e| rewrite_expr(e, &schema))
}

/// A UNION whose inputs give one column numeric values of different types:
/// each input projected to the common one.
fn rewrite_union(u: Union) -> Result<Transformed<LogicalPlan>> {
    let ncols = u.schema.fields().len();
    let mut targets: Vec<Option<DataType>> = vec![None; ncols];
    for (c, target) in targets.iter_mut().enumerate() {
        let cols: Vec<Expr> = u
            .inputs
            .iter()
            .map(|i| Expr::Column(datafusion::common::Column::from(i.schema().qualified_field(c))))
            .collect();
        let types: Vec<DataType> = u
            .inputs
            .iter()
            .map(|i| i.schema().field(c).data_type().clone())
            .collect();
        let pairs: Vec<(&Expr, &DataType)> = cols.iter().zip(&types).collect();
        *target = common_type(&pairs);
    }
    if targets.iter().all(Option::is_none) {
        return Ok(Transformed::no(LogicalPlan::Union(u)));
    }
    let inputs = u
        .inputs
        .into_iter()
        .map(|input| {
            let schema: DFSchemaRef = input.schema().clone();
            let exprs: Vec<Expr> = (0..ncols)
                .map(|c| {
                    let (q, f) = schema.qualified_field(c);
                    let col = Expr::Column(datafusion::common::Column::from((q, f)));
                    match &targets[c] {
                        Some(to) if f.data_type() != to => {
                            as_type(col, f.data_type(), to).alias_qualified(q.cloned(), f.name())
                        }
                        _ => col,
                    }
                })
                .collect();
            Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(exprs, input)?)))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Transformed::yes(LogicalPlan::Union(Union::try_new_with_loose_types(inputs)?)))
}
