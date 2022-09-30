// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder::{BoundBinaryOp, BoundExpr};
use crate::optimizer::plan_nodes::{LogicalTableScan};
use crate::types::DataValue;

pub struct RangeScanRule {}

impl Rule for RangeScanRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let table_scan = plan.as_logical_table_scan()?;
        let filter = table_scan.expr().ok_or_else(|| ())?;
        let pk_predicates = filter
            .get_pk_predicates(table_scan.column_descs())
            .ok_or_else(|| ())?;

        let find_intersection =
            |predicates: Vec<BoundBinaryOp>| -> (Option<DataValue>, Option<DataValue>) {
                use sqlparser::ast::BinaryOperator::*;
                use BoundExpr::*;

                // Normalize ops to the form: pk op constant
                let predicates = predicates.into_iter().map(|mut p| {
                    match (p.left_expr.as_ref(), p.right_expr.as_ref()) {
                        (InputRef(_), Constant(_)) => p,
                        (Constant(_), InputRef(_)) => {
                            (p.left_expr, p.right_expr) = (p.right_expr, p.left_expr);
                            p.op = match p.op {
                                Gt => Lt,
                                Lt => Gt,
                                GtEq => LtEq,
                                LtEq => GtEq,
                                Eq => Eq,
                                _ => panic!("Unexpected op"),
                            };
                            p
                        }
                        _ => panic!("Unexpected predicate"),
                    }
                });

                // Find lower bound and upper bound
                let mut lower_bound = None;
                let mut upper_bound = None;
                let mut may_update_lower_bound = |c| match &lower_bound {
                    None => lower_bound = Some(c),
                    Some(lower) => {
                        if lower < &c {
                            lower_bound = Some(c);
                        }
                    }
                };
                let mut may_update_upper_bound = |c| match &upper_bound {
                    None => upper_bound = Some(c),
                    Some(upper) => {
                        if upper > &c {
                            upper_bound = Some(c);
                        }
                    }
                };

                predicates.for_each(|p| match (*p.left_expr, *p.right_expr) {
                    (InputRef(_), Constant(c)) => match p.op {
                        Gt | GtEq => may_update_lower_bound(c),
                        Lt | LtEq => may_update_upper_bound(c),
                        Eq => {
                            may_update_lower_bound(c.clone());
                            may_update_upper_bound(c);
                        }
                        _ => panic!("Unexpected op"),
                    },
                    _ => panic!("Unexpected predicate"),
                });

                (lower_bound, upper_bound)
            };

        let (lower, upper) = find_intersection(pk_predicates);
        Ok(Arc::new(LogicalTableScan::new(
            table_scan.table_ref_id(),
            table_scan.column_ids().to_vec(),
            table_scan.column_descs().to_vec(),
            table_scan.with_row_handler(),
            if lower.is_none() {
                vec![]
            } else {
                vec![lower.unwrap()]
            },
            vec![upper.unwrap()],
            table_scan.is_sorted(),
            Some(filter.clone()),
        )))
    }
}
