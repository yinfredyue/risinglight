// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bit_set::BitSet;

use crate::binder::*;

mod expr_utils;
mod heuristic;
pub mod logical_plan_rewriter;
pub mod plan_nodes;
mod plan_visitor;
mod rules;

use self::heuristic::HeuristicOptimizer;
use self::logical_plan_rewriter::*;
use self::plan_nodes::PlanRef;
pub use self::plan_visitor::*;
use self::rules::*;

/// The optimizer will do query optimization.
///
/// It will do both rule-based optimization (predicate pushdown, constant folding and common
/// expression extraction) , and cost-based optimization (Join reordering and join algorithm
/// selection). It takes Plan as input and returns a new Plan which could be used to
/// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {
    pub enable_filter_scan: bool,
}

impl Optimizer {
    pub fn optimize(&mut self, mut plan: PlanRef) -> PlanRef {
        // Logical plan rewrite
        let mut constant_folding_rule = ConstantFoldingRule;
        let mut constant_moving_rule = ConstantMovingRule;
        let mut arith_expr_simplification_rule = ArithExprSimplificationRule;
        let mut bool_expr_simplification_rule = BoolExprSimplificationRule;

        // Heuristic rules
        plan = constant_folding_rule.rewrite(plan);
        plan = arith_expr_simplification_rule.rewrite(plan);
        plan = bool_expr_simplification_rule.rewrite(plan);
        plan = constant_moving_rule.rewrite(plan);
        let mut rules: Vec<Box<(dyn rules::Rule + 'static)>> = vec![
            Box::new(FilterAggRule {}),
            Box::new(FilterJoinRule {}),
            Box::new(LimitOrderRule {}),
        ];
        if self.enable_filter_scan {
            rules.push(Box::new(FilterScanRule {}));
            rules.push(Box::new(RangeScanRule {}))
        }
        let hep_optimizer = HeuristicOptimizer { rules };
        plan = hep_optimizer.optimize(plan);

        // Column pruning
        let out_types_num = plan.out_types().len();
        plan = plan.prune_col(BitSet::from_iter(0..out_types_num));

        // Convert logical plan to physical plan
        let mut phy_converter = PhysicalConverter;
        phy_converter.rewrite(plan)
    }
}
