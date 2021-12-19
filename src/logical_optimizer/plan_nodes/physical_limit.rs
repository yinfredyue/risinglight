use std::fmt;

use super::*;

/// The physical plan of limit operation.
#[derive(Debug, Clone)]
pub struct PhysicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: PlanRef,
}

impl_plan_node!(PhysicalLimit, [child]);

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalLimit: offset: {}, limit: {}",
            self.offset, self.limit
        )
    }
}