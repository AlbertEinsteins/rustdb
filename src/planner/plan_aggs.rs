#![allow(warnings)]
use crate::{binder::statement::select_stmt::SelectStmt, execution::plans::plan::{PlanNodeRef, PlanNode}};

use super::planner::Planner;


impl Planner {

    pub fn plan_select_agg(&self, select: &SelectStmt, child: PlanNodeRef) -> PlanNodeRef {
        todo!()
    }
}