use crate::{catalog::schema::Schema, common::rid::RID, execution::{executor_context::ExecutorContextRef, expressions::expr::{Expression, ExpressionFeat}, plans::plan::{PlanNode, PlanNodeRef}}, storage::page_based::table::tuple::{self, Tuple}};

use super::executor::Executor;

#[allow(warnings)]


// #[derive(Debug)]
pub struct FilterExecutor {
    filter_plan: PlanNodeRef,

    child_executor: Box<dyn Executor>,
    ctx: ExecutorContextRef,
}

impl FilterExecutor {
    pub fn new(plan: PlanNodeRef, child_executor: Box<dyn Executor>, ctx: ExecutorContextRef) -> Self {
        Self { filter_plan: plan, child_executor, ctx }
    }
}

impl Executor for FilterExecutor {
    fn init(&mut self) {
        self.child_executor.init();
    }

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String> {
        let PlanNode::Filter(filter_plan) = self.filter_plan.as_ref() else { panic!("Error"); };
        let Expression::CmpExpr(cmp_expr) = filter_plan.predicate.as_ref() else { panic!("Error"); };

        while let Some(tuple_pair) = self.child_executor.next()? {
            let tuple = tuple_pair.1;
            let res = cmp_expr.evalute(&tuple, self.child_executor.get_output_schema());
            
            if !res.is_null() && *res.as_ptr::<bool>() {
                return Ok(Some((tuple_pair.0, tuple)))
            }
        }

        return Ok(None)
    }

    fn get_context(&self) -> &ExecutorContextRef {
        &self.ctx
    }

    fn get_output_schema(&self) -> &Schema {
        self.child_executor.get_output_schema()
    }
}