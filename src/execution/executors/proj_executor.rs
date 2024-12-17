use crate::{catalog::schema::Schema, common::rid::RID, execution::{executor_context::{ExecutorContext, ExecutorContextRef}, plans::plan::{PlanNode, PlanNodeRef}}, storage::page_based::table::tuple::{Tuple, TupleMeta}};

use super::executor::Executor;

pub struct ProjectExecutor {
    plan: PlanNodeRef,
    child_exec: Box<dyn Executor>,

    ctx: ExecutorContextRef,
}


impl ProjectExecutor {
    pub fn new(plan: PlanNodeRef, child_exec: Box<dyn Executor>, ctx: ExecutorContextRef) -> Self {
        Self { plan, child_exec, ctx } 
    }

    
}

impl Executor for ProjectExecutor {
    fn init(&mut self) {
        self.child_exec.init();
    }

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String> {

        while let Some(tuple_pair) = self.child_exec.next()? {
            // do projection on every row

            let tuple = tuple_pair.1;
            let mut values = Vec::new();
            let PlanNode::Proj(proj_plan) = self.plan.as_ref() else { panic!("Err"); };
            values.reserve(self.get_output_schema().get_column_count());
            for col in &proj_plan.expressions {
                values.push(col.evalute(&tuple, self.child_exec.get_output_schema()));
            }   

            let out_tuple = Tuple::build(&values, self.get_output_schema());
            return Ok(Some((tuple_pair.0, out_tuple)));
        }

        Ok(None)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }

    fn get_context(&self) -> &ExecutorContextRef {
        &self.ctx
    }
}