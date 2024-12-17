use crate::{catalog::schema::Schema, common::{config::INVALID_TXN_ID, rid::RID}, execution::{executor_context::ExecutorContextRef, plans::plan::{PlanNode, PlanNodeRef, ValuesPlan}}, storage::page_based::table::tuple::{Tuple, TupleMeta}, typedef::value_factory::ValueFactory};

use super::executor::Executor;



pub struct ValuesExecutor {
    plan: PlanNodeRef,
    
    ctx: ExecutorContextRef,
    total_rows: usize,
    cursor: usize,
}

impl ValuesExecutor {
    pub fn new(plan: PlanNodeRef, ctx: ExecutorContextRef) -> Self {
        let PlanNode::Values(values_plan) = plan.as_ref() else { panic!("Error init values executor, with an incorrect plan"); };
        let total_rows = values_plan.values_list.len();
        Self {
            plan,
            ctx,
            total_rows,
            cursor: 0,
        }
    }
}

impl Executor for ValuesExecutor {
    fn init(&mut self) {
        
    }

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String> {
        if self.cursor >= self.total_rows {
            return Ok(None);
        }
        let PlanNode::Values(values_plan) = self.plan.as_ref() else { panic!("Error init values executor, with an incorrect plan"); };
        
        let row = values_plan.values_list[self.cursor].clone();
        let mut values = Vec::new();
        for expr in row {
            // api need a tuple and schema, we give it a dummy value
            values.push(expr.evalute(&Tuple::new(), &Schema::new(&vec![])));
        }

        self.cursor += 1;
        let tuple = Tuple::build(&values, self.plan.get_output_schema());
        Ok(Some(
            (RID::new(), tuple)
        ))
    }

    fn get_context(&self) -> &ExecutorContextRef {
        &self.ctx        
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}