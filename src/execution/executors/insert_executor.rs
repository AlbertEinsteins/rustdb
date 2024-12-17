#![allow(warnings)]

use core::panic;

use crate::{catalog::schema::Schema, common::{config::INVALID_TXN_ID, rid::RID}, execution::{executor_context::{ExecutorContext, ExecutorContextRef}, plans::plan::{InsertPlan, PlanNode, PlanNodeRef}}, storage::page_based::table::{table_heap::TableHeapRef, tuple::{Tuple, TupleMeta}}, typedef::value_factory::ValueFactory};

use super::executor::Executor;


pub struct InsertExecutor {
    insert_plan: PlanNodeRef,

    table_heap: TableHeapRef,
    child_executor: Box<dyn Executor>,
    ctx: ExecutorContextRef,
    is_inserted: bool,
}

impl InsertExecutor {
    pub fn new(plan: PlanNodeRef, child_exec: Box<dyn Executor>, ctx: ExecutorContextRef) -> Self {
        let PlanNode::Insert(insert_plan) = plan.as_ref() else { panic!("Error"); };
        
        let table_id = insert_plan.table_id;
        let catalog = ctx.get_catalog();
        let table_heap = catalog.borrow()
            .get_table_byid(table_id).unwrap().table_heap.clone();
        Self { 
            insert_plan: plan, 
            child_executor: child_exec, 
            ctx,
            table_heap,
            is_inserted: false,
        }
    }
}

impl Executor for InsertExecutor {
    fn init(&mut self) {
        self.child_executor.init();
    }

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String> {
        if self.is_inserted {
            return Ok(None);
        }

        let PlanNode::Insert(insert_plan) = self.insert_plan.as_ref() else { panic!("Error"); };
        let mut insert_rows = 0;
        
        while let Some(tuple_pair) = self.child_executor.next()? {
            let (_, tuple) = tuple_pair;
            
            let meta = TupleMeta::new(
                INVALID_TXN_ID,
                INVALID_TXN_ID, 
                false);
            self.table_heap.insert_tuple(&meta, &tuple);
            insert_rows += 1;
        }

        let value = ValueFactory::get_integer_value(insert_rows);
        let rtn_tuple = Tuple::build(&vec![value], self.insert_plan.get_output_schema());

        self.is_inserted = true;
        Ok(Some((RID::new(), rtn_tuple)))
    }

    fn get_output_schema(&self) -> &Schema {
        self.insert_plan.get_output_schema()
    }

    fn get_context(&self) -> &ExecutorContextRef {
        &self.ctx
    }
}