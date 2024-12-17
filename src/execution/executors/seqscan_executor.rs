#![allow(warnings)]

use std::sync::Arc;

use crate::{catalog::{catalog::TableInfoRef, schema::Schema}, common::rid::RID, execution::{executor_context::{ExecutorContext, ExecutorContextRef}, plans::plan::{PlanNode, PlanNodeRef, SeqScanPlan}}, storage::page_based::table::{table_heap::{TableHeap, TableHeapRef}, table_iter::TableIter, tuple::{Tuple, TupleMeta}}};

use super::executor::Executor;



pub struct SeqScanExecutor {
    plannode: PlanNodeRef,
    table_heap: TableHeapRef,
    table_iter: Option<TableIter>,

    ctx: ExecutorContextRef,
}


impl SeqScanExecutor {
    pub fn new(plan: PlanNodeRef, ctx: ExecutorContextRef) -> Self {
        let catalog = ctx.get_catalog();
        let catalog = catalog.borrow();
        
        let PlanNode::SeqScan(seqplan) = plan.as_ref() else { panic!("asd"); };
        let table_info = catalog.get_table(&seqplan.table_name).unwrap();
        
        Self { 
            plannode: plan, 
            table_heap: table_info.table_heap.clone(),
            ctx: ctx, 
            table_iter: None,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) {
        let table_heap = self.table_heap.clone();
        self.table_iter = Some(table_heap.make_iterator());
    }

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String> {
        let mut table_iter = self.table_iter.as_mut().unwrap();

        while let Some(tuple_pair) = table_iter.next() {
            let tuple = tuple_pair.1;

            return Ok(Some((tuple.get_rid(), tuple)));
        }
        Ok(None)
    }

    fn get_output_schema(&self) -> &Schema {
        let PlanNode::SeqScan(seqplan) = self.plannode.as_ref() else { panic!("asd"); };
        &seqplan.output_schema
    }

    fn get_context(&self) -> &ExecutorContextRef {
        &self.ctx        
    }
}