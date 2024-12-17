#![allow(warnings)]

use crate::{execution::executor_factory::ExecutorFactory, storage::page_based::table::tuple::Tuple, transaction::transaction::TransactionRef};

use super::{executor_context::{ExecutorContext, ExecutorContextRef}, executors::executor::{self, Executor}, plans::plan::{PlanNodeRef, PlanNode}};

pub struct ExecuteEngine {
}

impl ExecuteEngine {
    pub fn new() -> Self {
        Self {}
    }


    // execute a plan by using a vocalno model
    pub fn execute(&self, plan: PlanNodeRef, txn: TransactionRef, ctx: ExecutorContextRef) -> Result<Vec<Tuple>, String> {
        assert!(txn == ctx.get_txn());

        let mut executor = ExecutorFactory::create_executor(plan, ctx);
        Self::poll_executor(&mut executor)        
    }


    pub fn poll_executor(executor: &mut Box<dyn Executor>) -> Result<Vec<Tuple>, String> {
        let mut rows = Vec::new();

        executor.init();
        while let Some(tuple_pair) = executor.next()? {
            rows.push(tuple_pair.1);
        }

        Ok(rows)
    }

}
