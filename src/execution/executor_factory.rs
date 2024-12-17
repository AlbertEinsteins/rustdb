#![allow(warnings)]

use crate::execution::executors::{seqscan_executor::SeqScanExecutor, insert_executor::InsertExecutor};

use super::{executor_context::ExecutorContextRef, executors::{executor::Executor, proj_executor::ProjectExecutor, values_executor::ValuesExecutor}, plans::plan::{PlanNode, PlanNodeRef}};

pub struct ExecutorFactory {
}

impl ExecutorFactory {
    
    pub fn create_executor(plan: PlanNodeRef, ctx: ExecutorContextRef) -> Box<dyn Executor> {
        match plan.as_ref() {
            PlanNode::Insert(insert) => {
                let child_exec = Self::create_executor(insert.get_child_plan(), ctx.clone());
                return Box::new(InsertExecutor::new(plan, child_exec, ctx));
            },
            PlanNode::SeqScan(_) => {
                return Box::new(SeqScanExecutor::new(plan, ctx));
            },
            PlanNode::Proj(proj) => {
                let child_exec = Self::create_executor(proj.get_child_plan(), ctx.clone());
                return Box::new(ProjectExecutor::new(plan, child_exec, ctx));
            }
            PlanNode::Insert(insert) => {
                let child_exec = Self::create_executor(insert.get_child_plan(), ctx.clone());
                return Box::new(InsertExecutor::new(plan, child_exec, ctx));
            }
            PlanNode::Values(value_plan) => {
                return Box::new(ValuesExecutor::new(plan, ctx));
            },
            _ => {
                panic!("not support type");
            }
        }
    }
}