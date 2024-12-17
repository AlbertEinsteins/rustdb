
use crate::{catalog::schema::Schema, common::rid::RID, execution::executor_context::ExecutorContextRef, storage::page_based::table::tuple::{Tuple, TupleMeta}};


pub trait Executor {
    fn init(&mut self);

    fn next(&mut self) -> Result<Option<(RID, Tuple)>, String>;

    fn get_output_schema(&self) -> &Schema;

    fn get_context(&self) -> &ExecutorContextRef;
}

