use crate::binder::{bound_statement::BoundStatementFeat, table_ref::bound_base_table::BoundBaseTableRef, bound_table_ref::BoundTableRefFeat};

use super::select_stmt::SelectStmt;

#[derive(Debug)]
pub struct InsertStmt {
    pub table_ref: Box<BoundBaseTableRef>,
    pub select: Box<SelectStmt>
}

impl InsertStmt {
    pub fn new(table_ref: Box<BoundBaseTableRef>, sel: Box<SelectStmt>) -> Self {
        Self {
            table_ref,
            select: sel,
        }
    }
}

impl BoundStatementFeat for InsertStmt {
    fn to_string(&self) -> String {
        format!("{{{{ table={}, sel={} }}}}", 
            self.table_ref.to_string(), self.select.to_string())
    }
}