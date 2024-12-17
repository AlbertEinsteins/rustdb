use crate::{catalog::column::Column, binder::bound_statement::BoundStatementFeat};

pub struct CreateStmt {
    table_name: String,
    columns: Vec<Column>,
}

impl CreateStmt {
    pub fn new(table_name: String, cols: Vec<Column>) -> Self {
        Self {
            table_name,
            columns: cols,
        }
    }
}

impl BoundStatementFeat for CreateStmt {
    fn to_string(&self) -> String {
        let str: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        let str = str.join(", ");
        let str = "[".to_owned() + &str + "]";
        format!("{{{{\n    table={}\n    columns={}\n}}}}", self.table_name, str)
    }
}