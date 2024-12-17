use crate::binder::{bound_expression::{BoundExpression}, bound_table_ref::BoundTableRefFeat};




// here is a mock table, which do not has schema, this structure represents
// for that, a mock , usually occurs when in InsertStatement, 
// specifically in the keyword 'values' after

#[derive(Debug)]
pub struct BoundValuesList {
    pub values_list: Vec<Vec<Box<BoundExpression>>>,
    pub mock_name: String,
}

impl BoundTableRefFeat for BoundValuesList {
    fn to_string(&self) -> String {
        format!("{{{{ mock table: {} }}}}", self.mock_name)
    }

}