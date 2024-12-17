use crate::binder::bound_table_ref::BoundTableRefFeat;

#[derive(Debug)]
pub struct BoundEmptyTable { }

impl BoundTableRefFeat for BoundEmptyTable {
    fn to_string(&self) -> String {
        format!("Empty table")   
    }
}