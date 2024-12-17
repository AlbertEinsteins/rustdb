#![allow(warnings)]

use crate::{typedef::{value::Value, type_id::TypeId}, catalog::schema::Schema, storage::page_based::table::tuple::Tuple};

use super::expr::{ExpressionFeat, ExpressionRef};


#[derive(Debug, Clone)]
pub struct ConstantExpr {
    pub val: Value,
    

    pub children: Vec<ExpressionRef>,
    rtn_type: TypeId
}

impl ConstantExpr {
    pub fn new(val: Value, children: Vec<ExpressionRef>) -> Self {
        let rtn_type = val.get_type();
        Self { val, children, rtn_type }
    }
}

impl ExpressionFeat for ConstantExpr {
    fn to_string(&self) -> String {
        self.val.to_string()
    }

    fn get_return_type(&self) -> TypeId {
        self.rtn_type
    }

    fn evalute(&self, tuple: &Tuple, schema: &Schema) -> Value {
        self.val.clone()
    }

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value {
        self.val.clone()
    }
}