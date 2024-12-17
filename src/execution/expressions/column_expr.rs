use crate::{typedef::{type_id::TypeId, value::Value}, storage::page_based::table::tuple::Tuple, catalog::schema::Schema};

use super::expr::{ExpressionFeat, ExpressionRef};

#[derive(Debug, Clone)]
pub struct ColumnValueExpr {
    pub tuple_idx: usize,
    pub col_idx: usize,

    pub children: Vec<ExpressionRef>,
    rtn_type: TypeId
}

impl ColumnValueExpr {
    pub fn new(tuple_idx: usize, col_idx: usize, rtn_type: TypeId) -> Self {
        Self {
            tuple_idx,
            col_idx,
            children: Vec::new(),
            rtn_type,
        }
    }
}

impl ExpressionFeat for ColumnValueExpr {
    fn to_string(&self) -> String {
        format!("#{}.{}", self.tuple_idx, self.col_idx)
    }

    fn get_return_type(&self) -> TypeId {
        self.rtn_type        
    }

    fn evalute(&self, tuple: &Tuple, schema: &Schema) -> crate::typedef::value::Value {
        tuple.get_value(schema, self.col_idx)        
    }

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value {
        if self.tuple_idx == 0 {
            tuple_left.get_value(schema_left, self.col_idx)
        } else {
            tuple_right.get_value(schema_right, self.col_idx)
        }
    }
}