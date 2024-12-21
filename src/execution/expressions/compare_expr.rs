#![allow(warnings)]

use std::fmt::Display;

use crate::{catalog::schema::Schema, storage::page_based::table::tuple::Tuple, typedef::{type_id::TypeId, type_trait::CmpBool, value::Value}};

use super::expr::{Expression, ExpressionFeat, ExpressionRef};

#[derive(Debug)]
pub enum CmpType {
    Equal,
    NotEqual,
    GtEq,
    Gt,
    Lt,
    LtEq,
}

impl Display for CmpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Equal => { f.write_str("=") },
            Self::NotEqual => { f.write_str("!=") },
            Self::Lt => { f.write_str("<") },
            Self::LtEq => { f.write_str("<=") },
            Self::Gt => { f.write_str(">") },
            Self::GtEq => { f.write_str(">=") }
        }
    }
}

#[derive(Debug)]
pub struct CompareExpr {
    pub cmp_type: CmpType,
    
    rtn_type: TypeId,
    pub children: Vec<ExpressionRef>,
}

impl CompareExpr {
    pub fn get_child_at(&self, idx: usize) -> &Expression {
        &self.children[idx]
    }

    fn performe_compare(cmp_type: CmpType, lhs: &Value, rhs: &Value) -> CmpBool {
        match cmp_type {
            CmpType::Equal => { lhs.compare_equal(rhs) },
            CmpType::NotEqual => { lhs.compare_not_equal(rhs) }
            CmpType::Lt => { lhs.compare_less_than(rhs) },
            CmpType::LtEq => { lhs.compare_less_than_equal(rhs) },
            CmpType::Gt => { lhs.compare_greater_than(rhs) },
            CmpType::GtEq => { lhs.compare_greater_than_equal(rhs) }
        }
    }
}

impl ExpressionFeat for CompareExpr {
    fn evalute(&self, tuple: &Tuple, schema: &Schema) -> Value {
        self.get_child_at(0).evalute(tuple, schema)
    }

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value {
        let lhs = self.get_child_at(0).evalute(tuple_left, schema_left);
        let rhs = self.get_child_at(1).evalute(tuple_right, schema_right);
    }

    fn get_return_type(&self) -> TypeId {
        self.rtn_type
    }

    fn to_string(&self) -> String {
        format!("({}{}{})", self.get_child_at(0).to_string(), self.cmd_type, self.get_child_at(1).to_string())
    }

}


