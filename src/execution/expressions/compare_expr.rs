#![allow(warnings)]

use std::fmt::Display;

use crate::{catalog::schema::Schema, storage::page_based::table::tuple::Tuple, typedef::{type_id::TypeId, type_trait::CmpBool, value::Value, value_factory::ValueFactory}};

use super::expr::{Expression, ExpressionFeat, ExpressionRef};

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct CompareExpr {
    pub cmp_type: CmpType,
    
    rtn_type: TypeId,

    // as the common case, the children would have two elements.
    // [0] is the left operator
    // [1] is the right operator if has
    pub children: Vec<ExpressionRef>,
}

impl CompareExpr {
    pub fn new(cmp_type: CmpType, children: Vec<ExpressionRef>) -> Self {
        Self { cmp_type, children, rtn_type: TypeId::BOOLEAN }
    }

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
        let left_arg = self.get_child_at(0).evalute(tuple, schema);
        let right_arg = self.get_child_at(1).evalute(tuple, schema);

        ValueFactory::get_boolean_value(Self::performe_compare(self.cmp_type.clone(), &left_arg, &right_arg))
    }

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value {
        let lhs = self.get_child_at(0).evalute(tuple_left, schema_left);
        let rhs = self.get_child_at(1).evalute(tuple_right, schema_right);
        match self.cmp_type  {
            CmpType::Lt => {
                ValueFactory::get_boolean_value(lhs.compare_less_than(&rhs))
            },
            CmpType::LtEq => {
                ValueFactory::get_boolean_value(lhs.compare_less_than_equal(&rhs))
            },
            CmpType::Gt => {
                ValueFactory::get_boolean_value(lhs.compare_greater_than(&rhs))
            },
            CmpType::GtEq => {
                ValueFactory::get_boolean_value(lhs.compare_greater_than_equal(&rhs))
            },
            CmpType::NotEqual => {
                ValueFactory::get_boolean_value(lhs.compare_not_equal(&rhs))
            },
            CmpType::Equal => {
                ValueFactory::get_boolean_value(lhs.compare_equal(&rhs))
            }
        }        
    }

    fn get_return_type(&self) -> TypeId {
        self.rtn_type
    }

    fn to_string(&self) -> String {
        format!("({}{}{})", self.get_child_at(0).to_string(), self.cmp_type, self.get_child_at(1).to_string())
    }


}

