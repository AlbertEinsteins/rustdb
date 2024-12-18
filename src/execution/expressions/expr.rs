#![allow(warnings)]

use std::{fmt::Display, sync::Arc, thread::sleep};

use crate::{typedef::{type_id::TypeId, value::Value}, storage::page_based::table::tuple::Tuple, catalog::schema::Schema};

use super::{column_expr::ColumnValueExpr, compare_expr::CompareExpr, constant_expr::ConstantExpr};


pub type ExpressionRef = Arc<Expression>;

#[derive(Debug, Clone)]
pub enum Expression {
    ColumnExpr(ColumnValueExpr),
    ConstantExpr(ConstantExpr),
    CmpExpr(CompareExpr),
}

impl Expression {
    pub fn get_return_type(&self) -> TypeId {
        match self {
            Self::ColumnExpr(col) => { col.get_return_type() },
            Self::ConstantExpr(constant) => { constant.get_return_type() }
            _ => {
                panic!("Not support return type");
            }
        }
    }

    pub fn evalute(&self, tuple: &Tuple, schema: &Schema) -> Value {
        match self {
            Self::ColumnExpr(col) => { col.evalute(tuple, schema) },
            Self::ConstantExpr(constant) => { constant.evalute(tuple, schema) }
            _ => {
                panic!("Not support return type");
            }
        }
    }

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value {
        match self {
            Self::ColumnExpr(col) => { col.evalute_join(tuple_left, schema_left, tuple_right, schema_right) },
            Self::ConstantExpr(constant) => { constant.evalute_join(tuple_left, schema_left, tuple_right, schema_right) }
            _ => {
                panic!("Not support return type");
            }
        }
    }

}

pub trait ExpressionFeat {
    fn to_string(&self) -> String;

    fn get_return_type(&self) -> TypeId;

    fn evalute(&self, tuple: &Tuple, schema: &Schema) -> Value;

    fn evalute_join(&self, tuple_left: &Tuple, schema_left: &Schema, tuple_right: &Tuple, schema_right: &Schema) -> Value;    
}


impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ColumnExpr(column) => { f.write_str(&column.to_string()) },
            Self::ConstantExpr(constant) => { f.write_str(&constant.to_string()) },
            Self::CmpExpr(cmp_expr) => { f.write_str(&cmp_expr.to_string()) }
        }
    }
}
