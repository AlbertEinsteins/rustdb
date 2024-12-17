use std::fmt::Display;

use super::expression::{bound_constant::BoundConstant, bound_agg_call::BoundAggCall, bound_star::BoundStar, bound_unary_op::BoundUnaryOp, bound_func_call::BoundFuncCall, bound_binary_op::BoundBinaryOp, bound_alias::BoundAlias, bound_column_ref::BoundColumn};

#[derive(Debug)]
pub enum BoundExpression {
    Invalid,
    Constant(Box<BoundConstant>),
    ColumnRef(Box<BoundColumn>),
    TypeCast,
    Function,
    AggCall(Box<BoundAggCall>),
    Star(Box<BoundStar>),
    UnaryOp(Box<BoundUnaryOp>),
    BinaryOp(Box<BoundBinaryOp>),
    Alias(Box<BoundAlias>),
    FuncCall(Box<BoundFuncCall>)
}

pub trait BoundExpressionFeat {
    fn to_string(&self) -> String;

    fn has_aggregation(&self) -> bool;
}



impl Display for BoundExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid => f.write_str("Invalid"),
            Self::Constant(expr1) => f.write_str(&expr1.to_string()),
            Self::ColumnRef(expr2) => f.write_str(&expr2.to_string()),
            Self::AggCall(expr3) => f.write_str(&expr3.to_string()),
            Self::Star(expr4) => f.write_str(&expr4.to_string()),
            Self::UnaryOp(expr5) => f.write_str(&expr5.to_string()),
            Self::BinaryOp(expr6) => f.write_str(&expr6.to_string()),
            Self::Alias(expr7) => f.write_str(&expr7.to_string()),
            Self::FuncCall(expr8) => {
                f.write_str(&expr8.to_string())
            },
            _ => {
                f.write_str("Unknown")
            }
        }
    }
}