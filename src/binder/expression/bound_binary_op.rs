use std::fmt::Display;

use sqlparser::ast::BinaryOperator;

use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};

#[derive(Debug)]
pub struct BoundBinaryOp {
    pub op: BinaryOpType,
    pub left_arg: BoundExpression,
    pub right_arg: BoundExpression,
}


#[derive(Debug)]
pub enum BinaryOpType {
    Plus,
    Sub,
    Mul,
    Div,

    // logic op
    Gt,
    GtEq,
    Lt,
    LtEq,
    NEq,
    Eq,

    // and or
    And,
    Or,
}

impl Display for BinaryOpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => { f.write_str("+") },
            Self::Sub => { f.write_str("-") },
            Self::Mul => { f.write_str("*") },
            Self::Div => { f.write_str("/") },
            Self::Gt => { f.write_str(">") },
            Self::GtEq => { f.write_str(">=") },
            Self::Lt => { f.write_str("<") },
            Self::LtEq => { f.write_str("<=") },
            Self::NEq => { f.write_str("!=") },
            Self::Eq => { f.write_str("=") },
            Self::And => { f.write_str("and") },
            Self::Or => { f.write_str("or") }
        }
    }
}

impl From<BinaryOperator> for BinaryOpType {
    fn from(value: BinaryOperator) -> Self {
        match value {
            BinaryOperator::Plus => {
                Self::Plus
            },
            BinaryOperator::Minus => {
                Self::Sub
            },
            BinaryOperator::Multiply => {
                Self::Mul
            },
            BinaryOperator::Divide => {
                Self::Div
            },
            BinaryOperator::Gt => {
                Self::Gt
            },
            BinaryOperator::GtEq => {
                Self::GtEq
            },
            BinaryOperator::Lt => {
                Self::Lt
            },
            BinaryOperator::LtEq => {
                Self::LtEq
            }
            BinaryOperator::Eq => {
                Self::Eq
            },
            BinaryOperator::And => {
                Self::And
            },
            BinaryOperator::Or => {
                Self::Or
            },
            _ => {
                panic!("Not support operator type");
            }
        }        
    }
}


impl BoundBinaryOp {
    pub fn new(left: BoundExpression, right: BoundExpression, op: BinaryOpType) -> Self {
        Self {
            left_arg: left,
            right_arg: right,
            op
        }
    }
}

impl BoundExpressionFeat for BoundBinaryOp {
    fn to_string(&self) -> String {
        format!("({}{}{})", self.left_arg.to_string(), self.op, self.right_arg.to_string())
    }

    fn has_aggregation(&self) -> bool {
        matches!(self.left_arg, BoundExpression::AggCall(_))
            || matches!(self.right_arg, BoundExpression::AggCall(_))
    }
}