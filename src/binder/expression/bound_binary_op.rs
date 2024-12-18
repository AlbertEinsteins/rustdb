use sqlparser::ast::BinaryOperator;

use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};

#[derive(Debug)]
pub struct BoundBinaryOp {
    op: BinaryOpType,
    left_arg: BoundExpression,
    right_arg: BoundExpression,
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
        format!("({}{}{})", self.left_arg.to_string(), self.op_name, self.right_arg.to_string())
    }

    fn has_aggregation(&self) -> bool {
        matches!(self.left_arg, BoundExpression::AggCall(_))
            || matches!(self.right_arg, BoundExpression::AggCall(_))
    }
}