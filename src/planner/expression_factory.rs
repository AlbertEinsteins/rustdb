use crate::{binder::expression::bound_binary_op::{BinaryOpType, BoundBinaryOp}, execution::expressions::{compare_expr::{CmpType, CompareExpr}, expr::{Expression, ExpressionRef}}};

use super::planner::Planner;

/// Expression Factory
/// convert a binary bound operation to a `expressionref type`
/// 
/// 
/// 




impl Planner {
    pub fn get_binary_op_expr(&self, op_type: &BinaryOpType, left: ExpressionRef, right: ExpressionRef) -> Result<ExpressionRef, String> {
        match op_type {
            BinaryOpType::Gt => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::Gt, vec![left, right]))
                ));
            },
            BinaryOpType::GtEq => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::GtEq, vec![left, right]))
                ));
            },
            BinaryOpType::Eq => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::Equal, vec![left, right]))
                ));
            },
            BinaryOpType::NEq => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::NotEqual, vec![left, right]))
                ));
            },
            BinaryOpType::Lt => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::Lt, vec![left, right]))
                ));
            },
            BinaryOpType::LtEq => {
                return Ok(ExpressionRef::new(
                    Expression::CmpExpr(CompareExpr::new(CmpType::LtEq, vec![left, right]))
                ));
            },
            BinaryOpType::And => {
                return Ok(ExpressionRef::new(
                    Expression::
                ))
            },
            BinaryOpType::Or => {

            },
            _ => {
                // return Ok(ExpressionRef::new(
                //     Expression::CmpExpr(CompareExpr::new(CmpType::, vec![left, right]))
                // ));
                todo!()
            }
        }
    }

}