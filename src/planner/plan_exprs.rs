#![allow(warnings)]

use crate::{binder::{bound_expression::{BoundExpression, BoundExpressionFeat}, expression::{bound_binary_op::BoundBinaryOp, bound_column_ref::BoundColumn, bound_constant::BoundConstant}}, execution::{expressions::{column_expr::ColumnValueExpr, constant_expr::ConstantExpr, expr::{Expression, ExpressionRef}}, plans::plan::{PlanNode, PlanNodeRef}}};

use super::planner::Planner;

impl Planner {

    pub fn plan_expression(&self, expr: &BoundExpression, children: &Vec<PlanNodeRef>) -> Result<(String, ExpressionRef), String> {
        match expr {
            BoundExpression::Constant(constant) => {
                return Ok((Self::UNAMED_COLUMN.to_owned(), self.plan_constant(constant, children)?));
            },
            BoundExpression::ColumnRef(col) => {
                return self.plan_column_ref(col, children);
            },
            BoundExpression::BinaryOp(binary_op) => {
                return self.plan_binary_op(binary_op, children);
            },
            BoundExpression::Alias(alias) => {

            },
            BoundExpression::FuncCall(func) => {

            },
            _ => {
                panic!("Not support type");
            }
        }

        todo!()
    }

    pub fn plan_binary_op(&self, binary_op: &Box<BoundBinaryOp>, children: &Vec<PlanNodeRef>) -> Result<(String, ExpressionRef), String> {
        
    }

    pub fn plan_constant(&self, constant: &Box<BoundConstant>, children: &Vec<PlanNodeRef>) -> Result<ExpressionRef, String> {
        let expr = Expression::ConstantExpr(ConstantExpr::new(constant.val.clone(), Vec::new()));
        Ok(ExpressionRef::new(expr))
    }

    pub fn plan_column_ref(&self, col: &Box<BoundColumn>, children: &Vec<PlanNodeRef>) -> Result<(String, ExpressionRef), String> {
        if children.len() == 0 {
            return Err(format!("The column ref must have at least one child"));
        }

        let col_name = col.to_string();
        if children.len() == 1 {
            // the child plan is usually Projection or Filter Node
            let child = &children[0];
            let schema = child.get_output_schema();
            
            // check the sub node name, the col_name shoud not be duplicated
            let find_result = schema.get_columns().iter().filter(|col| col.get_name() == col_name).count();
            if find_result > 1 {
                return Err(format!("The column has duplicated name, {}", col_name));
            } else if find_result == 0 {
                return Err(format!("The column can not found, {}", col_name));
            }

            // make the columnexpr
            // println!("{}", col_name);
            // println!("{}", schema.get_columns().iter().map(|c| c.get_name()).collect::<Vec<String>>().join(", "));
            let col_idx = schema.get_column_idx(&col_name)?;
            let column = schema.get_column(col_idx)?;
            let column_expr = ColumnValueExpr::new(0, col_idx, column.get_type());
            return Ok((col_name, ExpressionRef::new(Expression::ColumnExpr(column_expr))));
        }

        todo!()
    }
}