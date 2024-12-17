use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};

#[derive(Debug)]
pub struct BoundBinaryOp {
    op_name: String,
    left_arg: BoundExpression,
    right_arg: BoundExpression,
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