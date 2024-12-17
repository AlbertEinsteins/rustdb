use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};


#[derive(Debug)]
pub struct BoundUnaryOp {
    op_name: String,
    arg: BoundExpression,
}

impl BoundExpressionFeat for BoundUnaryOp {
    fn to_string(&self) -> String {
        format!("({}{})", self.op_name, self.arg.to_string())
    }

    fn has_aggregation(&self) -> bool {
        matches!(self.arg, BoundExpression::AggCall(_))
    }
}
