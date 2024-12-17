use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};
#[derive(Debug)]
pub struct BoundAlias {
    pub alias: String,
    pub expr: BoundExpression,
}

impl BoundExpressionFeat for BoundAlias {
    fn to_string(&self) -> String {
        format!("({} as {})", self.expr.to_string(), self.alias)        
    }

    fn has_aggregation(&self) -> bool {
        false
    }
}