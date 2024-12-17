use crate::{typedef::value::Value, binder::bound_expression::BoundExpressionFeat};

#[derive(Debug)]
pub struct BoundConstant {
    pub val: Value
}

impl BoundConstant {
    pub fn new(val: Value) -> Self {
        Self { val }
    }
}

impl BoundExpressionFeat for BoundConstant {
    fn to_string(&self) -> String {
        self.val.to_string()        
    }

    fn has_aggregation(&self) -> bool {
        false
    }
}