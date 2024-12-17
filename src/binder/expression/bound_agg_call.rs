use crate::binder::bound_expression::{BoundExpressionFeat, BoundExpression};

#[derive(Debug)]
pub struct BoundAggCall {
    is_distinct: bool,
    func_name: String,
    arg: Vec<BoundExpression>,
}

impl BoundAggCall {
    pub fn new(func_name: String, is_distinct: bool, arg: Vec<BoundExpression>) -> Self {
        Self {
            is_distinct,
            func_name,
            arg
        }
    }
}


impl BoundExpressionFeat for BoundAggCall {
    fn to_string(&self) -> String {
        if self.is_distinct {
            format!("{}_distict({})", self.func_name, self.arg[0].to_string())
        } else {
            format!("{}({})", self.func_name, self.arg[0].to_string())
        }
    }


    fn has_aggregation(&self) -> bool {
        true
    }
}