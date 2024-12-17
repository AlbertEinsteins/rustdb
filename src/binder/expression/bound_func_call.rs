use crate::binder::bound_expression::{BoundExpression, BoundExpressionFeat};

#[derive(Debug)]
pub struct BoundFuncCall {
    func_name: String,
    args: Vec<BoundExpression>
}

impl BoundFuncCall {
}

impl BoundExpressionFeat for BoundFuncCall {
    fn to_string(&self) -> String {
        let args_str: Vec<String> = self.args.iter()
            .map(|arg| arg.to_string()).collect();
        format!("{}({})", self.func_name, args_str.join(", "))        
    }

    fn has_aggregation(&self) -> bool {
        false
    }
}
