use crate::binder::bound_expression::BoundExpressionFeat;


#[derive(Debug)] 
pub struct BoundStar {
    pub table_or_alias: Option<String>,
}


impl BoundExpressionFeat for BoundStar {
    fn to_string(&self) -> String {
        if let Some(name) = &self.table_or_alias {
            format!("{}.*", name)
        } else {
            format!("*")
        }
    }

    fn has_aggregation(&self) -> bool {
        panic!("Aggregation shoud not be called on BoundStar")
    }
}