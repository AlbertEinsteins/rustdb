use std::vec;

use crate::binder::bound_expression::BoundExpressionFeat;



/// this structure is responisble for building a column which style is `y.x` or 'x'
#[derive(Debug)]
pub struct BoundColumn {
    pub col_name: Vec<String>
}

impl BoundColumn {
    pub fn prepend(prefix: String, col_name: Box<BoundColumn>) -> Box<BoundColumn> {
        let mut new_col_name = vec![prefix];
        new_col_name.extend(col_name.col_name);
        Box::new(BoundColumn { col_name: new_col_name })
    }
}

impl BoundExpressionFeat for BoundColumn {
    fn to_string(&self) -> String {
        assert!(self.col_name.len() <= 2);
        if self.col_name.len() == 2 {
            format!("{}.{}", self.col_name[0], self.col_name[1])
        } else {
            format!("{}", self.col_name[0])
        }
    }

    fn has_aggregation(&self) -> bool {
        false
    }

}