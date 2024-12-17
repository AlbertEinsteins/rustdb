#![allow(warnings)]

use crate::binder::{bound_expression::BoundExpression, bound_statement::BoundStatementFeat, bound_table_ref::{BoundTableRef, BoundTableRefFeat}};

#[derive(Debug)]
pub struct SelectStmt {
    pub table_ref: BoundTableRef,
    pub select_list: Vec<Box<BoundExpression>>,
    pub where_by: Option<Box<BoundExpression>>,
    pub group_by: Vec<Box<BoundExpression>>,
    pub having: Option<Box<BoundExpression>>,
    pub limit: Option<Box<BoundExpression>>,
    pub offset: Option<Box<BoundExpression>>,
    pub sort_by: Vec<Box<BoundExpression>>,
    pub is_distinct: bool,
}


impl SelectStmt {
    pub fn new(table_ref: BoundTableRef, 
        select_list: Vec<Box<BoundExpression>>,
        where_by: Option<Box<BoundExpression>>,
        group_by: Vec<Box<BoundExpression>>,
        having: Option<Box<BoundExpression>>,
        limit: Option<Box<BoundExpression>>,
        offset: Option<Box<BoundExpression>>,
        sort_by: Vec<Box<BoundExpression>>,
        is_distinct: bool
    ) -> Self {
        Self {
            table_ref,
            select_list,
            where_by,
            group_by,
            having,
            limit,
            offset,
            sort_by,
            is_distinct,
        }
    }
}

impl BoundStatementFeat for SelectStmt {
    fn to_string(&self) -> String {
        // get select list
        let select_list: Vec<String> = self.select_list.iter()
            .map(|expr| expr.to_string()).collect();
        let select_str = select_list.join(", ");
        let select_str = "[".to_owned() + &select_str + "]";
        format!("{{{{ table={}, select={}, where={:#?}", 
            self.table_ref.to_string(), 
            select_str,
            self.where_by
        )
    }
}