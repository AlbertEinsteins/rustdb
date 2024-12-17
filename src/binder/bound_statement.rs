use std::fmt::Display;

use super::statement::{select_stmt::SelectStmt, insert_stmt::InsertStmt, create_stmt::CreateStmt};


pub enum BoundStatement {
    Invalid,
    Select(SelectStmt),
    Insert(InsertStmt),
    Update,
    Create(CreateStmt),
    Delete,
    Explain,
    Drop,
    Index,
    VariableSet,
    VariableShow,
}

pub trait BoundStatementFeat {
    fn to_string(&self) -> String;
}

impl Display for BoundStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(create) => { f.write_str(&create.to_string()) }
            Self::Select(sel) => { f.write_str(&sel.to_string()) }
            Self::Insert(insert) => { f.write_str(&insert.to_string()) }
            _ => { f.write_str("Unkown") }
        }
    }
}