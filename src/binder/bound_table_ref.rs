use std::{fmt::Display, sync::Arc};

use super::table_ref::{bound_base_table::BoundBaseTableRef, bound_empty_table::BoundEmptyTable, bound_values_list_table::BoundValuesList};


pub type BoundTableRef = Arc<BoundTable>;
#[derive(Debug)]
pub enum BoundTable {
    Invalid,
    Empty(BoundEmptyTable),
    BaseTable(BoundBaseTableRef),
    ValuesList(BoundValuesList),
}


pub trait BoundTableRefFeat {
    fn to_string(&self) -> String;
}

impl Display for BoundTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid => {
                f.write_str(&format!("{}", "Invalid"))
            },
            Self::BaseTable(base) => {
                f.write_str(&format!("{}", base.to_string()))
            },
            Self::ValuesList(values_list) => {
                f.write_str(&format!("{}", values_list.to_string()))
            },
            // Self::Join => {
            //     f.write_str(&format!("{}", "Join"))
            // },
            // Self::CrossProduct => {
            //     f.write_str(&format!("{}", "CrossProduct"))
            // },
            // Self::Empty => {
            //     f.write_str(&format!("{}", "Empty"))
            // },
            // Self::ExpressionList => {
            //     f.write_str(&format!("{}", "ExpressionList"))
            // },
            // Self::SubQuery => {
            //     f.write_str(&format!("{}", "SubQuery"))
            // },
            // Self::CTE => {
            //     f.write_str(&format!("{}", "CTE"))
            // },
            _ => {
                f.write_str(&format!("{}", "Unknown"))
            }
        }
    }
}