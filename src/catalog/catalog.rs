use crate::typedef::type_id::TypeId;

use super::{schema::Schema, column::Column};

pub struct CataLog {
    
}

impl CataLog {
    pub fn get_schema(&self, table_name: &str) -> Schema {
        let cols = vec![Column::new_varchar("a", TypeId::VARCHAR, 32)];
        Schema::new(&cols)
    }
}