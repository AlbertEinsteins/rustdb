use crate::{common::config::table_id_t, catalog::schema::Schema, binder::bound_table_ref::BoundTableRefFeat};

#[derive(Debug)]
pub struct BoundBaseTableRef {
    pub table_name: String,
    pub table_id: table_id_t,
    pub alias: Option<String>,
    pub schema: Schema
}

impl BoundBaseTableRef {
    pub fn new(table_name: String, table_id: table_id_t, alias: Option<String>, schema: Schema) -> Self {
        Self { table_name, table_id, alias, schema }
    }
}


impl BoundBaseTableRef {
    pub fn get_bound_name(&self) -> String {
        match &self.alias {
            None => self.table_name.clone(),
            Some(alias) => alias.clone(),
        }
    }
}

impl BoundTableRefFeat for BoundBaseTableRef {
    fn to_string(&self) -> String {
        match &self.alias {
            None => format!("BoundBaseTable {{{{ table={}, table_id={} }}}}", 
                self.table_name, self.table_id),
            Some(alias) => format!("BoundBaseTable {{{{ table={}, table_id={}, alias={} }}}}", 
                self.table_name, self.table_id, alias)
        }
    }
}