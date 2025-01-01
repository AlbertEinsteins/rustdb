#![allow(warnings)]

use std::{collections::HashMap, sync::{atomic::{AtomicI32, Ordering, AtomicU32}, Arc}, cell::RefCell};

use crate::{buffer::buffer_pool_manager::BufferPoolManager, common::config::table_id_t, storage::page_based::{disk::log_manager::LogManager, table::{self, table_heap::TableHeap}}, transaction::{lock_manager::LockManager, transaction::{Transaction, TransactionRef}}, typedef::type_id::TypeId};

use super::{schema::Schema, column::Column};


pub type TableInfoRef = Arc<TableInfo>;
#[derive(Debug)]
pub struct TableInfo {
    pub schema: Schema,
    pub table_name: String,

    // holds a pointer to the physical storage
    pub table_heap: Arc<TableHeap>,
    pub table_oid: table_id_t,
}

pub type CataLogRef = Arc<RefCell<CataLog>>;

#[derive(Debug)]
pub struct CataLog {
    // bpm
    bpm: Arc<BufferPoolManager>,
    // lock_mgr
    lock_mgr: Arc<LockManager>,
    // log_mgr
    log_mgr: Arc<LogManager>,

    // table meta infos
    table_info: HashMap<table_id_t, TableInfoRef>,
    table_name2id: HashMap<String, table_id_t>,
    table_id_generator: AtomicU32,
    
    // index meta infos
    // TODO:
}


impl CataLog {

    pub fn new(bpm: Arc<BufferPoolManager>, lock_mgr: Arc<LockManager>, log_mgr: Arc<LogManager>) -> Self {
        Self {
            bpm,
            lock_mgr,
            log_mgr,
            table_info: HashMap::new(),
            table_name2id: HashMap::new(),
            table_id_generator: AtomicU32::new(0),
        }
    }

    pub fn create_table(&mut self, txn: Option<TransactionRef>, table_name: &str, schema: Schema) -> Option<&TableInfoRef> {
        if self.table_name2id.contains_key(table_name) {
            return None;
        }

        // create a table_heap
        let table_heap = TableHeap::new(self.bpm.clone());

        let table_id = self.table_id_generator.fetch_add(1, Ordering::Relaxed);
        let table_info = TableInfo {
            table_name: String::from(table_name),
            schema,
            table_heap: Arc::new(table_heap),
            table_oid: table_id,
        };

        // insert to map
        self.table_name2id.insert(String::from(table_name), table_id);
        self.table_info.insert(table_id, Arc::new(table_info));
        self.table_info.get(&table_id)
    }



    pub fn get_schema(&self, table_name: &str) -> Option<Schema> {
        match self.table_name2id.get(table_name) {
            None => None,
            Some(tid) => {
                self.table_info.get(tid).map(|table_info| table_info.schema.clone())
            }
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<&TableInfoRef> {
        match self.table_name2id.get(table_name) {
            None => None,
            Some(tid) => {
                self.table_info.get(tid)
            }
        }
    }

    pub fn get_table_byid(&self, table_id: table_id_t) -> Option<&TableInfoRef> {
        self.table_info.get(&table_id)
    }

}