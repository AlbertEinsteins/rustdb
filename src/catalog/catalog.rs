#![allow(warnings)]

use std::{collections::HashMap, sync::{atomic::AtomicI32, Arc}};

use crate::{buffer::buffer_pool_manager::BufferPoolManager, common::config::table_id_t, storage::page_based::{disk::log_manager::LogManager, table::{self, table_heap::TableHeap}}, transaction::lock_manager::LockManager, typedef::type_id::TypeId};

use super::{schema::Schema, column::Column};

pub struct TableInfo {
    pub schema: Schema,
    pub table_name: String,

    // holds a pointer to the physical storage
    pub table_heap: Box<TableHeap>,
    pub table_oid: table_id_t,
}


pub struct CataLog {
    // bpm
    bpm: Option<Arc<BufferPoolManager>>,
    // lock_mgr
    lock_mgr: Option<Arc<LockManager>>,
    // log_mgr
    log_mgr: Option<Arc<LogManager>>,

    // table meta infos
    table_info: HashMap<table_id_t, Box<TableInfo>>,
    table_name2id: HashMap<String, table_id_t>,
    table_id_generator: AtomicI32,
    
    // index meta infos
    // TODO:
}


impl CataLog {

    pub fn new(bpm: Option<Arc<BufferPoolManager>>, lock_mgr: Option<Arc<LockManager>>, log_mgr: Option<Arc<LogManager>>) -> Self {
        Self {
            bpm,
            lock_mgr,
            log_mgr,
            table_info: HashMap::new(),
            table_name2id: HashMap::new(),
            table_id_generator: AtomicI32::new(0),
        }
    }

    pub fn get_schema(&self, table_name: &str) -> Option<Schema> {
        match self.table_name2id.get(table_name) {
            None => None,
            Some(tid) => {
                self.table_info.get(tid).map(|table_info| table_info.schema.clone())
            }
        }
    }
}