#![allow(warnings)]

use std::sync::Arc;

use crate::{catalog::catalog::CataLogRef, transaction::transaction::{Transaction, TransactionRef}};


//TOOD: a context associated with an executor
pub type ExecutorContextRef = Arc<ExecutorContext>;

pub struct ExecutorContext {
    catalog: CataLogRef,
    txn: TransactionRef,
}


impl ExecutorContext {
    pub fn new(catalog: CataLogRef, txn: TransactionRef) -> Self {
        Self { catalog, txn }
    }

    pub fn get_txn(&self) -> TransactionRef {
        self.txn.clone()
    }

    pub fn get_catalog(&self) -> CataLogRef {
        self.catalog.clone()
    }
}

