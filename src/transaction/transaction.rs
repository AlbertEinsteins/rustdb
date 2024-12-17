use std::sync::Arc;

use crate::common::config::txn_id_t;


pub type TransactionRef = Arc<Transaction>;


#[derive(Debug, Eq)]
pub struct Transaction {
    txn_id: txn_id_t,
}

impl PartialEq for Transaction {
    fn eq(&self, other: &Self) -> bool {
        self.txn_id == other.txn_id
    }
}

impl Transaction {
    pub fn new() -> Self {
        Self { txn_id: 0 }
    }
}