use std::sync::Arc;



pub type LockManagerRef = Arc<LockManager>;

#[derive(Debug)]

pub struct LockManager {}


impl LockManager {
    pub fn new() -> Self {
        Self {}
    }
}