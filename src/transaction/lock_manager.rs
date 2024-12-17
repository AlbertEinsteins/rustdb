use std::sync::Arc;



pub type LockManagerRef = Arc<LockManager>;
pub struct LockManager {}


impl LockManager {
    pub fn new() -> Self {
        Self {}
    }
}