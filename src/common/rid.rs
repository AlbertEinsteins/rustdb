use super::config::{page_id_t, slot_id_t, INVALID_PAGE_ID};


#[repr(C)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RID {
    pub pid: page_id_t,
    pub sid: slot_id_t,
}

impl RID {
    pub fn new() -> Self {
        Self {
            pid: INVALID_PAGE_ID,
            sid: 0,
        }
    }
}
