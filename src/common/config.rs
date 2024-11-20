pub static INVALID_PAGE_ID: i32 = -1;
pub static INVALID_TXN_ID: i32 = -1;
pub static INVALID_LSN: i32 = -1;
pub static HEADER_PAGE_ID: i32 = 0;
pub static PAGE_SIZE: i32 = 1 << 12;
pub static BUFFER_POOL_SIZE: i32 = 10;
pub static BUCKET_SIZE: i32 = 50;
pub static LRUK_REPLACER_K: i32 = 10;



// =================== define the type alias ================
pub type frame_id_t = i32;
pub type page_id_t = i32;
pub type txn_id_t = i32;
pub type lsn_t = i32;
pub type slot_offset_t = usize;
pub type oid_t = u16;

