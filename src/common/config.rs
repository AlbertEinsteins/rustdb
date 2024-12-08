pub const INVALID_PAGE_ID: i32 = -1;
pub const INVALID_TXN_ID: i32 = -1;
pub const INVALID_LSN: i32 = -1;
pub const HEADER_PAGE_ID: i32 = 0;
pub const PAGE_SIZE: i32 = 1 << 12;
pub const BUFFER_POOL_SIZE: i32 = 10;
pub const BUCKET_SIZE: i32 = 50;
pub const LRUK_REPLACER_K: i32 = 10;



// =================== define the type alias ================
pub type frame_id_t = usize;
pub type page_id_t = i32;
pub type txn_id_t = i32;
pub type lsn_t = i32;
pub type slot_id_t = u16;
pub type oid_t = u16;
pub type table_id_t = u32;
pub type index_id_t = u32;