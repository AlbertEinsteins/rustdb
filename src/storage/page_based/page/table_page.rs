#![allow(warnings)]

use crate::{common::{config::{page_id_t, PAGE_SIZE, INVALID_PAGE_ID}, rid::RID}, storage::page_based::table::tuple::{Tuple, TupleMeta}};




/// | offset | tuple length  | meta|
pub type TupleInfo = (u16, u16, TupleMeta);
const TUPLE_INFO_SIZE: usize = size_of::<TupleInfo>();
const PAGE_HEADER_SIZE: usize = size_of::<TablePage>();
// how many tuple infos can be satisfied in a page
const TUPLE_INFO_MAX_LEN: usize = (PAGE_SIZE as usize - PAGE_HEADER_SIZE) / TUPLE_INFO_SIZE;

#[repr(C)]
pub struct TablePage {
    page_start: [u8; 0],
    next_page_id: page_id_t,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuple_infos: [TupleInfo; 0]
}



impl TablePage {
    
    pub fn init(&mut self) {
        self.next_page_id = INVALID_PAGE_ID;
        self.num_tuples = 0;
        self.num_deleted_tuples = 0;
    }

    pub fn get_num_tuples(&self) -> u16 {
        self.num_tuples
    }

    pub fn get_next_page_id(&self) -> page_id_t {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, next_pid: page_id_t) {
        self.next_page_id = next_pid;
    }


    /// return slot id if has enough space
    /// otherwise, return None
    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        let insert_pos = self.get_next_offset(meta, tuple);
        match insert_pos {
            None => None,
            Some(write_pos) => {
                // write bytes
                let write_pos = write_pos as usize;
                let bytes = tuple.get_data();
                // transfer [u8; 0] -> [u8; 4096]

                // println!("page_start: {:p}", self.page_start.as_mut_ptr());
                let page_start = self.get_page_slice_mut();
                page_start[write_pos..write_pos+bytes.len()].copy_from_slice(&bytes);

                // write meta
                let slot_idx = self.num_tuples;
                let tuple_infos = self.get_tuple_info_slice_mut();
                tuple_infos[slot_idx as usize] = (
                    write_pos as u16,
                    tuple.get_length() as u16,
                    meta.clone(),
                );
                self.num_tuples += 1;
                Some(slot_idx)
            }
        }
    }

    // update tuple meta by rid
    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) -> Result<(), String> {
        if rid.sid >= self.num_tuples {
            return Err(format!("Slot idx is out of range"));
        }
        let slot_idx = rid.sid as usize;
        let num_tuples = self.num_tuples;
        let infos = self.get_tuple_info_slice();
        let (off, len, meta) = infos[slot_idx].clone();
        if !meta.is_deleted && meta.is_deleted {
            self.num_deleted_tuples = num_tuples + 1;
        }

        let infos = self.get_tuple_info_slice_mut();
        infos[slot_idx] = (off, len, meta);
        Ok(())
    }

    pub fn update_tuple_meta_inplace(&mut self, meta: &TupleMeta, tuple: &Tuple, rid: &RID) {
        todo!()
    }

    // returns the tuple in phisical page at the rid
    pub fn get_tuple(&self, rid: &RID) -> Result<(TupleMeta, Tuple), String> {
        if rid.sid >= self.num_tuples {
            return Err(format!("slot id out of range"));
        }

        let infos = self.get_tuple_info_slice();
        let (off, len, meta) = infos[rid.sid as usize].clone(); 
        let (off, len) = (off as usize, len as usize);
        let page = self.get_page_slice();

        Ok((meta, Tuple::deserialize(&Vec::from(&page[off..off+len]))?))
    }

    pub fn get_tuple_meta(&self, rid:  &RID) -> Result<TupleMeta, String> {
        if rid.sid >= self.num_tuples {
            return Err(format!("slot id out of range"));
        }

        let infos = self.get_tuple_info_slice();
        let (_, _, meta) = infos[rid.sid as usize].clone();
        Ok(meta)
    }

    /// ================== private methods ===============
    fn get_page_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.page_start.as_ptr(), PAGE_SIZE as usize)
        }
    }

    fn get_tuple_info_slice(&self) -> &[TupleInfo] {
        unsafe {
            std::slice::from_raw_parts(self.tuple_infos.as_ptr(), TUPLE_INFO_MAX_LEN)
        }
    }
    fn get_page_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.page_start.as_mut_ptr(), PAGE_SIZE as usize)
        }
    }

    fn get_tuple_info_slice_mut(&mut self) -> &mut [TupleInfo] {
        unsafe {
            std::slice::from_raw_parts_mut(self.tuple_infos.as_mut_ptr(), TUPLE_INFO_MAX_LEN)
        }
    }
    fn get_slot_offset(&self, slot_idx: u16) -> u16 {
        // return the offset of tuple in page 
        if slot_idx >= self.num_tuples {
            panic!("Slot idx {} is greater than num {}", slot_idx, self.num_tuples);
        }
        let tuple_infos = unsafe {
            std::slice::from_raw_parts(self.tuple_infos.as_ptr(), TUPLE_INFO_MAX_LEN)
        };
        return tuple_infos[slot_idx as usize].0;
    }

    /// return the tuple store position
    pub fn get_next_offset(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        let mut tuple_end_offset = {
            if self.num_tuples > 0 {
                self.get_slot_offset(self.num_tuples - 1) as i32
            } else {
                PAGE_SIZE
            }
        };

        tuple_end_offset = tuple_end_offset - tuple.get_length() as i32;
        let slot_end_offset = PAGE_HEADER_SIZE + 
                            (self.num_tuples + 1) as usize * TUPLE_INFO_SIZE;

        if tuple_end_offset < (slot_end_offset as i32) {
            return None;
        }
        Some(tuple_end_offset as u16)
    }
}



#[cfg(test)]
mod tests {
    use crate::{common::rid::RID, storage::page_based::{page::table_page::PAGE_HEADER_SIZE, table::tuple::{Tuple, TupleMeta}}};

    use super::TablePage;

    #[test]
    fn test() {
        println!("{}", PAGE_HEADER_SIZE)
    }


    #[test]
    fn sample_insert_test() {
        let mut buf: [u8; 4096] = [0; 4096];

        let t_page = unsafe { &mut *(buf.as_mut_ptr() as *mut TablePage) };

        let meta = TupleMeta {
            insert_txn_id: -1,
            delete_txn_id: -1,
            is_deleted: false,
        };
        let tuple = Tuple::new();

        println!("buf_: {:p}, page_start = {:p}", buf.as_mut_ptr(), t_page.page_start.as_mut_ptr());

        let slot_id = t_page.insert_tuple(&meta, &tuple);
        println!("{:#?}", slot_id);

        println!("buf_: {:p}, page_start = {:p}", buf.as_mut_ptr(), t_page.page_start.as_mut_ptr());

        let slot_id = t_page.insert_tuple(&meta, &tuple);   
        println!("{:#?}", slot_id);

    }


    #[test]
    fn insert_get_test() {
        let mut buf: [u8; 4096] = [0; 4096];
        let table_page = unsafe { &mut *(buf.as_mut_ptr() as *mut TablePage) };


        let meta = TupleMeta {
            insert_txn_id: -12,
            delete_txn_id: -1,
            is_deleted: true,
        };
        let tuple = Tuple::new();
        let slod_id = table_page.insert_tuple(&meta, &tuple);
        let rid = RID {  
            pid: 0,
            sid: slod_id.unwrap(),
        };

        // get tuple

        let tuple_res = table_page.get_tuple(&rid);
        match tuple_res {
            Err(_) => {},
            Ok((meta, t)) => {
                println!("{:#?}", meta);
            },
        }
    }

}