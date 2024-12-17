
use std::sync::Arc;

use crate::{common::{rid::RID, config::INVALID_PAGE_ID}, storage::page_based::page::table_page::TablePage};

use super::{table_heap::{TableHeap, TableHeapRef}, tuple::{Tuple, TupleMeta}};

pub struct TableIter {
    table_heap: TableHeapRef,
    cur_rid: RID,
    end_rid: RID,
}
impl TableIter {

    pub fn new(table_heap: TableHeapRef, cur: RID, end: RID) -> Self {
        Self {
            table_heap: table_heap, cur_rid: cur, end_rid: end
        }
    }
}

impl Iterator for TableIter {
    type Item = (TupleMeta, Tuple);

    fn next(&mut self) -> Option<Self::Item> {
        match self.cur_rid.pid {
            INVALID_PAGE_ID => None,
            pid => {
                if self.cur_rid == self.end_rid {
                    return None;
                }

                let bpm = self.table_heap.get_bpm();
                let page_guard = bpm.fetch_page_read(pid).unwrap();
                let table_page = page_guard.get_as::<TablePage>();

                let cur_tuple = self.table_heap.get_tuple(&self.cur_rid);
                
                if self.cur_rid.sid + 1 < table_page.get_num_tuples() {
                    self.cur_rid.sid += 1;
                } else {
                    // turn to next page
                    if self.cur_rid.pid == self.end_rid.pid {
                        self.cur_rid = RID { pid: INVALID_PAGE_ID, sid: 0 }
                    } else {
                        self.cur_rid = RID { pid: pid + 1, sid: 0  };

                    }
                }

                Some(cur_tuple)
            }
        }        
        
    }
}