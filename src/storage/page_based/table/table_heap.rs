#![allow(warnings)]

use std::sync::{Mutex, Arc};

use crate::{buffer::buffer_pool_manager::BufferPoolManager, common::{config::page_id_t, rid::RID}, storage::page_based::page::table_page::TablePage};

use super::{tuple::{TupleMeta, Tuple}, table_iter::TableIter};



// alias
pub type BufferPoolManagerRef = Arc<BufferPoolManager>;
// type LockManagerRef = Arc<LockManager>;

#[derive(Debug)]

struct TableHeapState {
    first_page_id: page_id_t,
    last_page_id: page_id_t,
}

pub type TableHeapRef = Arc<TableHeap>;

#[derive(Debug)]
pub struct TableHeap {
    bpm: BufferPoolManagerRef,
    state: Mutex<TableHeapState>,
}


impl TableHeap {
    // create a new table heap
    pub fn new(bpm: BufferPoolManagerRef) -> Self {
        match bpm.new_page_guarded() {
            None => {
                panic!("New a null page, check the bpm");
            },
            Some(mut page_guard) => {
                let pid = page_guard.get_pid().unwrap();

                let table_page = page_guard.get_mut_as::<TablePage>();
                table_page.init();
                let state = TableHeapState {
                    first_page_id: pid,
                    last_page_id: pid
                };
                TableHeap {
                    bpm: bpm.clone(),
                    state: Mutex::new(state)  
                }
            }
        }
    }

    pub fn insert_tuple(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<RID> {
        let mut lock_ = self.state.lock().unwrap();

        let mut last_page_guard;
        let mut last_pid = lock_.last_page_id;
        loop {
            let page_guard = self.bpm.fetch_page_write(lock_.last_page_id);
            let mut page_guard = page_guard.expect("can not fetch page, bpm err");
            let page = page_guard.get_mut_as::<TablePage>();

            let slot_off = page.get_next_offset(meta, tuple);

            if None != slot_off {
                last_page_guard = page_guard;
                last_pid = lock_.last_page_id;
                break;
            }
            // check if tuple is too large, means zero record, and slot_off is invalid
            assert!(page.get_num_tuples() != 0, "tuple is too large, can not insert");
            //here, means space is not enough, create a new page
            let new_page = self.bpm.new_page_guarded();
            if new_page.is_none() {
                panic!("can not allocate new page");
            }
            let new_page = new_page.unwrap();

            page.set_next_page_id(new_page.get_pid()?);
            lock_.last_page_id = new_page.get_pid()?;
        }   
        
        let last_table_page = last_page_guard.get_mut_as::<TablePage>();

        let slot_id = last_table_page.insert_tuple(meta, tuple);       

        Some(RID {
            pid: last_pid,
            sid: slot_id.unwrap(),
        })
    }

    pub fn update_meta(&self, rid: &RID) {
        todo!()
    }

    pub fn get_tuple(&self, rid: &RID) -> (TupleMeta, Tuple) {
        let page_guard = self.bpm.fetch_page_read(rid.pid).unwrap();
        let table_page = page_guard.get_as::<TablePage>();
        match table_page.get_tuple(rid) {
            Err(err) => {
                eprintln!("{}", err);
                panic!("{}", err);
            },
            Ok(tuple_pair) => tuple_pair,
        }
    }

    pub fn get_meta(&self, rid: &RID) -> TupleMeta {
        todo!()
    }

    pub fn make_iterator(self: Arc<Self>) -> TableIter {
        let _lock = self.state.lock().unwrap();
        let last_pid = _lock.last_page_id;


        let page_guard = self.bpm.fetch_page_read(last_pid).unwrap();
        let table_page = page_guard.get_as::<TablePage>();

        TableIter::new(self.clone(), 
            RID {pid: _lock.first_page_id, sid: 0},
            RID { pid: last_pid, sid: table_page.get_num_tuples()} 
        )
    }

    pub fn get_bpm(&self) -> BufferPoolManagerRef {
        self.bpm.clone()
    }

    
}







#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Instant};

    use rand::Rng;

    use crate::{buffer::buffer_pool_manager::BufferPoolManager, storage::page_based::{disk::{disk_manager::DiskManager, log_manager::LogManager}, table::tuple::{TupleMeta, Tuple}}, catalog::{schema::Schema, column::Column}, common::config::INVALID_TXN_ID, typedef::{type_id::TypeId, value::Value}};

    use super::TableHeap;
    
    fn create_bpm(pool_size: usize, k: usize) -> BufferPoolManager {
        // setup
        let disk_manager = DiskManager::new("test.db").unwrap();
        let bpm = BufferPoolManager::new(pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));
        bpm
    }

    fn create_table_heap() -> TableHeap {
        let bpm = create_bpm(20, 3);

        TableHeap::new(Arc::new(bpm))
    }

    fn create_schema() -> Schema {
        let schema = "a varchar(20), b int, c int, e varchar(16)";
        let col1 = Column::new_varchar("a", TypeId::VARCHAR, 20);
        let col2 = Column::new("b", TypeId::INTEGER);
        let col3 = Column::new("c", TypeId::INTEGER);
        let col4 = Column::new_varchar("e", TypeId::VARCHAR, 16);
        
        let cols = vec![col1, col2, col3, col4];
        Schema::new(&cols)
    }
    fn generate_tuple(schema: &Schema) -> Tuple {
        let mut rand_eng = rand::thread_rng();

        // according to the schema to build a tuple
        let mut values = Vec::new();

        let alphanums: String = String::from("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        let alphanums: Vec<char> = alphanums.chars().collect();
        for i in 0..schema.get_column_count() {
            let column = schema.get_column(i).ok().unwrap();

            match column.get_type() {
                TypeId::INTEGER => {
                    let random_val = rand_eng.gen_range(0..=1000);
                    values.push(Value::new_integer(TypeId::INTEGER, random_val));
                },
                TypeId::VARCHAR => {
                    let len = rand_eng.gen_range(1..=32);
                    let mut s = String::from("");
                    for i in 0..len {
                        let rand_idx = rand_eng.gen_range(0..alphanums.len());
                        s.push(alphanums[rand_idx]);
                    }
                    values.push(Value::new_varchar(TypeId::VARCHAR, &s));
                },
                _ => {
                    panic!("Not support type");
                }
            }
        }   

        Tuple::build(&values, schema)
    }

    #[test]
    fn test_sample() {
        let table_heap = create_table_heap();

        let schema = create_schema();
        let tuple = generate_tuple(&schema);

        let meta = TupleMeta {
            insert_txn_id: INVALID_TXN_ID,
            delete_txn_id: INVALID_TXN_ID,
            is_deleted: false,
        };

        let start = Instant::now();

        for i in 0..10000 {
            let tuple = generate_tuple(&schema);
            table_heap.insert_tuple(&meta, &tuple);
        }

        println!("{:.2?}", start.elapsed());


        let start = Instant::now();

        // let mut iter = table_heap.make_iterator();
        // while let Some((meta, tuple)) = iter.next() {
        //     println!("{:#?}", tuple.to_string(&schema));
        // }
    }
}
