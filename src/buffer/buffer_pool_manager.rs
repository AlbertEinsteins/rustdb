#![allow(warnings)]

use std::{borrow::Borrow, collections::{HashMap, LinkedList}, sync::{atomic::{AtomicI32, Ordering}, Arc, Mutex, MutexGuard }, usize};

use crate::{common::config::{frame_id_t, page_id_t}, storage::page_based::{disk::{disk_manager::DiskManager, disk_scheduler::DiskScheduler, log_manager::LogManager}, page::{page::Page, page_guard::{PageGuard, ReadPageGuard, WritePageGuard}}}};

use super::lruk_replacer::{LRUKReplacer, Replacer};




pub type PageRef<'a> = &'a mut Page;


#[derive(Debug, )]
struct BufferPoolManager_ {
    pub disk_scheduler: Box<DiskScheduler>,
    pub log_manager: Arc<LogManager>,
    pub pages: Vec<Page>,
    pub page_table: HashMap<page_id_t, frame_id_t>,
    pub free_list: LinkedList<usize>,
    pub pool_size: usize,
    // page id generator
    pub page_id_generator: AtomicI32,
    pub lru_replacer: Box<LRUKReplacer>,
}

#[derive(Debug)]
pub struct BufferPoolManager {
    // to protect state(BufferPoolManager_)
    state: Mutex<BufferPoolManager_>,
}

const INVALID_FRAME_ID: usize = usize::MAX;

impl BufferPoolManager {
    // Create a new buffer pool manager with n size and k related to replacer
    pub fn new(pool_size: usize, disk_mgr: Arc<DiskManager>, k_replacer: usize, lgr: Arc<LogManager>) -> Self {
        let mut bpm = BufferPoolManager_ {
            disk_scheduler: Box::new(DiskScheduler::new(disk_mgr)),
            log_manager: lgr,
            pool_size: pool_size,
            page_id_generator: AtomicI32::new(0),
            pages: Vec::new(),
            lru_replacer: Box::new(LRUKReplacer::new(pool_size, k_replacer)),
            page_table: HashMap::new(),
            free_list: LinkedList::new(),
        };

        bpm.pages.reserve(pool_size);
        for _ in 0..pool_size {
            bpm.pages.push(Page::new());
        }

        for i in 0..pool_size {
            bpm.free_list.push_back(i);
        }

        Self {
            state: Mutex::new(bpm),
        }
    }

    // return a new page if it creates successfully,
    // otherwise, return None
    pub fn new_page(&self) -> Option<PageRef> {
        let mut _lock = self.state.lock().unwrap();

        let mut fid = usize::MAX;
        {
            let state = &mut _lock;
            if !state.free_list.is_empty() {
                fid = state.free_list.pop_front().unwrap();
            }

            if INVALID_FRAME_ID == fid { // if still invalid
                if let Some(frame_id) = state.lru_replacer.evict() {
                    fid = frame_id;
                }
            }

            if INVALID_FRAME_ID == fid {
                return None;
            }
        }

        // then flush the dirty page
        let fid = fid as usize;
        let (pid, is_dirty) = {
            let page = &_lock.pages[fid];
            (page.page_id, page.is_dirty)
        };

        // let a = &mut _lock.pages[fid];
        let page = unsafe { &mut *(&mut _lock.pages[fid] as *mut Page) };
        if is_dirty {
            Self::write_page_data(&_lock.disk_scheduler, pid, page);
        }

        // set meta for new page in the frame
        let pid = self.allocate_page(&mut _lock);
        Self::reset_meta(&mut _lock.pages[fid], pid);
        _lock.pages[fid].pin_count = 1;
        _lock.lru_replacer.record_access(fid);
        _lock.page_table.insert(pid, fid as frame_id_t);

        Some(page)     
    }

    // fetch an existing page from bpm or disk
    pub fn fetch_page(&self, page_id: page_id_t) -> Option<PageRef> {
        let mut _lock = self.state.lock().unwrap();


        // check if in page_table
        if !_lock.page_table.contains_key(&page_id) {
            return None;
        }

        let mut find_idx = _lock.pages.len();
        for idx in 0.._lock.pages.len() {
            let p = &_lock.pages[idx];
            if p.page_id == page_id {
                find_idx = idx;
                break;
            }
        }

        if find_idx < _lock.pages.len() {
            // let p = unsafe { &mut *(&mut _lock.pages[find_idx] as *mut Page) };
            let p = &mut _lock.pages[find_idx];
            p.pin_count += 1;
            if p.pin_count > 0 {
                _lock.lru_replacer.set_evictable(find_idx, false);
            }
            let rtn = unsafe { &mut *(&mut _lock.pages[find_idx] as *mut Page) };
            return Some(rtn);
            // return None;
        }

        // check available frame in free_list
        let mut fid = usize::MAX;
        if !_lock.free_list.is_empty() {
            fid = _lock.free_list.pop_front().unwrap();
        } else if let Some(f) = _lock.lru_replacer.evict() {
            // do nothing, just receive a fid
            fid = f
        } else {
            return None;
        }

        let mut page = unsafe { &mut *(&mut _lock.pages[fid] as *mut Page) };
        if page.is_dirty {
            Self::write_page_data(&_lock.disk_scheduler, page.page_id, page);
        }

        // then set meta, read data
        Self::reset_meta(&mut page, page_id);
        Self::read_page_data(&_lock.disk_scheduler, page_id, &mut page);
        page.pin_count = 1;
        _lock.lru_replacer.record_access(fid);
        // update page table
        _lock.page_table.insert(page_id, fid);
        Some(page)
    }

    // new a page wrapped by a page guard
    pub fn new_page_guarded(&self) -> Option<PageGuard> {
        let page = self.new_page();
        match page {
            None => None,
            Some(page) => {
                Some(PageGuard::new(self, page))
            }
        }
    }

    pub fn fetch_page_basic(&self, page_id: page_id_t) -> Option<PageGuard> {
        let page = self.fetch_page(page_id);
        match page {
            None => None,
            Some(page) => {
                Some(PageGuard::new(self, page))
            }
        }
    }

    pub fn fetch_page_read(&self, page_id: page_id_t) -> Option<ReadPageGuard> {
        let page = self.fetch_page(page_id);
        match page {
            None => None,
            Some(page) => {
                let lock = page.get_read_lock();

                // cross the compiler checking
                let mut_page = unsafe {
                    &mut *(page as *const Page as *mut Page)
                };

                let guard = PageGuard::new(self, mut_page);
                Some(ReadPageGuard::new(guard, lock))
            }
        }
    }

    pub fn fetch_page_write(&self, page_id: page_id_t) -> Option<WritePageGuard> {
        let page = self.fetch_page(page_id);
        match page {
            None => None,
            Some(page) => {
                let lock = page.get_write_lock();

                // cross the compiler checking
                let mut_page = unsafe {
                    &mut *(page as *const Page as *mut Page)
                };

                let guard = PageGuard::new(self, mut_page);
                Some(WritePageGuard::new(guard, lock))
            }
        }
    }

    /// unpin a page
    /// we need to decrease the pin_count, and when it becomes 0, then make it evictable
    /// then, set the dirty flag if need
    pub fn unpin_page(&self, page_id: page_id_t, is_dirty: bool) -> bool {
        let mut _lock = self.state.lock().unwrap();

        let mut find_idx = usize::MAX;
        let frames = &mut _lock.pages;

        for idx in 0..frames.len() {
            let page = &frames[idx];
            if page.page_id == page_id {
                find_idx = idx;
                break;
            }
        }

        if usize::MAX == find_idx {
            return false;
        } 
        let page = &mut frames[find_idx];
        if 0 == page.pin_count {
            return false;
        }

        // then check if need set is_dirty
        if !page.is_dirty {
            page.is_dirty = is_dirty;
        }

        // then decrease pin_count
        page.pin_count -= 1;
        if 0 == page.pin_count {
            _lock.lru_replacer.set_evictable(find_idx, true);
        }
        return true;
    }

    pub fn flush_page(&self, page_id: page_id_t) -> bool {
        let mut _lock = self.state.lock().unwrap();

        let val = _lock.page_table.get(&page_id);

        match val {
            None => {
                return false;
            },
            Some(frame_id) => {
                let frame_id = *frame_id;
                
                let frame = unsafe { &mut *(&mut _lock.pages[frame_id] as *mut Page)};
                if frame.page_id != page_id {
                    return false;
                }

                Self::write_page_data(&_lock.disk_scheduler, page_id, frame);
                frame.is_dirty = false;
                return true;
            }
        }
    }

    pub fn flush_all_pages(&mut self) {

    }

    /// delete page in bufferpool, if not exists, return false
    /// if pin_count > 0, then return false
    /// else remove lru record, and bufferpool , then move it to free_list
    pub fn delete_page(&self, page_id: page_id_t) -> bool {
        let mut _lock = self.state.lock().unwrap();

        let mut find_idx: usize = INVALID_FRAME_ID;
        for idx in 0.._lock.pages.len() {
            if _lock.pages[idx].get_pid() == page_id {
                find_idx = idx;
                break;
            }
        }

        if INVALID_FRAME_ID == find_idx {
            return true;
        }

        // check pinnable
        let p = &_lock.pages[find_idx];
        if p.get_pincount() > 0 {
            return false;
        }

        _lock.page_table.remove(&page_id);
        _lock.lru_replacer.remove(find_idx);

        let p = &mut _lock.pages[find_idx];
        Self::reset_meta(p, page_id);
        _lock.free_list.push_back(find_idx);
        
        self.deallocate_page(page_id);
        return true;
    }

    // define the private functions
    // 
    // 

    /// this is not a safe guard function, better surroundded with lock
    fn read_page_data(disk_scheduler: &DiskScheduler, page_id: page_id_t, page: &mut Page) {
        let request = DiskScheduler::create_request(
            false, 
            page.get_mut_data(), 
            page_id);
        disk_scheduler.schedule(Some(request.clone()));
        request.callback.wait();
    }

    /// this is not a safe guard function, better surroundded with lock
    fn write_page_data(disk_scheduler: &DiskScheduler, page_id: page_id_t, page: &mut Page) {
        // let page = unsafe { &mut *page };
        let request = DiskScheduler::create_request(
            true, 
            page.get_mut_data(), 
            page_id);
        disk_scheduler.schedule(Some(request.clone()));
        request.callback.wait();
    }

    fn allocate_page(&self, lock_guard: &mut MutexGuard<BufferPoolManager_>) -> page_id_t {
        let pid = lock_guard.page_id_generator.fetch_add(1, Ordering::SeqCst);
        return pid; 
    }

    fn deallocate_page(&self, page_id: page_id_t) {
        // rightnow, it is a no-op 

    }

    fn reset_meta(page: &mut Page, page_id: page_id_t) {
        page.page_id = page_id;
        page.is_dirty = false;
        page.pin_count = 0;
        page.data.fill(0);
    }
}



#[cfg(test)]
mod tests {
    
    use std::{borrow::BorrowMut, fs, sync::Arc, thread};

    use rand::{seq::SliceRandom, Rng};

    use crate::storage::page_based::{disk::{disk_manager::DiskManager, log_manager::LogManager}, page::page::Page};

    use super::{BufferPoolManager};

    fn compare_value(expected_str: &str, page: &Page) -> bool {
        let len = expected_str.len();
        let page_data = page.get_data();
        page_data[..len] == expected_str.as_bytes().to_vec()
    }

    fn fill_value(page: &mut Page, fill_val: &str) {
        let len = fill_val.len();
        let page_data = page.get_mut_data();
        page_data[..len].copy_from_slice(fill_val.as_bytes());
    }

    fn create_bpm(pool_size: usize, k: usize) -> BufferPoolManager {
        // setup
        let disk_manager = DiskManager::new("test.db").unwrap();
        let bpm = BufferPoolManager::new(pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));
        bpm
    }

    #[test]
    fn sample_test() {
        let db_name = "test.db";
        let buffer_pool_size = 10;
        let k = 5;

        // setup
        let disk_manager = DiskManager::new(&db_name).unwrap();
        let bpm = BufferPoolManager::new(buffer_pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));

        // create a new page
        let page = bpm.new_page().unwrap();
        assert_eq!(0, page.get_pid());

        let buf: &mut Vec<u8> = &mut page.data;
        let s = "hello";
        buf[0..s.len()].copy_from_slice(s.as_bytes());

        // fill up the buffer pool
        for i in 1..buffer_pool_size {
            assert_ne!(None, bpm.new_page());
        }

        // create page will get none
        for i in buffer_pool_size..buffer_pool_size*2 {
            assert_eq!(None, bpm.new_page());
        }

        // scenario: unpin pages [0, 1, 2, 3, 4], pin anthoer 4 pages
        for i in 0..5 {
            assert_eq!(true, bpm.unpin_page(i, true));
        }

        for i in 0..4 {
            assert_ne!(None, bpm.new_page());
        }

        // then fetch page 0. cmp data wrote before
        let page = bpm.fetch_page(0).unwrap();
        let buf = &page.borrow_mut().data;
        println!("{:#?}", String::from_utf8(buf[..s.len()].to_vec()).unwrap_or_default());
        assert_eq!(true, bpm.unpin_page(0, true));

        // then new page, and could not fetch page 0
        assert_ne!(None, bpm.new_page());
        assert_eq!(None, bpm.fetch_page(0));

    }


    #[test]
    fn binary_data_test() {
        let db_name = "test.db";
        let buffer_pool_size = 10;
        let k = 5;

        // setup
        let disk_manager = DiskManager::new(&db_name).unwrap();
        let bpm = BufferPoolManager::new(buffer_pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));


        // create page0
        let page0 = bpm.new_page().unwrap();
        assert_eq!(0, page0.borrow_mut().page_id);

        // set the data in page0
        const PAGE_SIZE: usize = 4096 as usize;
        let mut buf = Vec::new();
        buf.resize(PAGE_SIZE, 0);
        let mut random_eng = rand::thread_rng();

        for i in 0..PAGE_SIZE {
            buf[i] = random_eng.gen::<u8>();
        }
        buf[PAGE_SIZE / 2] = b'\0';
        buf[PAGE_SIZE - 1] = b'\0';

        // copy data
        let page0 = page0.borrow_mut();
        let data = &mut page0.data;
        data.copy_from_slice(buf.as_slice());
        assert_eq!(true, (page0.data) == buf);

        // fill up the buffer pool
        for i in 1..buffer_pool_size {
            assert_ne!(None, bpm.new_page());
        }

        // then new page will failed
        for i in buffer_pool_size..2*buffer_pool_size {
            assert_eq!(None, bpm.new_page());
        }


        for i in 0..5 {
            assert_eq!(true, bpm.unpin_page(i, true));
        }

        for i in 0..4 {
            assert_ne!(None, bpm.new_page());
        }

        let page = bpm.fetch_page(0).unwrap();
        let recv_buf = &page.borrow_mut().data;

        assert_eq!(true, buf == *recv_buf);
    }

    #[test]
    fn new_paee_test() {
        let db_name = "test.db";
        let buffer_pool_size = 10;
        let k = 5;

        // setup
        let disk_manager = DiskManager::new(&db_name).unwrap();
        let bpm = BufferPoolManager::new(buffer_pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));

        
        // create page
        let mut page_ids = Vec::new();
        for i in 0..buffer_pool_size {
            let page = bpm.new_page().unwrap();
            // assert_ne!(None, page)
            
            let page = page.borrow_mut();
            let data = page.get_mut_data();
            // copy data
            let fake_data = format!("{}", i);
            data[..fake_data.len()].copy_from_slice(fake_data.as_bytes());
            page_ids.push(page.get_pid());
        }

        // all pages are pinned, the bp is full
        for i in 0..100 {
            assert_eq!(None, bpm.new_page());
        }

        // unpin 5 first pages, set as dirty
        for i in 0..5 {
            assert_eq!(true, bpm.unpin_page(i, true))
        }

        // create 5 new pages
        for i in 0..5 {
            let page = bpm.new_page().unwrap();
            page_ids[i] = page.get_pid();
        }

        // allocate new pages will fail
        for i in 0..100 {
            assert_eq!(None, bpm.new_page());
        }

        // then unpin 5 page with no dirty flag
        for idx in 0..5 {
            assert_eq!(true, bpm.unpin_page(page_ids[idx], false));
        }

        // create 5 new pages
        for i in 0..5 {
            let page = bpm.new_page();
            assert_ne!(None, page);
        }

        // create page will fail
        for i in 0..100 {
            assert_eq!(None, bpm.new_page());
        }
        
        match fs::remove_file("test.db") {
            Ok(_) => {
                println!("")
            },
            Err(er) => {
                println!("remove file failed, {}", er);
            }
        }
    }

    #[test]
    fn unpin_page_test() {
        let disk_mgr = DiskManager::new("test.db").unwrap();
        let log_mgr = LogManager::new();
        let bpm = BufferPoolManager::new(2, Arc::new(disk_mgr), 5, Arc::new(log_mgr));
        let mut pid0 = 0;
        let mut pid1 = 0;

        {
            let page0 = bpm.new_page();
            assert_ne!(None, page0);
            let page0 = page0.unwrap();
            page0.data[.."page0".len()].copy_from_slice("page0".as_bytes());

            pid0 = page0.get_pid();
        }

        {
            // create page1
            let page1 = bpm.new_page();
            assert_ne!(None, page1);
            let page1 = page1.unwrap();
            page1.data[.."page1".len()].copy_from_slice("page1".as_bytes());
            pid1 = page1.get_pid();
        }

        assert_eq!(true, bpm.unpin_page(pid0, true));
        assert_eq!(true, bpm.unpin_page(pid1, true));

        // create 2 new pages
        for _ in 0..2 {
            if let Some(page) = bpm.new_page() {
                let pid = page.get_pid();
                assert_eq!(true, bpm.unpin_page(pid, true));
            } else {
                panic!("expected non-none");
            }
        }

        // fetch p0 p1, update p1, set dirty, but update p0, do not set dirty
        let page0_dt = "page0";
        let page0 = bpm.fetch_page(pid0);
        let page0 = page0.unwrap();
        assert_eq!(true, page0.data[..page0_dt.len()] == "page0".as_bytes().to_vec());

        let page1 = bpm.fetch_page(pid1);
        let page1 = page1.unwrap();
        assert_eq!(true, page1.data[..page0_dt.len()] == "page1".as_bytes().to_vec());

        assert_eq!(true, bpm.unpin_page(pid0, true));
        assert_eq!(true, bpm.unpin_page(pid1, true));
        // create 2 new
        for _ in 0..2 {
            let page = bpm.new_page().unwrap();
            let pid = page.get_pid();
            bpm.unpin_page(pid, true);
        }


        // re-fetch p0,p1 update
        let page0 = bpm.fetch_page(pid0).unwrap();
        let dt1 = "page0updated";
        let page0_data = page0.get_mut_data();
        page0_data[..dt1.len()].copy_from_slice(dt1.as_bytes());

        let page1 = bpm.fetch_page(pid1).unwrap();
        let dt2 = "page1updated";
        let page1_data = page1.get_mut_data();
        page1_data[..dt2.len()].copy_from_slice(dt2.as_bytes());

        assert_eq!(true, bpm.unpin_page(pid0, false));
        assert_eq!(true, bpm.unpin_page(pid1, true));

        // refetch p0,p1, check
        let page0 = bpm.fetch_page(pid0).unwrap();
        let p0_data = page0.get_data();
        assert_eq!(true, p0_data[.."page0".len()] == "page0".as_bytes().to_vec());

        let page1 = bpm.fetch_page(pid1).unwrap();
        let p1_data = page1.get_data();
        assert_eq!(true, p1_data[..dt2.len()] == dt2.as_bytes().to_vec());

        let _ = fs::remove_file("test.db");
    }


    #[test]
    fn fetch_test() {
        let bpm = create_bpm(10, 5);

        let mut page_ids = Vec::new();
        let mut contents = Vec::new();

        for i in 0..10 {
            
            let p = bpm.new_page().unwrap();
            let write_data = format!("{}", i);
            let pid = p.get_pid();
            fill_value(p, &write_data);
            page_ids.push(pid);
            contents.push(write_data);
        }


        // fetch then unpin and flush
        for i in 0..10 {
            let p = bpm.fetch_page(page_ids[i]);
            match p {
                None => panic!("asd"),
                Some(page) => {
                    let pid = page.page_id;
                    assert_eq!(true, compare_value(&format!("{}", i), page));
                    assert_eq!(true, bpm.unpin_page(pid, true));
                    assert_eq!(true, bpm.unpin_page(pid, true));
                    bpm.flush_page(pid);
                }
            }
        }

        // create 10 pages, then unpin it
        for i in 0..10 {
            let page = bpm.new_page();
            assert_ne!(None, page);
            let pid = page.unwrap().get_pid();
            bpm.unpin_page(pid, true);
        }

        for i in 0..10 {
            bpm.fetch_page(page_ids[i]);
        }

        // unpin page 4, create new page, then fetch 4 will fail
        assert_eq!(true, bpm.unpin_page(page_ids[4], true));
        let new_page = bpm.new_page();
        assert_ne!(None, new_page);
        assert_eq!(None, bpm.fetch_page(page_ids[4]));

        // check clock
        {
            let p5 = bpm.fetch_page(page_ids[5]);
            assert_ne!(None, p5);
            fill_value(p5.unwrap(), "updatepage5");
        }
        {
            let p6 = bpm.fetch_page(page_ids[6]);
            assert_ne!(None, p6);
            fill_value(p6.unwrap(), "updatepage6");
        }
        let p7 = bpm.fetch_page(page_ids[7]);
        assert_ne!(None, p7);

        // update page 5,6,7, but set non-dirty flag
        fill_value(p7.unwrap(), "updatepage7");
        assert_eq!(true, bpm.unpin_page(5, false));
        assert_eq!(true, bpm.unpin_page(6, false));
        assert_eq!(true, bpm.unpin_page(7, false));
        
        assert_eq!(true, bpm.unpin_page(5, false));
        assert_eq!(true, bpm.unpin_page(6, false));
        assert_eq!(true, bpm.unpin_page(7, false));

        // page5 evict
        let new_page = bpm.new_page();
        assert_ne!(None, new_page);

        let new_page = new_page.unwrap();
        let pid = new_page.get_pid();
        // page6 will evict
        let p5 = bpm.fetch_page(page_ids[5]);
        assert_ne!(None, p5);
        assert_eq!(true, compare_value("5", p5.unwrap()));
        let p7 = bpm.fetch_page(page_ids[7]);
        assert_ne!(None, p7);
        assert_eq!(true, compare_value("updatepage7", p7.unwrap()));

        // all pages pinned
        assert_eq!(None, bpm.fetch_page(page_ids[6]));
        bpm.unpin_page(pid, false);
        let p6 = bpm.fetch_page(page_ids[6]);
        assert_ne!(None, p6);
        let p6 = p6.unwrap();
        assert_eq!(true, compare_value("6", p6));

        // set page6 new data
        fill_value(p6, "updatepage6");

        // new page will fail
        let new_page = bpm.new_page();
        assert_eq!(None, new_page);

        // unpin 6 7
        assert_eq!(true, bpm.unpin_page(page_ids[6], false));
        assert_eq!(true, bpm.unpin_page(page_ids[7], false));

        let mut pid = 0;
        {

            let new_page = bpm.new_page();
            assert_ne!(None, new_page);
            let new_page = new_page.unwrap();
            pid = new_page.get_pid();
        }
        { 
            let p6 = bpm.fetch_page(page_ids[6]);
            assert_ne!(None, p6);
            assert_eq!(true, compare_value("updatepage6", p6.unwrap()));
        }
        {
            let p7 = bpm.fetch_page(page_ids[7]);
            assert_eq!(None, p7);
            bpm.unpin_page(pid, false);
        }

        // then will get p7
        let p7 = bpm.fetch_page(page_ids[7]);
        assert_ne!(None, p7);
        assert_eq!(true, compare_value("7", p7.unwrap()));

    }


    #[test]
    fn delete_pagetest() {
        let bpm = create_bpm(10, 5);

        let mut page_ids = Vec::new();
        let mut contents = Vec::new();

        for i in 0..10 {
            
            let p = bpm.new_page().unwrap();
            let write_data = format!("{}", i);
            let pid = p.get_pid();
            fill_value(p, &write_data);
            page_ids.push(pid);
            contents.push(write_data);
        }


        // fetch then unpin and flush
        for i in 0..10 {
            let p = bpm.fetch_page(page_ids[i]);
            match p {
                None => panic!("asd"),
                Some(page) => {
                    let pid = page.page_id;
                    assert_eq!(true, compare_value(&format!("{}", i), page));
                    assert_eq!(true, bpm.unpin_page(pid, true));
                    assert_eq!(true, bpm.unpin_page(pid, true));
                }
            }
        }

        // create 10 new pages
        for i in 0..10 {
            let p = bpm.new_page();
            let pid = p.unwrap().get_pid();
            bpm.unpin_page(pid, true);
        }

        // fetch 
        for i in 0..10 {
            let p = bpm.fetch_page(page_ids[i]);
            assert_ne!(None, p);
        }

        // create will fail
        let p = bpm.new_page();
        assert_eq!(None, p);

        // delete 4 will fail, except unpinning it
        assert_eq!(false, bpm.delete_page(4));
        bpm.unpin_page(4, false);
        assert_eq!(true, bpm.delete_page(4));

        // new page will success
        let p = bpm.new_page();
        assert_ne!(None, p);

        let p5 = bpm.fetch_page(page_ids[5]);
        assert_ne!(None, p5);
        fill_value(p5.unwrap(), "updatepage5");

        let p6 = bpm.fetch_page(page_ids[6]);
        assert_ne!(None, p6);
        fill_value(p6.unwrap(), "updatepage6");

        let p7 = bpm.fetch_page(page_ids[7]);
        assert_ne!(None, p7);
        fill_value(p7.unwrap(), "updatepage7");

        // then unpin twice
        bpm.unpin_page(5, false);
        bpm.unpin_page(6, false);
        bpm.unpin_page(7, false);

        bpm.unpin_page(5, false);
        bpm.unpin_page(6, false);
        bpm.unpin_page(7, false);

        // then delete p7
        assert_eq!(true, bpm.delete_page(7));

        // create new page will occupy page7 then get 5, 6
        bpm.new_page();
        let p5 = bpm.fetch_page(page_ids[5]);
        assert_ne!(None, p5);
        assert_eq!(true, compare_value("updatepage5", p5.unwrap()));

        let p6 = bpm.fetch_page(page_ids[6]);
        assert_ne!(None, p6);
        assert_eq!(true, compare_value("updatepage6", p6.unwrap()));
    }

    #[test]
    fn is_dirty_test() {
        let bpm = create_bpm(1, 5);

        let p0 = bpm.new_page();
        assert_ne!(None, p0);
        let p0 = p0.unwrap();
        assert_eq!(false, p0.is_dirty);
        fill_value(p0, "page0");
        let pid = p0.get_pid();
        assert_eq!(true, bpm.unpin_page(pid, true));

        // fetch again, check is dirty
        let p0 = bpm.fetch_page(pid);
        assert_ne!(None, p0);
        let p0 = p0.unwrap();
        assert_eq!(true, p0.is_dirty);
        assert_eq!(true, bpm.unpin_page(pid, false));

        let p0 = bpm.fetch_page(pid);
        assert_ne!(None, p0);
        let p0 = p0.unwrap();
        assert_eq!(true, p0.is_dirty);
        assert_eq!(true, bpm.unpin_page(pid, false));

        let p1 = bpm.new_page();
        assert_ne!(None, p1);
        let p1 = p1.unwrap();
        assert_eq!(false, p1.is_dirty);

        // write data to p1, then unpin it
        fill_value(p1, "page1");
        let p1id = p1.get_pid();
        let p1_ref = &p1.is_dirty;
        assert_eq!(true, bpm.unpin_page(p1id, true));
        // assert_eq!(true, *p1_ref);
        assert_eq!(true, bpm.delete_page(p1id));

        // re-fetch p0
        let p0 = bpm.fetch_page(pid);
        assert_ne!(None, p0);
        let p0 = p0.unwrap();
        assert_eq!(false, p0.is_dirty);

    }

    /// below is all concurrent test
    #[test]
    fn concurent_test() {
        let num_threads = 5;
        let num_runs = 50;

        for i in 0..num_runs {
            let bpm = create_bpm(50, 10);

            let mut handlers = Vec::new();
            let bpm = Arc::new(bpm);

            for tid in 0..num_threads {
                let bpm_cp = bpm.clone();
                handlers.push(thread::spawn(move || {
                    let mut pids = Vec::new();

                    for pid in 0..10 {
                        let page = bpm_cp.new_page();
                        assert_ne!(None, page);
                        let page = page.unwrap();

                        // fill data
                        fill_value(page, &format!("{}", pid));
                        pids.push(page.get_pid());
                    }

                    // unpin them
                    for idx in 0..10 {
                        assert_eq!(true, bpm_cp.unpin_page(pids[idx], true));
                    }

                    // fetch page again compare then unpin
                    for idx in 0..10 {
                        let page = bpm_cp.fetch_page(pids[idx]);
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        assert_eq!(true, compare_value(&format!("{}", idx), page));
                        assert_eq!(true, bpm_cp.unpin_page(pids[idx], true));
                    }

                    // delete all pages
                    for idx in 0..10 {
                        assert_eq!(true, bpm_cp.delete_page(pids[idx]));
                    }
                }));
            }

            // wait for threads finished
            for h in handlers {
                let _ = h.join();
            }
        }
    }
    

    #[test]
    fn integrated_test() {
        let bpm = create_bpm(10, 5);

        // create 1000, then unpin
        let mut pids = Vec::new();
        for i in 0..1000 {
            for _ in 0..10 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("{}", page.get_pid()));
                pids.push(page.get_pid());
            }

            let s = pids.len() - 10;
            for j in s..pids.len() {
                assert_eq!(true, bpm.unpin_page(pids[j], true));
            }
        }

        // check value
        for pid in 0..1000 {
            let page = bpm.fetch_page(pid);
            assert_ne!(None, page);
            let page = page.unwrap();
            assert_eq!(true, compare_value(&format!("{}", pid), page));
            assert_eq!(true, bpm.unpin_page(pid, true));
        }
    }


    /// test 
    #[test]
    fn hard1_test() {
        let bpm = create_bpm(10, 5);

        // create 1000, then unpin
        let mut pids = Vec::new();
        for i in 0..2 {
            for _ in 0..10 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("{}",page.get_pid( )));
                pids.push(page.get_pid());
            }

            let s = pids.len() - 10;
            for j in s..s+5 {
                assert_eq!(true, bpm.unpin_page(pids[j], false));
            }
            for j in s+5..s+10 {
                assert_eq!(true, bpm.unpin_page(pids[j], true));
            }
        }

        // fetch and check data
        for i in 0..20 {
            let page = bpm.fetch_page(pids[i]);
            assert_ne!(None, page);
            let page = page.unwrap();

            if i % 10 < 5 {
                assert_eq!(false, compare_value(&format!("{}", pids[i]), page));
            } else {
                assert_eq!(true, compare_value(&format!("{}", pids[i]), page));
            }
            assert_eq!(true, bpm.unpin_page(pids[i], true));
        }

        //  fetch data random
        let mut rand_eng = rand::thread_rng();
        pids.shuffle(&mut rand_eng);

        for i in 0..10 {
            let page = bpm.fetch_page(pids[i]);
            assert_ne!(None, page);
            assert_eq!(true, bpm.unpin_page(pids[i], false));
            assert_eq!(true, bpm.delete_page(pids[i]));
        }

        for i in 10..20 {
            let i = i as usize;
            let page = bpm.fetch_page(pids[i]);
            assert_ne!(None, page);
            let page = page.unwrap();

            if pids[i] % 10 < 5 {
                assert_eq!(false, compare_value(&format!("{}", pids[i]), page));
            } else {
                assert_eq!(true, compare_value(&format!("{}", pids[i]), page));
            }
            assert_eq!(true, bpm.unpin_page(pids[i], false));
            assert_eq!(true, bpm.delete_page(pids[i]));
          }
    }



    #[test]
    fn hard2_test() {
        let num_threads = 5;
        let num_runs = 50;

        for run in 0..num_runs {
            println!("run-{}", run);
            let bpm = create_bpm(50, 10);
            let bpm = Arc::new(bpm);
            let mut handlers = Vec::new();
            let mut pids = Vec::new();

            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("{}",page.get_pid( )));
                pids.push(page.get_pid());
            }

            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                }
            }

            //create new pages
            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // modify data
            for i in 0..50 {
                let page = bpm.fetch_page(pids[i]);
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("Hard{}",page.get_pid()));
            }

            // unpin data
            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                }
            }
            // new pages to force write pages before
            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // create threads to test
            let pids = Arc::new(pids);
            for tid in 0..num_threads {
                let bpm_cp = bpm.clone();
                let pids_cp = pids.clone();
                handlers.push(thread::spawn(move || {
                    let mut j = tid * 10;

                    while j < 50 {
                        let mut page = bpm_cp.fetch_page(pids_cp[j]);
                        while None == page {
                            page = bpm_cp.fetch_page(pids_cp[j]);
                        }
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        // check 
                        if j % 2 == 0 {
                            assert_eq!(true, compare_value(&format!("{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        } else {
                            assert_eq!(true, compare_value(&format!("Hard{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        }
                        j += 1;
                    }   
                }));
            }

            for h in handlers {
                let _ = h.join();
            }
        }    
    }



    #[test]
    fn hard3_test() {
        let num_threads = 5;
        let num_runs = 50;

        for run in 0..num_runs {
            println!("run-{}", run);
            let bpm = Arc::new(create_bpm(50, 10));
            let mut handlers = Vec::new();
            let mut pids = Vec::new();

            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("{}",page.get_pid( )));
                pids.push(page.get_pid());
            }

            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                }
            }

            //create new pages
            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // modify data
            for i in 0..50 {
                let page = bpm.fetch_page(pids[i]);
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("Hard{}",page.get_pid()));
            }

            // unpin data
            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                }
            }
            // new pages to force write pages before
            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // create threads to test
            let pids = Arc::new(pids);
            
            for tid in 0..num_threads {
                let bpm_cp = bpm.clone();
                let pids_cp = pids.clone();
                handlers.push(thread::spawn(move || {
                    let mut j = tid * 10;
                    let mut new_pid = -1;

                    while j < 50 {
                        if j != tid * 10 {

                            let mut page_local = bpm_cp.fetch_page(new_pid);
                            while None == page_local {
                                page_local = bpm_cp.fetch_page(new_pid);
                            }
                            assert_ne!(None, page_local);
                            let page_local = page_local.unwrap();

                            assert_eq!(true, compare_value(&format!("{}", new_pid), page_local));
                            assert_eq!(true, bpm_cp.unpin_page(new_pid, true));
                            assert_eq!(true, bpm_cp.delete_page(new_pid));
                        }

                        let mut page = bpm_cp.fetch_page(pids_cp[j]);
                        while None == page {
                            page = bpm_cp.fetch_page(pids_cp[j]);
                        }
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        // check 
                        if j % 2 == 0 {
                            assert_eq!(true, compare_value(&format!("{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        } else {
                            assert_eq!(true, compare_value(&format!("Hard{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        }
                        j += 1;

                        // create new page
                        let mut page = bpm_cp.new_page();
                        while None == page {
                            page = bpm_cp.new_page();
                        }
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        new_pid = page.get_pid();
                        fill_value(page, &format!("{}",page.get_pid( )));
                        assert_eq!(true, bpm_cp.unpin_page(page.get_pid(), true));
                    }
                }));
            }

            for h in handlers {
                let _ = h.join();
            }

            for i in 0..50 {
                assert_eq!(true, bpm.delete_page(pids[i]));
            }

            let _ = fs::remove_file("test.db");
        }    
    }

    #[test]
    fn hard4_test() {
        let num_threads = 5;
        let num_runs = 50;

        for run in 0..num_runs {
            println!("run-{}", run);
            let bpm = Arc::new(create_bpm(50, 10));
            let mut handlers = Vec::new();
            let mut pids = Vec::new();

            for _ in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("{}",page.get_pid( )));
                pids.push(page.get_pid());
            }

            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                }
            }

            //create new pages
            for _ in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // modify data
            for i in 0..50 {
                let page = bpm.fetch_page(pids[i]);
                assert_ne!(None, page);
                let page = page.unwrap();
                fill_value(page, &format!("Hard{}",page.get_pid()));
            }

            // unpin data
            for i in 0..50 {
                if i % 2 == 0 {
                    assert_eq!(true, bpm.unpin_page(pids[i], false));
                } else {
                    assert_eq!(true, bpm.unpin_page(pids[i], true));
                }
            }
            // new pages to force write pages before
            for i in 0..50 {
                let page = bpm.new_page();
                assert_ne!(None, page);
                let page = page.unwrap();
                assert_eq!(true, bpm.unpin_page(page.get_pid(), true));
            }

            // create threads to test
            let pids = Arc::new(pids);
            
            for tid in 0..num_threads {
                let bpm_cp = bpm.clone();
                let pids_cp = pids.clone();
                handlers.push(thread::spawn(move || {
                    let mut j = tid * 10;
                    let mut new_pid = -1;

                    while j < 50 {
                        if j != tid * 10 {

                            let mut page_local = bpm_cp.fetch_page(new_pid);
                            while None == page_local {
                                page_local = bpm_cp.fetch_page(new_pid);
                            }
                            assert_ne!(None, page_local);
                            let page_local = page_local.unwrap();

                            assert_eq!(true, compare_value(&format!("{}", new_pid), page_local));
                            assert_eq!(true, bpm_cp.unpin_page(new_pid, true));
                            assert_eq!(true, bpm_cp.delete_page(new_pid));
                        }

                        let mut page = bpm_cp.fetch_page(pids_cp[j]);
                        while None == page {
                            page = bpm_cp.fetch_page(pids_cp[j]);
                        }
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        // check 
                        if j % 2 == 0 {
                            assert_eq!(true, compare_value(&format!("{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        } else {
                            assert_eq!(true, compare_value(&format!("Hard{}", pids_cp[j]), page));
                            assert_eq!(true, bpm_cp.unpin_page(pids_cp[j], false));
                        }
                        j += 1;

                        // create new page
                        let mut page = bpm_cp.new_page();
                        while None == page {
                            page = bpm_cp.new_page();
                        }
                        assert_ne!(None, page);
                        let page = page.unwrap();
                        new_pid = page.get_pid();
                        fill_value(page, &format!("{}",page.get_pid( )));
                        assert_eq!(true, bpm_cp.flush_page(new_pid));
                        assert_eq!(true, bpm_cp.unpin_page(page.get_pid(), false));

                        for _ in 0..10 {
                            let mut page = bpm_cp.new_page();
                            while None == page {
                                page = bpm_cp.new_page();
                            }
                            assert_ne!(None, page);
                            let page = page.unwrap();
                            assert_eq!(true, bpm_cp.unpin_page(page.get_pid(), false));
                            assert_eq!(true, bpm_cp.delete_page(page.get_pid()));
                        }
                    }
                }));
            }

            for h in handlers {
                let _ = h.join();
            }

            for i in 0..50 {
                assert_eq!(true, bpm.delete_page(pids[i]));
            }

            let _ = fs::remove_file("test.db");
        }    
    }
}