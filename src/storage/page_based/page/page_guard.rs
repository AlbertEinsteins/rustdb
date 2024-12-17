#![allow(warnings)]

use std::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{buffer::buffer_pool_manager::BufferPoolManager, common::config::page_id_t};

use super::page::{Page};


pub struct PageGuard<'a> {
    bpm: &'a BufferPoolManager,
    page: Option<&'a mut Page>,
    is_dirty: bool,
}

impl<'a>  PageGuard<'a> {
    pub fn new(bpm: &'a BufferPoolManager, page: &'a mut Page) -> Self {
        Self { bpm, is_dirty: false, page: Some(page) }
    }

    // when u called multiple times, the only one that has effect is the first time call
    pub fn upgrade_read(mut self) -> Option<ReadPageGuard<'a>> {
        match self.page.take() {
            None => None,
            Some(page) => {
                let lock = page.get_read_lock();
                // cross the compiler check
                let mut_page = unsafe {
                    &mut *(page as *const Page as *mut Page)
                };
                let page_guard = PageGuard::new(&self.bpm, mut_page);
                Some(ReadPageGuard::new(page_guard, lock))
            }
        }
    }

    pub fn upgrade_write(mut self) -> Option<WritePageGuard<'a>> {
    //    WritePageGuard::new(self, wlock)
        match self.page.take() {
            None => None,
            Some(page) => {
                let lock = page.get_write_lock();

                // cross the compiler check
                let mut_page = unsafe {
                    &mut *(page as *const Page as *mut Page)
                };

                let page_guard = PageGuard::new(&self.bpm, mut_page);
                Some(WritePageGuard::new(page_guard, lock))
            }
        }
    }

    pub fn get_data(&self) -> Option<&Vec<u8>> {
        self.page.as_ref().map(|p| p.get_data())
    }

    pub fn get_mut_data(&mut self) -> Option<&mut Vec<u8>> {
        match &mut self.page {
            None => None,
            Some(page) => {
                Some(page.get_mut_data())
            }
        }
    }

    pub fn get_pid(&self) -> Option<page_id_t> {
        self.page.as_ref().map(|p| p.get_pid())
    }

    pub fn get_as<T>(&self) -> &'a T {
        unsafe { & *(self.get_data().map(|p| p.as_ptr() as *const T).unwrap()) }
    }

    pub fn get_mut_as<T>(&mut self) -> &'a mut T {
        self.is_dirty = true;
        unsafe { &mut *(self.get_mut_data().map(|p| p.as_mut_ptr() as *mut T).unwrap()) }
    }

    #[inline]
    pub fn reset_data(page_guard: &mut PageGuard) {
        page_guard.is_dirty = false;
    }
}

impl<'a> Drop for PageGuard<'a> {
    fn drop(&mut self) {
        // do nothing
        match &self.page {
            None => {},
            Some(page) => {
                self.bpm.unpin_page(page.get_pid(), self.is_dirty);
                Self::reset_data(self);
            }
        }
    }
}


/// read page guard code section
/// 
/// 
pub struct ReadPageGuard<'a> {
    page_guard: PageGuard<'a>,
    lock_guard: RwLockReadGuard<'a, ()>,
}

impl<'a> ReadPageGuard<'a> {
    pub fn new(page_guard: PageGuard<'a>, lock_guard: RwLockReadGuard<'a, ()>) -> Self {
        Self {
            page_guard,
            lock_guard,
        }
    }

    pub fn get_as<T>(&self) -> &'a T {
        unsafe { & *(self.page_guard.get_data().map(|p| p.as_ptr() as *const T).unwrap()) }
    }

}

impl<'a> Drop for ReadPageGuard<'a> {
    fn drop(&mut self) {
        // release lock
    }
}

/// write page guarad code section
/// 
/// 
pub struct WritePageGuard<'a> {
    // here, we need drop lock first, so it must be afterwards,
    // cause, drop-called is according to the stack style
    page_guard: PageGuard<'a>,
    lock_guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> WritePageGuard<'a> {
    pub fn new(page_guard: PageGuard<'a>, lock_guard: RwLockWriteGuard<'a, ()>) -> Self {
        Self { page_guard, lock_guard }
    }

    pub fn get_mut_as<T>(&mut self) -> &'a mut T {
        self.page_guard.is_dirty = true;
        unsafe { &mut *(self.page_guard.get_mut_data().map(|p| p.as_mut_ptr() as *mut T).unwrap()) }
    }
}

impl<'a> Drop for WritePageGuard<'a> {
    fn drop(&mut self) {
    }
}




#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{storage::page_based::disk::{disk_manager::DiskManager, log_manager::LogManager}, buffer::buffer_pool_manager::BufferPoolManager};

    use super::PageGuard;



    fn create_bpm(pool_size: usize, k: usize) -> BufferPoolManager {
        // setup
        let disk_manager = DiskManager::new("test.db").unwrap();
        let bpm = BufferPoolManager::new(pool_size, Arc::new(disk_manager), 
            k, Arc::new(LogManager::new()));
        bpm
    }

    #[test]
    fn test_simple_guard() {
        let bpm = create_bpm(5, 2);

        let page0 = bpm.new_page();
        let page0 = page0.unwrap();
        let (pid, pin_count) = (page0.get_pid(), page0.get_pincount());

        let guard0 = PageGuard::new(&bpm, page0);

        assert_eq!(pid, guard0.get_pid().unwrap());
        assert_eq!(1, pin_count);

        drop(guard0);

        assert_eq!(0, page0.get_pincount());

    }

    #[test]
    fn read_test() {
        let bpm = create_bpm(5, 2);

        let p0 = bpm.new_page().unwrap();

        {
            let rg = bpm.fetch_page_read(p0.get_pid());
            assert_eq!(2, p0.pin_count);
        }
        assert_eq!(1, p0.get_pincount());

        {
            let rg = bpm.fetch_page_read(p0.get_pid());
            let rg2 = rg;
            assert_eq!(2, p0.get_pincount());
        }
        assert_eq!(1, p0.get_pincount());

        // test move
        {
            let mut rg1 = bpm.fetch_page_read(p0.get_pid());
            let rg2 = bpm.fetch_page_read(p0.get_pid());
            assert_eq!(3, p0.get_pincount());
            rg1 = rg2;
            assert_eq!(2, p0.get_pincount());
        }

        assert_eq!(1, p0.get_pincount());
    }

    #[test]
    fn write_test() {
        let bpm = create_bpm(5, 2);
        let p0 = bpm.new_page().unwrap();

        {
            let wg = bpm.fetch_page_write(p0.get_pid());
            println!("Lock");

            drop(wg);
        }
    }

    #[test]
    fn read3_test() {
        let bpm = create_bpm(5, 2);

        // create p0
        let p0 = bpm.new_page().unwrap();
        {
            let rg1 = bpm.fetch_page_read(p0.get_pid());
            let rg2 = bpm.fetch_page_read(p0.get_pid());
            assert_eq!(3, p0.get_pincount());
            let rg3 = rg1;
            assert_eq!(3, p0.get_pincount());
            drop(rg3);
            assert_eq!(2, p0.get_pincount());
        }
        assert_eq!(1, p0.get_pincount());
    }

    #[test]
    fn hhtest() {
        let bpm = create_bpm(3, 4);

        let p0 = bpm.new_page().unwrap();
        let p1 = bpm.new_page().unwrap();

        let p0id = p0.get_pid();
        let p1id = p1.get_pid();
        // let p0_gd = PageGuard::new(&bpm, p0);
        // let p1_gd = PageGuard::new(&bpm, p1);

        {
            let rg1 = bpm.fetch_page_read(p1id);
            assert_eq!(2, p1.get_pincount());
            drop(rg1);
            assert_eq!(1, p1.get_pincount());
        }
        assert_eq!(1, p0.get_pincount());
        assert_eq!(1, p1.get_pincount());

        {
            let rg1 = bpm.fetch_page_read(p0id);
            let mut rg2 = bpm.fetch_page_read(p1id);
            assert_eq!(2, p1.get_pincount());
            assert_eq!(2, p0.get_pincount());
            rg2 = rg1;
            assert_eq!(2, p0.get_pincount());
            assert_eq!(1, p1.get_pincount());
        }
        assert_eq!(1, p0.get_pincount());
    }


    #[test]
    fn pin_test() {
        let bpm = create_bpm(3, 4);

        let p0 = bpm.new_page().unwrap();
        let p1 = bpm.new_page().unwrap();

        {
            let pg = bpm.fetch_page_basic(p0.get_pid());
            assert_eq!(2, p0.get_pincount());
        }

        {
            let pg = bpm.fetch_page_read(p0.get_pid());
            assert_eq!(2, p0.get_pincount());
        }

        let read_page_guard;
        {
            let guard = bpm.fetch_page_basic(p1.get_pid()).unwrap();
            read_page_guard = guard.upgrade_read();
            assert_eq!(2, p1.get_pincount());
        }
        drop(read_page_guard);

        let write_page_guard;
        {
            let guard = bpm.fetch_page_basic(p1.get_pid());
            write_page_guard = guard.unwrap().upgrade_write();
            assert_eq!(2, p1.get_pincount());
            println!("{}", "write get");

            // let write_get = bpm.fetch_page_write(p1.get_pid());
            // println!("{}", "get 2？");
        }
        assert_eq!(2, p1.get_pincount());

        drop(write_page_guard);

        assert_eq!(1, p1.get_pincount());

    }
}