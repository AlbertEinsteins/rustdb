#![allow(warnings)]

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::common::config::{page_id_t, INVALID_PAGE_ID, PAGE_SIZE};

#[derive(Debug)]
pub struct Page {
    pub page_id: page_id_t,
    pub pin_count: i32,
    pub is_dirty: bool,
    pub data: Vec<u8>,
    pub rwlatch: RwLock<()>,
}

impl PartialEq for Page {
    fn eq(&self, other: &Self) -> bool {
        //TODO
        return self.page_id == other.page_id;
    }
}

// unsafe impl Send for Page {}
// unsafe impl Sync for Page {}

impl Page {
    pub fn new() -> Self {
        let mut data = Vec::new();
        data.resize(PAGE_SIZE as usize, 0);
        Self {
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: false,
            data: data,
            rwlatch: RwLock::new(()),
        }
    }

    pub fn get_pid(&self) -> page_id_t {
        self.page_id
    }

    pub fn get_pincount(&self) -> i32 {
        self.pin_count
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    // return the actual data, not include the header
    pub fn get_data(&self) -> &Vec<u8>{
        self.data.as_ref()
    }

    pub fn get_read_lock(&self) -> RwLockReadGuard<'_, ()> {
        self.rwlatch.read().unwrap()
    }
    pub fn get_write_lock(&self) -> RwLockWriteGuard<'_, ()> {
        self.rwlatch.write().unwrap()
    }

    // return the actual mut data, exclude the Vec header
    pub fn get_mut_data(&mut self) -> &mut Vec<u8> {
        self.data.as_mut()
    }

    // transfer to const T&
    pub fn cast_as<T>(&self) -> &T {
        unsafe { &*(self.data.as_ptr() as *const T) }
    }
    
    // transfer to const T&
    pub fn cast_as_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut T) }
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let page = Page::new();
        let data = page.get_data();
    }

    #[test]
    fn test_rwlatch() {

        let page = Page::new();

        println!("release write lock");


        println!("XXX");
    }

    #[derive(Debug)]
    struct A {
        a: u8,
        b: u8,
        c: u8,
    }


    #[test]
    fn test_cast() {
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(4096, 0);

        buf[0] = 1;
        buf[1] = 2;
        buf[2] = 3;
        let mut page = Page {
            page_id: 1,
            pin_count: 0,
            is_dirty: false,
            data: buf,
            rwlatch: RwLock::new(()),
        };
        
        let another = page.cast_as_mut::<A>();
        println!("{:#?}", another);
    }

}