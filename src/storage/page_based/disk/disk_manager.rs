#![allow(warnings)]

use std::{fs::{File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, sync};


use crate::common::config::{page_id_t, PAGE_SIZE};


pub trait PageStore {
    fn read_page(&self, page_id: page_id_t, buf: &mut Vec<u8>) -> Result<(), String>;
    fn write_page(&self, page_id: page_id_t, page_data: &Vec<u8>) -> Result<(), String>;
}

/// DiskManager, responsible for actually read-write operations based on the page_id
/// May exists not one writer
#[derive(Debug)]
pub struct DiskManager {
    filename: String,
    file_fd: sync::RwLock<File>,
    num_writes: u32,
    num_flushed: u32,
}

impl DiskManager {
    pub fn new(filename: &str) -> Result<Self, String> {
        let find_res = filename.rfind(".");
        match find_res {
            None => Err(format!("Error: invalid find name, must surround with a suffix")),
            Some(_) => {
                // TOOD: use the same name to create a log file

                // try to open directly
                let mut res = OpenOptions::new()
                                                                .read(true).write(true)
                                                                .open(filename);
                if res.is_err() {
                    res = File::create_new(filename);
                }

                if res.is_err() {
                    Err(format!("Error: can not open a file, err is {}", res.err().unwrap()))
                } else {
                    Ok(Self {
                        filename: filename.to_owned(),
                        file_fd: sync::RwLock::new(res.expect(&format!("Error: file {filename} opened failed."))),
                        num_writes: 0,
                        num_flushed: 0,
                    })
                }
            }
        }
    }    

    pub fn get_filesize(&self) -> u32 {
        let read_guard = self.file_fd.read().unwrap();
        let meta = read_guard.metadata();
        // println!("{}", meta.unwrap().)
        meta.expect("Erorr: get filesize error").len() as u32
    }

} 

impl PageStore for DiskManager {
    fn read_page(&self, page_id: page_id_t, buf: &mut Vec<u8>) -> Result<(), String> {
        // println!("{}", buf.len());
        assert!(buf.len() == PAGE_SIZE as usize);
        
        let offset: u32 = (page_id * PAGE_SIZE) as u32;
        if offset >= self.get_filesize() {
            println!("Error: invalid page size {}",  offset);
            return Err(format!("Error: invalid page size"));
        }

        // get shared_lock 
        let mut write_guard = self.file_fd.write().unwrap();
        write_guard.seek(SeekFrom::Start(offset as u64)).unwrap();
        let read_res = write_guard.read(buf.as_mut_slice());
        match read_res {
            Err(err) => {
                println!("Error: read_file error, error is {}", err);
                Err(format!("Error: invalid page size"))
            },
            Ok(read_size) => {
                assert!(read_size == PAGE_SIZE as usize);
                Ok(())
            }
        }
    }

    fn write_page(&self, page_id: page_id_t, page_data: &Vec<u8>) -> Result<(), String> {
        assert!(page_data.len() == PAGE_SIZE as usize);
        
        let offset: u32 = (page_id * PAGE_SIZE) as u32;

        let mut write_guard = self.file_fd.write().unwrap();
        write_guard.seek(SeekFrom::Start(offset as u64)).expect("Seek Error");
        let write_res = write_guard.write(&page_data);
        match write_res {
            Err(err) => {
                eprintln!("Error: I/O error {}", err);
                Err(format!("Error: write error"))
            },
            Ok(write_size) => {
                assert!(write_size == PAGE_SIZE as usize);
                write_guard.flush();
                Ok(())
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use crate::common::config::PAGE_SIZE;
    use super::{DiskManager, PageStore};

    #[test]
    fn test_simple_read_write() {
        let disk = DiskManager::new("test.db").unwrap();

        let mut buf = Vec::new();
        buf.resize(PAGE_SIZE as usize, 0);
        
        let s = "world";
        buf[0..s.len()].copy_from_slice(s.as_bytes());

        disk.write_page(0, &buf.to_vec()).unwrap();

        let mut page = Vec::new();
        page.resize(PAGE_SIZE as usize, 0);
        let res = disk.read_page(0, &mut page).unwrap();
        println!("{:#?}", String::from_utf8(page[..s.len()].to_vec()));
    }

    #[test]
    fn test_read_write() {
        let disk = DiskManager::new("test.db").unwrap();

        let mut buf = Vec::new();
        buf.resize(PAGE_SIZE as usize, 0);

        let s = "hello";

        buf[0..s.len()].copy_from_slice(s.as_bytes());
        disk.write_page(1, &buf.to_vec()).unwrap();

        let mut page = Vec::new();
        page.resize(PAGE_SIZE as usize, 0);
        let _ = disk.read_page(0, &mut page).unwrap();
        println!("{:#?}", String::from_utf8(page[..s.len()].to_vec()));
        
        let _ = disk.read_page(1, &mut page).unwrap();
        println!("{:#?}", String::from_utf8(page[..s.len()].to_vec()));
    }
}