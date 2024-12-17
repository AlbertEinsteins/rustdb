use std::{sync::{Arc, mpsc, atomic::*}, thread::{JoinHandle, self}};

use crate::common::config::page_id_t;

use super::disk_manager::{DiskManager, PageStore};



/// we use tokio async framework to shcedule i/o read/write

/// like a promise<bool> in c++
pub struct Promise {
    sender: mpsc::Sender<bool>,
    receiver: mpsc::Receiver<bool>,
}

impl Promise {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            sender: tx,
            receiver: rx,
        }
    }

    pub fn wait(&self) {
        match self.receiver.recv() {
            Err(err) => {
                println!("Error: {}", err);
            },
            Ok(_) => {

            }
        }
    }

    pub fn completed(&self) {
        match self.sender.send(true) {
            Err(err) => {
                println!("Error: {}", err);
            },
            Ok(_) => {}
        }
    }
}


pub struct DiskRequest {
    pub is_write: bool,
    pub data: *mut Vec<u8>,
    pub page_id: page_id_t,

    // need a signal callback
    pub callback: Promise,
}

unsafe impl Send for DiskRequest {}
unsafe impl Sync for DiskRequest {}

pub type Msg = Option<Arc<DiskRequest>>;

/// we need a msg channel to send and recv msg, so we set it a field in DiskScheudler
#[derive(Debug)]
pub struct DiskScheduler {
    // execute async task necessarily
    disk_manager: Arc<DiskManager>,

    // thread handle
    handler: Option<JoinHandle<()>>,

    // stop service
    stop: Arc<AtomicBool>,

    // writer, this should be in the outside of the thread
    writer: Arc<mpsc::Sender<Msg>>,
}


impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        // reader, we need move it into thread itself
        // reader: mpsc::Receiver<Msg>,
        let (wr, rd) = mpsc::channel();

        let mut scheduler = Self {
            disk_manager,
            stop: Arc::new(AtomicBool::new(false)),
            writer: Arc::new(wr),
            handler: None,
        };
        scheduler.start_thread(rd);
        scheduler
    }

    pub fn create_request(is_write: bool, data: *mut Vec<u8>, pid: page_id_t) -> Arc<DiskRequest> {
        Arc::new(DiskRequest { is_write, data, page_id: pid, callback: Promise::new() })
    }

    fn start_thread(&mut self, recv: mpsc::Receiver<Msg>) {
        let rd = recv;
        let disk_mgr = self.disk_manager.clone();
        let stop = self.stop.clone();

        let handler = thread::spawn(move || {
            Self::start(rd, disk_mgr, stop);
        });

        self.handler = Some(handler);
    }

    fn start(reader: mpsc::Receiver<Msg>, disk_manager: Arc<DiskManager>, stop: Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {

            match reader.recv() {
                Err(err) => {
                    println!("Err: {}", err);
                    break;
                },
                Ok(msg) => {
                    if let Some(msg) = msg {

                        if msg.is_write {
                            let page_data = unsafe { &(*msg.data) };
                            let _ = disk_manager.write_page(msg.page_id, page_data.as_ref());
                        } else {
                            let page_data = unsafe { &mut (*msg.data)};
                            let _ = disk_manager.read_page(msg.page_id, page_data);
                        }
                        msg.callback.completed();
                    } else {
                        break;
                    }

                }
            }

        }
        println!("Background thread exit");
    }

    pub fn schedule(&self, req_msg: Msg) {
        let _ = self.writer.send(req_msg);
    }

}

// we need close the thread when drop
impl Drop for DiskScheduler {
    fn drop(&mut self) {
        if let Some(handler) = self.handler.take() {
            // send none msg
            match self.writer.send(None) {
                Err(_) => { panic!("Error occurred") },
                Ok(_) => {},
            }
            let _ = handler.join();
        }
    }
}



#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use crate::storage::page_based::{disk::{disk_manager::DiskManager}, page::page::Page};
    use super::{DiskScheduler, DiskRequest, Promise};


    #[test]
    fn test_schedule() {
        let disk_mgr = DiskManager::new("test.db").unwrap();
        let disk_mgr_wrap = Arc::new(disk_mgr);

        let disk_scheduler = DiskScheduler::new(disk_mgr_wrap);


        // we simulate a page
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(4096, 0);

        let mut page = Page {
            page_id: 1,
            pin_count: 0,
            is_dirty: false,
            data: buf,
            rwlatch: RwLock::new(()),
        };

        // let page_ref = Arc::new(RwLock::new(page));
        // let rec = unsafe { &mut (*page.get_raw_mut_ptr().unwrap()) };
        // println!("{:#?}", rec.len())

        let req = Arc::new( DiskRequest {
            page_id: page.page_id,
            is_write: false,
            data: page.get_mut_data(),
            callback: Promise::new(),
        });

        disk_scheduler.schedule(Some(req.clone()));

        // wait channel
        req.callback.wait();

        // check buf
        println!("{:#?}", &page.get_data()[..20]);
        println!("{:#?}", String::from_utf8(page.get_data()[..5].to_vec()))

    }
}