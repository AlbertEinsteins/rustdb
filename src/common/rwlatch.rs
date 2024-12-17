#![allow(warnings)]
use std::sync::{Condvar, Mutex};


/// This mod is useless, Deprecated
/// for performance, deprecated
struct State {
    pub reader_cnt: i32,
    pub writer_cnt: i32,
}

impl State {
    pub fn new() -> Self {
        Self {
            reader_cnt: 0,
            writer_cnt: 0,
        }
    }    
}

pub struct ReaderWriterLatch {
    mutex: Mutex<State>,
    cond: Condvar,
}

impl ReaderWriterLatch {
    pub fn new() -> Self {
        Self {
            mutex: Mutex::new(State::new()),
            cond: Condvar::new(),
        }
    }

    pub fn read_lock(&self) {
        let mut state = self.mutex.lock().unwrap();
        while state.writer_cnt > 0 { // wait writer finished
            state = self.cond.wait(state).unwrap();
        }
        state.reader_cnt += 1;
    }
    
    pub fn read_unlock(&self) {
        let mut state = self.mutex.lock().unwrap();
        state.reader_cnt = i32::max(state.reader_cnt - 1, 0);
        if state.reader_cnt == 0 {
            self.cond.notify_one();
        }
    }

    pub fn write_lock(&self) {
        let mut state = self.mutex.lock().unwrap();
        while state.writer_cnt > 0 || state.reader_cnt > 0 {
            state = self.cond.wait(state).unwrap();
        }
        state.writer_cnt += 1;
    }

    pub fn write_unlock(&self) {
        let mut state = self.mutex.lock().unwrap();
        state.writer_cnt = i32::max(state.writer_cnt - 1, 0);
        self.cond.notify_all();
    }
}




#[cfg(test)]
mod tests {
    use std::{ops::AddAssign, sync::{Arc, RwLock}, thread, time::Instant};

    use super::ReaderWriterLatch;
    pub struct SharedData {
        value: i32,
    }
    
    impl SharedData {
        fn new() -> Self {
            SharedData { value: 0 }
        }
    
        fn read(&self) -> i32 {
            self.value
        }
    
        fn write(&mut self, new_value: i32) {
            self.value = new_value;
        }
    }

    #[test]
    fn performance_test2() {
        const NUM_READERS: usize = 100; // 读线程数量
        const NUM_WRITERS: usize = 10;  // 写线程数量
        const NUM_OPERATIONS: usize = 1000; // 每个线程执行的操作数量

        let shared_data = Arc::new(RwLock::new(SharedData::new()));

        let start = Instant::now();

        let mut handles = vec![];

        // 启动读线程
        for _ in 0..NUM_READERS {
            let shared_data = Arc::clone(&shared_data);
            let handle = thread::spawn(move || {
                for _ in 0..NUM_OPERATIONS {
                    // 获取读锁
                    let data = shared_data.read().unwrap();
                    // 模拟读取操作
                    let _ = data.read(); // 读取值
                }
            });
            handles.push(handle);
        }

        // 启动写线程
        for _ in 0..NUM_WRITERS {
            let shared_data = Arc::clone(&shared_data);
            let handle = thread::spawn(move || {
                for i in 0..NUM_OPERATIONS {
                    // 获取写锁
                    let mut data = shared_data.write().unwrap();
                    // 模拟写操作
                    data.write(i as i32); // 将值更新为 i
                }
            });
            handles.push(handle);
        }

        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!("Total execution time: {:?}", duration);
    }
    #[test]
    fn performance_test1() {
        const NUM_READERS: usize = 100;
        const NUM_WRITERS: usize = 10;
        const NUM_OPERATIONS: usize = 1000;

        let latch = Arc::new(ReaderWriterLatch::new());

        let start = Instant::now();

        let mut handles = vec![];

        // Spawn reader threads
        for _ in 0..NUM_READERS {
            let latch = Arc::clone(&latch);
            let handle = thread::spawn(move || {
                for _ in 0..NUM_OPERATIONS {
                    latch.read_lock();
                    // Simulate read work
                    latch.read_unlock();
                }
            });
            handles.push(handle);
        }

        // Spawn writer threads
        for _ in 0..NUM_WRITERS {
            let latch = Arc::clone(&latch);
            let handle = thread::spawn(move || {
                for _ in 0..NUM_OPERATIONS {
                    latch.write_lock();
                    // Simulate write work
                    latch.write_unlock();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!("Total execution time: {:?}", duration);
    }

    #[test]
    fn test_lock() {
        let cnt = Arc::new(RwLock::new(1));
        // let rw_latch = Arc::new(ReaderWriterLatch::new());

        // create 5 reader, 2 writer
        let mut handles = vec![];
        
        for i in 0..5 {
            let cnt_wrap = cnt.clone();
            handles.push(thread::spawn(move || {
                let mut cnt_guard = cnt_wrap.write().unwrap();
                cnt_guard.add_assign(1);
                println!("write cnt = {} {:#?}", cnt_guard, thread::current().id());
            }));
        }

        for i in 0..5 {
            let cnt_wrap = cnt.clone();
            handles.push(thread::spawn(move || {
                let cnt_guard = cnt_wrap.read().unwrap();
                println!("cnt = {} {:#?}", cnt_guard, thread::current().id());
            }));
        }



        for h in handles {
            h.join();
        }
    }
}