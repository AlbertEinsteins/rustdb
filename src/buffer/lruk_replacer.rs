#![allow(warnings)]

use std::{cmp::Reverse, collections::{BinaryHeap, HashMap, LinkedList}, sync::Mutex};
use crate::common::config::frame_id_t;




pub trait Replacer {
    // evict a frame if exists, otherwise return none
    fn evict(&mut self) -> Option<frame_id_t>;

    // record access
    fn record_access(&mut self, frame_id: frame_id_t);

    // set evictable given a frame id
    fn set_evictable(&mut self, frame_id: frame_id_t, is_evictable: bool);

    // remove a frame no matter what its k-distance is
    fn remove(&mut self, frame_id: frame_id_t);

    // return the replacer size
    fn get_size(&self) -> usize;
}

type timestamp_t = usize;
const MAX_TIMESTAMP: usize = usize::MAX;

#[derive(Debug)]
struct LRUNode {
    pub history: LinkedList<timestamp_t>,
    pub k: usize,
    pub is_evictable: bool,
}

impl LRUNode {
    pub fn new(k: usize) -> Self {
        Self {
            history: LinkedList::new(),
            k,
            is_evictable: false,
        }
    }

    pub fn add_history(&mut self, ts: timestamp_t) {
        self.history.push_back(ts);
        if self.history.len() > self.k {
            self.history.pop_front();
        }
    }

    // return the earlist access timestamp
    pub fn get_far_history(&self) -> timestamp_t {
        if let Some(v) = self.history.front() {
            return *v;
        }
        return MAX_TIMESTAMP;
    }

    pub fn get_k_distance(&self, cur_ts: timestamp_t) -> timestamp_t {
        if self.history.len() < self.k {
           return  MAX_TIMESTAMP;
        }
        return cur_ts - self.history.front().unwrap();
    }
}

#[derive(Debug)]
struct LRUKReplacer_ {
    pub k: usize,
    pub frame_record: HashMap<frame_id_t, LRUNode>,
    pub replacer_size: usize,
    pub current_ts: timestamp_t,
    pub maximun_frame_size: usize,
}

#[derive(Debug)]
pub struct LRUKReplacer {
    state: LRUKReplacer_,
    mutex: Mutex<()>,
}



impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        let state = LRUKReplacer_ {
            k,
            frame_record: HashMap::new(),
            replacer_size: 0,
            current_ts: 0,
            maximun_frame_size: num_frames,
        };

        Self {
            state,
            mutex: Mutex::new(()),
        }
    }

}

#[derive(Debug, PartialEq, Eq)]
struct Item<K, V> 
where 
    K: Ord,
    V: Eq,
{
    key: K,
    val: V,
}


impl<K: Ord, V: Eq> Ord for Item<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.key.cmp(&self.key)
    }

    
}

impl<K: Ord, V: Eq> PartialOrd for Item<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


impl Replacer for LRUKReplacer {
    fn evict(&mut self) -> Option<frame_id_t> {
        // find the maximum k-distance frame
        // if only exists inf distances, then use the lru strategy to evict frame with the most far access ts 
        // let mut near_access_heap = BinaryHeap::new();
        let _unused = self.mutex.lock().unwrap();

        if 0 == self.state.replacer_size {
            return None
        }

        let mut dis_mx_heap = BinaryHeap::new();
        let mut inf_mn_heap = BinaryHeap::new();

        for (frame_id, node) in &self.state.frame_record {
            if node.is_evictable {
                let k_dis = node.get_k_distance(self.state.current_ts);

                if MAX_TIMESTAMP == k_dis {
                    let far_ts = node.get_far_history();
                    inf_mn_heap.push(Item { key: far_ts, val: frame_id });
                } else {
                    dis_mx_heap.push(Reverse(Item { key: k_dis, val: frame_id }));
                }
            }
        }

        let res;
        if inf_mn_heap.is_empty() { // return min k-distance
            if dis_mx_heap.is_empty() {
                return None;
            }
            res = dis_mx_heap.pop().map(|item| *(item.0.val));
            self.state.replacer_size -= 1;
        } else { 
            res = inf_mn_heap.pop().map(|item| *item.val);
            self.state.replacer_size -= 1;
        }
        
        if let Some(fid) = res {
            self.state.frame_record.remove(&fid);
        }
        res
    }

    fn record_access(&mut self, frame_id: frame_id_t) {
        let _unused = match self.mutex.lock() {
            Ok(guard) => guard,
            Err(posiner) => posiner.into_inner(),
        };

        if frame_id as usize > self.state.maximun_frame_size {
            panic!("Error: frame id is greater <{}> than the maxmimum frame size <{}>", 
                frame_id, self.state.maximun_frame_size);
        }
        
        let cur_ts = self.state.current_ts;
        let node = self.state.frame_record.entry(frame_id).or_insert(LRUNode::new(self.state.k));
        
        node.add_history(cur_ts);
        self.state.current_ts += 1;
    }

    fn set_evictable(&mut self, frame_id: frame_id_t, is_evictable: bool) {
        let _unused = self.mutex.lock().unwrap();

        match self.state.frame_record.get_mut(&frame_id) {
            None => {},
            Some(node) => {
                // we need set it 'is_evictable'

                if node.is_evictable == is_evictable {
                    return ;
                }

                node.is_evictable = is_evictable;
                if is_evictable {
                    self.state.replacer_size += 1;
                } else {
                    self.state.replacer_size -= 1;
                }
            },
        }
        
    }

    // a sspl
    fn remove(&mut self, frame_id: frame_id_t) {
        let _unused = self.mutex.lock().unwrap();

        match self.state.frame_record.get_mut(&frame_id) {
            None => {},
            Some(_) => {
                self.state.frame_record.remove(&frame_id);
                self.state.replacer_size -= 1;
            },
        }
    }

    fn get_size(&self) -> usize {
        let _unused = self.mutex.lock().unwrap();
        self.state.replacer_size
    }
}


#[cfg(test)]
mod tests {
    use std::collections::BinaryHeap;

    

    use super::{Item, LRUKReplacer, Replacer};


    #[test]
    fn test_mn_heap() {
        let mut mn_heap = BinaryHeap::new();

        mn_heap.push(Item { key: 1, val: 400 });
        mn_heap.push(Item { key: 2, val: 300 });
        mn_heap.push(Item { key: 3, val: 200 });
        mn_heap.push(Item { key: 4, val: 100 });
        
        while !mn_heap.is_empty() {
            println!("{:?}", mn_heap.pop().unwrap());
        }
    }

    #[test]
    fn sample_test() {
        let mut lru_replacer = LRUKReplacer::new(7, 2);

        // senario: add six frame, set [1,2,3,4,5] evictable, 6 as non-evictable
        lru_replacer.record_access(1);
        lru_replacer.record_access(2);
        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(6);

        lru_replacer.set_evictable(6, false);
        lru_replacer.set_evictable(1, true);
        lru_replacer.set_evictable(2, true);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        lru_replacer.set_evictable(5, true);

        assert_eq!(5, lru_replacer.get_size());


        // add history fror frame 1
        lru_replacer.record_access(1);

        let val = lru_replacer.evict();
        assert_eq!(2, val.unwrap());
        let val = lru_replacer.evict();
        assert_eq!(3, val.unwrap());
        let val = lru_replacer.evict();
        assert_eq!(4, val.unwrap());
        assert_eq!(2, lru_replacer.get_size());

        lru_replacer.record_access(3);
        lru_replacer.record_access(4);
        lru_replacer.record_access(5);
        lru_replacer.record_access(4);
        lru_replacer.set_evictable(3, true);
        lru_replacer.set_evictable(4, true);
        assert_eq!(4, lru_replacer.get_size());

        // continue replace, expect 3
        let val = lru_replacer.evict();
        assert_eq!(3, val.unwrap());
        assert_eq!(3, lru_replacer.get_size());

        // set 6 to be evictable, 6 should be evicted, has max backward k-dis
        lru_replacer.set_evictable(6, true);
        assert_eq!(4, lru_replacer.get_size());
        let val = lru_replacer.evict();
        assert_eq!(6, val.unwrap());
        assert_eq!(3, lru_replacer.get_size());

        // now [1, 5, 4], look for victim
        lru_replacer.set_evictable(1, false);
        assert_eq!(2, lru_replacer.get_size());
        assert_eq!(5, lru_replacer.evict().unwrap());
        assert_eq!(1, lru_replacer.get_size());

        //update history for frame 1, now [4, 1], next is 4
        lru_replacer.record_access(1);
        lru_replacer.record_access(1);
        lru_replacer.set_evictable(1, true);
        assert_eq!(2, lru_replacer.get_size());
        assert_eq!(4, lru_replacer.evict().unwrap());


        assert_eq!(1, lru_replacer.get_size());
        assert_eq!(1, lru_replacer.evict().unwrap());
        assert_eq!(0, lru_replacer.get_size());

        // this operation should not modify size
        assert_eq!(None, lru_replacer.evict());
        assert_eq!(0, lru_replacer.get_size());
    }


    #[test]
    fn evict_test1() {
        let mut replacer = LRUKReplacer::new(4, 3);
        assert_eq!(0, replacer.get_size());

        replacer.record_access(1);
        replacer.record_access(1);
        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(2);
        replacer.record_access(2);
        replacer.record_access(1);
        
        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        assert_eq!(2, replacer.get_size());

        replacer.record_access(3);
        replacer.set_evictable(3, true);

        let val = replacer.evict();
        assert_eq!(3, val.unwrap());

        let val = replacer.evict();
        assert_eq!(1, val.unwrap());

        replacer.record_access(1);
        replacer.record_access(3);
        replacer.record_access(1);
        
        replacer.set_evictable(1, true);
        replacer.set_evictable(3, true);

        let val = replacer.evict();
        assert_eq!(1, val.unwrap());

        replacer.record_access(3);
        replacer.record_access(3);
        replacer.set_evictable(3, true);

        let val = replacer.evict();
        assert_eq!(2, val.unwrap());

        let val = replacer.evict();
        assert_eq!(3, val.unwrap());
    }

    #[test]
    fn evict_test2() {
        let mut replacer = LRUKReplacer::new(4, 3);

        assert_eq!(0, replacer.get_size());
        replacer.record_access(1);
        replacer.record_access(1);
        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(2);
        replacer.record_access(3);
        replacer.record_access(3);
        replacer.record_access(3);
        replacer.record_access(2);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        assert_eq!(3, replacer.get_size());

        let val = replacer.evict();
        assert_eq!(1, val.unwrap());

        let val = replacer.evict();
        assert_eq!(2, val.unwrap());

        let val = replacer.evict();
        assert_eq!(3, val.unwrap());
    }
}