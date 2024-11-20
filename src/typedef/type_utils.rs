
pub fn compare_bytes_lt(left: &Vec<u8>, right: &Vec<u8>) -> bool {
    left.as_slice() < right.as_slice()
}

pub fn compare_bytes_gt(left: &Vec<u8>, right: &Vec<u8>) -> bool {
    left.as_slice() < right.as_slice()
}

pub fn compare_bytes_le(left: &Vec<u8>, right: &Vec<u8>) -> bool {
    left.as_slice() <= right.as_slice()
}

pub fn compare_bytes_ge(left: &Vec<u8>, right: &Vec<u8>) -> bool {
    left.as_slice() >= right.as_slice()
}

pub fn compare_bytes_eq(left: &Vec<u8>, right: &Vec<u8>) -> bool {
    left.as_slice() == right.as_slice()
}


