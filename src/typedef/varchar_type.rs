use crate::typedef::type_id;

use super::{type_trait::*, value::Value};

pub struct VarcharType {
}


// Also use defautl implementation in VarcharType
impl Compare for VarcharType {
}

impl Type for VarcharType {
    fn serialize_value(val: &Value) -> Vec<u8> {
        let data = val.get_data();
        let len = data.len() as u32;
        let mut len_bytes = len.to_ne_bytes().to_vec();
        len_bytes.extend(data);
        len_bytes
    }

    fn deserialize_value(bytes: &Vec<u8>) -> Value {
        let u32_size = size_of::<u32>();
        assert!(bytes.len() >= u32_size);
        
        let len = unsafe { *(bytes.as_ptr() as *const u32) };
        assert_eq!(bytes.len() - u32_size, len as usize);
        Value::new_varchar_with_bytes(type_id::TypeId::VARCHAR, &bytes[u32_size..])
    }
}




