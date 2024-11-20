use super::{type_id::{TypeId}, integer_type::IntegerType, type_trait::Type, limits::{DB_VALUE_NULL}};


#[derive(Debug, Clone)]
pub struct Value {
    val: Option<Vec<u8>>,
    type_id: TypeId,
    len: u32,
}

impl Value {
    pub fn new_null(type_id: TypeId) -> Self {
        Self {
            val: None,
            type_id,
            len: DB_VALUE_NULL,
        }
    }

    pub fn new_boolean(type_id: TypeId, val: bool) -> Self {
        let bytes = (val as u8).to_ne_bytes();
        Value::new(type_id, &bytes)
    }

    pub fn new(type_id: TypeId, bytes: &[u8]) -> Self {
        Self {
            val: Some(bytes.to_vec()),
            type_id,
            len: bytes.len() as u32,
        }
    }

    pub fn new_integer(type_id: TypeId, val: i32) -> Self {
        Value::new(type_id, &val.to_ne_bytes())        
    }

    pub fn new_varchar(type_id: TypeId, str: &str) -> Self {
        Value::new(type_id, str.as_bytes())
    }

    pub fn new_varchar_with_bytes(type_id: TypeId, bytes: &[u8]) -> Self {
        Value::new(type_id, bytes)
    }

    ///========================================= static method of struct =====================
    // transfer data to another type with no check
    pub fn as_mut_ptr<T>(&mut self) -> &mut T {
        match self.val.as_mut() {
            None => {
                panic!("Error, null exception in value reinterpret");
            },
            Some(bytes) => {
                unsafe { &mut *(bytes.as_mut_ptr() as *mut T) }
            }
        }
    }

    pub fn as_ptr<T>(&self) -> &T {
        match self.val.as_ref() {
            None => {
                panic!("Error, null exception in value reinterpret");
            },
            Some(bytes) => {
                unsafe { &*(bytes.as_ptr() as *const T) }
            }
        }
    }

    // get data slice
    pub fn get_data(&self) -> Vec<u8> {
        match self.val.clone() {
            None => Vec::new(),
            Some(val) => val
        }
    }

    pub fn get_serilaize_len(&self) -> usize {
        let mut size = size_of::<u32>() + size_of::<TypeId>();

        if let Some(bytes) = &self.val {
            size += bytes.len();
        }
        size
    }

    pub fn is_null(&self) -> bool {
        return self.len == DB_VALUE_NULL;
    }

    pub fn get_type(&self) -> TypeId {
        self.type_id
    }

}


pub trait Serialization {
    fn serialize(val: &Value) -> Option<Vec<u8>>;
    fn deserialize(bytes: &Vec<u8>) -> Option<Value>;
}


impl Serialization for Value {
    /// serilize the value itself, accorrding to a protocol in which
    /// | type_id(1B)| length (4B) | data |
    fn serialize(val: &Value) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        buf.push(val.get_type() as u8);

        let body_bytes = val.get_data();
        let length = body_bytes.len() as u32;
        buf.extend_from_slice(&length.to_ne_bytes());
        buf.extend(body_bytes);

        Some(buf)
    }

    /// the revered operation used in serilize
    fn deserialize(bytes: &Vec<u8>) -> Option<Value> {
        if bytes.len() < 5 {
            None
        } else {
            let type_id: TypeId = bytes[0].into();
            let size_slice = &bytes[1..5];
            let size: u32 = u32::from_ne_bytes(size_slice.try_into()
                                                            .expect("Slice with incorrent length"));
            
            // check buffer 
            let expected_size = 1 + 4 + size;
            if bytes.len() != expected_size as usize {
                panic!("Error, deserilization err value, the bytes size missmatch");
            }
            
            Some(Value::new(type_id, &bytes[5..]))
        }
    }
}