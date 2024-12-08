use super::{integer_type::IntegerType, limits::DB_VALUE_NULL, type_id::TypeId, type_trait::Type, varchar_type::VarcharType};


#[derive(Debug, Clone)]
pub struct Value {
    val: Option<Vec<u8>>,
    type_id: TypeId,
    len: u32,
}

impl Value {
    // ========================= new method ======================
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

    ///========================================= method of struct =====================
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

    // dp
    pub fn get_length(&self) -> u32 {
        match &self.val {
            None => 0,
            Some(bytes) => {
                return bytes.len() as u32
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

    pub fn is_null(&self) -> bool {
        return self.len == DB_VALUE_NULL;
    }

    pub fn get_type(&self) -> TypeId {
        self.type_id
    }

    pub fn to_string(&self) -> String {
        if self.len == DB_VALUE_NULL {
            return "null".to_owned();
        }

        match self.get_type() {
            TypeId::INTEGER => {
                let val = unsafe { * (self.val.clone().unwrap().as_ptr() as *const i32) };
                format!("{}", val)
            },
            TypeId::VARCHAR => {
                // check len only shows the first 10 characters
                let str = String::from_utf8(self.val.clone().unwrap()).unwrap();
                if str.len() <= 10 {
                    return str
                }
                return str[..10].to_owned();
            },
            _ => {
                panic!("Not support yet.");
            }
        }
    }

    // ======================== static method =========================
    pub fn serialize(val: &Value) -> Vec<u8> {
        match val.get_type() {
            TypeId::VARCHAR => {
                VarcharType::serialize_value(val)
            },
            TypeId::INTEGER => {
                IntegerType::serialize_value(val)
            },
            _ => {
                panic!("Err: not support type");
            }
        }
    }
    pub fn deserialize(bytes: &Vec<u8>, type_id: TypeId) -> Result<Value, String> {
        match type_id {
            TypeId::VARCHAR => {
                Ok(VarcharType::deserialize_value(bytes))
            },
            TypeId::INTEGER => {
                Ok(IntegerType::deserialize_value(bytes))
            },
            _ => {
                Err(format!("Error: not support type"))
            }
        }
    }

}
