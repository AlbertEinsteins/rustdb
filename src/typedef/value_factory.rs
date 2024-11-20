use super::{type_id::TypeId, value::*};

pub struct ValueFactory { }


impl ValueFactory {
    #[inline]
    pub fn get_integer_value(val: i32) -> Value {
        Value::new_integer(TypeId::INTEGER, val)
    }

    #[inline]
    pub fn get_varchar_value(val: &str) -> Value {
        Value::new_varchar(TypeId::VARCHAR, val)
    }

    #[inline]
    pub fn get_null_value(t: TypeId) -> Value {
        match t {
            TypeId::INTEGER => {
                Value::new_null(t)
            },
            TypeId::VARCHAR => {
                Value::new_null(t)
            },
            _ => {
                panic!("Not implemented yet.")
            }
        }
    }
}