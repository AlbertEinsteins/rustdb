use super::{type_id::TypeId, type_trait::CmpBool, value::*};

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
        Value::new_null(t)
    }

    #[inline]
    pub fn get_boolean_value(b: CmpBool) -> Value {
        match b {
            CmpBool::CmpTrue => {
                Value::new_boolean(TypeId::BOOLEAN, true)
            },
            CmpBool::CmpFalse => {
                Value::new_boolean(TypeId::BOOLEAN, false)
            },
            CmpBool::CmpNull => {
                Value::new_null(TypeId::BOOLEAN)
            }
        }
    }
}