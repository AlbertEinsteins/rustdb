use super::{type_id::TypeId, value::Value};
use super::{type_utils::*, type_id};


#[derive(Debug, PartialEq, PartialOrd)]
pub enum CmpBool {
    CmpFalse,
    CmpTrue,
    CmpNull,
}

/// transform bool to CmpBool
impl From<bool> for CmpBool {
    fn from(value: bool) -> Self {
        match value {
            true => CmpBool::CmpTrue,
            false => CmpBool::CmpFalse,
        }
    }
}


pub trait Compare {
    // logical functions
    fn compare_equal(left: &Value, right: &Value) -> CmpBool {
        compare_bytes_eq(&left.get_data(), &right.get_data()).into()
    }

    fn compare_greater_than_equal(left: &Value, right: &Value) -> CmpBool {
        compare_bytes_ge(&left.get_data(), &right.get_data()).into()
    }

    fn compare_greater_than(left: &Value, right: &Value) -> CmpBool {
        compare_bytes_gt(&left.get_data(), &right.get_data()).into()
    }

    fn compare_less_than(left: &Value, right: &Value) -> CmpBool {
        compare_bytes_lt(&left.get_data(), &right.get_data()).into()
    }

    fn compare_less_than_equal(left: &Value, right: &Value) -> CmpBool {
        compare_bytes_le(&left.get_data(), &right.get_data()).into()
    }

    fn compare_not_equal(left: &Value, right: &Value) -> CmpBool {
        (!compare_bytes_eq(&left.get_data(), &right.get_data())).into()
    }
}

pub trait MathOp {
    // other mathmatical functions
    fn add(left: &Value, right: &Value) -> Value;
    fn sub(left: &Value, right: &Value) -> Value;
    fn multiply(left: &Value, right: &Value) -> Value;
    fn divide(left: &Value, right: &Value) -> Value;
    fn modulo(left: &Value, right: &Value) -> Value;
    fn min(left: &Value, right: &Value) -> Value;
    fn max(left: &Value, right: &Value) -> Value;
    fn sqrt(left: &Value, right: &Value) -> Value;
}

pub trait Type {
    fn is_zero(left: &Value) -> bool {
        let val = left.as_ptr::<i32>().clone();
        val == 0
    }

    fn operate_null(left: &Value, right: &Value) -> Value {
        Value::new_null(right.get_type())
    }

    fn is_null(val: &Value) -> bool {
        return val.is_null();
    }

    fn to_string(val: &Value) -> String {
        let data = val.get_data();
        match val.get_type() {
            type_id::TypeId::INTEGER => {
                if data.len() == 0 {
                    format!("{{integer(null)}}")
                } else {
                    format!("{{integer({})}}", val.as_ptr::<i32>())
                }
            },
            type_id::TypeId::VARCHAR => {
                if data.len() == 0 {
                    format!("{{varchar(null)}}")
                } else {
                    format!("{{varchar({})}}", String::from_utf8(data).expect("Not support codec in varchar"))
                }
            },
            _ => {
                panic!("Not implemented already")
            }
        }
    }    

    /// get the length of the data actual need space
    /// such as:
    /// int for 4bytes
    /// bigint for 8 bytes
    /// varchar depends on itself
    fn get_length(val: &Value) -> u32 {
        return val.get_length();
    }

    fn serialize_value(val: &Value) -> Vec<u8>;

    fn deserialize_value(bytes: &Vec<u8>) -> Value;


    //TODO:
    fn cast_as(val: &Value, type_id: TypeId) -> Value {
        panic!("Not implemented yet.")
    }

    fn copy(val: &Value) -> Value {
        val.clone()
    }

    fn get_data(val: &Value) -> Vec<u8> {
        val.get_data()
    }

    
    fn get_type_id(val: &Value) -> TypeId {
        val.get_type()
    }

}

