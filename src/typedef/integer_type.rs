use super::type_id;
use super::type_trait::*;
use super::value::*;

pub struct IntegerType {
}

// use default implemetation
impl Compare for IntegerType {
}

impl MathOp for IntegerType {
    fn add(left: &Value, right: &Value) -> Value {
        let res: i32 = left.as_ptr::<i32>() + right.as_ptr::<i32>();
        Value::new_integer(type_id::TypeId::INTEGER, res)
    }

    fn sub(left: &Value, right: &Value) -> Value {
        let res: i32 = left.as_ptr::<i32>() - right.as_ptr::<i32>();
        Value::new_integer(type_id::TypeId::INTEGER, res)
    }

    fn multiply(left: &Value, right: &Value) -> Value {
        let res: i32 = left.as_ptr::<i32>() * right.as_ptr::<i32>();
        Value::new_integer(type_id::TypeId::INTEGER, res)
    }

    fn divide(left: &Value, right: &Value) -> Value {
        let res: i32 = left.as_ptr::<i32>() / right.as_ptr::<i32>();
        Value::new_integer(type_id::TypeId::INTEGER, res)
    }

    fn modulo(left: &Value, right: &Value) -> Value {
        let res: i32 = left.as_ptr::<i32>() % right.as_ptr::<i32>();
        Value::new_integer(type_id::TypeId::INTEGER, res)
    }
    fn sqrt(left: &Value, right: &Value) -> Value {
        // TODO: Not impemented
        Value::new_null(type_id::TypeId::INTEGER)
    }

    fn min(left: &Value, right: &Value) -> Value {
        let lt = left.as_ptr::<i32>().clone();
        let rt = right.as_ptr::<i32>().clone();
        Value::new_integer(type_id::TypeId::INTEGER, i32::min(lt, rt))
    }

    fn max(left: &Value, right: &Value) -> Value {
        let lt = left.as_ptr::<i32>().clone();
        let rt = right.as_ptr::<i32>().clone();
        Value::new_integer(type_id::TypeId::INTEGER, i32::max(lt, rt))
    }


}


/// Use default implementation is ok
impl Type for IntegerType {
}




/// below lines are totally test codes
#[cfg(test)]
mod tests {

    use crate::typedef::integer_type::IntegerType;
    use crate::typedef::type_id::*;
    use crate::typedef::type_trait::{CmpBool, Compare, MathOp, Type};
    use crate::typedef::value::*;


    #[test]
    fn test_integer_funcs() {
        let a: Value = Value::new_integer(TypeId::INTEGER, 1);
        let b = Value::new_integer(TypeId::INTEGER, 2);

        // add
        let c = IntegerType::add(&a, &b);

        // sub
        let d = IntegerType::sub(&a, &b);

        // mul 
        let e = IntegerType::multiply(&a, &b);

        // divide
        let f = IntegerType::divide(&a, &b);

        // mod 
        let g = IntegerType::modulo(&a, &b);

        // min
        let h = IntegerType::min(&a, &b);

        // max
        let i = IntegerType::max(&a, &b);

        println!("{}", IntegerType::to_string(&c));
        println!("{}", IntegerType::to_string(&d));
        println!("{}", IntegerType::to_string(&e));
        println!("{}", IntegerType::to_string(&f));
        println!("{}", IntegerType::to_string(&g));
        println!("{}", IntegerType::to_string(&h));
        println!("{}", IntegerType::to_string(&i));
    }


    #[test]
    fn test_serilization() {
        let a = Value::new_integer(TypeId::INTEGER, 12);

        let bytes = IntegerType::serialize_value(&a);
        println!("{:#?} {}", bytes.clone().unwrap(), bytes.clone().unwrap().len());

        let recover = IntegerType::deserialize_value(bytes.unwrap().clone().as_ref());
        println!("{:#?}", IntegerType::to_string(&recover.unwrap()));

        // check varchar serialization
        let b = Value::new_varchar(TypeId::VARCHAR, "test name");


        let bytes = IntegerType::serialize_value(&b);
        println!("{:#?} {}", bytes.clone().unwrap(), bytes.clone().unwrap().len());

        let recover = IntegerType::deserialize_value(bytes.unwrap().clone().as_ref());
        println!("{:#?}", IntegerType::to_string(&recover.unwrap()));

    }


    #[test]
    fn test_logic_funcs() {
        let a = Value::new_integer(TypeId::INTEGER, 12);
        let b = Value::new_integer(TypeId::INTEGER, 12);
        let c = Value::new_integer(TypeId::INTEGER, 13);
        let d = Value::new_integer(TypeId::INTEGER, 11);
        
        assert_eq!(CmpBool::CmpTrue, IntegerType::compare_equal(&a, &b));
        assert_eq!(CmpBool::CmpFalse, IntegerType::compare_less_than(&a, &b));
        assert_eq!(CmpBool::CmpFalse, IntegerType::compare_less_than(&a, &b));
        assert_eq!(CmpBool::CmpTrue, IntegerType::compare_greater_than_equal(&b, &d));
        assert_eq!(CmpBool::CmpTrue, IntegerType::compare_not_equal(&a, &c));
    }



}