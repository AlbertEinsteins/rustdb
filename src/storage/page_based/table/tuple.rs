#![allow(warnings)]

use crate::{catalog::schema::Schema, common::{config::txn_id_t, rid::RID}, typedef::{type_id::TypeId, value::{ Value}}};

/// Version 0.1
/// tuple meta diffs from different concret transaction theory 

#[derive(Debug, Clone)]
pub struct TupleMeta {
    // insert txn_id/ts
    pub insert_txn_id: txn_id_t,
    pub delete_txn_id: txn_id_t,
    pub is_deleted: bool,
}

impl TupleMeta {
    pub fn new(insert_txn_id: txn_id_t, delete_txn_id: txn_id_t, is_deleted: bool) -> Self {
        Self {
            insert_txn_id,
            delete_txn_id,
            is_deleted
        }
    }
}



#[derive(Debug, Clone)]
pub struct Tuple {
    //TODO
    rid: RID,
    data: Vec<u8>
}

impl Tuple {
    
    pub fn new() -> Self {
        Self { rid: RID::new(), data: Vec::new() }
    }

    pub fn new_with_rid(rid: &RID) -> Self {
        Self { rid: rid.clone(), data: Vec::new() }
    }

    pub fn build(values: &Vec<Value>, schema: &Schema) -> Self {
        assert_eq!(values.len(), schema.get_column_count() as usize);

        // get fixed len, then add varlen size
        let mut total_len = schema.get_len();
        for unlined_idx in schema.get_uninlined_inds() {
            let idx = *unlined_idx as usize;
            // get data length
            let len = values[idx].get_length();

            // store length itself, and data length
            total_len += size_of::<u32>() as u32 + len; 
        }

        // allocate space and store data
        let mut buf = Vec::new();
        buf.resize(total_len as usize, 0);

        let total_col_count = schema.get_column_count();
        let mut offset = schema.get_len() as usize;

        for i in 0..total_col_count {
            match schema.get_column(i) {
                Err(err) => {
                    panic!("{}", err);
                },
                Ok(col) => {
                    if !col.is_inlined() {
                        let off_ptr = col.get_offset() as usize;
                        // varchar type, writes where to store data
                        buf[off_ptr..off_ptr+size_of::<u32>()].copy_from_slice(&(offset as u32).to_ne_bytes());

                        // ser_bytes, [data]
                        let ser_bytes = Value::serialize(&values[i]);
                        // write val serials
                        buf[offset..offset+ser_bytes.len()].copy_from_slice(&ser_bytes);
                        offset += ser_bytes.len();
                    } else {
                        let bytes = Value::serialize(&values[i]);
                        let col_off = col.get_offset() as usize;
                        let fixed_len = col.get_fixed_len() as usize;
                        buf[col_off..col_off+fixed_len].copy_from_slice(&bytes);
                    }
                }
            }
        }

        Self {
            rid: RID::new(),
            data: buf,
        }
    }

    pub fn get_key(schema: &Schema, key_inds: &Vec<i32>, key_schema: &Schema) -> Self {
        todo!()
    }

    pub fn get_value(&self, schema: &Schema, col_index: usize) -> Value {
        let col_count = schema.get_column_count();
        if col_index >= col_count {
            panic!("Error: col index {} is out of range {}", col_index, col_count);
        }

        let column = schema.get_column(col_index);
        if column.is_err() {
            return Value::new_null(TypeId::INVALID);
        }

        let column = column.ok().unwrap();
                // depend on different type
        match column.get_type() {
            TypeId::VARCHAR => {
                let off_ptr = column.get_offset() as usize; 
                let off = unsafe { *(self.data[off_ptr..off_ptr+4].as_ptr() as *const u32) } as usize;
                let len_bytes = &self.data[off..off+4];
                // can't just use u32 pointer, if needs 4 byte aligned
                let str_len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                // deserialize need [len, data]
                let res = Value::deserialize(&self.data[off..off+4+str_len].to_vec(), column.get_type());
                
                self.resolve_value_result(res)
            },
            TypeId::INTEGER => {
                let off = column.get_offset() as usize; 
                let len = column.get_len() as usize;
                let res: Result<Value, String> = Value::deserialize(&self.data[off..off+len].to_vec(), column.get_type());
                self.resolve_value_result(res)
            },
            _ => {
                panic!("Not support type");
            }
        }
    }

    fn resolve_value_result(&self, res: Result<Value, String>) -> Value {
        match res.is_ok() {
            true => {
                res.ok().unwrap()
            },
            false => {
                Value::new_null(TypeId::INVALID)
            }
        }
    }


    #[inline]
    pub fn get_length(&self) -> usize {
        return self.data.len();
    }

    #[inline]
    pub fn get_data(&self) -> &Vec<u8> {
        &self.data
    }

    #[inline]
    pub fn get_rid(&self) -> RID {
        self.rid
    }

    // ================================ static method ===============
    pub fn deserialize(bytes: &Vec<u8>) -> Result<Tuple, String> {
        let tuple = Tuple {
            rid: RID::new(),
            data: bytes[..].to_vec(),
        };
        Ok(tuple)
    }

    pub fn serialize(val: &Tuple) -> Vec<u8> {
        let data = val.get_data();
        let len = data.len();

        let mut len_bytes = len.to_ne_bytes().to_vec();
        len_bytes.extend(data);
        len_bytes
    }


    pub fn to_string(&self, schema: &Schema) -> String {
        let columns = schema.get_columns();

        let mut str = "(".to_owned();
        for idx in 0..columns.len() {
            let val = self.get_value(schema, idx);
            if 0 == idx {
                str.push_str(&val.to_string());
            } else {
                str.push_str(&format!(", {}", val.to_string()));
            }
        }
        str += ")";
        str
    }    
}


#[cfg(test)]
mod tests {
    use rand::Rng;

    use crate::{catalog::{column::Column, schema::Schema}, typedef::{type_id::TypeId, value::Value}};

    use super::Tuple;


    fn generate_tuple(schema: &Schema) -> Tuple {
        let mut rand_eng = rand::thread_rng();

        // according to the schema to build a tuple
        let mut values = Vec::new();

        let alphanums: String = String::from("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
        let alphanums: Vec<char> = alphanums.chars().collect();
        for i in 0..schema.get_column_count() {
            let column = schema.get_column(i).ok().unwrap();

            match column.get_type() {
                TypeId::INTEGER => {
                    let random_val = rand_eng.gen_range(0..=1000);
                    values.push(Value::new_integer(TypeId::INTEGER, random_val));
                },
                TypeId::VARCHAR => {
                    let len = rand_eng.gen_range(1..=10);
                    let mut s = String::from("");
                    for i in 0..len {
                        let rand_idx = rand_eng.gen_range(0..alphanums.len());
                        s.push(alphanums[rand_idx]);
                    }
                    values.push(Value::new_varchar(TypeId::VARCHAR, &s));
                },
                _ => {
                    panic!("Not support type");
                }
            }
        }   

        Tuple::build(&values, schema)
    }

    #[test]
    fn simple_test() {
        let schema = "a varchar(20), b int, c int, e varchar(16)";
        let col1 = Column::new_varchar("a", TypeId::VARCHAR, 20);
        let col2 = Column::new("b", TypeId::INTEGER);
        let col3 = Column::new("c", TypeId::INTEGER);
        let col4 = Column::new_varchar("e", TypeId::VARCHAR, 16);
        
        let cols = vec![col1, col2, col3, col4];
        let schema = Schema::new(&cols);

        // we create a tuple
        let tuple = generate_tuple(&schema);

        // test get every column        

        println!("{:#?}", schema.to_string());
        println!("{}", tuple.to_string(&schema));
    }
}
