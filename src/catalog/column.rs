use crate::typedef::{type_id::TypeId};

#[derive(Debug, Clone)]
pub struct Column {
    column_name: String,
    type_id: TypeId,

    offset_index: u32,
    // when column is not-inlined (means varchar, or blob etc..), we use variabel_len to represent the length
    variable_len: u32,
    // when column is inlined (means not varchar type, likewise blob etc..), we use fixed_len to represent the length
    fixed_len: u32,
}

impl Column {
    pub fn new(col_name: &str, t: TypeId) -> Self {
        assert!(t != TypeId::VARCHAR, "Wrong constructor for variable column");
        Self {
            column_name: String::from(col_name),
            type_id: t,
            fixed_len: Self::compute_fixed_len(t),
            variable_len: 0,
            offset_index: 0,
        }
    }

    pub fn new_varchar(col_name: &str, t: TypeId, var_len: u32) -> Self {
        assert!(t == TypeId::VARCHAR, "Wrong constructor for fixed column");
        Self {
            column_name: String::from(col_name),
            type_id: t,
            fixed_len: Self::compute_fixed_len(t),
            variable_len: var_len,
            offset_index: 0,
        }
    }

    pub fn is_inlined(&self) -> bool {
        self.type_id != TypeId::VARCHAR
    } 

    pub fn get_name(&self) -> String {
        self.column_name.clone()
    }

    pub fn get_fixed_len(&self) -> u32 {
        self.fixed_len
    }
    
    pub fn get_var_len(&self) -> u32 {
        self.variable_len
    }

    pub fn get_len(&self) -> u32 {
        if self.is_inlined() {
            return self.fixed_len
        } else {
            return self.variable_len
        }
    }

    pub fn get_offset_index(&self) -> u32 {
        self.offset_index
    }

    pub fn get_type(&self) -> TypeId {
        self.type_id
    }

    pub fn to_string(&self) -> String {
        match self.type_id {
            TypeId::VARCHAR => {
                format!("{{ {}:{}({}) }}", self.column_name, self.type_id.to_string(), self.variable_len)
            },
            _ => {
                format!("{{ {}:{} }}", self.column_name, self.type_id.to_string())
            }
        }
    }

    pub fn set_offset(&mut self, off_index: u32) {
        self.offset_index = off_index
    }

    fn compute_fixed_len(t: TypeId) -> u32 {
        match t {
            TypeId::INTEGER => {
                4
            },
            TypeId::BOOLEAN => {
                1
            },
            TypeId::VARCHAR => {
                //TODO: It depends on the implementation, not ready yet.
                // set a non-zero temporarily
                4
            },
            _ => {
                panic!("Not supprted type value")
            }
        }
    }
}