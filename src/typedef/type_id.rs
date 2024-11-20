#[derive(Debug, Clone, PartialEq, Copy)]
pub enum TypeId {
    INVALID,
    BOOLEAN,
    INTEGER,
    VARCHAR,
    TIMESTAMP,
}

impl TypeId {
    pub fn to_string(&self) -> String {
        match self {
            Self::BOOLEAN => "bool".to_owned(),
            Self::INTEGER => "integer".to_owned(),
            Self::VARCHAR => "varchar".to_owned(),
            _ => {
                panic!("Not support type")
            }
        }
    }
}

impl From <u8> for TypeId {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::INVALID,
            1 => Self::BOOLEAN,
            2 => Self::INTEGER,
            3 => Self::VARCHAR,
            4 => Self::TIMESTAMP,
            _ => {
                panic!("Not implemented type id")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TypeId;

    #[test]
    fn test_size() {
        let a = TypeId::INTEGER;
        // println!("{}", std::mem::size_of_val(&a));
        println!("{}", a as i32);

    }
}