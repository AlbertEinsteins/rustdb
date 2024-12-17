use std::fmt::Display;

use super::bound_expression::BoundExpression;

#[repr(u8)]
pub enum OrderByType {
    Invalid = 0,
    Default = 1,
    Asc = 2,
    Desc = 3,
}

pub struct BoundOrderBy {
    pub order_type: OrderByType,
    pub expr: BoundExpression,
}


impl BoundOrderBy {
    pub fn to_string(&self) -> String {
        return format!("BoundOrderBy {{ type={}, expr={} }}", self.order_type, self.expr.to_string());
        // todo!()
    }
}



impl Display for OrderByType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self {
            Self::Invalid => {
                f.write_str(&format!("{}", "Invalid"))
            },
            Self::Default => {
                f.write_str(&format!("{}", "Default"))
            },
            Self::Asc => {
                f.write_str(&format!("{}", "Asc"))
            },
            Self::Desc => {
                f.write_str(&format!("{}", "Desc"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OrderByType;


    #[test]
    fn test_string() {
        let a = OrderByType::Invalid;
        println!("{}", a.to_string());
    }
}