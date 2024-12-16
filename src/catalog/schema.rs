use std::sync::Arc;

use super::column::Column;


pub type SchemaRef = Arc<Schema>;

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Column>,

    // the indices of all un-inlined col in `columns`
    // (means varchar, or blob etc..)
    uninlined_inds: Vec<u32>,
    is_tuple_inlined: bool,
    len: u32,
}


impl Schema {
    pub fn new(cols: &Vec<Column>) -> Self {
        let mut columns = cols.clone();
        let mut uninlined_inds: Vec<u32> = Vec::new();
        let mut cur_offset: u32 = 0;
        let mut is_tuple_inlined = true;

        for idx in 0..cols.len() {
            let col = &cols[idx];
            if !col.is_inlined() {
                uninlined_inds.push(idx as u32);
                is_tuple_inlined = false;
            }

            // set offset
            columns[idx].set_offset(cur_offset);
            cur_offset += col.get_fixed_len();
        }

        Self {
            columns,
            uninlined_inds,
            is_tuple_inlined,
            len: cur_offset,
        }
    }

    pub fn copy(s: &Schema) -> Self {
        s.clone()
    }

    pub fn get_column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column(&self, ind: usize) -> Result<&Column, String> {
        if ind >= self.columns.len() {
            return Err(format!("Column index {} out of schema range {}", ind, self.columns.len()));
        }
        Ok(&self.columns[ind])
    }

    pub fn get_column_idx(&self, col_name: &str) -> Result<usize, String> {
        for idx in 0..self.columns.len() {
            if self.columns[idx].get_name() == col_name {
                return Ok(idx);
            }
        }
        Err(format!("Column not found"))
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn is_tuple_inlined(&self) -> bool {
        self.is_tuple_inlined
    }

    pub fn get_uninlined_inds(&self) -> &Vec<u32> {
        &self.uninlined_inds
    }

    pub fn get_uninlined_count(&self) -> usize {
        self.uninlined_inds.len()
    }

    // if varchar or other unlined column, return a u32 size pointer
    // if int/bigint or other fixed data, return bytes len as its self size, 
    //   such as 4 for int
    pub fn get_len(&self) -> u32 {
        self.len
    }

    pub fn to_string(&self) -> String {
        let mut res = "Schema(".to_owned();

        let mut is_first = true;
        for col in &self.columns {
            if !is_first {
                res = res + ", ";
            }
            res.push_str(&col.to_string());
            is_first = false;
        }

        res += ")";
        res
    }

}


#[cfg(test)]
mod tests {
    use crate::catalog::column::Column;
    use crate::typedef::type_id::*;

    use super::Schema;


    #[test]
    fn test_create() {
        let schema1 = Schema::new(&vec![Column::new("a", TypeId::INTEGER),
            Column::new_varchar("b", TypeId::VARCHAR, 32)]);


        println!("{}", schema1.to_string())
    }
}
