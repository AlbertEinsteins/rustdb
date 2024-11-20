use super::column::Column;


#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Column>,

    // the indices of all un-inlined col in `columns`
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

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn is_tuple_inlined(&self) -> bool {
        self.is_tuple_inlined
    }

    pub fn get_uninlined_index(&self) -> &Vec<u32> {
        &self.uninlined_inds
    }

    pub fn get_uninlined_count(&self) -> usize {
        self.uninlined_inds.len()
    }

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
