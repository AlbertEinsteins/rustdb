use tabled::{builder::Builder, grid::records::vec_records::Text, Table};

pub trait FormatWriter {
    
    fn add_header(&mut self, header: Vec<String>);

    fn add_rows(&mut self, rows: Vec<Vec<String>>);

    fn print(&self);

    fn to_string(&self) -> String;
}

pub struct DefaultFormatWriter {
    header: Vec<String>,
    rows: Vec<Vec<String>>
}

impl DefaultFormatWriter {
    pub fn build_format(header: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self { header, rows }
    }
}

impl FormatWriter for DefaultFormatWriter {
    fn add_header(&mut self, header: Vec<String>) {        
        self.header = header
    }

    fn add_rows(&mut self, rows: Vec<Vec<String>>) {
        self.rows = rows;
    }

    fn print(&self) {
        println!("{}\n", self.to_string());
    }

    fn to_string(&self) -> String {
        let mut table_data = Vec::new();
        // add header
        let header= self.header.iter().map(|h| Text::new(h.clone()))
            .collect::<Vec<Text<String>>>();
        table_data.push(header);
        for row in &self.rows {
            let row = row.iter().map(|cell| Text::new(cell.clone()))
                .collect::<Vec<Text<String>>>();
            table_data.push(row);            
        }

        let table = Builder::from_vec(table_data).build();
        table.to_string()
    }


}