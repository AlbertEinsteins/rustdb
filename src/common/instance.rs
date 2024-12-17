#![allow(warnings)]
use std::{sync::{Arc, RwLock}, vec, cell::RefCell};

use sqlparser::{dialect::GenericDialect, ast::Statement};

use crate::{binder::{binder::Binder, bound_statement::BoundStatement, statement::create_stmt::CreateStmt}, buffer::buffer_pool_manager::BufferPoolManager, catalog::{catalog::{CataLog, CataLogRef}, column::Column, schema::Schema}, execution::{execute_engine::ExecuteEngine, executor_context::{ExecutorContext, ExecutorContextRef}}, planner::planner::Planner, storage::page_based::{disk::{disk_manager::DiskManager, log_manager::LogManager}, table::tuple::Tuple}, transaction::{lock_manager::{LockManager, LockManagerRef}, transaction::{Transaction, TransactionRef}}, typedef::type_id::TypeId};

use super::{config::LRUK_REPLACER_K, formatwriter::{DefaultFormatWriter, FormatWriter}};



pub struct DBInstance {
    disk_mgr: Arc<DiskManager>,
    bp_mgr: Arc<BufferPoolManager>,
    lock_mgr: LockManagerRef,
    log_mgr: Arc<LogManager>,

    catalog: RwLock<CataLogRef>,
    
    execute_engine: ExecuteEngine,
}

impl DBInstance {
    pub fn new(db_filename: &str) -> Result<Self, String> {
        // create the necessary components
        
        let disk_mgr = Arc::new(DiskManager::new(db_filename)?);
        let log_mgr = Arc::new(LogManager::new());

        let bpm = Arc::new(BufferPoolManager::new(
            128, 
            disk_mgr.clone(), 
            LRUK_REPLACER_K as usize, 
            log_mgr.clone())
        );
        
        let lock_mgr = LockManagerRef::new(LockManager::new());
        // create catalog
        let catalog = Arc::new(
            RefCell::new(CataLog::new(bpm.clone(), lock_mgr.clone(), log_mgr.clone()))
        );
        
        // create execute engine
        let execute_engine = ExecuteEngine::new();

        Ok(Self {
            disk_mgr,
            bp_mgr: bpm,
            lock_mgr,
            log_mgr,
            catalog: RwLock::new(catalog),
            execute_engine,
        })
    }

    pub fn execute_sql(sql: &str) {
        
    }

    // execute sql with a transaction
    pub fn execute_sql_txn(&mut self, sql: &str, txn: TransactionRef) -> Result<(), String> {
        // check other statements
        // 1.dt disply tables
        // 2.di diplay indexes
        // 3.help
        
        if !sql.is_empty() && sql.chars().nth(0).unwrap() == '\\' {
            if sql == "\\dt" {

            } else if sql == "\\help" {
    
            } else {
                return Err(format!("Not support cmd"));
            }

            todo!()
        }
        
        // execute a bunch of statements with a txn
        let has_delete = false;
        let mut binder;
        {
            let read_guard = self.catalog.read().unwrap();
            binder = Binder::new(read_guard.clone());

            if let Err(err) = binder.parse_and_save(sql, &GenericDialect{}) {
                return Err(format!("parse error"));
            }
        }

        let Some(stmts) = binder.stmts.take() else { panic!("Error occurred, impossible branch"); };
        for i in 0..stmts.len() {
            let mut is_delete = false;
            let bound_stmt = binder.bind_statement(&stmts[i])?;
            match &bound_stmt {
                BoundStatement::Create(create) => {
                    self.handle_create_stmt(create);
                },
                _ => {

                }
            }


            // plan statement
            let mut planner;
            let mut plan;
            let mut execute_context;
            {
                let read_guard = self.catalog.read().unwrap();
                planner = Planner::new(read_guard.clone());

                let res = planner.plan_statement(&bound_stmt);
                if res.is_err() {
                    let err = res.unwrap_err();
                    println!("{}", err);
                    return Err(err);
                }
                plan = res.unwrap();
                execute_context = self.make_context(read_guard.clone(), txn.clone(), is_delete);
            }

            // execute
            println!("{}", plan.to_string());
            let res_schema = plan.get_output_schema().clone();
            let result_rows = self.execute_engine.execute(plan, txn.clone(), execute_context);
    
            // formated rows, and print them
            match result_rows {
                Err(err) => {
                    eprintln!("{}", err);
                },
                Ok(tuples) => {
                    let writer = Self::format_res(tuples, &res_schema);
                    writer.print();
                }
            }
        }


        Ok(())
    }



    pub fn handle_create_stmt(&self, create_stmt: &CreateStmt) {

    }

    pub fn make_context(&self, catalog: CataLogRef, txn: TransactionRef, is_delete: bool) -> ExecutorContextRef {
        ExecutorContextRef::new(ExecutorContext::new(catalog, txn))
    }

    // transform a Vec<Tuple> and Schema to table 
    pub fn format_res(tuples: Vec<Tuple>, schema: &Schema) -> Box<dyn FormatWriter> {
        let columns = schema.get_columns();
        let mut header = Vec::new();
        let mut rows = Vec::new();

        header.reserve(columns.len());
        rows.reserve(tuples.len());
        for col in columns {
            header.push(col.get_name());
        }

        let col_size = columns.len();
        for tuple in tuples {
            let mut row = Vec::new();
            for col_idx in 0..col_size {
                row.push(tuple.get_value(schema, col_idx).to_string());
            }
            rows.push(row);
        }

        Box::new(DefaultFormatWriter::build_format(header, rows))
    }

    // for test
    pub fn generate_test_tables(&self) {
        // create t1 { a: int, b: varchar(32), c: varchar(32), d: int }
        let t1 = Schema::new(&vec![
            Column::new("a", TypeId::INTEGER),
            Column::new_varchar("b", TypeId::VARCHAR, 32),
            Column::new_varchar("c", TypeId::VARCHAR, 32),
            Column::new("d", TypeId::INTEGER),
        ]);


        let mut write_guard = self.catalog.write().unwrap();
        write_guard.borrow_mut().create_table(None, "t1", t1);
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tabled::{builder::Builder, grid::records::vec_records::Text, settings::Style};

    use crate::transaction::transaction::Transaction;

    use super::DBInstance;



    #[test]
    fn test_sample() {

        let data = vec![vec!["name", "age", "size"], vec!["a", "12", "13"], vec!["c", "2", "13"]];
        
        let mut dt = Vec::new();
        for line in data {
            let mut row = Vec::new();
            for item in line {
                row.push(Text::new(item.to_string()))
            }
            dt.push(row);
        }
        let mut table = Builder::from_vec(dt).build();
        // table.with(Style::psql());
        println!("{}", table.to_string());

    }


    fn generate_test_table(db: &DBInstance) {
        db.generate_test_tables();
    }

    #[test]
    fn test_simple_sql() -> Result<(), String> {
        let mut instance = DBInstance::new("test.db")?;
        generate_test_table(&instance); 

        let sql = "insert into t1 values (1, 'test1', 'man', 1), (2, 'test2', 'female', 2)";
        let txn = Arc::new(Transaction::new());
        
        let res = instance.execute_sql_txn(sql, txn);
        if res.is_err() {
            println!("{}", res.unwrap_err())
        }

        let sql = "select * from t1 where a = 1";
        let txn = Arc::new(Transaction::new());
        
        let res = instance.execute_sql_txn(sql, txn);
        if res.is_err() {
            println!("{}", res.unwrap_err())
        }

        Ok(())
    }
}

