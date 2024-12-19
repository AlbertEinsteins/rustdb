#![allow(warnings)]

use sqlparser::{ast::{self, CharacterLength, ColumnDef, DataType, Expr, GroupByExpr, Select, SelectItem, SetExpr, Statement, TableWithJoins, TableFactor, Insert, Query, Values, CreateTable}, dialect::Dialect, parser::{Parser, ParserError}};

use crate::{binder::{bound_table_ref::BoundTable, expression::{bound_alias::BoundAlias, bound_column_ref::BoundColumn, bound_constant::BoundConstant, bound_star::BoundStar}, table_ref::bound_base_table::BoundBaseTableRef}, catalog::{catalog::{CataLog, CataLogRef}, column::Column, schema::Schema}, typedef::{type_id::TypeId, value_factory::ValueFactory}};

use super::{bound_expression::BoundExpression, bound_statement::BoundStatement, bound_table_ref::BoundTableRef, expression::bound_binary_op::{BinaryOpType, BoundBinaryOp}, statement::{create_stmt::CreateStmt, insert_stmt::InsertStmt, select_stmt::SelectStmt}, table_ref::{bound_empty_table::BoundEmptyTable, bound_values_list_table::BoundValuesList}};


pub struct Binder {
    catalog: CataLogRef,
    universal_id: i32,
    scope: Option<BoundTableRef>,

    pub stmts: Option<Vec<Statement>>,
}


impl Binder {

    // this function will create a the-thirty-party parser, 
    // and receive a catalog as the parameter which we will need later.
    pub fn new(catalog: CataLogRef) -> Self {
        Self {
            catalog,
            stmts: None,
            universal_id: 0,
            scope: None,
        }
    }

    pub fn parse_and_save(&mut self, sql: &str, dialect: &dyn Dialect) -> Result<(), ParserError> {
        let parse_res = Parser::parse_sql(dialect, sql)?;
        self.stmts = Some(parse_res);
        Ok(())
    }

    pub fn bind_statement(&mut self, stmt: &Statement) -> Result<BoundStatement, String> {
        match stmt {
            Statement::CreateTable(_) => {
                return Ok(BoundStatement::Create(*self.bind_create(stmt)?));
            },
            Statement::Insert(_) => {
                return Ok(BoundStatement::Insert(*self.bind_insert(stmt)?));
            },
            Statement::Query(_) => {
                return Ok(BoundStatement::Select(*self.bind_select(stmt)?));
            },
            _ => {
                panic!("Not supprot yet.");
            }
        }
    }


    // parse a create statements
    pub fn bind_create(&self, stmt: &Statement) -> Result<Box<CreateStmt>, String> {
        assert!(matches!(stmt, Statement::CreateTable(_)));
        let Statement::CreateTable(create_stmt) = stmt else {
            panic!("Impossible Branch");
        };

        // assert column count
        if create_stmt.columns.len() == 0 {
            return Err(format!("Table definition needs at least one column"));
        }
        
        let mut cols: Vec<Column> = Vec::new();
        for col in &create_stmt.columns {
            // check other things
            // col.options.
            cols.push(Binder::get_column(&col)?);
        }

        let stmt = CreateStmt::new(create_stmt.name.0[0].value.clone(), cols);
        Ok(Box::new(stmt))
    }

    // parse a insert statement
    pub fn bind_insert(&mut self, stmt: &Statement) -> Result<Box<InsertStmt>, String> {
        assert!(matches!(stmt, Statement::Insert(_)));

        let Statement::Insert(insert_stmt) = stmt else { panic!("Impossible Branch"); };

        // bind table name
        let table_name = insert_stmt.table_name.0[0].value.clone();
        let table_oid;
        let table_schema;
        {
            let catalog = self.catalog.borrow();
            let table_info = catalog.get_table(&table_name);
            if table_info.is_none() {
                return Err(format!("Non exist table"));
            }
            let table_info = table_info.unwrap();
            table_oid = table_info.table_oid;
            table_schema = table_info.schema.clone();
        }
        let bound_base_table = BoundBaseTableRef::new(table_name, table_oid, None, table_schema);
        
        Ok(
            Box::new(
                InsertStmt::new(Box::new(bound_base_table), self.bind_select(stmt)?)
            )
        )
    }

    // parse a 
    pub fn bind_select(&mut self, stmt: &Statement) -> Result<Box<SelectStmt>, String> {
        assert!(matches!(stmt, Statement::Query(_)) 
            || matches!(stmt, Statement::Insert(_)) );
        
        let Statement::Query(query_stmt) = &stmt else {
            let Statement::Insert(insert_stmt) = &stmt else { panic!("Impossible branch"); };
            // if is Insert just handle it and return directly
            let values_list_table_name = format!("__values#{}", self.universal_id);
            self.universal_id += 1;
            
            // bind a table
            let mut values_list_table = self.bind_values_list(&insert_stmt)?;
            values_list_table.mock_name = values_list_table_name.clone();
            // and with column refs
            let col_len = values_list_table.values_list[0].len();
            let mut bound_cols = Vec::new();
            for i in 0..col_len {
                bound_cols.push(
                    Box::new(BoundExpression::ColumnRef(
                        Box::new(BoundColumn { col_name: vec![values_list_table_name.clone(), format!("{}", i)]   }))
                    )
                );
            }

            let sel_stmt = SelectStmt::new(
                BoundTableRef::new(BoundTable::ValuesList(values_list_table)),
                bound_cols,
                None,
                Vec::new(),
                None,
                None,
                None,
                Vec::new(),
                false
            );
            return Ok(Box::new(sel_stmt));
        };

        assert!(matches!(*query_stmt.body, SetExpr::Select(_)));
        let SetExpr::Select(sel) = &*(query_stmt.body) else {
            panic!("impossible branch");
        };

        // bind from
        let tables = self.bind_from(sel)?;
        self.scope = Some(tables.clone());

        // bind select list
        let select_items = &sel.projection;
        let mut select_list = Vec::new();
        if !select_items.is_empty() {
            select_list.extend(self.bind_select_list(&select_items)?);
        }
        
        // bind distinct
        let mut is_distinct = false;
        if let Some(_) = sel.distinct {
            is_distinct = true;
        }

        // bind where
        let mut where_cond = None;
        if let Some(selection) = &sel.selection {
            where_cond = Some(self.bind_where(selection)?);
        }

        // bind group by
        let mut group_by = Vec::new();
        if let GroupByExpr::Expressions(exprs, _) = &sel.group_by {
            if !exprs.is_empty() {
                group_by = Self::bind_group_by()?;
            }
        }

        // bind having
        let mut having_cond = None;
        if let Some(having) = &sel.having {
            having_cond = Some(Self::bind_having()?);
        }

        // bind limit
        let mut limit_val = None;
        if let Some(limit) = &query_stmt.limit {
            limit_val = Some(Self::bind_limit()?);
        }

        // bind offset
        let mut offset_val = None;
        if let Some(offset) = &query_stmt.offset {
            offset_val = Some(Self::bind_offset()?);
        }

        // bind order by
        let mut order_by = Vec::new();
        if let Some(order) = &query_stmt.order_by {
            order_by = Self::bind_order_by()?;
        }

        let select_stmt = SelectStmt::new(tables, select_list, where_cond, 
            group_by, 
            having_cond,
            limit_val,
            offset_val,
            order_by,
            is_distinct
        );
        Ok(Box::new(select_stmt))

    }




    // ========================== static method ===============================
    pub fn bind_values_list(&self, insert_stmt: &Insert) -> Result<BoundValuesList, String> {
        let Some(Query { body, .. } ) = &insert_stmt.source.as_deref() else {
            panic!("Error");
        };

        // extract rows
        let SetExpr::Values(Values{ rows, .. }) = body.as_ref() else {
            panic!("Error");
        };

        let mut values_list = Vec::new();
        for row in rows {
            let mut values =  Vec::new();
            for v in row {
                values.push(self.bind_expr(v)?);
            }

            values_list.push(values);
        }

        Ok(BoundValuesList { values_list, mock_name: "<unnamed>".to_owned() })
    }

    pub fn get_column(col: &ColumnDef) -> Result<Column, String> {
        let col_name = col.name.value.clone();

        match col.data_type {
            DataType::Int(_) => {
                Ok(Column::new(&col_name, TypeId::INTEGER))
            },
            DataType::Varchar(len) => {
                let Some(len) = len else {
                    return Err(format!("The varchar type needs a length"));
                };

                let CharacterLength::IntegerLength{length, unit: _} = len else {
                    return Err(format!("Not support"));
                };

                Ok(Column::new_varchar(&col_name, TypeId::VARCHAR, length as u32))
            },
            _ => {
                Err(format!("Not support data type"))
            }
        }
    }   


    pub fn bind_select_list(&self, projection: &Vec<SelectItem>) -> Result<Vec<Box<BoundExpression>>, String> {
        let mut expr_list = Vec::new();

        let mut has_star = false;
        for select_item in projection {
            let expr = self.bind_select_item(select_item)?;

            // check if select *
            if let BoundExpression::Star(bound_star) = expr.as_ref() {
                if has_star {
                    return Err(format!("select * should only exists once"));
                }
                let bound_cols = self.get_all_columns();
                expr_list.extend(bound_cols);
            } else {
                if has_star {
                    return Err(format!("should not exist other column if exists select *"));
                }
                expr_list.push(expr);
            }
        }

        Ok(expr_list)
    }

    // we rewrite * to the actual column names with a bound table name
    pub fn get_all_columns(&self) -> Vec<Box<BoundExpression>> {
        let scope = self.scope.as_ref().unwrap();
        match scope.as_ref() {
            BoundTable::BaseTable(base_table) => {
                // get the schmea
                let bound_table_name = base_table.get_bound_name();
                let mut bound_exprs = Vec::new();

                for col in base_table.schema.get_columns() {
                    bound_exprs.push(Box::new(BoundExpression::ColumnRef(
                        Box::new(BoundColumn { col_name: vec![bound_table_name.clone(), col.get_name()] })
                    )))
                }
                return bound_exprs;
            },
            _ => {

            }
        }

        todo!()
    }

    // handle different silutations, such as 
    // 1.column itself
    // 2.column with alias
    // 3.alias.* 
    // 4.only * 
    pub fn bind_select_item(&self, item: &SelectItem) -> Result<Box<BoundExpression>, String> {
        // todo!()
        match item {
            SelectItem::UnnamedExpr(expr) => {
                return self.bind_expr(expr);
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound_expr = self.bind_expr(expr)?;
                let alias_name = alias.value.clone();

                let bound_alias = BoundExpression::Alias(
                    Box::new(BoundAlias { alias: alias_name, expr: *bound_expr })
                );

                return Ok(Box::new(bound_alias));
            },
            SelectItem::QualifiedWildcard(alias, wild_opts) => {
                // TODO: table_alias.*
                let bound_star = BoundStar { table_or_alias: Some(alias.0[0].value.clone()) };
                let bound_expr = BoundExpression::Star(Box::new(bound_star));
                return Ok(Box::new(bound_expr));
            },
            SelectItem::Wildcard(wild_opts) => {
                let bound_star = BoundStar { table_or_alias: None };
                let bound_expr = BoundExpression::Star(Box::new(bound_star));
                return Ok(Box::new(bound_expr));
            }
        }

        todo!()
    }

    pub fn bind_expr(&self, item: &Expr) -> Result<Box<BoundExpression>, String> {
        match item {
            Expr::Identifier(ident) => {
                return Ok(
                    Box::new(
                        BoundExpression::ColumnRef(
                            self.bind_column(self.scope.as_ref().unwrap(), &vec![ident.clone()])?
                        )
                    )
                );
            },
            Expr::Value(val) => {
                return Self::bind_value(val);
            },
            Expr::CompoundIdentifier(idents) => {
                if idents.len() > 2 {
                    return Err(String::from("Not support expr"));
                }

                return Ok(
                    Box::new(
                        BoundExpression::ColumnRef(self.bind_column(self.scope.as_ref().unwrap(), idents)?)
                    ) 
                );
            },
            Expr::BinaryOp { left, op, right } => {
                let bound_left = self.bind_expr(&left)?;
                let bound_right = self.bind_expr(&right)?;

                return Ok(
                    Box::new(
                        BoundExpression::BinaryOp(
                            Box::new(BoundBinaryOp::new(*bound_left, *bound_right, BinaryOpType::from(op.clone())))
                        )
                    )
                )
            }
            // TODO: other types handles
            _ => {
               return Err(String::from("Not support expr"));
            }
        }
    }

    pub fn bind_column(&self, scope: &BoundTableRef, idents: &Vec<ast::Ident>) -> Result<Box<BoundColumn>, String> {
        // 1.table.col
        // 2.col

        let bound_col = self.resolve_column_internal(scope, idents);        
        if bound_col.is_err() {
            return Err(format!("col {} not found", idents.iter().map(|id| id.value.clone()).collect::<Vec<String>>().join(",")));
        }
        return bound_col;
    }

    // resolve from different tables
    // such as base_table,
    // cross_table
    // join table
    // etc..
    fn resolve_column_internal(&self, scope: &BoundTableRef, col_name: &Vec<ast::Ident>) -> Result<Box<BoundColumn>, String> {
        match scope.as_ref() {
            BoundTable::BaseTable(base) => {
                self.resolve_column_from_base_table(base, col_name)
            },


            _ => {
                panic!("Not ready");
            }
        }
    }

    // bind col from a base table, returns (table_name.col_name)
    // has two conditions usually
    // 1.(table_name/table_alias_name).col_name
    // 2. col_name
    fn resolve_column_from_base_table(&self, base_table: &BoundBaseTableRef, col_name: &Vec<ast::Ident>) -> Result<Box<BoundColumn>, String> {
        let direct_name = self.resolve_column_from_schema(&base_table.schema, col_name)
            .map(|col_name| BoundColumn::prepend(base_table.get_bound_name(), col_name));
        
        // check if has the prefix
        let mut strip_name = Err(format!("None"));
        if col_name.len() > 1 {
            if col_name[0].value == base_table.get_bound_name() {
                strip_name = self.resolve_column_from_schema(&base_table.schema, &vec![col_name[1].clone()])
                    .map(|col_name| BoundColumn::prepend(base_table.get_bound_name(), col_name));
            }
        }

        if direct_name.is_ok() && strip_name.is_ok() {
            return Err(format!("ambiguous column {} in table {}", direct_name.expect("").col_name.join(","), base_table.table_name));
        }   

        if strip_name.is_ok() {
            return strip_name;
        }
        return direct_name;
    }

    // check if a col_name is in the shcema
    fn resolve_column_from_schema(&self, schema: &Schema, col_name: &Vec<ast::Ident>) -> Result<Box<BoundColumn>, String> {
        if col_name.len() != 1 {
            return Err(format!("col is must a simple name, excludes the prefix when bound a col from a base table"));
        }

        let mut col_ref = None;
        for col in schema.get_columns() {
            if col_name[0].value == col.get_name() {
                if col_ref.is_some() {
                    return Err(format!("col {} is ambigous in schema", col_name[0].value))
                }
                col_ref = Some(Box::new(
                    BoundColumn { col_name: vec![col.get_name()] }
                ))
            }
        }
        Ok(col_ref.unwrap())
    }

    // TODO: bind a literal value
    pub fn bind_value(val: &ast::Value) -> Result<Box<BoundExpression>, String> {
        match val {
            ast::Value::Number(literal, _) => {
                let int_res = literal.parse();
                match int_res {
                    Ok(integer) => {
                        let val = ValueFactory::get_integer_value(integer);
                        let const_expr = Box::new(BoundConstant::new(val));
                        return Ok(Box::new(BoundExpression::Constant(const_expr)));
                    },
                    Err(err) => {
                        return Err(format!("{}", err));
                    }
                }
            },
            ast::Value::SingleQuotedString(s) => {
                let val = ValueFactory::get_varchar_value(s);
                let const_expr = Box::new(BoundConstant::new(val));
                return Ok(Box::new(BoundExpression::Constant(const_expr)));
            },
            _ => {

            }
        }

        todo!()
    }

    pub fn bind_from(&self, sel: &Box<Select>) -> Result<BoundTableRef, String> {
        let tables = &sel.from;

        if tables.is_empty() {
            // empty table
            let empty_table = BoundEmptyTable {};
            return Ok(BoundTableRef::new(BoundTable::Empty(empty_table)));
        }

        if tables.len() > 1 {
            //TODO: 
        } else {
            if tables[0].joins.is_empty() {
                return self.bind_table_ref(&tables[0]);
            }
            return Self::bind_table_with_join(&tables[0]);
        }
        todo!()
    }

    pub fn bind_table_with_join(table: &TableWithJoins) -> Result<BoundTableRef, String> {
        todo!()
    }

    pub fn bind_table_ref(&self, table: &TableWithJoins) -> Result<BoundTableRef, String> {
        
        let table_rel = &table.relation;
        if let TableFactor::Table { name, alias, ..  } = table_rel {
            let table_name = name.0[0].value.clone();
            // check name exists
            let schema = self.catalog.borrow().get_schema(&table_name);
            if schema.is_none() {
                return Err(format!("table not found"));
            }
            let mut base_table_ref = BoundBaseTableRef::new(table_name, 0, None, schema.unwrap()); 
            base_table_ref.alias = alias.clone().map(|t| t.name.value);

            Ok(BoundTableRef::new(BoundTable::BaseTable(base_table_ref)))
        } else {
            panic!("Impossible branch");
        }
    }

    pub fn bind_where(&self, selection: &Expr) -> Result<Box<BoundExpression>, String> {
        self.bind_expr(selection)
    }

    pub fn bind_group_by() -> Result<Vec<Box<BoundExpression>>, String> {
        todo!()
    }
    
    pub fn bind_having() -> Result<Box<BoundExpression>, String> {
        todo!()
    }

    pub fn bind_limit() -> Result<Box<BoundExpression>, String> {
        todo!()
    }

    pub fn bind_offset() -> Result<Box<BoundExpression>, String> {
        todo!()
    }

    pub fn bind_order_by() -> Result<Vec<Box<BoundExpression>>, String> {
        todo!()
    }


}







#[cfg(test)]
mod tests {
    use sqlparser::{dialect::GenericDialect, ast::{Statement, SetExpr}, parser::Parser};

    use crate::{catalog::catalog::CataLog, binder::bound_statement::BoundStatementFeat};

    use super::Binder;


    #[test]
    fn parser() {
        let sql2 = "create table product(a int, b varchar(20), c int)";
        let dialect = GenericDialect{};

        let asts = Parser::parse_sql(&dialect, sql2).unwrap();
        let ast = &asts[0];
        let Statement::CreateTable(stmt) = ast else {
            todo!()
        };

        println!("{:#?}", stmt.name)
    }

    #[test]
    fn parser_test_select() {
        let sql = "select 1, 'name', t.b as tt, a, t.* from 
                            t join t2 on t.id = t2.tid
                            join t3 on t.id = t3.tid 
                            where a > 1 and b > 1 order by a limit 10";

        let ast = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
        let Statement::Query(stmt) = &ast[0] else {
            todo!()
        };

        println!("{:#?}", stmt);
        let a = stmt.body.clone();
        match *a {
            SetExpr::Select(sel) => {
                println!("{:#?}", sel);
            },
            SetExpr::Values(_) => {

            },
            _ => {

            }
        }
        println!()
    }

    #[test]
    fn bind_insert_test() {
        let sql = "insert into a values (1, 2, 'asd'), (2, 3, 'add')";

        let ast = Parser::parse_sql(&GenericDialect{}, sql).unwrap();
        let Statement::Insert(stmt) = &ast[0] else {
            todo!()
        };

        println!("{:#?}", stmt);
        // let a = stmt.body.clone();
        // match *a {
        //     SetExpr::Select(sel) => {
        //         println!("{:#?}", sel);
        //     },
        //     SetExpr::Values(_) => {

        //     },
        //     _ => {

        //     }
        // }
        println!()
    }

    #[test]
    fn test_base() {
        // let calog = CataLog::new(None, None, None);

        // let mut binder = Binder::new(&calog);
        // let sql = "insert into a values (1, 2, 'asd'), (2, 3, 'add')";

        // let _ = binder.parse_and_save(sql, &GenericDialect{});

        // let stmts = binder.stmts.clone();
        // let res = binder.bind_insert(&stmts.unwrap()[0]).unwrap();

        // println!("{}", res.to_string())
    }
}
