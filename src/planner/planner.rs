#![allow(warnings)]

use crate::{binder::{bound_expression::{BoundExpression, BoundExpressionFeat}, bound_statement::BoundStatement, bound_table_ref::{BoundTable, BoundTableRef}, statement::{insert_stmt::InsertStmt, select_stmt::SelectStmt}, table_ref::{bound_base_table::BoundBaseTableRef, bound_values_list_table::BoundValuesList}}, catalog::{catalog::{CataLog, CataLogRef}, column::Column, schema::{Schema, SchemaRef}}, common::config::VARCHAR_DEFAULT_LENGTH, execution::{expressions::expr::{Expression, ExpressionRef}, plans::plan::{InsertPlan, PlanNode, PlanNodeRef, SeqScanPlan, ValuesPlan}}, typedef::{integer_type::IntegerType, type_id::TypeId}};

pub struct Planner {
    catalog: CataLogRef,
    universal_id: u32,
}


impl Planner {
    pub const UNAMED_COLUMN: &str = "<unamed>";

    pub fn new(catalog: CataLogRef) -> Self {
        Self {
            catalog,
            universal_id: 0,
        }
    }

    pub fn get_incr_universal_id(&mut self) -> u32 {
        let res = self.universal_id;
        self.universal_id += 1;
        res
    }

    pub fn is_agg_expr(expr: &BoundExpression) -> bool {
        match &expr {
            BoundExpression::AggCall(_) => true,
            BoundExpression::UnaryOp(unary) => unary.has_aggregation(),
            BoundExpression::BinaryOp(binary) => binary.has_aggregation(),
            _ => false,
        }
    }

    pub fn plan_statement(&mut self, stmt: &BoundStatement) -> Result<PlanNodeRef, String> {
        match stmt {
            BoundStatement::Select(sel) => {
                self.plan_select(sel)
            },
            BoundStatement::Insert(insert) => {
                self.plan_insert(insert)
            },
            _ => {
                Err(format!("Not support statement type"))
            }
        }
    }



    pub fn plan_insert(&mut self, insert: &InsertStmt) -> Result<PlanNodeRef, String> {
        let bound_table = &insert.table_ref;
        let sel_plan = self.plan_select(&insert.select)?;
        
        let table_schema = &bound_table.schema.get_columns();
        let child_schema = sel_plan.get_output_schema().get_columns();
        if !table_schema.iter().zip(child_schema.iter()).all(|(a, b)| a.get_type() == b.get_type()) {
            return Err(format!("child schema mismatched the table schema"));
        }
        
        let insert_plan_schema = Schema::new(&vec![Column::new("__rows", TypeId::INTEGER)]);
        let insert_plan = PlanNodeRef::new(
            PlanNode::Insert(
                InsertPlan { 
                    output_schema: insert_plan_schema,
                    children: vec![sel_plan],
                    table_id: bound_table.table_id,
                }
            )
        );
        Ok(insert_plan)
    }


    pub fn plan_table_ref(&self, table_ref: &BoundTableRef) -> Result<PlanNodeRef, String> {
        match table_ref.as_ref() {
            BoundTable::BaseTable(base_table) => {
                self.plan_base_table_ref(base_table)
            },
            BoundTable::ValuesList(values_table) => {
                self.plan_values_list_table(values_table)
            },
            _ => { panic!("not support"); }
        }
    } 
 
    pub fn plan_values_list_table(&self, values_list: &BoundValuesList) -> Result<PlanNodeRef, String> {
        let mut rows = Vec::new();
        let bound_rows = &values_list.values_list;
        let children = Vec::new();
        for bound_row in bound_rows {
            let mut row = Vec::new();
            for bound_expr in bound_row {
                let (_, expr) = self.plan_expression(bound_expr, &children)?;
                row.push(expr);
            }
            rows.push(row);
        }

        let mut col_idx = 0;
        let mock_name = &values_list.mock_name;
        let mut cols = Vec::new();
        for bound_expr in &bound_rows[0] {
            let BoundExpression::Constant(constant) = bound_expr.as_ref() else { panic!("Error: bound_values_list err"); };
            let constant_type = constant.val.get_type();
            let col_name = format!("{}.{}", mock_name.clone(), col_idx);
            if constant_type != TypeId::VARCHAR {
                cols.push(Column::new(&col_name, constant_type))
            } else {
                cols.push(Column::new_varchar(&col_name, constant_type, VARCHAR_DEFAULT_LENGTH))
            }

            col_idx += 1;
        }

        let plan = PlanNodeRef::new(PlanNode::Values(
            ValuesPlan::new(
                SchemaRef::new(Schema::new(&cols)),
                rows,
            )
        ));

        Ok(plan)
    }

    pub fn plan_base_table_ref(&self, base_table: &BoundBaseTableRef) -> Result<PlanNodeRef, String> {
        let table_name = &base_table.table_name;
        let catalog = self.catalog.borrow();
        if catalog.get_schema(table_name).is_none() {
            return Err(format!("table not found"));
        }

        // println!("{}", catalog.get_schema(table_name).unwrap().get_columns().iter().map(|col| col.get_name()).collect::<Vec<String>>().join(","));
        let table = catalog.get_table(table_name).unwrap();
        let plannode = PlanNodeRef::new(PlanNode::SeqScan(
            SeqScanPlan::new(SeqScanPlan::infer_schema(base_table), 
                table.table_oid, 
                table.table_name.clone(), 
                None)
        ));

        Ok(plannode)
    }


}