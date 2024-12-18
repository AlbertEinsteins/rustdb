#![allow(warnings)]

use std::{sync::Arc, fmt::Display, process::ChildStderr};

use crate::{binder::{bound_expression::BoundExpression, table_ref::bound_base_table::BoundBaseTableRef}, catalog::{column::Column, schema::{Schema, SchemaRef}}, common::config::{table_id_t, VARCHAR_DEFAULT_LENGTH}, execution::expressions::expr::ExpressionRef, typedef::type_id::TypeId};



pub type PlanNodeRef = Arc<PlanNode>;

// define somple plan
#[derive(Debug)]
pub enum PlanNode {
    SeqScan(SeqScanPlan),
    Proj(ProjectionPlan),
    Insert(InsertPlan),
    Values(ValuesPlan)
}

impl PlanNode {
    pub fn get_output_schema(&self) -> &Schema {
        match self {
            PlanNode::SeqScan(seqscan) => { seqscan.output_schema() },
            PlanNode::Proj(proj) => { proj.output_schema() },
            PlanNode::Insert(insert) => { insert.output_schema() },
            PlanNode::Values(values) => { values.output_schema() },
        }
    }
}

impl Display for PlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SeqScan(seqscan) => {
                f.write_str(&seqscan.to_string(true))
            },
            Self::Proj(proj) => { f.write_str(&proj.to_string(true)) }
            Self::Insert(insert) => { f.write_str(&insert.to_string(true)) },
            Self::Values(vals) => { f.write_str(&vals.to_string(true)) },
        }
    }
}


pub trait PlanNodeFeat {
    fn to_string(&self, with_schema: bool) -> String {
        if with_schema {
            format!("{} | {}{}", self.plannode_tostring(), 
                self.output_schema().to_string(), self.children_tostring(2, with_schema))
        } else {
            format!("{}{}", self.plannode_tostring(), self.output_schema().to_string())
        }
    }

    fn children_tostring(&self, indent: i32, with_schema: bool) -> String {
        if (self.get_children().len() == 0) {
            return String::from("");
        }

        let children = self.get_children();
        let mut child_str = Vec::new();
        let indent = " ".repeat(indent as usize);
        child_str.reserve(children.len());
        for child in children {
            let s = child.to_string();
            let lines = s.split("\n");
            for line in lines {
                child_str.push(format!("{}{}", indent, line));
            }
        }

        format!("\n{}", child_str.join("\n"))
    }


    fn get_child_at(&self, idx: usize) -> PlanNodeRef {
        self.get_children()[idx].clone()
    }

    fn plannode_tostring(&self) -> String;

    fn get_children(&self) -> &Vec<PlanNodeRef>;
    fn output_schema(&self) -> &Schema;
}


// TODO
pub trait Predicate {

}





//=============================== Seq Scan ====================
#[derive(Debug)]
pub struct SeqScanPlan {
    pub output_schema: SchemaRef,
    pub children: Vec<PlanNodeRef>,


    pub table_id: table_id_t,
    pub table_name: String,
    // possisble predicate
    pub predicate: Option<ExpressionRef>
}

impl SeqScanPlan {
    pub fn new(out_schema: SchemaRef, table_id: table_id_t, table_name: String, 
        predicate: Option<ExpressionRef>) -> Self {
        Self { output_schema: out_schema, children: Vec::new(), table_id, table_name, predicate  }
    }

    pub fn infer_schema(base_table: &BoundBaseTableRef) -> SchemaRef {
        let mut cols = Vec::new();
        for c in base_table.schema.get_columns() {
            cols.push(c.replicate(format!("{}.{}", base_table.get_bound_name(), c.get_name())));
        }
        SchemaRef::new(Schema::new(&cols))
    }
}


impl PlanNodeFeat for SeqScanPlan {
    fn plannode_tostring(&self) -> String {
        format!("SeqScan {{{{ table={} }}}}", self.table_name)
    }

    fn get_children(&self) -> &Vec<PlanNodeRef> {
        &self.children
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema        
    }
}


//========================== Project Plan =================
#[derive(Debug)]

pub struct ProjectionPlan {
    pub output_schema: SchemaRef,
    pub children: Vec<PlanNodeRef>,

    pub expressions: Vec<ExpressionRef>,
}

impl ProjectionPlan {
    pub fn new(output_schema: SchemaRef, exprs: Vec<ExpressionRef>, child: Vec<PlanNodeRef>) -> Self {
        Self { output_schema, children: child, expressions: exprs }
    }

    pub fn infer_schema(exprs: &Vec<ExpressionRef>) -> SchemaRef {
        let mut columns = Vec::new();

        for expr in exprs {
            let rtn_type = expr.get_return_type();
            if TypeId::INTEGER == rtn_type {
                columns.push(Column::new("<unnamed>", rtn_type));
            } else {
                columns.push(Column::new_varchar("<unnamed>", rtn_type, VARCHAR_DEFAULT_LENGTH));
            }
        }

        SchemaRef::new(Schema::new(&columns))
    }

    pub fn get_child_plan(&self) -> PlanNodeRef {
        assert_eq!(1, self.children.len());
        self.get_child_at(0)
    }

    pub fn rename_schema(schema: SchemaRef, new_col_names: Vec<String>) -> Result<SchemaRef, String> {
        if (schema.get_column_count() != new_col_names.len()) {
            return Err(format!("mismatch column size"));
        }

        let mut cols = Vec::new();
        for (c, name) in schema.get_columns().iter().zip(new_col_names.iter()) {
            cols.push(c.replicate(name.clone()));
        }

        Ok(SchemaRef::new(Schema::new(&cols)))
    }
}



impl PlanNodeFeat for ProjectionPlan {

    fn plannode_tostring(&self) -> String {
        let exprs_str: Vec<String> = self.expressions.iter().map(|expr| expr.to_string()).collect();
        let out_str = String::from("[") + &exprs_str.join(", ") + "]";
        format!("Projection {{{{ exprs={} }}}}", out_str)
    }

    fn get_children(&self) -> &Vec<PlanNodeRef> {
        &self.children        
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema        
    }
}


//========================== Values Plan ==================
#[derive(Debug)]

pub struct ValuesPlan {
    pub output_schema: SchemaRef,
    pub children: Vec<PlanNodeRef>,

    pub values_list: Vec<Vec<ExpressionRef>>,
}

impl ValuesPlan {
    pub fn new(output_schema: SchemaRef, rows: Vec<Vec<ExpressionRef>>) -> Self {
        Self { output_schema, children: Vec::new(), values_list: rows }
    }
}


impl PlanNodeFeat for ValuesPlan {

    fn plannode_tostring(&self) -> String {
        format!("Values {{{{ rows={} }}}}", self.values_list.len())
    }


    fn get_children(&self) -> &Vec<PlanNodeRef> {
        &self.children        
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema        
    }
}


//========================== Insert Plan ==================
#[derive(Debug)]
pub struct InsertPlan {
    pub output_schema: Schema,
    pub children: Vec<PlanNodeRef>,

    pub table_id: table_id_t,
}

impl InsertPlan {
    pub fn get_child_plan(&self) -> PlanNodeRef {
        assert_eq!(1, self.children.len());
        self.get_child_at(0)
    }
}


impl PlanNodeFeat for InsertPlan {

    fn plannode_tostring(&self) -> String {
        format!("Insert {{{{ table_id={} }}}}", self.table_id)
    }

    fn get_children(&self) -> &Vec<PlanNodeRef> {
        &self.children        
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema        
    }
}


//============================= Filter Plan =========================//
// related to where clause

pub struct FilterPlan {
    pub output_schema: SchemaRef,
    pub children: Vec<PlanNodeRef>,
    pub predicate: BoundExpression
}


impl FilterPlan {
    pub fn new()
}

impl PlanNodeFeat for FilterPlan {
    fn plannode_tostring(&self) -> String {
        format!("FilterPlan {{{{ predicate={} }}}}", self.predicate)
    }

    fn get_children(&self) -> &Vec<PlanNodeRef> {
        
    }

    fn output_schema(&self) -> &Schema {
        
    }
}