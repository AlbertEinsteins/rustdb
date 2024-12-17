#![allow(warnings)]

use std::sync::Arc;

use crate::{binder::{bound_table_ref::{BoundTable, BoundTableRef}, statement::select_stmt::SelectStmt}, catalog::schema::Schema, execution::plans::plan::{PlanNode, PlanNodeRef, ProjectionPlan, ValuesPlan}};

use super::planner::Planner;


impl Planner {

    pub fn plan_select(&mut self, select: &SelectStmt) -> Result<PlanNodeRef, String> {

        // plan from 
        let mut plan: PlanNodeRef;
        if let BoundTable::Empty(_) = &select.table_ref.as_ref() {
            let values_plan_node = ValuesPlan::new(
                Arc::new(Schema::new(&Vec::new())), 
                Vec::new());
            plan = Arc::new(PlanNode::Values(values_plan_node));
        } else {
            plan = self.plan_table_ref(&select.table_ref)?;
        }

        // plan where
        if let Some(where_cond) = &select.where_by {
            todo!()
        }

        // plan agg or normal select
        let mut has_agg = false;
        for item in &select.select_list {
            if Self::is_agg_expr(item) {
                has_agg = true;
                break;
            }
        }

        if select.having.is_some() || select.group_by.len() > 0 || has_agg {
            plan = self.plan_select_agg(select, plan);
        } else {
            // plan normal select
            let mut cols = Vec::new();
            let mut exprs = Vec::new();
            let children = vec![plan];

            for item in &select.select_list {
                let (mut name, expr) = self.plan_expression(&item, &children)?;
                if Self::UNAMED_COLUMN == name {
                    name = format!("__unamed#{}", self.get_incr_universal_id());
                }
                exprs.push(expr);
                cols.push(name);
            }

            plan = Arc::new(PlanNode::Proj(
                    ProjectionPlan::new(
                        ProjectionPlan::rename_schema(
                            ProjectionPlan::infer_schema(&exprs),
                            cols,
                        )?, 
                        exprs,
                        children)
            ));
        }
        Ok(plan)
    }
}