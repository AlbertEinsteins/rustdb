#![allow(warnings)]

use crate::{catalog::catalog::CataLogRef, execution::plans::plan::PlanNodeRef};


pub struct Optimizer {
    catalog: CataLogRef,
}

pub trait Optimizable {
    fn optimize(plan: PlanNodeRef) -> PlanNodeRef;
}


impl Optimizable for Optimizer {
    fn optimize(plan: PlanNodeRef) -> PlanNodeRef {
        todo!()
    }
}
