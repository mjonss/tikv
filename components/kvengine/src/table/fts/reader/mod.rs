// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

mod brute_force;
mod brute_force_cond;
mod delta;
mod index;
mod index_columnar;
mod join;
mod misc;

pub use brute_force::*;
pub use brute_force_cond::*;
pub use delta::*;
pub use index::*;
pub use index_columnar::*;
pub use join::*;
pub use misc::*;

#[inline]
fn effective_top_k(query: &clara_fts::Query) -> Option<usize> {
    if query.info().get_query_type() != tipb::FtsQueryType::FtsQueryTypeWithScore {
        return None;
    }
    let top_k = query.info().get_top_k() as usize;
    if top_k == 0 { None } else { Some(top_k) }
}
