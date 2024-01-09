use crate::graph::GraphEdgeType;

use super::{
    DelayType, OperatorCategory, OperatorConstraints, IDENTITY_WRITE_FN, RANGE_0, RANGE_1,
};

/// See `defer_tick`
/// This operator is identical to defer_tick except that it does not eagerly cause a new tick to be scheduled.
pub const DEFER_TICK_LAZY: OperatorConstraints = OperatorConstraints {
    name: "defer_tick_lazy",
    categories: &[OperatorCategory::Control],
    hard_range_inn: RANGE_1,
    soft_range_inn: RANGE_1,
    hard_range_out: RANGE_1,
    soft_range_out: RANGE_1,
    num_args: 0,
    persistence_args: RANGE_0,
    type_args: RANGE_0,
    is_external_input: false,
    ports_inn: None,
    ports_out: None,
    input_delaytype_fn: |_| Some(DelayType::TickLazy),
    input_edgetype_fn: |_| Some(GraphEdgeType::Value), output_edgetype_fn: |_| GraphEdgeType::Value,
    flow_prop_fn: None,
    write_fn: IDENTITY_WRITE_FN,
};
