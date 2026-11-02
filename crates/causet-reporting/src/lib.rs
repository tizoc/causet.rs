#![cfg_attr(test, allow(clippy::arithmetic_side_effects, clippy::indexing_slicing))]

//! The reporting pipeline: error, metric, event and healthcheck reports;
//! dedupe, rate limits, sampling and metric aggregation; JSON-lines dumps;
//! context trails. It knows nothing of the state machine: the application
//! wires it in as a Service, and delivers, signs and identifies records
//! itself.

pub mod config;
pub mod context_trail;
pub mod filesystem;
pub mod pipeline;
pub mod types;

#[doc(hidden)]
pub use causet_pooled_payload::pool_record_object;
pub use causet_pooled_payload::{FieldValue, display, json};
pub use config::{
    Config, DedupeConfig, DumpConfig, ENVELOPE_VERSION, MetricAggregateConfig, MetricConfig,
    MetricPolicy, RateLimitConfig, SamplingConfig,
};
pub use context_trail::ContextTrail;
pub use filesystem::Filesystem;
pub use pipeline::Pipeline;
pub use types::{
    AcceptOutcome, AcceptStats, DedupeKey, Payload, PayloadBuilder, Record, Report, ReportKind,
    Source,
};

#[cfg(test)]
mod alloc_tests;
#[cfg(test)]
mod pipeline_tests;
#[cfg(test)]
mod size_tests;
#[cfg(test)]
// Test-only global allocator so we can assert allocation-free report creation.
#[global_allocator]
static ALLOC: alloc_tests::TrackingAlloc = alloc_tests::TrackingAlloc;

/// Build an `Payload` with JSON-like syntax using the TLS pool.
///
/// This macro accepts the same object DSL as `pool_record_object!`:
/// - `key: value`, nested `key: { ... }`, nested arrays `key: [ ... ]`
/// - dynamic keys: `[expr]: value`
/// - optional fields: `key?: option_expr`, `[expr]?: option_expr`
/// - shorthand fields: `key` and optional shorthand `key?`
#[macro_export]
macro_rules! payload {
    ({ $($tt:tt)* }) => {{
        let __payload = $crate::PayloadBuilder::empty();
        let __payload = $crate::payload_object!(__payload, { $($tt)* });
        __payload.finish()
    }};
}

/// Build an `PayloadBuilder` with JSON-like syntax using the TLS pool.
///
/// Use this when you want to seed fields with the macro DSL and continue building
/// via combinators (`with_array_objects`, `with_field_opt`, etc.) before finishing.
#[macro_export]
macro_rules! payload_builder {
    ({ $($tt:tt)* }) => {{
        let __payload = $crate::PayloadBuilder::empty();
        $crate::payload_object!(__payload, { $($tt)* })
    }};
}

/// Apply an object body to an `PayloadBuilder` or object builder.
///
/// This is useful inside closures such as `with_array_objects(..., |obj, item| ...)`.
#[macro_export]
macro_rules! payload_object {
    ($builder:expr, { $($tt:tt)* }) => {{
        $crate::pool_record_object!($builder, { $($tt)* })
    }};
}
