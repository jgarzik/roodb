//! Protocol-neutral query execution.
//!
//! Wire protocols (the RooDB/MySQL-compatible protocol, TDS) parse client
//! requests and hand SQL to the query layer; the query layer reports what
//! happened through a [`ResponseSink`], which each protocol implements as
//! its encoder.

pub mod engine;
pub mod error;
pub mod metrics;
pub mod prepared;
pub mod sink;

pub use engine::QueryEngine;
pub use error::QueryError;
pub use sink::{OkCommand, OkInfo, ResponseSink, Status};
