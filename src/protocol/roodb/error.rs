//! RooDB protocol error types
//!
//! The error type and the error-code catalogue are shared with the query
//! layer and defined in [`crate::server::query::error`].

pub use crate::server::query::error::{codes, states, ProtocolError, ProtocolResult};
