//! RooDB integration test suite.
//!
//! Comprehensive SQL tests organized by category:
//! - ddl: CREATE/DROP TABLE, INDEX
//! - dml: INSERT, UPDATE, DELETE
//! - queries: SELECT, WHERE, JOINs, aggregates, ordering
//! - types: Data type coverage
//! - functions: Scalar functions
//! - transactions: BEGIN/COMMIT/ROLLBACK
//! - errors: Error handling
//! - edge_cases: Complex scenarios

mod harness;

pub mod cluster;
pub mod ddl;
pub mod dml;
pub mod edge_cases;
pub mod errors;
pub mod queries;
pub mod types;

pub use harness::TestServer;
