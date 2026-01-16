//! SQL layer - parsing, resolution, and type checking
//!
//! This module provides:
//! - `Parser`: Parses SQL text into sqlparser AST
//! - `Resolver`: Resolves names and types against the catalog
//! - `TypeChecker`: Validates types in expressions

pub mod error;
pub mod parser;
pub mod resolver;
pub mod typecheck;

pub use error::{SqlError, SqlResult};
pub use parser::Parser;
pub use resolver::{convert_data_type, Resolver};
pub use typecheck::TypeChecker;
