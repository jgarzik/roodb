//! SQL layer - parsing, resolution, and type checking
//!
//! This module provides:
//! - `Parser`: Parses SQL text into internal AST
//! - `Resolver`: Resolves names against the catalog
//! - `TypeChecker`: Validates types in expressions

pub mod ast;
pub mod error;
pub mod parser;
pub mod resolver;
pub mod typecheck;

pub use ast::*;
pub use error::{SqlError, SqlResult};
pub use parser::Parser;
pub use resolver::Resolver;
pub use typecheck::TypeChecker;
