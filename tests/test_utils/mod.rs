//! Shared test utilities
//!
//! Note: clippy reports false-positive dead_code warnings because it can't
//! trace usage across test binaries. These utilities are used by multiple tests.

#![allow(dead_code)]

pub mod auth;
pub mod certs;
pub mod cleanup;
pub mod server_manager;
