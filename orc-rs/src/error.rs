//! Contains [`Error`]
use std::sync::Arc;

/// Errors generated by this crate
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// General ORC error.
    General(String),
    /// Error caused when an ORC file doesn't get parsed correctly.
    ParseError(String),
    /// An error originating from a consumer or dependency
    External(String, Arc<dyn std::error::Error + Send + Sync>),
}

impl std::error::Error for Error {}