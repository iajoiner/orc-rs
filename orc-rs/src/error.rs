//! Contains [`Error`]
use std::sync::Arc;
use thiserror::Error;

/// Errors generated by this crate
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum OrcError {
    #[error("General ORC Error")]
    General(String),
    #[error("Error caused when an ORC file doesn't get parsed correctly")]
    ParseError(String),
    #[error("Error caused when a function is called on a wrong datatype")]
    DataTypeError(String),
    #[error("An error originating from a consumer or dependency")]
    External(String, Arc<dyn std::error::Error + Send + Sync>),
}
pub type OrcResult<T> = std::result::Result<T, OrcError>;
