use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum ReplError {
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("authentication error: {0}")]
    Auth(String),
    #[allow(dead_code)]
    #[error("task error: {0}")]
    Task(String),
}

impl From<std::io::Error> for ReplError {
    fn from(e: std::io::Error) -> Self {
        ReplError::Io(Arc::new(e))
    }
}

pub type ReplResult<T> = std::result::Result<T, ReplError>;
