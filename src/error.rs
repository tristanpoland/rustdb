use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Type error: {0}")]
    Type(String),
    
    #[error("Query error: {0}")]
    Query(String),
    
    #[error("Storage error: {0}")]
    Storage(String),
}