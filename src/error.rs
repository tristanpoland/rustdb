use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("SQL syntax error: {0}")]
    Syntax(String),
    
    #[error("Type error: {0}")]
    Type(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Execution error: {0}")]
    Execution(String),
    
    #[error("Transaction error: {0}")]
    Transaction(String),
}