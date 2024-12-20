// src/lib.rs
pub mod error;
pub mod parser;
pub mod types;
//pub mod executor;

use async_trait::async_trait;

//pub mod connection;
pub use error::Error;

use std::fmt;
use async_trait::async_trait;

// Core traits and types
#[async_trait]
pub trait Connection: Send + Sync {
    // async fn execute(&self, query: &str) -> Result<QueryResult, Error>;
    // async fn prepare(&self, query: &str) -> Result<PreparedStatement, Error>;
    // async fn transaction(&self) -> Result<Transaction, Error>;
}

#[async_trait]
pub trait Transaction: Send + Sync {
    async fn commit(self) -> Result<(), Error>;
    async fn rollback(self) -> Result<(), Error>;
    // async fn execute(&self, query: &str) -> Result<QueryResult, Error>;
}