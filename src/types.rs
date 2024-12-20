// src/types.rs
use std::convert::TryFrom;
use chrono::{DateTime, NaiveDateTime, Utc};
use bigdecimal::BigDecimal;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Decimal(BigDecimal),
    String(String),
    Bytes(Vec<u8>),
    DateTime(DateTime<Utc>),
    Date(NaiveDateTime),
    Time(NaiveDateTime),
    // Add more types as needed
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::String(s) => write!(f, "'{}'", s),
            Value::DateTime(dt) => write!(f, "'{}'", dt),
            // Implement other variants
            _ => write!(f, "?"),
        }
    }
}
