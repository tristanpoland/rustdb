use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use parking_lot::RwLock;
use crate::error::Error;
pub mod buffer_pool;
pub mod page;

/// Core type definitions for RustDB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Type {
    // Primitive types
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float32,
    Float64,
    Bool,
    String,
    
    // Complex types
    Array(Box<Type>, Option<usize>),  // Type and optional fixed size
    Vec(Box<Type>),                   // Dynamic array
    Tuple(Vec<Type>),                 // Named tuple support
    Struct(HashMap<String, Type>),    // Custom struct types
    Enum(HashMap<String, Option<Type>>), // Enum variants with optional data
    Option(Box<Type>),                // Optional values
    Result(Box<Type>, Box<Type>),     // Result type with Ok and Err
    Map(Box<Type>, Box<Type>),        // Key-value map type
}

/// Runtime values that correspond to Types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    Array(Vec<Value>),
    Vec(Vec<Value>),
    Tuple(Vec<Value>),
    Struct(HashMap<String, Value>),
    Enum(String, Option<Box<Value>>),
    Option(Option<Box<Value>>),
    Result(Box<Either<Value, Value>>),
    Map(HashMap<Value, Value>),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

/// Manages custom types and type validation
pub struct TypeSystem {
    types: RwLock<HashMap<String, TypeDefinition>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDefinition {
    pub name: String,
    pub type_: Type,
    pub constraints: Vec<Constraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Constraint {
    NotNull,
    Unique,
    Range { min: Value, max: Value },
    Length { min: usize, max: usize },
    Regex(String),
    Custom(String), // Custom validation function name
}

impl TypeSystem {
    pub fn new() -> Self {
        Self {
            types: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new custom type
    pub fn register_type(&self, def: TypeDefinition) -> Result<(), Error> {
        let mut types = self.types.write();
        if types.contains_key(&def.name) {
            return Err(Error::Type(format!("Type already exists: {}", def.name)));
        }
        types.insert(def.name.clone(), def);
        Ok(())
    }

    /// Get a type definition by name
    pub fn get_type(&self, name: &str) -> Option<TypeDefinition> {
        self.types.read().get(name).cloned()
    }

    /// Convert a MySQL type to our type system
    pub fn from_mysql_type(&self, mysql_type: &str) -> Result<Type, Error> {
        match mysql_type.to_lowercase().as_str() {
            "tinyint" => Ok(Type::Int8),
            "smallint" => Ok(Type::Int16),
            "int" => Ok(Type::Int32),
            "bigint" => Ok(Type::Int64),
            "float" => Ok(Type::Float32),
            "double" => Ok(Type::Float64),
            "varchar" | "text" => Ok(Type::String),
            "bool" | "boolean" => Ok(Type::Bool),
            t if t.starts_with("enum(") => {
                // Parse enum values and create Enum type
                let values: HashMap<String, Option<Type>> = t
                    .trim_start_matches("enum(")
                    .trim_end_matches(")")
                    .split(',')
                    .map(|s| (s.trim().trim_matches('\'').to_string(), None))
                    .collect();
                Ok(Type::Enum(values))
            }
            _ => Err(Error::Type(format!("Unsupported MySQL type: {}", mysql_type))),
        }
    }

    /// Convert our type to a MySQL type
    pub fn to_mysql_type(&self, type_: &Type) -> Result<String, Error> {
        match type_ {
            Type::Int8 => Ok("TINYINT".to_string()),
            Type::Int16 => Ok("SMALLINT".to_string()),
            Type::Int32 => Ok("INT".to_string()),
            Type::Int64 => Ok("BIGINT".to_string()),
            Type::Float32 => Ok("FLOAT".to_string()),
            Type::Float64 => Ok("DOUBLE".to_string()),
            Type::String => Ok("TEXT".to_string()),
            Type::Bool => Ok("BOOLEAN".to_string()),
            Type::Enum(variants) => {
                let values: Vec<String> = variants.keys()
                    .map(|v| format!("'{}'", v))
                    .collect();
                Ok(format!("ENUM({})", values.join(",")))
            }
            Type::Option(inner) => {
                // Optional types are represented as nullable columns
                let inner_type = self.to_mysql_type(inner)?;
                Ok(inner_type)
            }
            _ => Err(Error::Type(format!("Type cannot be represented in MySQL: {:?}", type_))),
        }
    }

    /// Validate a value against a type definition
    pub fn validate_value(&self, value: &Value, type_: &Type) -> Result<(), Error> {
        match (value, type_) {
            // Primitive type validation
            (Value::Int8(_), Type::Int8) => Ok(()),
            (Value::Int16(_), Type::Int16) => Ok(()),
            (Value::Int32(_), Type::Int32) => Ok(()),
            (Value::Int64(_), Type::Int64) => Ok(()),
            (Value::Uint8(_), Type::Uint8) => Ok(()),
            (Value::Uint16(_), Type::Uint16) => Ok(()),
            (Value::Uint32(_), Type::Uint32) => Ok(()),
            (Value::Uint64(_), Type::Uint64) => Ok(()),
            (Value::Float32(_), Type::Float32) => Ok(()),
            (Value::Float64(_), Type::Float64) => Ok(()),
            (Value::Bool(_), Type::Bool) => Ok(()),
            (Value::String(_), Type::String) => Ok(()),

            // Array validation
            (Value::Array(values), Type::Array(element_type, size)) => {
                if let Some(expected_size) = size {
                    if values.len() != *expected_size {
                        return Err(Error::Type(format!(
                            "Array size mismatch: expected {}, got {}",
                            expected_size,
                            values.len()
                        )));
                    }
                }
                for value in values {
                    self.validate_value(value, element_type)?;
                }
                Ok(())
            }

            // Vec validation
            (Value::Vec(values), Type::Vec(element_type)) => {
                for value in values {
                    self.validate_value(value, element_type)?;
                }
                Ok(())
            }

            // Struct validation
            (Value::Struct(fields), Type::Struct(expected_fields)) => {
                for (name, field_type) in expected_fields {
                    match fields.get(name) {
                        Some(field_value) => self.validate_value(field_value, field_type)?,
                        None => return Err(Error::Type(format!("Missing field: {}", name))),
                    }
                }
                Ok(())
            }

            // Enum validation
            (Value::Enum(variant, data), Type::Enum(variants)) => {
                match variants.get(variant) {
                    Some(expected_type) => {
                        match (data, expected_type) {
                            (None, None) => Ok(()),
                            (Some(value), Some(type_)) => self.validate_value(value, type_),
                            _ => Err(Error::Type("Enum variant data mismatch".to_string())),
                        }
                    }
                    None => Err(Error::Type(format!("Invalid enum variant: {}", variant))),
                }
            }

            // Option validation
            (Value::Option(None), Type::Option(_)) => Ok(()),
            (Value::Option(Some(value)), Type::Option(inner_type)) => {
                self.validate_value(value, inner_type)
            }

            // Result validation
            (Value::Result(result), Type::Result(ok_type, err_type)) => {
                match &**result {
                    Either::Left(ok) => self.validate_value(ok, ok_type),
                    Either::Right(err) => self.validate_value(err, err_type),
                }
            }

            // Map validation
            (Value::Map(entries), Type::Map(key_type, value_type)) => {
                for (k, v) in entries {
                    self.validate_value(k, key_type)?;
                    self.validate_value(v, value_type)?;
                }
                Ok(())
            }

            // Handle null values
            (Value::Null, Type::Option(_)) => Ok(()),
            (Value::Null, _) => Err(Error::Type("Unexpected null value".to_string())),

            _ => Err(Error::Type(format!(
                "Type mismatch: value {:?} does not match type {:?}",
                value, type_
            ))),
        }
    }

    /// Apply constraints to a value
    pub fn apply_constraints(&self, value: &Value, constraints: &[Constraint]) -> Result<(), Error> {
        for constraint in constraints {
            match constraint {
                Constraint::NotNull => {
                    if let Value::Null = value {
                        return Err(Error::Type("Value cannot be null".to_string()));
                    }
                }
                Constraint::Unique => {
                    // Unique constraint is handled at the storage layer
                    continue;
                }
                Constraint::Range { min, max } => {
                    if !self.is_in_range(value, min, max) {
                        return Err(Error::Type(format!(
                            "Value {:?} outside range [{:?}, {:?}]",
                            value, min, max
                        )));
                    }
                }
                Constraint::Length { min, max } => {
                    if let Value::String(s) = value {
                        let len = s.len();
                        if len < *min || len > *max {
                            return Err(Error::Type(format!(
                                "String length {} outside range [{}, {}]",
                                len, min, max
                            )));
                        }
                    }
                }
                Constraint::Regex(pattern) => {
                    if let Value::String(s) = value {
                        let re = regex::Regex::new(pattern).map_err(|e| {
                            Error::Type(format!("Invalid regex pattern: {}", e))
                        })?;
                        if !re.is_match(s) {
                            return Err(Error::Type(format!(
                                "String '{}' does not match pattern '{}'",
                                s, pattern
                            )));
                        }
                    }
                }
                Constraint::Custom(name) => {
                    // Custom constraints would be registered separately
                    self.apply_custom_constraint(name, value)?;
                }
            }
        }
        Ok(())
    }

    // Helper function to compare values for range constraints
    fn is_in_range(&self, value: &Value, min: &Value, max: &Value) -> bool {
        match (value, min, max) {
            (Value::Int8(v), Value::Int8(min), Value::Int8(max)) => v >= min && v <= max,
            (Value::Int16(v), Value::Int16(min), Value::Int16(max)) => v >= min && v <= max,
            (Value::Int32(v), Value::Int32(min), Value::Int32(max)) => v >= min && v <= max,
            (Value::Int64(v), Value::Int64(min), Value::Int64(max)) => v >= min && v <= max,
            (Value::Float32(v), Value::Float32(min), Value::Float32(max)) => v >= min && v <= max,
            (Value::Float64(v), Value::Float64(min), Value::Float64(max)) => v >= min && v <= max,
            _ => false,
        }
    }

    fn apply_custom_constraint(&self, name: &str, value: &Value) -> Result<(), Error> {
        // In a real implementation, this would look up and apply registered custom constraints
        Err(Error::Type(format!("Unknown custom constraint: {}", name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_type_validation() {
        let ts = TypeSystem::new();
        
        // Test valid cases
        assert!(ts.validate_value(&Value::Int32(42), &Type::Int32).is_ok());
        assert!(ts.validate_value(&Value::String("hello".into()), &Type::String).is_ok());
        
        // Test invalid cases
        assert!(ts.validate_value(&Value::Int32(42), &Type::String).is_err());
        assert!(ts.validate_value(&Value::String("hello".into()), &Type::Int32).is_err());
    }

    #[test]
    fn test_struct_validation() {
        let ts = TypeSystem::new();
        
        let person_type = Type::Struct({
            let mut fields = HashMap::new();
            fields.insert("name".into(), Type::String);
            fields.insert("age".into(), Type::Int32);
            fields
        });

        let valid_person = Value::Struct({
            let mut fields = HashMap::new();
            fields.insert("name".into(), Value::String("Alice".into()));
            fields.insert("age".into(), Value::Int32(30));
            fields
        });

        assert!(ts.validate_value(&valid_person, &person_type).is_ok());
    }

    #[test]
    fn test_enum_validation() {
        let ts = TypeSystem::new();
        
        let status_type = Type::Enum({
            let mut variants = HashMap::new();
            variants.insert("Active".into(), None);
            variants.insert("Inactive".into(), None);
            variants.insert("Suspended".into(), Some(Type::String));  // Variant with data
            variants
        });

        // Test valid enum values
        let active = Value::Enum("Active".into(), None);
        let suspended = Value::Enum("Suspended".into(), Some(Box::new(Value::String("Violation".into()))));
        
        assert!(ts.validate_value(&active, &status_type).is_ok());
        assert!(ts.validate_value(&suspended, &status_type).is_ok());

        // Test invalid enum values
        let invalid_variant = Value::Enum("Unknown".into(), None);
        let invalid_data = Value::Enum("Active".into(), Some(Box::new(Value::String("".into()))));
        
        assert!(ts.validate_value(&invalid_variant, &status_type).is_err());
        assert!(ts.validate_value(&invalid_data, &status_type).is_err());
    }

    #[test]
    fn test_option_validation() {
        let ts = TypeSystem::new();
        
        let optional_int = Type::Option(Box::new(Type::Int32));
        
        // Test valid optional values
        let some_value = Value::Option(Some(Box::new(Value::Int32(42))));
        let no_value = Value::Option(None);
        
        assert!(ts.validate_value(&some_value, &optional_int).is_ok());
        assert!(ts.validate_value(&no_value, &optional_int).is_ok());
        
        // Test invalid optional value
        let wrong_type = Value::Option(Some(Box::new(Value::String("not an int".into()))));
        assert!(ts.validate_value(&wrong_type, &optional_int).is_err());
    }

    #[test]
    fn test_array_validation() {
        let ts = TypeSystem::new();
        
        // Test fixed-size array
        let fixed_array_type = Type::Array(Box::new(Type::Int32), Some(3));
        let valid_fixed = Value::Array(vec![
            Value::Int32(1),
            Value::Int32(2),
            Value::Int32(3),
        ]);
        let invalid_size = Value::Array(vec![Value::Int32(1), Value::Int32(2)]);
        
        assert!(ts.validate_value(&valid_fixed, &fixed_array_type).is_ok());
        assert!(ts.validate_value(&invalid_size, &fixed_array_type).is_err());
        
        // Test dynamic array (Vec)
        let vec_type = Type::Vec(Box::new(Type::String));
        let valid_vec = Value::Vec(vec![
            Value::String("one".into()),
            Value::String("two".into()),
        ]);
        let invalid_vec = Value::Vec(vec![
            Value::String("one".into()),
            Value::Int32(2),
        ]);
        
        assert!(ts.validate_value(&valid_vec, &vec_type).is_ok());
        assert!(ts.validate_value(&invalid_vec, &vec_type).is_err());
    }

    #[test]
    fn test_result_validation() {
        let ts = TypeSystem::new();
        
        let result_type = Type::Result(
            Box::new(Type::String),
            Box::new(Type::Int32),
        );
        
        // Test Ok value
        let ok_value = Value::Result(Box::new(Either::Left(Value::String("success".into()))));
        assert!(ts.validate_value(&ok_value, &result_type).is_ok());
        
        // Test Err value
        let err_value = Value::Result(Box::new(Either::Right(Value::Int32(404))));
        assert!(ts.validate_value(&err_value, &result_type).is_ok());
        
        // Test invalid Ok type
        let invalid_ok = Value::Result(Box::new(Either::Left(Value::Int32(42))));
        assert!(ts.validate_value(&invalid_ok, &result_type).is_err());
    }

    #[test]
    fn test_constraints() {
        let ts = TypeSystem::new();
        
        // Test NotNull constraint
        assert!(ts.apply_constraints(&Value::Null, &[Constraint::NotNull]).is_err());
        assert!(ts.apply_constraints(&Value::Int32(42), &[Constraint::NotNull]).is_ok());
        
        // Test Range constraint
        let range = Constraint::Range {
            min: Value::Int32(0),
            max: Value::Int32(100),
        };
        assert!(ts.apply_constraints(&Value::Int32(42), &[range.clone()]).is_ok());
        assert!(ts.apply_constraints(&Value::Int32(101), &[range.clone()]).is_err());
        
        // Test Length constraint
        let length = Constraint::Length {
            min: 1,
            max: 5,
        };
        assert!(ts.apply_constraints(&Value::String("hello".into()), &[length.clone()]).is_ok());
        assert!(ts.apply_constraints(&Value::String("too long".into()), &[length.clone()]).is_err());
        
        // Test Regex constraint
        let regex = Constraint::Regex(r"^\d{3}-\d{2}-\d{4}$".into());
        assert!(ts.apply_constraints(&Value::String("123-45-6789".into()), &[regex.clone()]).is_ok());
        assert!(ts.apply_constraints(&Value::String("invalid".into()), &[regex.clone()]).is_err());
    }

    #[test]
    fn test_mysql_type_conversion() {
        let ts = TypeSystem::new();
        
        // Test MySQL to RustDB type conversion
        assert_eq!(ts.from_mysql_type("int").unwrap(), Type::Int32);
        assert_eq!(ts.from_mysql_type("bigint").unwrap(), Type::Int64);
        assert_eq!(ts.from_mysql_type("varchar").unwrap(), Type::String);
        
        // Test enum conversion
        let enum_type = ts.from_mysql_type("enum('active','inactive')").unwrap();
        match enum_type {
            Type::Enum(variants) => {
                assert!(variants.contains_key("active"));
                assert!(variants.contains_key("inactive"));
            }
            _ => panic!("Expected Enum type"),
        }
        
        // Test RustDB to MySQL type conversion
        assert_eq!(ts.to_mysql_type(&Type::Int32).unwrap(), "INT");
        assert_eq!(ts.to_mysql_type(&Type::String).unwrap(), "TEXT");
        
        // Test complex type conversion errors
        assert!(ts.to_mysql_type(&Type::Array(Box::new(Type::Int32), Some(3))).is_err());
    }
}