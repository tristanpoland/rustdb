use std::sync::Arc;
use crate::error::Error;
use crate::storage::Storage;
use crate::types::{Type, Value, TypeSystem};
use serde::{Serialize, Deserialize};

mod parser;
mod planner;
mod executor;

use parser::{Parser, ParsedQuery};
use planner::{QueryPlanner, QueryPlan};
use executor::{QueryExecutor, QueryResult};

/// Main query engine that coordinates parsing, planning, and execution
pub struct QueryEngine {
    storage: Arc<Storage>,
    type_system: Arc<TypeSystem>,
    parser: Parser,
    planner: QueryPlanner,
    executor: QueryExecutor,
}

/// Query operation types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
    Create(CreateQuery),
    Drop(DropQuery),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub conditions: Vec<Condition>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertQuery {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateQuery {
    pub table: String,
    pub set: Vec<(String, Value)>,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteQuery {
    pub table: String,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateQuery {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropQuery {
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub default: Option<Value>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    NotNull,
    Check(Condition),
    ForeignKey {
        ref_table: String,
        ref_column: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    Check(Condition),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    Equals(String, Value),
    NotEquals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    GreaterEquals(String, Value),
    LessEquals(String, Value),
    Between(String, Value, Value),
    Like(String, String),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBy {
    pub column: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

impl QueryEngine {
    /// Create a new query engine
    pub fn new(storage: Arc<Storage>, type_system: Arc<TypeSystem>) -> Self {
        let parser = Parser::new();
        let planner = QueryPlanner::new(Arc::clone(&storage), Arc::clone(&type_system));
        let executor = QueryExecutor::new(Arc::clone(&storage));

        Self {
            storage,
            type_system,
            parser,
            planner,
            executor,
        }
    }

    /// Execute a SQL query string
    pub async fn execute(&self, query: &str) -> Result<QueryResult, Error> {
        // Parse SQL into AST
        let parsed_query = self.parser.parse(query)?;

        // Convert to our internal Query representation
        let query = self.convert_parsed_query(parsed_query)?;

        // Plan the query
        let plan = self.planner.plan(query).await?;

        // Execute the plan
        self.executor.execute(plan).await
    }

    /// Execute multiple queries in a transaction
    pub async fn execute_transaction(&self, queries: &[&str]) -> Result<Vec<QueryResult>, Error> {
        let transaction = self.storage.begin_transaction().await?;

        let mut results = Vec::new();
        for query in queries {
            match self.execute(query).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    transaction.rollback().await?;
                    return Err(e);
                }
            }
        }

        transaction.commit().await?;
        Ok(results)
    }

    // Internal helper methods

    fn convert_parsed_query(&self, parsed: ParsedQuery) -> Result<Query, Error> {
        match parsed {
            ParsedQuery::Select { table, columns, conditions, order_by, limit, offset } => {
                Ok(Query::Select(SelectQuery {
                    table,
                    columns,
                    conditions: self.convert_conditions(conditions)?,
                    order_by: self.convert_order_by(order_by),
                    limit,
                    offset,
                }))
            }
            ParsedQuery::Insert { table, columns, values } => {
                Ok(Query::Insert(InsertQuery {
                    table,
                    columns,
                    values: self.convert_values(values)?,
                }))
            }
            ParsedQuery::Update { table, set, conditions } => {
                Ok(Query::Update(UpdateQuery {
                    table,
                    set: self.convert_set_clauses(set)?,
                    conditions: self.convert_conditions(conditions)?,
                }))
            }
            ParsedQuery::Delete { table, conditions } => {
                Ok(Query::Delete(DeleteQuery {
                    table,
                    conditions: self.convert_conditions(conditions)?,
                }))
            }
            ParsedQuery::Create { table, columns, constraints } => {
                Ok(Query::Create(CreateQuery {
                    table,
                    columns: self.convert_column_defs(columns)?,
                    constraints: self.convert_table_constraints(constraints)?,
                }))
            }
            ParsedQuery::Drop { table, if_exists } => {
                Ok(Query::Drop(DropQuery {
                    table,
                    if_exists,
                }))
            }
        }
    }

    fn convert_conditions(&self, conditions: Vec<parser::Condition>) -> Result<Vec<Condition>, Error> {
        conditions.into_iter()
            .map(|c| self.convert_condition(c))
            .collect()
    }

    fn convert_condition(&self, condition: parser::Condition) -> Result<Condition, Error> {
        match condition {
            parser::Condition::Equals(col, val) => {
                Ok(Condition::Equals(col, self.convert_value(val)?))
            }
            parser::Condition::NotEquals(col, val) => {
                Ok(Condition::NotEquals(col, self.convert_value(val)?))
            }
            parser::Condition::GreaterThan(col, val) => {
                Ok(Condition::GreaterThan(col, self.convert_value(val)?))
            }
            parser::Condition::LessThan(col, val) => {
                Ok(Condition::LessThan(col, self.convert_value(val)?))
            }
            parser::Condition::GreaterEquals(col, val) => {
                Ok(Condition::GreaterEquals(col, self.convert_value(val)?))
            }
            parser::Condition::LessEquals(col, val) => {
                Ok(Condition::LessEquals(col, self.convert_value(val)?))
            }
            parser::Condition::Between(col, val1, val2) => {
                Ok(Condition::Between(
                    col,
                    self.convert_value(val1)?,
                    self.convert_value(val2)?,
                ))
            }
            parser::Condition::Like(col, pattern) => {
                Ok(Condition::Like(col, pattern))
            }
            parser::Condition::In(col, vals) => {
                Ok(Condition::In(
                    col,
                    vals.into_iter()
                        .map(|v| self.convert_value(v))
                        .collect::<Result<Vec<_>, _>>()?,
                ))
            }
            parser::Condition::IsNull(col) => Ok(Condition::IsNull(col)),
            parser::Condition::IsNotNull(col) => Ok(Condition::IsNotNull(col)),
            parser::Condition::And(conditions) => {
                Ok(Condition::And(self.convert_conditions(conditions)?))
            }
            parser::Condition::Or(conditions) => {
                Ok(Condition::Or(self.convert_conditions(conditions)?))
            }
            parser::Condition::Not(condition) => {
                Ok(Condition::Not(Box::new(self.convert_condition(*condition)?)))
            }
        }
    }

    fn convert_value(&self, value: parser::Value) -> Result<Value, Error> {
        match value {
            parser::Value::Integer(i) => Ok(Value::Int64(i)),
            parser::Value::Float(f) => Ok(Value::Float64(f)),
            parser::Value::String(s) => Ok(Value::String(s)),
            parser::Value::Boolean(b) => Ok(Value::Bool(b)),
            parser::Value::Null => Ok(Value::Null),
        }
    }

    fn convert_order_by(&self, order_by: Vec<parser::OrderBy>) -> Vec<OrderBy> {
        order_by.into_iter()
            .map(|o| OrderBy {
                column: o.column,
                direction: match o.direction {
                    parser::OrderDirection::Asc => OrderDirection::Ascending,
                    parser::OrderDirection::Desc => OrderDirection::Descending,
                },
            })
            .collect()
    }

    fn convert_values(&self, values: Vec<Vec<parser::Value>>) -> Result<Vec<Vec<Value>>, Error> {
        values.into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| self.convert_value(v))
                    .collect()
            })
            .collect()
    }

    fn convert_set_clauses(&self, set: Vec<(String, parser::Value)>) -> Result<Vec<(String, Value)>, Error> {
        set.into_iter()
            .map(|(col, val)| Ok((col, self.convert_value(val)?)))
            .collect()
    }

    fn convert_column_defs(&self, columns: Vec<parser::ColumnDef>) -> Result<Vec<ColumnDef>, Error> {
        columns.into_iter()
            .map(|c| Ok(ColumnDef {
                name: c.name,
                type_name: c.type_name,
                nullable: !c.constraints.iter().any(|c| matches!(c, parser::ColumnConstraint::NotNull)),
                default: c.default.map(|v| self.convert_value(v)).transpose()?,
                constraints: self.convert_column_constraints(c.constraints)?,
            }))
            .collect()
    }

    fn convert_column_constraints(&self, constraints: Vec<parser::ColumnConstraint>) -> Result<Vec<ColumnConstraint>, Error> {
        constraints.into_iter()
            .map(|c| match c {
                parser::ColumnConstraint::PrimaryKey => Ok(ColumnConstraint::PrimaryKey),
                parser::ColumnConstraint::Unique => Ok(ColumnConstraint::Unique),
                parser::ColumnConstraint::NotNull => Ok(ColumnConstraint::NotNull),
                parser::ColumnConstraint::Check(cond) => {
                    Ok(ColumnConstraint::Check(self.convert_condition(cond)?))
                }
                parser::ColumnConstraint::ForeignKey { ref_table, ref_column } => {
                    Ok(ColumnConstraint::ForeignKey { ref_table, ref_column })
                }
            })
            .collect()
    }

    fn convert_table_constraints(&self, constraints: Vec<parser::TableConstraint>) -> Result<Vec<TableConstraint>, Error> {
        constraints.into_iter()
            .map(|c| match c {
                parser::TableConstraint::PrimaryKey(cols) => Ok(TableConstraint::PrimaryKey(cols)),
                parser::TableConstraint::Unique(cols) => Ok(TableConstraint::Unique(cols)),
                parser::TableConstraint::ForeignKey { columns, ref_table, ref_columns } => {
                    Ok(TableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    })
                }
                parser::TableConstraint::Check(cond) => {
                    Ok(TableConstraint::Check(self.convert_condition(cond)?))
                }
            })
            .collect()
    }
}