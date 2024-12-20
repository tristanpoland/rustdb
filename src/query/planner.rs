use std::sync::Arc;
use crate::error::Error;
use crate::storage::{Storage, Table};
use crate::types::{Type, Value, TypeSystem};
use crate::query::{Query, Condition, OrderBy};
use crate::index::{Index, IndexConfig};

/// Query execution plan types
#[derive(Debug)]
pub enum QueryPlan {
    Scan {
        table: Arc<Table>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
        projections: Vec<String>,
    },
    IndexScan {
        table: Arc<Table>,
        index: Arc<Index>,
        range: Option<(Value, Value)>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
        projections: Vec<String>,
    },
    Insert {
        table: Arc<Table>,
        values: Vec<Vec<Value>>,
    },
    Update {
        table: Arc<Table>,
        values: Vec<(String, Value)>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
    },
    Delete {
        table: Arc<Table>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
    },
    CreateTable {
        name: String,
        schema: Arc<crate::storage::TableSchema>,
    },
    DropTable {
        name: String,
    },
}

/// Statistics for cost estimation
#[derive(Debug, Clone)]
struct TableStats {
    row_count: u64,
    avg_row_size: u32,
    page_count: u64,
    distinct_values: HashMap<String, u64>,
}

/// Cost model parameters
struct CostParams {
    sequential_page_cost: f64,
    random_page_cost: f64,
    cpu_tuple_cost: f64,
    cpu_operator_cost: f64,
}

impl Default for CostParams {
    fn default() -> Self {
        Self {
            sequential_page_cost: 1.0,
            random_page_cost: 4.0,
            cpu_tuple_cost: 0.01,
            cpu_operator_cost: 0.0025,
        }
    }
}

pub struct QueryPlanner {
    storage: Arc<Storage>,
    type_system: Arc<TypeSystem>,
    cost_params: CostParams,
}

impl QueryPlanner {
    pub fn new(storage: Arc<Storage>, type_system: Arc<TypeSystem>) -> Self {
        Self {
            storage,
            type_system,
            cost_params: CostParams::default(),
        }
    }

    /// Plan a query for execution
    pub async fn plan(&self, query: Query) -> Result<QueryPlan, Error> {
        match query {
            Query::Select(select) => {
                let table = self.storage.get_table(&select.table).await?;
                
                // Get available indexes
                let indexes = table.get_indexes();
                
                // Find best index for conditions
                if let Some(best_index) = self.find_best_index(&indexes, &select.conditions).await? {
                    // Create index scan plan
                    let range = self.get_index_range(&select.conditions, &best_index)?;
                    let predicate = self.create_predicate(&select.conditions)?;
                    
                    Ok(QueryPlan::IndexScan {
                        table: Arc::clone(&table),
                        index: best_index,
                        range,
                        predicate,
                        projections: select.columns,
                    })
                } else {
                    // Fall back to table scan
                    let predicate = self.create_predicate(&select.conditions)?;
                    
                    Ok(QueryPlan::Scan {
                        table: Arc::clone(&table),
                        predicate,
                        projections: select.columns,
                    })
                }
            }
            Query::Insert(insert) => {
                let table = self.storage.get_table(&insert.table).await?;
                
                // Validate values against schema
                for row in &insert.values {
                    self.validate_row_values(row, &table)?;
                }
                
                Ok(QueryPlan::Insert {
                    table: Arc::clone(&table),
                    values: insert.values,
                })
            }
            Query::Update(update) => {
                let table = self.storage.get_table(&update.table).await?;
                
                // Validate update values
                for (column, value) in &update.set {
                    self.validate_column_value(column, value, &table)?;
                }
                
                let predicate = self.create_predicate(&update.conditions)?;
                
                Ok(QueryPlan::Update {
                    table: Arc::clone(&table),
                    values: update.set,
                    predicate,
                })
            }
            Query::Delete(delete) => {
                let table = self.storage.get_table(&delete.table).await?;
                let predicate = self.create_predicate(&delete.conditions)?;
                
                Ok(QueryPlan::Delete {
                    table: Arc::clone(&table),
                    predicate,
                })
            }
            Query::Create(create) => {
                // Validate schema
                self.validate_schema(&create.columns)?;
                
                Ok(QueryPlan::CreateTable {
                    name: create.table,
                    schema: Arc::new(create.into()),
                })
            }
            Query::Drop(drop) => {
                Ok(QueryPlan::DropTable {
                    name: drop.table,
                })
            }
        }
    }

    // Helper methods

    async fn find_best_index(
        &self,
        indexes: &[Arc<Index>],
        conditions: &[Condition],
    ) -> Result<Option<Arc<Index>>, Error> {
        let mut best_index = None;
        let mut lowest_cost = f64::INFINITY;

        for index in indexes {
            if self.can_use_index(index, conditions) {
                let cost = self.estimate_index_cost(index, conditions).await?;
                if cost < lowest_cost {
                    lowest_cost = cost;
                    best_index = Some(Arc::clone(index));
                }
            }
        }

        Ok(best_index)
    }

    fn can_use_index(&self, index: &Index, conditions: &[Condition]) -> bool {
        let index_columns = index.get_columns();
        let mut usable = false;

        for condition in conditions {
            match condition {
                Condition::Equals(col, _) |
                Condition::GreaterThan(col, _) |
                Condition::LessThan(col, _) |
                Condition::GreaterEquals(col, _) |
                Condition::LessEquals(col, _) => {
                    if index_columns.contains(col) {
                        usable = true;
                        break;
                    }
                }
                Condition::Between(col, _, _) => {
                    if index_columns.contains(col) {
                        usable = true;
                        break;
                    }
                }
                _ => continue,
            }
        }

        usable
    }

    async fn estimate_index_cost(&self, index: &Index, conditions: &[Condition]) -> Result<f64, Error> {
        let stats = self.get_table_stats(index.get_table()).await?;
        let selectivity = self.estimate_selectivity(conditions, &stats)?;
        
        // Basic cost model:
        // - Random page access for index lookup
        // - Sequential scan of matching pages
        let index_height = index.get_height().await?;
        let matching_rows = (stats.row_count as f64 * selectivity).ceil() as u64;
        let matching_pages = (matching_rows * stats.avg_row_size as u64) / 4096;

        let cost = index_height as f64 * self.cost_params.random_page_cost +
                   matching_pages as f64 * self.cost_params.sequential_page_cost +
                   matching_rows as f64 * self.cost_params.cpu_tuple_cost;

        Ok(cost)
    }

    async fn get_table_stats(&self, table: &str) -> Result<TableStats, Error> {
        // In a real implementation, this would load cached statistics
        // For now, return some reasonable defaults
        Ok(TableStats {
            row_count: 1000,
            avg_row_size: 100,
            page_count: 25,
            distinct_values: HashMap::new(),
        })
    }

    fn estimate_selectivity(&self, conditions: &[Condition], stats: &TableStats) -> Result<f64, Error> {
        let mut selectivity = 1.0;

        for condition in conditions {
            selectivity *= match condition {
                Condition::Equals(col, _) => {
                    if let Some(distinct) = stats.distinct_values.get(col) {
                        1.0 / *distinct as f64
                    } else {
                        0.1 // Default assumption
                    }
                }
                Condition::GreaterThan(_, _) |
                Condition::LessThan(_, _) => 0.3,
                Condition::Between(_, _, _) => 0.2,
                Condition::Like(_, pattern) => {
                    if pattern.contains('%') {
                        0.1
                    } else {
                        0.01
                    }
                }
                Condition::In(_, values) => values.len() as f64 * 0.01,
                Condition::And(conditions) => {
                    self.estimate_selectivity(conditions, stats)?
                }
                Condition::Or(conditions) => {
                    1.0 - conditions.iter().map(|c| {
                        1.0 - self.estimate_selectivity(&[c.clone()], stats).unwrap_or(1.0)
                    }).product::<f64>()
                }
                _ => 0.5,
            };
        }

        Ok(selectivity)
    }

    fn get_index_range(
        &self,
        conditions: &[Condition],
        index: &Index,
    ) -> Result<Option<(Value, Value)>, Error> {
        let index_columns = index.get_columns();
        
        for condition in conditions {
            match condition {
                Condition::Equals(col, val) if index_columns.contains(col) => {
                    return Ok(Some((val.clone(), val.clone())));
                }
                Condition::Between(col, start, end) if index_columns.contains(col) => {
                    return Ok(Some((start.clone(), end.clone())));
                }
                Condition::GreaterThan(col, val) if index_columns.contains(col) => {
                    // Use maximum possible value for upper bound
                    return Ok(Some((val.clone(), Value::max_value(val.get_type())?)));
                }
                Condition::LessThan(col, val) if index_columns.contains(col) => {
                    // Use minimum possible value for lower bound
                    return Ok(Some((Value::min_value(val.get_type())?, val.clone())));
                }
                _ => continue,
            }
        }

        Ok(None)
    }

    fn create_predicate(
        &self,
        conditions: &[Condition],
    ) -> Result<Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>, Error> {
        if conditions.is_empty() {
            return Ok(None);
        }

        let conditions = conditions.to_vec();
        Ok(Some(Box::new(move |row_data: &[u8]| {
            // Deserialize row and evaluate conditions
            let row = bincode::deserialize(row_data)
                .map_err(|e| Error::Storage(format!("Failed to deserialize row: {}", e)))?;
            Self::evaluate_conditions(&conditions, &row)
        })))
    }

    fn evaluate_conditions(conditions: &[Condition], row: &HashMap<String, Value>) -> Result<bool, Error> {
        for condition in conditions {
            match condition {
                Condition::Equals(col, val) => {
                    if row.get(col) != Some(val) {
                        return Ok(false);
                    }
                }
                Condition::NotEquals(col, val) => {
                    if row.get(col) == Some(val) {
                        return Ok(false);
                    }
                }
                Condition::GreaterThan(col, val) => {
                    if let Some(row_val) = row.get(col) {
                        if row_val <= val {
                            return Ok(false);
                        }
                    }
                }
                // Add more condition evaluations...
            }
        }

        Ok(true)
    }

    fn validate_schema(&self, columns: &[ColumnDef]) -> Result<(), Error> {
        for column in columns {
            if !self.type_system.type_exists(&column.type_name) {
                return Err(Error::Type(format!("Unknown type: {}", column.type_name)));
            }
        }
        Ok(())
    }

    fn validate_row_values(&self, values: &[Value], table: &Table) -> Result<(), Error> {
        let schema = table.get_schema();
        
        if values.len() != schema.columns.len() {
            return Err(Error::Query("Column count mismatch".into()));
        }

        for (value, column) in values.iter().zip(schema.columns.iter()) {
            self.validate_column_value(&column.name, value, table)?;
        }

        Ok(())
    }

    fn validate_column_value(&self, column: &str, value: &Value, table: &Table) -> Result<(), Error> {
        let schema = table.get_schema();
        let column_def = schema.columns.iter()
            .find(|c| c.name == column)
            .ok_or_else(|| Error::Query(format!("Column not found: {}", column)))?;

        let type_def = self.type_system.get_type(&column_def.type_name)
            .ok_or_else(|| Error::Type(format!("Unknown type: {}", column_def.type_name)))?;

        self.type_system.validate_value(value, &type_def)
    }
}