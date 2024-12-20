use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::error::Error;
use crate::storage::{Storage, Table};
use crate::types::{Type, Value, TypeSystem};
use crate::query::{QueryPlan, OrderBy, OrderDirection};
use crate::index::Index;

/// Results returned from query execution
#[derive(Debug)]
pub enum QueryResult {
    Select(Vec<HashMap<String, Value>>),
    Insert(u64),   // Number of rows inserted
    Update(u64),   // Number of rows updated
    Delete(u64),   // Number of rows deleted
    Create,
    Drop,
}

pub struct QueryExecutor {
    storage: Arc<Storage>,
}

impl QueryExecutor {
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Execute a query plan
    pub async fn execute(&self, plan: QueryPlan) -> Result<QueryResult, Error> {
        match plan {
            QueryPlan::Scan { table, predicate, projections } => {
                self.execute_scan(table, predicate, projections).await
            }
            QueryPlan::IndexScan { table, index, range, predicate, projections } => {
                self.execute_index_scan(table, index, range, predicate, projections).await
            }
            QueryPlan::Insert { table, values } => {
                self.execute_insert(table, values).await
            }
            QueryPlan::Update { table, values, predicate } => {
                self.execute_update(table, values, predicate).await
            }
            QueryPlan::Delete { table, predicate } => {
                self.execute_delete(table, predicate).await
            }
            QueryPlan::CreateTable { name, schema } => {
                self.execute_create_table(name, schema).await
            }
            QueryPlan::DropTable { name } => {
                self.execute_drop_table(name).await
            }
        }
    }

    /// Execute a table scan
    async fn execute_scan(
        &self,
        table: Arc<Table>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
        projections: Vec<String>,
    ) -> Result<QueryResult, Error> {
        let mut results = Vec::new();
        let mut scanner = table.scan().await?;

        while let Some((row_id, row_data)) = scanner.next().await? {
            // Apply predicate if any
            if let Some(ref pred) = predicate {
                if !pred(&row_data)? {
                    continue;
                }
            }

            // Deserialize and project row
            let row: HashMap<String, Value> = bincode::deserialize(&row_data)?;
            let projected = self.project_row(&row, &projections)?;
            results.push(projected);
        }

        Ok(QueryResult::Select(results))
    }

    /// Execute an index scan
    async fn execute_index_scan(
        &self,
        table: Arc<Table>,
        index: Arc<Index>,
        range: Option<(Value, Value)>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
        projections: Vec<String>,
    ) -> Result<QueryResult, Error> {
        let mut results = Vec::new();

        // Get row IDs from index
        let row_ids = match range {
            Some((start, end)) => index.range_scan(start, end).await?,
            None => index.full_scan().await?,
        };

        // Fetch rows using row IDs
        for row_id in row_ids {
            let row_data = table.read_row(row_id).await?;

            // Apply predicate if any
            if let Some(ref pred) = predicate {
                if !pred(&row_data)? {
                    continue;
                }
            }

            // Deserialize and project row
            let row: HashMap<String, Value> = bincode::deserialize(&row_data)?;
            let projected = self.project_row(&row, &projections)?;
            results.push(projected);
        }

        Ok(QueryResult::Select(results))
    }

    /// Execute an insert operation
    async fn execute_insert(
        &self,
        table: Arc<Table>,
        values: Vec<Vec<Value>>,
    ) -> Result<QueryResult, Error> {
        let mut inserted = 0;
        let schema = table.get_schema();

        for row_values in values {
            // Validate values against schema
            if row_values.len() != schema.columns.len() {
                return Err(Error::Query("Column count mismatch".into()));
            }

            // Create row data
            let mut row = HashMap::new();
            for (value, column) in row_values.iter().zip(schema.columns.iter()) {
                row.insert(column.name.clone(), value.clone());
            }

            // Insert row
            let row_data = bincode::serialize(&row)?;
            table.insert_row(row_data).await?;
            inserted += 1;
        }

        Ok(QueryResult::Insert(inserted))
    }

    /// Execute an update operation
    async fn execute_update(
        &self,
        table: Arc<Table>,
        values: Vec<(String, Value)>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
    ) -> Result<QueryResult, Error> {
        let mut updated = 0;
        let mut scanner = table.scan().await?;

        while let Some((row_id, row_data)) = scanner.next().await? {
            // Apply predicate if any
            if let Some(ref pred) = predicate {
                if !pred(&row_data)? {
                    continue;
                }
            }

            // Update matching row
            let mut row: HashMap<String, Value> = bincode::deserialize(&row_data)?;
            for (column, value) in &values {
                row.insert(column.clone(), value.clone());
            }

            // Write updated row
            let updated_data = bincode::serialize(&row)?;
            table.update_row(row_id, updated_data).await?;
            updated += 1;
        }

        Ok(QueryResult::Update(updated))
    }

    /// Execute a delete operation
    async fn execute_delete(
        &self,
        table: Arc<Table>,
        predicate: Option<Box<dyn Fn(&[u8]) -> Result<bool, Error> + Send + Sync>>,
    ) -> Result<QueryResult, Error> {
        let mut deleted = 0;
        let mut scanner = table.scan().await?;

        while let Some((row_id, row_data)) = scanner.next().await? {
            // Apply predicate if any
            if let Some(ref pred) = predicate {
                if !pred(&row_data)? {
                    continue;
                }
            }

            // Delete matching row
            table.delete_row(row_id).await?;
            deleted += 1;
        }

        Ok(QueryResult::Delete(deleted))
    }

    /// Execute create table operation
    async fn execute_create_table(
        &self,
        name: String,
        schema: Arc<crate::storage::TableSchema>,
    ) -> Result<QueryResult, Error> {
        self.storage.create_table(&name, (*schema).clone()).await?;
        Ok(QueryResult::Create)
    }

    /// Execute drop table operation
    async fn execute_drop_table(&self, name: String) -> Result<QueryResult, Error> {
        self.storage.drop_table(&name).await?;
        Ok(QueryResult::Drop)
    }

    // Helper methods

    /// Project specific columns from a row
    fn project_row(
        &self,
        row: &HashMap<String, Value>,
        projections: &[String],
    ) -> Result<HashMap<String, Value>, Error> {
        if projections.contains(&"*".to_string()) {
            return Ok(row.clone());
        }

        let mut result = HashMap::new();
        for column in projections {
            if let Some(value) = row.get(column) {
                result.insert(column.clone(), value.clone());
            } else {
                return Err(Error::Query(format!("Column not found: {}", column)));
            }
        }
        Ok(result)
    }

    /// Sort results by specified order
    fn sort_results(
        &self,
        mut results: Vec<HashMap<String, Value>>,
        order_by: &[OrderBy],
    ) -> Result<Vec<HashMap<String, Value>>, Error> {
        if order_by.is_empty() {
            return Ok(results);
        }

        results.sort_by(|a, b| {
            for order in order_by {
                let a_val = a.get(&order.column);
                let b_val = b.get(&order.column);

                let cmp = match (a_val, b_val) {
                    (Some(a), Some(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
                    (None, Some(_)) => std::cmp::Ordering::Less,
                    (Some(_), None) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };

                if cmp != std::cmp::Ordering::Equal {
                    return match order.direction {
                        OrderDirection::Ascending => cmp,
                        OrderDirection::Descending => cmp.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(results)
    }

    /// Apply limit and offset to results
    fn apply_limit_offset(
        &self,
        mut results: Vec<HashMap<String, Value>>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Vec<HashMap<String, Value>> {
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(results.len());
        results.into_iter().skip(start).take(end - start).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_table() -> Result<(Arc<Storage>, Arc<Table>), Error> {
        let dir = tempdir()?;
        let storage = Arc::new(Storage::new(dir.path().to_str().unwrap())?);

        let schema = crate::storage::TableSchema {
            name: "test".to_string(),
            columns: vec![
                crate::storage::Column {
                    name: "id".to_string(),
                    type_name: "Integer".to_string(),
                    nullable: false,
                    default: None,
                },
                crate::storage::Column {
                    name: "name".to_string(),
                    type_name: "String".to_string(),
                    nullable: false,
                    default: None,
                },
            ],
            primary_key: vec!["id".to_string()],
            indexes: vec![],
        };

        storage.create_table("test", schema).await?;
        let table = storage.get_table("test").await?;

        Ok((storage, table))
    }

    #[tokio::test]
    async fn test_scan_execution() -> Result<(), Error> {
        let (storage, table) = create_test_table().await?;
        let executor = QueryExecutor::new(Arc::clone(&storage));

        // Insert test data
        let values = vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())],
        ];

        executor.execute(QueryPlan::Insert {
            table: Arc::clone(&table),
            values,
        }).await?;

        // Test full scan
        let result = executor.execute(QueryPlan::Scan {
            table: Arc::clone(&table),
            predicate: None,
            projections: vec!["*".to_string()],
        }).await?;

        match result {
            QueryResult::Select(rows) => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].get("name").unwrap(), &Value::String("Alice".to_string()));
            }
            _ => panic!("Expected Select result"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_update_execution() -> Result<(), Error> {
        let (storage, table) = create_test_table().await?;
        let executor = QueryExecutor::new(Arc::clone(&storage));

        // Insert test data
        let values = vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
        ];

        executor.execute(QueryPlan::Insert {
            table: Arc::clone(&table),
            values,
        }).await?;

        // Update row
        let result = executor.execute(QueryPlan::Update {
            table: Arc::clone(&table),
            values: vec![("name".to_string(), Value::String("Alice Smith".to_string()))],
            predicate: Some(Box::new(|row_data| {
                let row: HashMap<String, Value> = bincode::deserialize(row_data)?;
                Ok(row.get("id") == Some(&Value::Integer(1)))
            })),
        }).await?;

        match result {
            QueryResult::Update(count) => assert_eq!(count, 1),
            _ => panic!("Expected Update result"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_execution() -> Result<(), Error> {
        let (storage, table) = create_test_table().await?;
        let executor = QueryExecutor::new(Arc::clone(&storage));

        // Insert test data
        let values = vec![
            vec![Value::Integer(1), Value::String("Alice".to_string())],
            vec![Value::Integer(2), Value::String("Bob".to_string())],
        ];

        executor.execute(QueryPlan::Insert {
            table: Arc::clone(&table),
            values,
        }).await?;

        // Delete one row
        let result = executor.execute(QueryPlan::Delete {
            table: Arc::clone(&table),
            predicate: Some(Box::new(|row_data| {
                let row: HashMap<String, Value> = bincode::deserialize(row_data)?;
                Ok(row.get("id") == Some(&Value::Integer(1)))
            })),
        }).await?;

        match result {
            QueryResult::Delete(count) => assert_eq!(count, 1),
            _ => panic!("Expected Delete result"),
        }

        Ok(())
    }
}