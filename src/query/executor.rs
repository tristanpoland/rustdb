pub struct QueryExecutor {
    buffer_pool: Arc<BufferPool>,
}

impl QueryExecutor {
    pub fn new(buffer_pool: Arc<BufferPool>) -> Self {
        Self { buffer_pool }
    }

    pub async fn execute(&self, query: Query) -> Result<QueryResult, Error> {
        match query {
            Query::Select(select) => self.execute_select(select).await,
            Query::Insert(insert) => self.execute_insert(insert).await,
            Query::Update(update) => self.execute_update(update).await,
            Query::Delete(delete) => self.execute_delete(delete).await,
        }
    }

    async fn execute_select(&self, select: SelectQuery) -> Result<QueryResult, Error> {
        let table = self.get_table(&select.table)?;
        let mut results = Vec::new();

        // Check if we can use an index
        if let Some(index) = self.find_usable_index(&table, &select.conditions) {
            results = self.scan_index(index, &select.conditions).await?;
        } else {
            results = self.full_table_scan(&table, &select.conditions).await?;
        }

        Ok(QueryResult::Rows(results))
    }

    async fn scan_index(
        &self,
        index: &Index,
        conditions: &[Condition],
    ) -> Result<Vec<Row>, Error> {
        let mut results = Vec::new();
        let range = index.get_scan_range(conditions)?;
        
        for page_id in range {
            let page = self.buffer_pool.get_page(index.file(), page_id).await?;
            let page = page.read();
            
            // Read and filter rows from index
            let rows = page.read_rows()?;
            for row in rows {
                if self.matches_conditions(&row, conditions)? {
                    results.push(row);
                }
            }
        }

        Ok(results)
    }

    fn matches_conditions(&self, row: &Row, conditions: &[Condition]) -> Result<bool, Error> {
        for condition in conditions {
            match condition {
                Condition::Equals(column, value) => {
                    if row.get(column)? != value {
                        return Ok(false);
                    }
                }
                Condition::Range(column, range) => {
                    let col_value = row.get(column)?;
                    if !range.contains(&col_value) {
                        return Ok(false);
                    }
                }
                // Add more condition types
            }
        }
        Ok(true)
    }
}