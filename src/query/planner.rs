pub struct QueryPlanner {
    statistics: Arc<Statistics>,
}

impl QueryPlanner {
    pub fn new(statistics: Arc<Statistics>) -> Self {
        Self { statistics }
    }

    pub fn plan(&self, query: &Query) -> Result<QueryPlan, Error> {
        match query {
            Query::Select(select) => self.plan_select(select),
            Query::Insert(insert) => self.plan_insert(insert),
            Query::Update(update) => self.plan_update(update),
            Query::Delete(delete) => self.plan_delete(delete),
        }
    }

    fn plan_select(&self, select: &SelectQuery) -> Result<QueryPlan, Error> {
        let table = self.get_table(&select.table)?;
        
        // Find best index
        let best_index = self.find_best_index(&table, &select.conditions);
        
        if let Some(index) = best_index {
            QueryPlan::IndexScan {
                index,
                conditions: select.conditions.clone(),
            }
        } else {
            QueryPlan::TableScan {
                table: table.clone(),
                conditions: select.conditions.clone(),
            }
        }
    }

    fn find_best_index(&self, table: &Table, conditions: &[Condition]) -> Option<Index> {
        let mut best_index = None;
        let mut lowest_cost = f64::INFINITY;

        for index in table.indexes() {
            if index.can_handle_conditions(conditions) {
                let cost = self.estimate_index_cost(index, conditions);
                if cost < lowest_cost {
                    lowest_cost = cost;
                    best_index = Some(index.clone());
                }
            }
        }

        best_index
    }

    fn estimate_index_cost(&self, index: &Index, conditions: &[Condition]) -> f64 {
        // Use statistics to estimate cost
        // Consider factors like:
        // - Number of pages to read
        // - Selectivity of conditions
        // - Index height
        unimplemented!()
    }
}