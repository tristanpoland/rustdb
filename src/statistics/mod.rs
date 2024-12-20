use std::collections::HashMap;

pub struct Statistics {
    table_stats: RwLock<HashMap<String, TableStats>>,
}

#[derive(Debug, Clone)]
pub struct TableStats {
    row_count: u64,
    page_count: u64,
    column_stats: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    distinct_values: u64,
    min_value: Value,
    max_value: Value,
    histogram: Option<Histogram>,
}