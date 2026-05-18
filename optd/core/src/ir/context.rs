//! IRContext holds shared context for the IR, including catalog access,
//! cardinality estimation, and cost modeling.

use crate::ir::{
    Column, ColumnMeta, ColumnMetaStore, DataType,
    catalog::{Catalog, DataSourceId, Schema},
    cost::CostModel,
    properties::CardinalityEstimator,
    
};

use crate::parser_records::{parse_nullable_string, parse_string,parse_int,parse_data_type};
use anyhow::{bail, Result};
use datafusion::arrow::array::RecordBatch;
use itertools::Itertools;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct IRContext {
    /// An accessor to the catalog interface.
    pub cat: Arc<dyn Catalog>,
    /// An accessor to the cardinality estimator.
    pub card: Arc<dyn CardinalityEstimator>,
    /// An accessor to the cost model.
    pub cm: Arc<dyn CostModel>,

    pub(crate) source_to_first_column_id: Arc<Mutex<HashMap<DataSourceId, Column>>>,
    pub(crate) column_meta: Arc<Mutex<ColumnMetaStore>>,
}

impl IRContext {
    pub fn new(
        cat: Arc<dyn Catalog>,
        card: Arc<dyn CardinalityEstimator>,
        cost: Arc<dyn CostModel>,
    ) -> Self {
        Self {
            card,
            cat,
            cm: cost,
            source_to_first_column_id: Arc::default(),
            column_meta: Arc::default(),
        }
    }

    pub fn add_base_table_columns(&self, source: DataSourceId, schema: &Schema) -> Column {
        let mut mapping = self.source_to_first_column_id.lock().unwrap();
        use std::collections::hash_map::Entry;
        match mapping.entry(source) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let mut column_meta = self.column_meta.lock().unwrap();
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        column_meta
                            .new_column(field.data_type().clone(), Some(field.name().clone()))
                    })
                    .collect_vec();
                vacant.insert(columns[0]);
                columns[0]
            }
        }
    }

    pub fn define_column(&self, data_type: DataType, name: Option<String>) -> Column {
        let mut column_meta = self.column_meta.lock().unwrap();
        column_meta.new_column(data_type, name)
    }

    pub fn rename_column_alias(&self, column: Column, alias: String) {
        let mut column_meta = self.column_meta.lock().unwrap();
        column_meta.add_column_alias(column, alias);
    }

    pub fn column_by_name(&self, ident: &str) -> Option<Column> {
        self.columns_by_name(&[ident]).map(|v| v[0])
    }

    pub fn columns_by_name(&self, idents: &[&str]) -> Option<Vec<Column>> {
        let column_meta = self.column_meta.lock().unwrap();
        idents
            .iter()
            .map(|name| column_meta.column_by_name(name))
            .collect()
    }

    pub fn get_column_meta(&self, column: &Column) -> Arc<ColumnMeta> {
        let column_meta = self.column_meta.lock().unwrap();
        column_meta.get(column)
    }

    fn parse_string_value(batch: &RecordBatch, row: usize, column: &str) -> Result<String> {
        parse_string(batch, row, column)
    }

    fn parse_nullable_string_value(
        batch: &RecordBatch,
        row: usize,
        column: &str,
    ) -> Result<Option<String>> {
        parse_nullable_string(batch, row, column)
    }

    fn parse_int_value(batch: &RecordBatch, row: usize, column: &str) -> Result<i64> {
        parse_int(batch, row, column)
    }

    fn parse_data_type_debug(value: &str) -> Option<DataType> {
        parse_data_type(value)
    }

    /// Load context state from database RecordBatches.
    /// 
    /// The `schema_resolver` closure should resolve table schemas using the same approach as the planner:
    /// - Given a table name, use `DFSchema::try_from_qualified_schema(qualified_name, source_schema)`
    /// - Convert to optd schema using `into_optd_schema(&df_schema)?`
    /// - Return the optd `Arc<Schema>`
    pub async fn load_from_db<F>(&self, db_rows: HashMap<String, Vec<RecordBatch>>, mut schema_resolver: F) -> Result<()>
    where
        F: FnMut(&str) -> Pin<Box<dyn Future<Output = Result<Arc<Schema>>> + Send>>,
    {
        #[derive(Clone)]
        struct SourceRow {
            id: i64,
            name: String,
        }

        #[derive(Clone)]
        struct ColumnRow {
            id: i64,
            name: String,
            secondary_names: Vec<String>,
            data_type: DataType,
        }

        let mut source_rows = Vec::<SourceRow>::new();
        for batch in db_rows.get("source_table").into_iter().flatten() {
            for row in 0..batch.num_rows() {
                source_rows.push(SourceRow {
                    id: Self::parse_int_value(batch, row, "id")?,
                    name: Self::parse_string_value(batch, row, "name")?,
                });
            }
        }
        source_rows.sort_by_key(|row| row.id);

        let mut column_rows = Vec::<ColumnRow>::new();
        for batch in db_rows.get("memo_column").into_iter().flatten() {
            for row in 0..batch.num_rows() {
                let data_type_raw = Self::parse_string_value(batch, row, "data_type")?;
                let data_type = Self::parse_data_type_debug(&data_type_raw)
                    .ok_or_else(|| anyhow::anyhow!("unsupported data_type '{}'", data_type_raw))?;
                let secondary_names = Self::parse_nullable_string_value(batch, row, "secondary_name")?
                    .map(|value| {
                        value
                            .split('|')
                            .map(|part| part.trim().to_string())
                            .filter(|part| !part.is_empty())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                column_rows.push(ColumnRow {
                    id: Self::parse_int_value(batch, row, "id")?,
                    name: Self::parse_string_value(batch, row, "name")?,
                    secondary_names,
                    data_type,
                });
            }
        }
        column_rows.sort_by_key(|row| row.id);

        {
            let mut mapping = self.source_to_first_column_id.lock().unwrap();
            mapping.clear();
        }
        {
            let mut column_meta = self.column_meta.lock().unwrap();
            *column_meta = ColumnMetaStore::default();
        }

        for source_row in source_rows.iter() {
            let schema = schema_resolver(&source_row.name).await?;
            let created_id = self
                .cat
                .try_create_table(source_row.name.clone(), schema.clone())
                .unwrap_or_else(|existing| existing);

            if created_id.0 != source_row.id {
                bail!(
                    "table id mismatch for '{}': expected {}, got {}",
                    source_row.name,
                    source_row.id,
                    created_id.0
                );
            }
        }

        let source_prefixes: Vec<(i64, String)> = source_rows
            .iter()
            .map(|source_row| {
                let prefix = source_row
                    .name
                    .rsplit('.')
                    .next()
                    .unwrap_or(&source_row.name)
                    .to_string();
                (source_row.id, prefix)
            })
            .collect();

        let mut next_source_index = 0usize;

        for row in column_rows {
            let expected_id = row.id as usize;
            let current_len = {
                let column_meta = self.column_meta.lock().unwrap();
                column_meta.len()
            };

            let column = if expected_id == current_len {
                self.define_column(row.data_type.clone(), Some(row.name.clone()))
            } else if expected_id < current_len {
                Column(expected_id)
            } else {
                bail!(
                    "column id gap while loading '{}': expected next id {}, got {}",
                    row.name,
                    current_len,
                    expected_id
                );
            };

            let mut column_meta = self.column_meta.lock().unwrap();
            let meta = column_meta.get(&column);
            if meta.data_type != row.data_type {
                bail!(
                    "column type mismatch for '{}': expected {:?}, got {:?}",
                    row.name,
                    row.data_type,
                    meta.data_type
                );
            }

            // Ensure persisted primary and secondary names map to the persisted ID.
            column_meta.insert_name_to_column_id(row.name.clone(), column);
            for secondary_name in row.secondary_names {
                if secondary_name != row.name {
                    column_meta.insert_name_to_column_id(secondary_name, column);
                }
            }

            if next_source_index < source_prefixes.len() {
                let (source_id, source_prefix) = &source_prefixes[next_source_index];
                let is_first_base_column = row.name.starts_with(&format!("{source_prefix}."));
                if is_first_base_column {
                    let mut mapping = self.source_to_first_column_id.lock().unwrap();
                    mapping.insert(DataSourceId(*source_id), column);
                    next_source_index += 1;
                }
            }
        }

        if next_source_index != source_prefixes.len() {
            let missing_sources = source_prefixes[next_source_index..]
                .iter()
                .map(|(_, prefix)| prefix.clone())
                .collect_vec()
                .join(", ");
            bail!(
                "unable to resolve first column for source(s): {}",
                missing_sources
            );
        }

        Ok(())
    }

    fn escape_sql_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn column_dump_rows(&self) -> Vec<(Column, String, Option<String>, DataType)> {
        let entries = {
            let column_meta = self.column_meta.lock().unwrap();
            column_meta.name_to_column_id_entries()
        };

        let mut names_by_column: HashMap<Column, Vec<String>> = HashMap::new();
        for (name, column) in entries {
            names_by_column.entry(column).or_default().push(name);
        }

        let mut columns: Vec<Column> = names_by_column.keys().copied().collect();
        columns.sort_by_key(|column| column.0);

        let column_meta = self.column_meta.lock().unwrap();
        columns
            .into_iter()
            .map(|column| {
                let meta = column_meta.get(&column);
                let mut secondary_names = names_by_column.remove(&column).unwrap_or_default();
                secondary_names.retain(|name| name != &meta.name);
                secondary_names.sort();
                secondary_names.dedup();
                let secondary_name = if secondary_names.is_empty() {
                    None
                } else {
                    Some(secondary_names.join("|"))
                };

                (column, meta.name.clone(), secondary_name, meta.data_type.clone())
            })
            .collect()
    }

    pub fn dump_to_db(&self) -> HashMap<String, Vec<String>> {
        let mut db_statements: HashMap<String, Vec<String>> = HashMap::new();

        {
            let mapping = self.source_to_first_column_id.lock().unwrap();
            for source in mapping.keys() {
                let table = self.cat.describe_table(*source);
                db_statements
                    .entry("insert into source_table (id, name)".to_string())
                    .or_default()
                    .push(format!(
                        "({}, '{}')",
                        table.id.0,
                        Self::escape_sql_string(&table.name)
                    ));
            }
        }

        for (column, name, secondary_name, data_type) in self.column_dump_rows() {
            let secondary_sql = secondary_name
                .as_deref()
                .map(|value| format!("'{}'", Self::escape_sql_string(value)))
                .unwrap_or_else(|| "NULL".to_string());

            db_statements
                .entry("insert into column (id, name, secondary_name, data_type)".to_string())
                .or_default()
                .push(format!(
                    "({}, '{}', {}, '{}')",
                    column.0,
                    Self::escape_sql_string(&name),
                    secondary_sql,
                    Self::escape_sql_string(&format!("{:?}", data_type))
                ));
        }

        db_statements
    }

    pub fn dump_to_json(&self) -> Value {
        let source_table_rows = {
            let mapping = self.source_to_first_column_id.lock().unwrap();
            let mut rows = Vec::with_capacity(mapping.len());
            for source in mapping.keys() {
                let table = self.cat.describe_table(*source);
                rows.push(json!({
                    "id": table.id.0,
                    "name": table.name,
                }));
            }
            rows
        };

        let column_rows = self
            .column_dump_rows()
            .into_iter()
            .map(|(column, name, secondary_name, data_type)| {
                json!({
                    "id": column.0,
                    "name": name,
                    "secondary_name": secondary_name.map_or(Value::Null, |value| json!(value)),
                    "data_type": format!("{:?}", data_type),
                })
            })
            .collect::<Vec<_>>();

        json!({
            "source_table": source_table_rows,
            "column": column_rows,
        })
    }


    

}

impl std::fmt::Debug for IRContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IRContext")
            .field("source_to_first_column_id", &self.source_to_first_column_id)
            .field("column_meta", &self.column_meta)
            .finish()
    }
}

