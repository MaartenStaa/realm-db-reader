mod column;
mod header;
mod row;

use std::collections::HashMap;

use anyhow::{Ok, anyhow, bail};
use log::debug;
use tracing::instrument;

use crate::array::Array;
use crate::column::Column;
use crate::index::Index;
pub use crate::table::column::ColumnAttributes;
pub use crate::table::header::TableHeader;
pub use crate::table::row::Row;
use crate::value::Value;

#[derive(Debug)]
#[allow(unused)]
pub struct Table {
    header: TableHeader,
    indexes: HashMap<usize, Index>,
}

impl Table {
    #[instrument(target = "Table", level = "debug")]
    pub fn build(array: Array) -> anyhow::Result<Self> {
        let header_array = array.get_node(0)?.unwrap();
        let data_array = array.get_node(1)?.unwrap();

        Self::build_from(&header_array, data_array)
    }

    #[instrument(target = "Table", level = "debug")]
    pub(crate) fn build_from(header_array: &Array, data_array: Array) -> anyhow::Result<Self> {
        let header = TableHeader::build(header_array, &data_array)?;

        let result = Self {
            header,
            indexes: HashMap::new(),
        };

        debug!(target: "Table", "data: {:?}", result);
        Ok(result)
    }
}

impl Table {
    pub fn get_column_spec(&self, column_index: usize) -> anyhow::Result<&dyn Column> {
        self.header.get_column(column_index)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn row_count(&self) -> anyhow::Result<usize> {
        let first_column = self.header.get_column(0)?;
        first_column.count()
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_row<'a>(&'a self, row_index: usize) -> anyhow::Result<Row<'a>> {
        let values = self.load_row(row_index)?;

        Ok(Row::new(
            values,
            self.header
                .get_columns()
                .iter()
                .filter_map(|c| c.name())
                .map(|n| n.into())
                .collect(),
        ))
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_row_owned(&self, row_index: usize) -> anyhow::Result<Row<'static>> {
        let values = self.load_row(row_index)?;

        Ok(Row::new(
            values,
            self.header
                .get_columns()
                .iter()
                .filter_map(|c| c.name())
                .map(|n| n.to_string().into())
                .collect(),
        ))
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    fn load_row(&self, row_index: usize) -> anyhow::Result<Vec<Value>> {
        let column_count = self.header.column_count();
        let mut values = Vec::with_capacity(column_count);
        for column_index in 0..column_count {
            log::info!(target: "Table", "loading column {column_index} for row {row_index}");
            values.push(self.load_column(column_index, row_index)?);
        }

        Ok(values)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn find_row_index_from_indexed_column(
        &mut self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<usize>> {
        // Find the column index for the given column name
        let (column_index, column_spec) = self
            .header
            .get_columns()
            .iter()
            .enumerate()
            .find(|(_, col)| matches!(col.name(), Some(indexed_column_name)))
            .ok_or_else(|| anyhow!("Column not found: {}", indexed_column_name))?;

        if !column_spec.is_indexed() {
            bail!(
                "Column '{}' is not indexed, cannot perform lookup",
                indexed_column_name
            );
        }

        // Then, ensure we load all values for that column, and create an "index" for them.
        if !self.indexes.contains_key(&column_index) {
            // let Some(index_ref) = self
            //     .data_array
            //     .get_ref(column_spec.get_data_array_index() + 1)
            // else {
            //     bail!(
            //         "cannot find index data for column {indexed_column_name} at index {column_index}"
            //     );
            // };
            // let index = Index::from_ref(Arc::clone(&self.data_array.node.realm), index_ref)?;
            todo!();

            // self.indexes.insert(column_index, index);
        }

        let column_lookup = self
            .indexes
            .get(&column_index)
            .ok_or_else(|| anyhow!("Column index not found: {}", column_index))?;
        let Some(row_index) = column_lookup.find_first(value)? else {
            return Ok(None);
        };

        Ok(Some(row_index))
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn find_row_from_indexed_column<'a>(
        &'a mut self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<Row<'a>>> {
        let row_index = self.find_row_index_from_indexed_column(indexed_column_name, value)?;
        if let Some(row_index) = row_index {
            return Ok(Some(self.get_row(row_index)?));
        }

        Ok(None)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_rows<'a>(&'a self) -> anyhow::Result<Vec<Row<'a>>> {
        let row_count = self.row_count()?;
        let mut rows = Vec::with_capacity(row_count);

        for i in 0..row_count {
            rows.push(self.get_row(i)?);
        }

        Ok(rows)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_rows_owned(&self) -> anyhow::Result<Vec<Row<'static>>> {
        let row_count = self.row_count()?;
        let mut rows = Vec::with_capacity(row_count);

        for i in 0..row_count {
            rows.push(self.get_row_owned(i)?);
        }

        Ok(rows)
    }

    #[instrument(target = "Table", level = "debug", skip(self))]
    fn load_column(&self, column_index: usize, row_index: usize) -> anyhow::Result<Value> {
        let column_spec = self.header.get_column(column_index)?;
        let value = column_spec.get(row_index)?;

        debug!(
            target: "Table",
            "Loaded column {column_index} at row {row_index}: {:?}",
            value
        );

        Ok(value)
    }

    // #[instrument(target = "Table", level = "debug", skip(self))]
    // fn read_column_row_table(
    //     &self,
    //     data_array_index: usize,
    //     table_header: &TableHeader,
    //     name: &str,
    //     attributes: &ColumnAttributes,
    //     row_index: usize,
    // ) -> anyhow::Result<Value> {
    //     // let array: Array = match self.data_array.get_ref(data_array_index) {
    //     //     Some(ref_) => Array::from_ref(self.data_array.node.realm.clone(), ref_)?,
    //     //     _ => return Ok(Value::None),
    //     // };
    //     //
    //     let Some(ref_) = self.data_array.get_ref(data_array_index) else {
    //         return Ok(Value::None);
    //     };
    //
    //     Ok(Value::Table(ref_))
    //
    //     // Ok(Value::Table(Table::new_for_subtable(
    //     //     table_header.clone(),
    //     //     array,
    //     // )))
    // }
    //
    // #[instrument(target = "Table", level = "debug", skip(self))]
    // fn read_column_row_link(
    //     &self,
    //     data_array_index: usize,
    //     target_table_index: usize,
    //     name: &str,
    //     attributes: &ColumnAttributes,
    //     row_index: usize,
    // ) -> anyhow::Result<Value> {
    //     unimplemented!("link column {name} at index {data_array_index}");
    // }
}
