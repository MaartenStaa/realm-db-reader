mod column;
mod header;
mod row;

use anyhow::{Ok, anyhow, bail};
use log::debug;
use tracing::instrument;

use crate::array::Array;
use crate::column::Column;
pub(crate) use crate::table::column::ColumnAttributes;
use crate::table::header::TableHeader;
pub use crate::table::row::Row;
use crate::value::Value;

/// A view into a single Realm database table.
#[derive(Debug)]
#[allow(unused)]
pub struct Table {
    header: TableHeader,
    table_number: usize,
}

impl Table {
    /// Construct a new table instance, from the given Realm array.
    #[instrument(level = "debug")]
    pub(crate) fn build(array: Array, table_number: usize) -> anyhow::Result<Self> {
        let header_array = array.get_node(0)?.unwrap();
        let data_array = array.get_node(1)?.unwrap();

        Self::build_from(&header_array, data_array, table_number)
    }

    /// Construct a new table instance, from the given Realm arrays for the
    /// header and data. This is used primarily by subtables, as their header
    /// and data arrays are in disjointed locations compared to regular tables.
    #[instrument(level = "debug")]
    pub(crate) fn build_from(
        header_array: &Array,
        data_array: Array,
        table_number: usize,
    ) -> anyhow::Result<Self> {
        let header = TableHeader::build(header_array, &data_array)?;

        let result = Self {
            header,
            table_number,
        };

        debug!("data: {:?}", result);
        Ok(result)
    }
}

impl Table {
    /// Get the number of the table, starting with 0, within the
    /// [`Group`](`crate::group::Group`).
    ///
    /// Subtables have a table number of [`usize::MAX`].
    pub fn get_table_number(&self) -> usize {
        self.table_number
    }

    /// Get the column specifications for the table.
    pub fn get_column_specs(&self) -> &[Box<dyn Column>] {
        self.header.get_columns()
    }

    /// Get the specification for the column with the given number (starting with 0).
    ///
    /// Returns an error if the column number is out of range.
    pub fn get_column_spec(&self, column_number: usize) -> Option<&dyn Column> {
        self.header.get_column(column_number)
    }

    /// Get the number of rows in the table.
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    pub fn row_count(&self) -> anyhow::Result<usize> {
        let first_column = self
            .header
            .get_column(0)
            .ok_or_else(|| anyhow::anyhow!("No column at index 0: can't load row count"))?;
        first_column.count()
    }

    /// Get the row with the given number (starting with 0).
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_row<'a>(&'a self, row_number: usize) -> anyhow::Result<Row<'a>> {
        let values = self.load_row(row_number)?;

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

    /// Load the values for the row with the given number (starting with 0).
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    fn load_row(&self, row_number: usize) -> anyhow::Result<Vec<Value>> {
        let column_count = self.header.column_count();
        let mut values = Vec::with_capacity(column_count);
        for column_number in 0..column_count {
            log::info!("loading column {column_number} for row {row_number}");
            values.push(self.load_column(column_number, row_number)?);
        }

        Ok(values)
    }

    /// Determine the row number for the given value in an indexed column.
    /// Note that if there are multiple rows with the same value, this function
    /// will return the first one.
    ///
    /// Returns an error if there is no column with the given name or if the column is not indexed.
    ///
    /// Returns `None` if the value is not found in the indexed column.
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    pub fn find_row_number_from_indexed_column(
        &self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<usize>> {
        // Find the column index for the given column name
        let column_spec = self
            .header
            .get_columns()
            .iter()
            .find(|col| col.name() == Some(indexed_column_name))
            .ok_or_else(|| anyhow!("Column not found: {}", indexed_column_name))?;

        if !column_spec.is_indexed() {
            bail!(
                "Column '{}' is not indexed, cannot perform lookup",
                indexed_column_name
            );
        }

        column_spec.get_row_number_by_index(value)
    }

    /// Find and load the row with the given value in an indexed column.
    /// Note that if there are multiple rows with the same value, only the first one is returned.
    ///
    /// Returns an error if there is no column with the given name or if the column is not indexed.
    ///
    /// Returns `None` if the value is not found in the indexed column.
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    pub fn find_row_from_indexed_column<'a>(
        &'a self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<Row<'a>>> {
        let Some(row_number) =
            self.find_row_number_from_indexed_column(indexed_column_name, value)?
        else {
            return Ok(None);
        };

        self.get_row(row_number).map(Some)
    }

    /// Get all rows in the table.
    #[instrument(level = "debug", skip(self), fields(header = ?self.header))]
    pub fn get_rows<'a>(&'a self) -> anyhow::Result<Vec<Row<'a>>> {
        let row_count = self.row_count()?;
        let mut rows = Vec::with_capacity(row_count);

        for i in 0..row_count {
            rows.push(self.get_row(i)?);
        }

        Ok(rows)
    }

    /// Load the value at the specified column and row.
    ///
    /// Panics if the column or row number is out of range.
    #[instrument(level = "debug", skip(self))]
    fn load_column(&self, column_number: usize, row_number: usize) -> anyhow::Result<Value> {
        let column_spec = self
            .header
            .get_column(column_number)
            .unwrap_or_else(|| panic!("Invalid column number {column_number}"));
        let value = column_spec.get(row_number)?;

        debug!(
            "Loaded column {column_number} at row {row_number}: {:?}",
            value
        );

        Ok(value)
    }
}
