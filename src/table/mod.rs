mod column;
mod header;
mod row;

use anyhow::{Ok, anyhow, bail};
use log::debug;
use tracing::instrument;

use crate::array::Array;
use crate::column::Column;
pub use crate::table::column::ColumnAttributes;
pub use crate::table::header::TableHeader;
pub use crate::table::row::Row;
use crate::value::Value;

#[derive(Debug)]
#[allow(unused)]
pub struct Table {
    header: TableHeader,
    table_number: usize,
}

impl Table {
    #[instrument(target = "Table", level = "debug")]
    pub fn build(array: Array, table_number: usize) -> anyhow::Result<Self> {
        let header_array = array.get_node(0)?.unwrap();
        let data_array = array.get_node(1)?.unwrap();

        Self::build_from(&header_array, data_array, table_number)
    }

    #[instrument(target = "Table", level = "debug")]
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

        debug!(target: "Table", "data: {:?}", result);
        Ok(result)
    }
}

impl Table {
    pub fn get_table_number(&self) -> usize {
        self.table_number
    }

    pub fn get_column_spec(&self, column_number: usize) -> anyhow::Result<&dyn Column> {
        self.header.get_column(column_number)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn row_count(&self) -> anyhow::Result<usize> {
        let first_column = self.header.get_column(0)?;
        first_column.count()
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
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

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    fn load_row(&self, row_number: usize) -> anyhow::Result<Vec<Value>> {
        let column_count = self.header.column_count();
        let mut values = Vec::with_capacity(column_count);
        for column_number in 0..column_count {
            log::info!(target: "Table", "loading column {column_number} for row {row_number}");
            values.push(self.load_column(column_number, row_number)?);
        }

        Ok(values)
    }

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
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

    #[instrument(target = "Table", level = "debug", skip(self), fields(header = ?self.header))]
    pub fn find_row_from_indexed_column<'a>(
        &'a self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<Row<'a>>> {
        let row_number = self.find_row_number_from_indexed_column(indexed_column_name, value)?;
        if let Some(row_number) = row_number {
            return Ok(Some(self.get_row(row_number)?));
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

    #[instrument(target = "Table", level = "debug", skip(self))]
    fn load_column(&self, column_number: usize, row_number: usize) -> anyhow::Result<Value> {
        let column_spec = self.header.get_column(column_number)?;
        let value = column_spec.get(row_number)?;

        debug!(
            target: "Table",
            "Loaded column {column_number} at row {row_number}: {:?}",
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
    //     row_number: usize,
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
    //     target_table_number: usize,
    //     name: &str,
    //     attributes: &ColumnAttributes,
    //     row_number: usize,
    // ) -> anyhow::Result<Value> {
    //     unimplemented!("link column {name} at index {data_array_index}");
    // }
}
