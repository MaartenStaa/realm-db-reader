mod column;
mod header;
mod spec;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Ok, anyhow, bail};
use log::{debug, warn};
use tracing::instrument;

use crate::array::{Array, ArrayBasic, ArrayTimestamp};
use crate::build::Build;
use crate::index::Index;
use crate::node::Node;
use crate::spec::{ColumnType, ThinColumnType};
use crate::table::column::ColumnAttributes;
use crate::table::header::TableHeader;
use crate::table::spec::{ColumnSpec, FatColumnType};
use crate::value::{Backlink, Value};

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Table {
    data_array: ArrayBasic,
    header: TableHeader,
    data_columns: Vec<Vec<Option<Value>>>,
    data_rows: Vec<Option<Vec<Value>>>,
    indexes: HashMap<usize, Index>,
}

impl Build for Table {
    #[instrument(target = "Table")]
    fn build(array: ArrayBasic) -> anyhow::Result<Self> {
        let header = {
            let array: ArrayBasic = array.get_node(0)?;
            TableHeader::build(array)?
        };

        let data_columns = header.get_columns().iter().map(|_| vec![]).collect();

        let result = Self {
            data_array: array.get_node(1)?,
            header,
            data_columns,
            data_rows: vec![],
            indexes: HashMap::new(),
        };

        warn!(target: "Table", "data: {:?}", result);
        Ok(result)
    }
}

impl Table {
    #[instrument(target = "Table")]
    fn new_for_subtable(header: TableHeader, data_array: ArrayBasic) -> Self {
        let data_columns = header.get_columns().iter().map(|_| vec![]).collect();

        Self {
            data_array,
            header,
            data_columns,
            data_rows: vec![],
            indexes: HashMap::new(),
        }
    }

    pub fn get_column_spec(&self, column_index: usize) -> anyhow::Result<&ColumnSpec> {
        self.header.get_column(column_index)
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    pub fn row_count(&self) -> anyhow::Result<usize> {
        let first_column = self.header.get_column(0)?;
        let first_column_type = first_column.as_column_type();
        let data_array_index = first_column.get_data_array_index();

        match first_column_type {
            ColumnType::Int | ColumnType::Bool => {
                let array: Array<u64> = self.data_array.get_node(data_array_index)?;
                Ok(array.element_count())
            }
            ColumnType::String => {
                let array: Array<String> = self.data_array.get_node(data_array_index)?;
                Ok(array.element_count())
            }
            _ => {
                unimplemented!(
                    "Unsupported column type for row count: {:?}",
                    first_column_type
                );
            }
        }
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    pub fn get_row(&mut self, index: usize) -> anyhow::Result<&[Value]> {
        self.ensure_row_loaded(index)?;

        Ok(self.data_rows[index].as_ref().unwrap())
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    pub fn get_row_mut(&mut self, index: usize) -> anyhow::Result<&mut [Value]> {
        self.ensure_row_loaded(index)?;

        Ok(self.data_rows[index].as_mut().unwrap())
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    pub fn find_row_from_index(
        &mut self,
        indexed_column_name: &str,
        value: &Value,
    ) -> anyhow::Result<Option<&[Value]>> {
        // Find the column index for the given column name
        let (column_index, column_spec) = self
            .header
            .get_columns()
            .iter()
            .enumerate()
            .find(|(_, col)| match col {
                ColumnSpec::Regular { name, .. } => name == indexed_column_name,
                _ => false,
            })
            .ok_or_else(|| anyhow!("Column not found: {}", indexed_column_name))?;

        if !column_spec.get_attributes().is_indexed() {
            bail!(
                "Column '{}' is not indexed, cannot perform lookup",
                indexed_column_name
            );
        }

        // Then, ensure we load all values for that column, and create an "index" for them.
        if !self.indexes.contains_key(&column_index) {
            let Some(index_ref) = self
                .data_array
                .get_ref(column_spec.get_data_array_index() + 1)
            else {
                bail!(
                    "cannot find index data for column {indexed_column_name} at index {column_index}"
                );
            };
            let index = Index::from_ref(Arc::clone(&self.data_array.node.realm), index_ref)?;

            self.indexes.insert(column_index, index);
            // dbg!(&index);
            // bail!("loaded the index");
        }

        let column_lookup = self
            .indexes
            .get(&column_index)
            .ok_or_else(|| anyhow!("Column index not found: {}", column_index))?;
        let Some(row_index) = column_lookup.find_first(value)? else {
            return Ok(None);
        };

        Ok(Some(self.get_row(row_index)?))
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    fn ensure_row_loaded(&mut self, index: usize) -> anyhow::Result<()> {
        if self.data_rows.len() > index && self.data_rows[index].is_some() {
            return Ok(());
        }

        self.ensure_columns_loaded(index)?;

        let mut row = Vec::with_capacity(self.header.column_count());
        for i in 0..self.header.column_count() {
            let column_data = &self.data_columns[i][index];
            // TODO: Avoid this clone?
            row.push(column_data.clone().unwrap());
        }

        self.data_rows.resize(index + 1, None);
        self.data_rows[index] = Some(row);

        Ok(())
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    pub fn get_rows(&mut self) -> anyhow::Result<Vec<&[Value]>> {
        let row_count = self.row_count()?;
        if self.data_rows.len() < row_count || self.data_rows.iter().any(|r| r.is_none()) {
            for i in 0..row_count {
                self.get_row(i)?;
            }
        }

        Ok(self
            .data_rows
            .iter()
            .map(|r| r.as_ref().unwrap().as_slice())
            .collect())
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    fn ensure_columns_loaded(&mut self, row_index: usize) -> anyhow::Result<()> {
        for column_index in 0..self.header.column_count() {
            log::warn!(target: "Table", "loading column {column_index} for row {row_index}");
            self.ensure_column_loaded(column_index, row_index)?;
        }

        Ok(())
    }

    #[instrument(target = "Table", skip(self), fields(header = ?self.header))]
    fn ensure_column_loaded(
        &mut self,
        column_index: usize,
        row_index: usize,
    ) -> anyhow::Result<()> {
        // Ensure the column array is long enough (pre-fill with None)
        if self.data_columns[column_index].len() <= row_index {
            debug!(
                target: "Table",
                "Resizing column {column_index} to fit row {row_index}"
            );
            self.data_columns[column_index].resize(row_index + 1, None);
        }

        if self.data_columns[column_index][row_index].is_some() {
            debug!(
                target: "Table",
                "Column {column_index} at row {row_index} is already loaded"
            );
            return Ok(());
        }

        let column_spec = &self.header.get_column(column_index)?;
        self.data_columns[column_index][row_index] =
            Some(self.read_column_row(column_index, column_spec, row_index)?);

        debug!(
            target: "Table",
            "Loaded column {column_index} at row {row_index}: {:?}",
            self.data_columns[column_index][row_index]
        );

        Ok(())
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row(
        &self,
        column_index: usize,
        column_spec: &'_ ColumnSpec,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        match column_spec {
            ColumnSpec::Regular {
                type_,
                data_array_index,
                name,
                attributes,
            } => {
                self.read_column_row_regular(*data_array_index, type_, name, attributes, row_index)
            }
            ColumnSpec::BackLink {
                data_array_index,
                attributes,
                origin_table_index,
                origin_column_index,
            } => self.read_column_row_backlink(
                *data_array_index,
                *origin_table_index,
                *origin_column_index,
                attributes,
                row_index,
            ),
        }
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_regular(
        &self,
        data_array_index: usize,
        type_: &FatColumnType,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        match type_ {
            FatColumnType::Thin(type_) => {
                self.read_column_row_thin(data_array_index, type_, name, attributes, row_index)
            }
            FatColumnType::Table(table_header) => self.read_column_row_table(
                data_array_index,
                table_header,
                name,
                attributes,
                row_index,
            ),
            FatColumnType::Link { target_table_index } => self.read_column_row_link(
                data_array_index,
                *target_table_index,
                name,
                attributes,
                row_index,
            ),
            FatColumnType::LinkList { target_table_index } => self.read_column_row_link_list(
                data_array_index,
                *target_table_index,
                name,
                attributes,
                row_index,
            ),
        }
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_thin(
        &self,
        data_array_index: usize,
        type_: &ThinColumnType,
        _name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        match type_ {
            ThinColumnType::Int => {
                let array: Array<u64> = self.data_array.get_node(data_array_index)?;
                let value = array.get_integer(row_index)?;
                Ok(Value::Int(value))
            }
            ThinColumnType::Bool => {
                let array: Array<bool> = self.data_array.get_node(data_array_index)?;
                let value = array.get_bool(row_index)?;
                Ok(Value::Bool(value))
            }
            ThinColumnType::String => {
                let array: Array<String> = self.data_array.get_node(data_array_index)?;
                let value = array.get_string(row_index)?;
                Ok(match (value, attributes.is_nullable()) {
                    (Some(value), _) => Value::String(value),
                    (_, true) => Value::None,
                    (_, false) => {
                        bail!("Expected string value for non-nullable column")
                    }
                })
            }
            ThinColumnType::Timestamp => {
                let array: ArrayTimestamp = self.data_array.get_node(data_array_index)?;
                let value = array.get(row_index)?;
                Ok(match (value, attributes.is_nullable()) {
                    (Some(value), _) => Value::Timestamp(value),
                    (_, true) => Value::None,
                    (_, false) => {
                        bail!("Expected timestamp value for non-nullable column")
                    }
                })
            }
            _ => unimplemented!("column_type: {:?}", type_),
        }
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_table(
        &self,
        data_array_index: usize,
        table_header: &TableHeader,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        // let array: Array = match self.data_array.get_ref(data_array_index) {
        //     Some(ref_) => Array::from_ref(self.data_array.node.realm.clone(), ref_)?,
        //     _ => return Ok(Value::None),
        // };
        //
        let Some(ref_) = self.data_array.get_ref(data_array_index) else {
            return Ok(Value::None);
        };

        Ok(Value::Table(ref_))

        // Ok(Value::Table(Table::new_for_subtable(
        //     table_header.clone(),
        //     array,
        // )))
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_link(
        &self,
        data_array_index: usize,
        target_table_index: usize,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        unimplemented!("link column {name} at index {data_array_index}");
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_link_list(
        &self,
        data_array_index: usize,
        target_table_index: usize,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        let array: Array<Vec<usize>> = self.data_array.get_node(data_array_index)?;
        let value = array.get_link_list(row_index)?;

        Ok(match (value, attributes.is_nullable()) {
            (Some(value), _) => Value::LinkList(value),
            (_, false) => Value::LinkList(vec![]),
            (_, true) => Value::None,
        })
    }

    #[instrument(target = "Table", skip(self))]
    fn read_column_row_backlink(
        &self,
        data_array_index: usize,
        origin_table_index: usize,
        origin_column_index: usize,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        let array: Array<u64> = self.data_array.get_node(data_array_index)?;
        let value = array.get_tagged_integer(row_index)?;

        Ok(match value {
            Some(value) => Value::BackLink(Backlink::new(
                origin_table_index,
                origin_column_index,
                value as usize,
            )),
            None => Value::None,
        })
    }
}
