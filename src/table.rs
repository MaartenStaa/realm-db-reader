use anyhow::{Ok, anyhow, bail};
use log::{debug, warn};
use tracing::instrument;

use crate::array::{
    Array, ArrayLinkList, ArrayString, ArrayTimestamp, IntegerArray, RefOrTaggedValue,
};
use crate::build::Build;
use crate::column::ColumnAttributes;
use crate::node::Node;
use crate::spec::{ColumnType, ThinColumnType};
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct TableHeader {
    columns: Vec<ColumnSpec>,
}

#[derive(Debug, Clone)]
pub enum FatColumnType {
    Thin(ThinColumnType),
    Table(TableHeader),
    // TODO: Payload for these
    Link { target_table_index: usize },
    LinkList { target_table_index: usize },
}

impl FatColumnType {
    fn as_column_type(&self) -> ColumnType {
        match self {
            FatColumnType::Thin(type_) => type_.as_column_type(),
            FatColumnType::Table(_) => ColumnType::Table,
            FatColumnType::Link { .. } => ColumnType::Link,
            FatColumnType::LinkList { .. } => ColumnType::LinkList,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnSpec {
    Regular {
        type_: FatColumnType,
        name: String,
        attributes: ColumnAttributes,
    },
    /// Backlink columns don't have a name, so acount for this
    /// as a separate column spec variant.
    BackLink {
        attributes: ColumnAttributes,
        origin_table_index: usize,
        origin_column_index: usize,
    },
}

impl ColumnSpec {
    fn as_column_type(&self) -> ColumnType {
        match self {
            ColumnSpec::Regular { type_, .. } => type_.as_column_type(),
            ColumnSpec::BackLink { .. } => ColumnType::BackLink,
        }
    }

    fn get_attributes(&self) -> ColumnAttributes {
        match self {
            ColumnSpec::Regular { attributes, .. } => *attributes,
            ColumnSpec::BackLink { attributes, .. } => *attributes,
        }
    }
}

impl TableHeader {
    #[instrument(target = "TableHeader")]
    fn from_parts(
        column_types: Vec<ColumnType>,
        mut column_names: Vec<String>,
        column_attributes: Vec<ColumnAttributes>,
        sub_spec_array: Option<Array>,
    ) -> anyhow::Result<Self> {
        // NOTE: The same does not apply for column names, as backlinks don't have a name.
        assert_eq!(
            column_types.len(),
            column_attributes.len(),
            "number of column types and column attributes should match"
        );

        let mut columns = Vec::with_capacity(column_types.len());
        // Reverse the column names so we can do a low-cost pop for each column that has a name.
        column_names.reverse();
        let mut sub_spec_index = 0;
        for (i, column_type) in column_types.into_iter().enumerate() {
            let spec = match column_type {
                ColumnType::Table => {
                    let other_table_header_array = sub_spec_array
                        .as_ref()
                        .ok_or(anyhow::anyhow!("Expected sub-spec array for table column"))?
                        .get_node(sub_spec_index)?;
                    sub_spec_index += 1;
                    let table_header = TableHeader::build(other_table_header_array)?;
                    let name = column_names
                        .pop()
                        .ok_or(anyhow!("Expected column name for column index {i}"))?;
                    ColumnSpec::Regular {
                        type_: FatColumnType::Table(table_header),
                        name,
                        attributes: column_attributes[i],
                    }
                }
                ct @ (ColumnType::Link | ColumnType::LinkList) => {
                    let target_table = Self::get_sub_spec_index_value(
                        sub_spec_array
                            .as_ref()
                            .ok_or(anyhow::anyhow!("Expected sub-spec array for link column"))?,
                        sub_spec_index,
                    )?;
                    sub_spec_index += 1;
                    let name = column_names
                        .pop()
                        .ok_or(anyhow!("Expected column name for column index {i}"))?;
                    ColumnSpec::Regular {
                        type_: if ct == ColumnType::Link {
                            FatColumnType::Link {
                                target_table_index: target_table,
                            }
                        } else {
                            FatColumnType::LinkList {
                                target_table_index: target_table,
                            }
                        },
                        name,
                        attributes: column_attributes[i],
                    }
                }
                ColumnType::BackLink => {
                    let sub_spec_array = sub_spec_array.as_ref().ok_or(anyhow::anyhow!(
                        "Expected sub-spec array for backlink column"
                    ))?;
                    let target_table_index =
                        Self::get_sub_spec_index_value(sub_spec_array, sub_spec_index)?;
                    sub_spec_index += 1;
                    let target_table_column_index =
                        Self::get_sub_spec_index_value(sub_spec_array, sub_spec_index)?;
                    sub_spec_index += 1;
                    ColumnSpec::BackLink {
                        attributes: column_attributes[i],
                        origin_table_index: target_table_index,
                        origin_column_index: target_table_column_index,
                    }
                }
                other => {
                    let name = column_names.pop().ok_or(anyhow::anyhow!(
                        "Expected column name for column index {i} (type {other:?})"
                    ))?;
                    let attributes = column_attributes[i];
                    let type_ = FatColumnType::Thin(other.as_thin_column_type()?);
                    ColumnSpec::Regular {
                        type_,
                        name,
                        attributes,
                    }
                }
            };

            warn!("column spec {i}: {spec:?}");
            columns.push(spec);
        }

        Ok(Self { columns })
    }

    fn get_sub_spec_index_value(
        sub_spec_array: &Array,
        sub_spec_index: usize,
    ) -> anyhow::Result<usize> {
        match sub_spec_array.get_ref_or_tagged_value(sub_spec_index) {
            Some(RefOrTaggedValue::Ref(_)) => bail!("Expected tagged integer for link column"),
            Some(RefOrTaggedValue::TaggedRef(target_table_index)) => {
                Ok(target_table_index as usize)
            }
            _ => bail!("Expected tagged integer for link column"),
        }
    }
}

impl Build for TableHeader {
    #[instrument(target = "TableHeader")]
    fn build(array: Array) -> anyhow::Result<Self> {
        let column_types = {
            let array: IntegerArray<ColumnType> = array.get_node(0)?;
            array.get_integers_generic()
        };

        warn!(target: "TableHeader", "column_types: {:?}", column_types);

        let column_names = {
            let array: ArrayString<String> = array.get_node(1)?;
            array.get_strings()?
        };

        warn!(target: "TableHeader", "column_names: {:?}", column_names);

        let column_attributes = {
            let array: IntegerArray<ColumnAttributes> = array.get_node(2)?;
            array.get_integers_generic()
        };

        warn!(target: "TableHeader", "column_attributes: {:?}", column_attributes);

        let sub_spec_array = if array.node.header.size > 3 {
            Some(array.get_node(3)?)
        } else {
            None
        };

        Self::from_parts(
            column_types,
            column_names,
            column_attributes,
            sub_spec_array,
        )
    }
}

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Table {
    data_array: Array,
    header: TableHeader,
    data_columns: Vec<Vec<Option<Value>>>,
    data_rows: Vec<Option<Vec<Value>>>,
}

impl Build for Table {
    #[instrument(target = "Table")]
    fn build(array: Array) -> anyhow::Result<Self> {
        let header = {
            let array: Array = array.get_node(0)?;
            TableHeader::build(array)?
        };

        // 1. Each table array has two elements, the first element is the table spec, containing the column types, names, attributes, and so forth.
        // 2. The second element in the table array is the data! So use the table spec to decode the data; figuring out what the type is based on the columns read.
        // 3. In other words, `table_array[1][0]` is the data for the first column, `table_array[1][1]` for the second column, and so on.
        // let data_array: Array = array.get_node(1)?;
        // let data_array_len = data_array.node.header.size as usize;

        // assert!(
        //     data_array_len >= header.column_types.len(),
        //     "Data array length ({}) must be at least as long as the number of columns ({})",
        //     data_array_len,
        //     header.column_types.len()
        // );

        // let mut data = Vec::with_capacity(data_array_len);
        // for i in 0..data_array_len {
        //     let column_type = &header.column_types[i];
        //     let column_name = &header.column_names[i];
        //     let column_attribute = &header.column_attributes[i];

        //     data.push(Self::read_column(
        //         &data_array,
        //         i,
        //         column_type,
        //         column_name,
        //         column_attribute,
        //     )?);
        // }

        let data_columns = header.columns.iter().map(|_| vec![]).collect();

        let result = Self {
            data_array: array.get_node(1)?,
            header,
            data_columns,
            data_rows: vec![],
        };
        warn!(target: "Table", "data: {:?}", result);
        Ok(result)
    }
}

impl Table {
    #[instrument(target = "Table")]
    fn new_for_subtable(header: TableHeader, data_array: Array) -> Self {
        let data_columns = header.columns.iter().map(|_| vec![]).collect();

        Self {
            data_array,
            header,
            data_columns,
            data_rows: vec![],
        }
    }

    pub fn row_count(&self) -> anyhow::Result<usize> {
        let first_column = &self.header.columns[0];
        let first_column_type = first_column.as_column_type();

        match first_column_type {
            ColumnType::Int | ColumnType::Bool => {
                let array: IntegerArray<u64> = self.data_array.get_node(0)?;
                Ok(array.element_count())
            }
            ColumnType::String => {
                let array: ArrayString<String> = self.data_array.get_node(0)?;
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

    #[instrument(target = "Table")]
    pub fn get_row(&mut self, index: usize) -> anyhow::Result<&[Value]> {
        if self.data_rows.len() > index && self.data_rows[index].is_some() {
            return Ok(self.data_rows[index].as_ref().unwrap());
        }

        self.ensure_columns_loaded(index)?;

        let mut row = Vec::with_capacity(self.header.columns.len());
        for i in 0..self.header.columns.len() {
            let column_data = &self.data_columns[i][index];
            // TODO: Avoid this clone?
            row.push(column_data.clone().unwrap());
        }

        self.data_rows.resize(index + 1, None);
        self.data_rows[index] = Some(row);
        Ok(self.data_rows[index].as_ref().unwrap())
    }

    #[instrument(target = "Table")]
    fn ensure_columns_loaded(&mut self, row_index: usize) -> anyhow::Result<()> {
        for i in 0..self.header.columns.len() {
            self.ensure_column_loaded(i, row_index)?;
        }

        Ok(())
    }

    #[instrument(target = "Table")]
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

        let column_spec = &self.header.columns[column_index];
        self.data_columns[column_index][row_index] =
            Some(self.read_column_row(column_index, column_spec, row_index)?);

        debug!(
            target: "Table",
            "Loaded column {column_index} at row {row_index}: {:?}",
            self.data_columns[column_index][row_index]
        );

        Ok(())
    }

    #[instrument(target = "Table")]
    fn read_column_row(
        &self,
        column_index: usize,
        column_spec: &'_ ColumnSpec,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        let data_array_index = if column_index > 0 {
            column_index + 1
        } else {
            column_index
        };

        match column_spec {
            ColumnSpec::Regular {
                type_,
                name,
                attributes,
            } => self.read_column_row_regular(data_array_index, type_, name, attributes, row_index),
            ColumnSpec::BackLink {
                attributes,
                origin_table_index,
                origin_column_index,
            } => self.read_column_row_backlink(
                data_array_index,
                *origin_table_index,
                *origin_column_index,
                attributes,
                row_index,
            ),
        }
    }

    #[instrument(target = "Table")]
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

    #[instrument(target = "Table")]
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
                let array: IntegerArray<u64> = self.data_array.get_node(data_array_index)?;
                let value = array.get(row_index);
                Ok(Value::Int(value))
            }
            ThinColumnType::Bool => {
                let array: IntegerArray<u64> = self.data_array.get_node(data_array_index)?;
                let value = array.get(row_index);
                Ok(Value::Bool(value != 0))
            }
            ThinColumnType::String => {
                let array: ArrayString<String> = self.data_array.get_node(data_array_index)?;
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

    #[instrument(target = "Table")]
    fn read_column_row_table(
        &self,
        data_array_index: usize,
        table_header: &TableHeader,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        let array: Array = match self.data_array.get_ref(data_array_index) {
            Some(ref_) => Array::from_ref(self.data_array.node.realm.clone(), ref_)?,
            _ => return Ok(Value::None),
        };

        Ok(Value::Table(Table::new_for_subtable(
            table_header.clone(),
            array,
        )))
    }

    #[instrument(target = "Table")]
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

    #[instrument(target = "Table")]
    fn read_column_row_link_list(
        &self,
        data_array_index: usize,
        target_table_index: usize,
        name: &str,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        let array: ArrayLinkList = self.data_array.get_node(data_array_index)?;
        let value = array.get(row_index)?;

        Ok(match (value, attributes.is_nullable()) {
            (Some(value), _) => Value::LinkList(value),
            (_, false) => Value::LinkList(vec![]),
            (_, true) => Value::None,
        })
    }

    #[instrument(target = "Table")]
    fn read_column_row_backlink(
        &self,
        data_array_index: usize,
        origin_table_index: usize,
        origin_column_index: usize,
        attributes: &ColumnAttributes,
        row_index: usize,
    ) -> anyhow::Result<Value> {
        unimplemented!("backlink column at index {data_array_index}");
    }

    // #[instrument(target = "Table")]
    // fn read_column(
    //     data_array: &Array,
    //     i: usize,
    //     column_type: &ColumnType,
    //     column_name: &str,
    //     column_attribute: &ColumnAttributes,
    // ) -> anyhow::Result<Vec<Value>> {
    //     let data_array_index = i + 1;
    //     match column_type {
    //         ColumnType::Int => {
    //             let array: IntegerArray<u64> = data_array.get_node(data_array_index)?;
    //             let values = array.get_integers();
    //             Ok(values.into_iter().map(Value::Int).collect())
    //         }
    //         ColumnType::String => {
    //             let array: ArrayString<String> = data_array.get_node(data_array_index)?;
    //             let values = array.get_strings()?;
    //             Ok(values.into_iter().map(Value::String).collect())
    //         }
    //         _ => unimplemented!("column_type: {:?}", column_type),
    //     }
    // }
}
