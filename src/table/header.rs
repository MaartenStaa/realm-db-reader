use std::fmt::Debug;
use std::sync::Arc;

use anyhow::{anyhow, bail};
use log::{info, warn};
use tracing::instrument;

use crate::array::{Array, ArrayStringShort, Expectation, FromU64, IntegerArray, RefOrTaggedValue};
use crate::column::{
    Column, create_backlink_column, create_bool_column, create_int_column, create_linklist_column,
    create_string_column, create_subtable_column, create_timestamp_column,
};
use crate::spec::ColumnType;
use crate::table::column::ColumnAttributes;

#[derive(Debug)]
pub struct TableHeader {
    columns: Vec<Box<dyn Column>>,
}

impl TableHeader {
    #[instrument(target = "TableHeader", level = "debug")]
    fn from_parts(
        data_array: &Array,
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
        let mut data_array_index = 0;
        let mut sub_spec_index = 0;

        // Reverse the column names so we can do a low-cost pop for each column that has a name.
        column_names.reverse();

        for (i, column_type) in column_types.into_iter().enumerate() {
            let attributes = column_attributes[i];
            let data_ref = data_array
                .get_ref(data_array_index)
                .ok_or_else(|| anyhow!("failed to find data entry for column {i}"))?;

            log::debug!(target: "TableHeader", "column type {i}: {column_type:?} has data array index {data_array_index} with ref {data_ref:?}");

            let index_ref = if attributes.is_indexed() {
                Some(
                    data_array
                        .get_ref(data_array_index + 1)
                        .ok_or_else(|| anyhow!("failed to find index entry for column {i}"))?,
                )
            } else {
                None
            };

            let column = match column_type {
                ColumnType::Int => {
                    if attributes.is_nullable() {
                        create_int_null_column(
                            Arc::clone(&data_array.node.realm),
                            data_ref,
                            index_ref,
                            attributes,
                            column_names.pop().unwrap(),
                        )?
                    } else {
                        create_int_column(
                            Arc::clone(&data_array.node.realm),
                            data_ref,
                            index_ref,
                            attributes,
                            column_names.pop().unwrap(),
                        )?
                    }
                }
                ColumnType::Bool => {
                    if attributes.is_nullable() {
                        create_bool_null_column(
                            Arc::clone(&data_array.node.realm),
                            data_ref,
                            index_ref,
                            attributes,
                            column_names.pop().unwrap(),
                        )?
                    } else {
                        create_bool_column(
                            Arc::clone(&data_array.node.realm),
                            data_ref,
                            index_ref,
                            attributes,
                            column_names.pop().unwrap(),
                        )?
                    }
                }
                ColumnType::String => create_string_column(
                    Arc::clone(&data_array.node.realm),
                    data_ref,
                    index_ref,
                    attributes,
                    column_names.pop().unwrap(),
                )?,
                ColumnType::Timestamp => create_timestamp_column(
                    Arc::clone(&data_array.node.realm),
                    data_ref,
                    index_ref,
                    attributes,
                    column_names.pop().unwrap(),
                )?,
                ColumnType::LinkList => {
                    let target_table_index = Self::get_sub_spec_index_value(
                        sub_spec_array
                            .as_ref()
                            .ok_or(anyhow::anyhow!("Expected sub-spec array for link column"))?,
                        sub_spec_index,
                    )?;
                    sub_spec_index += 1;

                    create_linklist_column(
                        Arc::clone(&data_array.node.realm),
                        data_ref,
                        attributes,
                        target_table_index,
                        column_names.pop().unwrap(),
                    )?
                }
                ColumnType::Table => {
                    let other_table_header_ref = sub_spec_array
                        .as_ref()
                        .ok_or(anyhow::anyhow!("Expected sub-spec array for table column"))?
                        .get_ref(sub_spec_index)
                        .unwrap();
                    sub_spec_index += 1;
                    let name = column_names.pop().unwrap();

                    create_subtable_column(
                        Arc::clone(&data_array.node.realm),
                        other_table_header_ref,
                        data_ref,
                        attributes,
                        name,
                    )?
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
                    create_backlink_column(
                        Arc::clone(&data_array.node.realm),
                        data_ref,
                        attributes,
                        target_table_index,
                        target_table_column_index,
                    )?
                }
                _ => {
                    bail!("Unsupported column type: {column_type:?}");
                }
            };

            log::info!(target: "TableHeader", "Created column {column:?}");

            columns.push(column);

            data_array_index += 1;
            if attributes.is_indexed() {
                // Indexed columns have an additional data array, so we need to increment the data
                // index. In other words, for column with data index N, with attribute is_indexed,
                // there's an index entry at N+1 in the data array.
                data_array_index += 1;
            }
        }

        Ok(Self { columns })

        // for (i, column_type) in column_types.into_iter().enumerate() {
        //     let spec = match column_type {
        //         ct @ (ColumnType::Link | ColumnType::LinkList) => {
        //             let target_table = Self::get_sub_spec_index_value(
        //                 sub_spec_array
        //                     .as_ref()
        //                     .ok_or(anyhow::anyhow!("Expected sub-spec array for link column"))?,
        //                 sub_spec_index,
        //             )?;
        //             sub_spec_index += 1;
        //             let name = column_names
        //                 .pop()
        //                 .ok_or(anyhow!("Expected column name for column index {i}"))?;
        //             ColumnSpec::Regular {
        //                 type_: if ct == ColumnType::Link {
        //                     FatColumnType::Link {
        //                         target_table_index: target_table,
        //                     }
        //                 } else {
        //                     FatColumnType::LinkList {
        //                         target_table_index: target_table,
        //                     }
        //                 },
        //                 data_array_index,
        //                 name,
        //                 attributes: column_attributes[i],
        //             }
        //         }
        //         other => {
        //             let name = column_names.pop().ok_or(anyhow::anyhow!(
        //                 "Expected column name for column index {i} (type {other:?})"
        //             ))?;
        //             let attributes = column_attributes[i];
        //             let type_ = FatColumnType::Thin(other.as_thin_column_type()?);
        //             ColumnSpec::Regular {
        //                 data_array_index,
        //                 type_,
        //                 name,
        //                 attributes,
        //             }
        //         }
        //     };
        //
        //     warn!(target: "Table", "column spec {i}: {spec:?}");
    }

    fn get_sub_spec_index_value(
        sub_spec_array: &Array,
        sub_spec_index: usize,
    ) -> anyhow::Result<usize> {
        match sub_spec_array.get_ref_or_tagged_value(sub_spec_index) {
            Some(RefOrTaggedValue::Ref(_)) => bail!("Expected tagged integer for link column"),
            Some(RefOrTaggedValue::TaggedValue(target_table_index)) => {
                Ok(target_table_index as usize)
            }
            _ => bail!("Expected tagged integer for link column"),
        }
    }

    pub(crate) fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn get_columns(&self) -> &[Box<dyn Column>] {
        &self.columns
    }

    pub(crate) fn get_column(&self, index: usize) -> anyhow::Result<&dyn Column> {
        self.columns
            .get(index)
            .map(|c| c.as_ref())
            .ok_or_else(|| anyhow::anyhow!("No column at index {index}"))
    }
}

impl TableHeader {
    #[instrument(target = "TableHeader", level = "debug")]
    pub(crate) fn build(header_array: &Array, data_array: &Array) -> anyhow::Result<Self> {
        let column_types = {
            let array: IntegerArray = header_array.get_node(0)?.unwrap();
            array
                .get_integers()
                .into_iter()
                .map(ColumnType::from_u64)
                .collect::<Vec<_>>()
        };

        info!(target: "TableHeader", "column_types: {:?}", column_types);

        let column_names = {
            let array: ArrayStringShort<String> = header_array.get_node(1)?.unwrap();
            array.get_strings(Expectation::NotNullable)
        };

        info!(target: "TableHeader", "column_names: {:?}", column_names);

        let column_attributes = {
            let array: IntegerArray = header_array.get_node(2)?.unwrap();
            array
                .get_integers()
                .into_iter()
                .map(ColumnAttributes::from_u64)
                .collect::<Vec<_>>()
        };

        info!(target: "TableHeader", "column_attributes: {:?}", column_attributes);

        let sub_spec_array = if header_array.node.header.size > 3 {
            header_array.get_node(3)?
        } else {
            None
        };

        Self::from_parts(
            data_array,
            column_types,
            column_names,
            column_attributes,
            sub_spec_array,
        )
    }
}
