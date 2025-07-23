use anyhow::{anyhow, bail};
use log::warn;
use tracing::instrument;

use crate::array::{ArrayBasic, ArrayString, Expectation, IntegerArray, RefOrTaggedValue};
use crate::build::Build;
use crate::spec::ColumnType;
use crate::table::column::ColumnAttributes;
use crate::table::spec::{ColumnSpec, FatColumnType};

#[derive(Debug, Clone)]
pub struct TableHeader {
    columns: Vec<ColumnSpec>,
}

impl TableHeader {
    #[instrument(target = "TableHeader", level = "debug")]
    fn from_parts(
        column_types: Vec<ColumnType>,
        mut column_names: Vec<String>,
        column_attributes: Vec<ColumnAttributes>,
        sub_spec_array: Option<ArrayBasic>,
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
        let mut data_array_index = 0;
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
                        data_array_index,
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
                        data_array_index,
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
                        data_array_index,
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
                        data_array_index,
                        type_,
                        name,
                        attributes,
                    }
                }
            };

            data_array_index += 1;
            if column_attributes[i].is_indexed() {
                // Indexed columns have an additional data array, so we need to increment the data
                // index. In other words, for column with data index N, with attribute is_indexed,
                // there's an index entry at N+1 in the data array.
                data_array_index += 1;
            }

            warn!(target: "Table", "column spec {i}: {spec:?}");
            columns.push(spec);
        }

        Ok(Self { columns })
    }

    fn get_sub_spec_index_value(
        sub_spec_array: &ArrayBasic,
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

    pub(crate) fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn get_columns(&self) -> &[ColumnSpec] {
        &self.columns
    }

    pub(crate) fn get_column(&self, index: usize) -> anyhow::Result<&ColumnSpec> {
        self.columns
            .get(index)
            .ok_or_else(|| anyhow::anyhow!("No column at index {index}"))
    }
}

impl Build for TableHeader {
    #[instrument(target = "TableHeader", level = "debug")]
    fn build(array: ArrayBasic) -> anyhow::Result<Self> {
        let column_types = {
            let array: IntegerArray<ColumnType> = array.get_node(0)?;
            array.get_integers_generic()
        };

        warn!(target: "TableHeader", "column_types: {:?}", column_types);

        let column_names = {
            let array: ArrayString<String> = array.get_node(1)?;
            array.get_strings(Expectation::NotNullable)?
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
