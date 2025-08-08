use log::warn;
use tracing::instrument;

use crate::array::{Array, ArrayStringShort};
use crate::table::Table;
use crate::traits::ArrayLike;

#[derive(Debug)]
pub struct Group {
    tables_array: Array,
    table_names: Vec<String>,
}

impl Group {
    #[instrument(target = "Group", level = "debug")]
    pub fn build(array: Array) -> anyhow::Result<Self> {
        let table_names = {
            let array: ArrayStringShort = array.get_node(0)?.unwrap();
            array.get_all()?
        };

        let tables_array = array.get_node(1)?.unwrap();

        Ok(Self {
            tables_array,
            table_names,
        })
    }
}

impl Group {
    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table(&self, table_number: usize) -> anyhow::Result<Table> {
        let table_array = self.tables_array.get_node(table_number)?.unwrap();

        let table = Table::build(table_array, table_number)?;

        Ok(table)
    }

    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table_by_name(&self, name: &str) -> anyhow::Result<Table> {
        let table_number = self
            .table_names
            .iter()
            .position(|n| n == name)
            .ok_or(anyhow::anyhow!("No table with name {name}"))?;

        self.get_table(table_number)
    }

    pub fn table_count(&self) -> usize {
        self.table_names.len()
    }

    pub fn get_table_name(&self, index: usize) -> &str {
        &self.table_names[index]
    }

    pub fn get_table_names(&self) -> &[String] {
        &self.table_names
    }
}
