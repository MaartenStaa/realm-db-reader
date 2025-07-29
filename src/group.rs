use log::warn;
use tracing::instrument;

use crate::array::{ArrayBasic, ArrayStringShort, Expectation};
use crate::table::Table;

#[derive(Debug)]
pub struct Group {
    tables_array: ArrayBasic,
    table_names: Vec<String>,
    tables: Vec<Option<Table>>,
}

impl Group {
    #[instrument(target = "Group", level = "debug")]
    pub fn build(array: ArrayBasic) -> anyhow::Result<Self> {
        let table_names = {
            let array: ArrayStringShort<String> = array.get_node(0)?;
            array.get_strings(Expectation::NotNullable)
        };

        let tables = table_names.iter().map(|_| None).collect();
        let tables_array = array.get_node(1)?;

        Ok(Self {
            // group_array: array,
            tables_array,
            table_names,
            tables,
        })
    }
}

impl Group {
    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table(&mut self, index: usize) -> anyhow::Result<&Table> {
        Ok(&*self.get_or_load_table(index)?)
    }

    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table_by_name(&mut self, name: &str) -> anyhow::Result<&Table> {
        let index = self
            .table_names
            .iter()
            .position(|n| n == name)
            .ok_or(anyhow::anyhow!("No table with name {name}"))?;

        Ok(&*self.get_or_load_table(index)?)
    }

    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table_mut(&mut self, index: usize) -> anyhow::Result<&mut Table> {
        self.get_or_load_table(index)
    }

    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    pub fn get_table_by_name_mut(&mut self, name: &str) -> anyhow::Result<&mut Table> {
        let index = self
            .table_names
            .iter()
            .position(|n| n == name)
            .ok_or(anyhow::anyhow!("No table with name {name}"))?;

        self.get_or_load_table(index)
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

    #[instrument(target = "Group", level = "debug", skip(self), fields(table_names = ?self.table_names))]
    fn get_or_load_table(&mut self, index: usize) -> anyhow::Result<&mut Table> {
        if self.tables[index].is_some() {
            return Ok(self.tables[index].as_mut().unwrap());
        }

        let table_array = self.tables_array.get_node(index)?;

        let table = Table::build(table_array, index)?;
        self.tables[index] = Some(table);

        Ok(self.tables[index].as_mut().unwrap())
    }
}
