use log::warn;
use tracing::instrument;

use crate::array::{Array, ArrayStringShort};
use crate::build::Build;
use crate::table::Table;

// #[derive(Debug)]
// #[allow(unused)]
// pub struct Group {
//     tables: HashMap<String, Table>,
// }

// impl Build<'_> for Group {
//     #[instrument(target = "Group")]
//     fn build(array: Array) -> anyhow::Result<Self> {
//         let table_names = {
//             let array: ArrayStringShort<String> = array.get_node(0)?;
//             array.get_strings()
//         };

//         warn!(target: "Group", "table_names: {:?}", table_names);

//         let table_refs = {
//             let array: GenericArray<Table> = array.get_node(1)?;
//             array.get_elements()?
//         };

//         warn!(target: "Group", "table_refs: {:?}", table_refs);

//         assert_eq!(
//             table_names.len(),
//             table_refs.len(),
//             "table_names and table_refs must have the same length"
//         );

//         Ok(Self {
//             tables: table_names.into_iter().zip(table_refs).collect(),
//         })
//     }
// }

#[derive(Debug)]
pub struct Group {
    // group_array: Array<'a>,
    tables_array: Array,
    table_names: Vec<String>,
    tables: Vec<Option<Table>>,
}

impl Build for Group {
    #[instrument(target = "Group")]
    fn build(array: Array) -> anyhow::Result<Self> {
        let table_names = {
            let array: ArrayStringShort<String> = array.get_node(0)?;
            array.get_strings()
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
    #[instrument(target = "Group")]
    pub fn get_table(&mut self, index: usize) -> anyhow::Result<&Table> {
        Ok(&*self.get_or_load_table(index)?)
    }

    #[instrument(target = "Group")]
    pub fn get_table_by_name(&mut self, name: &str) -> anyhow::Result<&Table> {
        let index = self
            .table_names
            .iter()
            .position(|n| n == name)
            .ok_or(anyhow::anyhow!("No table with name {name}"))?;

        Ok(&*self.get_or_load_table(index)?)
    }

    #[instrument(target = "Group")]
    pub fn get_table_mut(&mut self, index: usize) -> anyhow::Result<&mut Table> {
        self.get_or_load_table(index)
    }

    #[instrument(target = "Group")]
    pub fn get_table_by_name_mut(&mut self, name: &str) -> anyhow::Result<&mut Table> {
        let index = self
            .table_names
            .iter()
            .position(|n| n == name)
            .ok_or(anyhow::anyhow!("No table with name {name}"))?;

        self.get_or_load_table(index)
    }

    #[instrument(target = "Group")]
    fn get_or_load_table(&mut self, index: usize) -> anyhow::Result<&mut Table> {
        if self.tables[index].is_some() {
            return Ok(self.tables[index].as_mut().unwrap());
        }

        let table_array = self.tables_array.get_node(index)?;

        let table = Table::build(table_array)?;
        self.tables[index] = Some(table);

        Ok(self.tables[index].as_mut().unwrap())
    }
}
