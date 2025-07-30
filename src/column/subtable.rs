use anyhow::bail;

use crate::array::{Array, RealmRef};
use crate::column::Column;
use crate::node::Node;
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::value::Value;
use std::sync::Arc;

#[derive(Debug)]
pub struct SubtableColumn {
    header_root: Array,
    data_root: Array,
    attributes: ColumnAttributes,
    // header: TableHeader,
    name: String,
}

impl SubtableColumn {
    pub fn new(
        realm: Arc<Realm>,
        header_ref: RealmRef,
        data_ref: RealmRef,
        attributes: ColumnAttributes,
        // header: TableHeader,
        name: String,
    ) -> anyhow::Result<Self> {
        let header_root = Array::from_ref(Arc::clone(&realm), header_ref)?;
        let data_root = Array::from_ref(realm, data_ref)?;

        // B+Tree subtables not yet implemented
        assert!(!data_root.node.header.is_inner_bptree());

        Ok(SubtableColumn {
            header_root,
            data_root,
            attributes,
            // header,
            name,
        })
    }
}

impl Column for SubtableColumn {
    /// Get the value for this column for the row with the given index.
    fn get(&self, index: usize) -> anyhow::Result<Value> {
        let Some(array): Option<Array> = self.data_root.get_node(index)? else {
            return Ok(Value::None);
        };

        dbg!(&array);
        todo!();
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        Ok(self.data_root.get_ref(index).is_none())
    }

    /// Get the total number of values in this column.
    fn count(&self) -> anyhow::Result<usize> {
        bail!("todo: SubtableColumn::count not implemented");
    }

    /// Get whether this column is nullable.
    fn nullable(&self) -> bool {
        self.attributes.is_nullable()
    }

    fn is_indexed(&self) -> bool {
        self.attributes.is_indexed()
    }

    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }
}

impl SubtableColumn {
    fn root_is_leaf(&self) -> bool {
        !self.data_root.node.header.is_inner_bptree()
    }
}

// Factory function for subtable columns
pub fn create_subtable_column(
    realm: Arc<Realm>,
    header_ref: RealmRef,
    data_ref: RealmRef,
    attributes: ColumnAttributes,
    // header: TableHeader,
    name: String,
) -> anyhow::Result<Box<SubtableColumn>> {
    Ok(Box::new(SubtableColumn::new(
        realm, header_ref, data_ref, attributes, name,
    )?))
}
