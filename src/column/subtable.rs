use anyhow::bail;

use crate::array::{ArrayBasic, Expectation, LongBlobsArray, RealmRef, SmallBlobsArray};
use crate::column::Column;
use crate::node::Node;
use crate::realm::Realm;
use crate::table::{ColumnAttributes, TableHeader};
use crate::value::Value;
use std::sync::Arc;

#[derive(Debug)]
pub struct SubtableColumn {
    root: ArrayBasic,
    attributes: ColumnAttributes,
    header: TableHeader,
    name: String,
}

impl SubtableColumn {
    pub fn new(
        realm: Arc<Realm>,
        ref_: RealmRef,
        attributes: ColumnAttributes,
        header: TableHeader,
        name: String,
    ) -> anyhow::Result<Self> {
        Ok(SubtableColumn {
            root: unsafe { ArrayBasic::from_ref_bypass_bptree(realm, ref_)? },
            attributes,
            header,
            name,
        })
    }
}

impl Column for SubtableColumn {
    /// Get the value for this column for the row with the given index.
    fn get(&self, index: usize) -> anyhow::Result<Value> {
        bail!("todo: SubtableColumn::get not implemented");
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        todo!();
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
        !self.root.node.header.is_inner_bptree()
    }

    fn get_from_small_blob(&self, ref_: RealmRef, index: usize) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(
            SmallBlobsArray::from_ref(Arc::clone(&self.root.node.realm), ref_)?.get(
                index,
                if self.nullable() {
                    Expectation::Nullable
                } else {
                    Expectation::NotNullable
                },
            ),
        )
    }

    fn get_from_long_blobs(&self, ref_: RealmRef, index: usize) -> anyhow::Result<Option<Vec<u8>>> {
        LongBlobsArray::from_ref(Arc::clone(&self.root.node.realm), ref_)?.get(
            index,
            if self.nullable() {
                Expectation::Nullable
            } else {
                Expectation::NotNullable
            },
        )
    }
}

// Factory function for subtable columns
pub fn create_subtable_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    header: TableHeader,
    name: String,
) -> anyhow::Result<Box<SubtableColumn>> {
    Ok(Box::new(SubtableColumn::new(
        realm, ref_, attributes, header, name,
    )?))
}
