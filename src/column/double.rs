use std::sync::Arc;

use crate::array::{RealmRef, ScalarArray};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;

// Double column type implementation
#[derive(Debug, Clone)]
pub struct DoubleColumnType;

impl ColumnType for DoubleColumnType {
    type Value = f64;
    type LeafType = DoubleArrayLeaf;
    type LeafContext = ();
}

#[derive(Debug)]
pub struct DoubleArrayLeaf {
    array: ScalarArray<f64>,
}

impl NodeWithContext<()> for DoubleArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, context: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = ScalarArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<f64, ()> for DoubleArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<f64> {
        Ok(self.array.get(index))
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        context: (),
    ) -> anyhow::Result<f64> {
        todo!()
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Doubles are never null in Realm
    }

    fn size(&self) -> usize {
        self.array.node.header.size as usize
    }
}

// Factory function for Double columns
pub fn create_double_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(DoubleColumn::new(
        realm,
        data_ref,
        // Double columns are not indexed
        None,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type DoubleColumn = ColumnImpl<DoubleColumnType>;
