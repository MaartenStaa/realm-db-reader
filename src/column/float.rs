use std::sync::Arc;

use crate::array::{RealmRef, ScalarArray};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;

// Float column type implementation
#[derive(Debug, Clone)]
pub struct FloatColumnType;

impl ColumnType for FloatColumnType {
    type Value = f32;
    type LeafType = FloatArrayLeaf;
    type LeafContext = ();
}

#[derive(Debug)]
pub struct FloatArrayLeaf {
    array: ScalarArray<f32>,
}

impl NodeWithContext<()> for FloatArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, context: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = ScalarArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<f32, ()> for FloatArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<f32> {
        Ok(self.array.get(index))
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        context: (),
    ) -> anyhow::Result<f32> {
        todo!()
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Floats are never null in Realm
    }

    fn size(&self) -> usize {
        self.array.node.header.size as usize
    }
}

// Factory function for float columns
pub fn create_float_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(FloatColumn::new(
        realm,
        data_ref,
        // Float columns are not indexed
        None,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type FloatColumn = ColumnImpl<FloatColumnType>;
