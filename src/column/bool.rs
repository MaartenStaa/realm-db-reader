use crate::array::{IntegerArray, RealmRef};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

// Boolean column type implementation
#[derive(Debug, Clone)]
pub struct BoolColumnType;

impl ColumnType for BoolColumnType {
    type Value = bool;
    type LeafType = BoolArrayLeaf;
    type LeafContext = ();
}

// Boolean leaf implementation - wraps Array<bool>
#[derive(Debug)]
pub struct BoolArrayLeaf {
    array: IntegerArray,
}

impl NodeWithContext<()> for BoolArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, _: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = IntegerArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<bool, ()> for BoolArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<bool> {
        Ok(self.array.get(index) != 0)
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Booleans are never null in Realm
    }

    fn size(&self) -> usize {
        self.array.element_count()
    }

    fn get_direct(realm: Arc<Realm>, ref_: RealmRef, index: usize, _: ()) -> anyhow::Result<bool> {
        let header = realm.header(ref_)?;
        let width = header.width();

        let value = read_array_value(realm.payload(ref_, header.payload_len()), width, index);
        Ok(value != 0)
    }
}

// Factory function for boolean columns
pub fn create_bool_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    index_ref: Option<RealmRef>,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(BoolColumn::new(
        realm,
        data_ref,
        index_ref,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type BoolColumn = ColumnImpl<BoolColumnType>;
