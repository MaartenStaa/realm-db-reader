use crate::array::{IntegerArray, RealmRef};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

// Integer column type implementation
#[derive(Debug, Clone)]
pub struct IntColumnType;

impl ColumnType for IntColumnType {
    type Value = i64;
    type LeafType = IntegerArrayLeaf;
    type LeafContext = ();

    const IS_NULLABLE: bool = false;
}

// Integer leaf implementation - wraps IntegerArray
// This shows how integer columns delegate to the underlying IntegerArray class
#[derive(Debug)]
pub struct IntegerArrayLeaf {
    array: IntegerArray,
}

impl NodeWithContext<()> for IntegerArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, _: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = IntegerArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<i64, ()> for IntegerArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<i64> {
        // Delegate to the underlying IntegerArray
        Ok(self.array.get(index) as i64)
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Integers are never null in Realm
    }

    fn size(&self) -> usize {
        // Delegate to the underlying IntegerArray
        self.array.element_count()
    }

    fn get_direct(realm: Arc<Realm>, ref_: RealmRef, index: usize, _: ()) -> anyhow::Result<i64> {
        let header = realm.header(ref_)?;
        let width = header.width();

        let value = read_array_value(realm.payload(ref_, header.payload_len()), width, index);
        Ok(i64::from_le_bytes(value.to_le_bytes()))
    }
}

// Factory function for integer columns
pub fn create_int_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    index_ref: Option<RealmRef>,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(IntColumn::new(
        realm,
        data_ref,
        index_ref,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type IntColumn = ColumnImpl<IntColumnType>;
