use crate::array::{IntegerArray, RealmRef};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IntNullableColumnType;

impl ColumnType for IntNullableColumnType {
    type Value = Option<i64>;
    type LeafType = OptionalIntegerArrayLeaf;
    type LeafContext = ();

    const IS_NULLABLE: bool = false;
}

#[derive(Debug)]
pub struct OptionalIntegerArrayLeaf {
    array: IntegerArray,
}

impl NodeWithContext<()> for OptionalIntegerArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, _: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = IntegerArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<Option<i64>, ()> for OptionalIntegerArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<Option<i64>> {
        let value = self.array.get(index + 1);
        if value == self.null_value() {
            return Ok(None);
        }

        Ok(Some(i64::from_le_bytes(value.to_le_bytes())))
    }

    fn is_null(&self, index: usize) -> bool {
        self.array.get(index + 1) == self.null_value()
    }

    fn size(&self) -> usize {
        // Delegate to the underlying IntegerArray
        self.array.element_count()
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        _: (),
    ) -> anyhow::Result<Option<i64>> {
        let header = realm.header(ref_)?;
        let width = header.width();

        let value = read_array_value(realm.payload(ref_, header.payload_len()), width, index + 1);
        let null_value = read_array_value(realm.payload(ref_, header.payload_len()), width, 0);

        Ok(if value == null_value {
            None
        } else {
            Some(i64::from_le_bytes(value.to_le_bytes()))
        })
    }
}

impl OptionalIntegerArrayLeaf {
    fn null_value(&self) -> u64 {
        self.array.get(0)
    }
}

// Factory function for integer columns
pub fn create_int_null_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(IntNullColumn::new(
        realm,
        ref_,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type IntNullColumn = ColumnImpl<IntNullableColumnType>;
