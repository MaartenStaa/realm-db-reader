use crate::array::{IntegerArray, RealmRef};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BoolNullableColumnType;

impl ColumnType for BoolNullableColumnType {
    type Value = Option<bool>;
    type LeafType = OptionalBoolArrayLeaf;
    type LeafContext = ();

    const IS_NULLABLE: bool = false;
}

#[derive(Debug)]
pub struct OptionalBoolArrayLeaf {
    array: IntegerArray,
}

impl NodeWithContext<()> for OptionalBoolArrayLeaf {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, _: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = IntegerArray::from_ref(realm, ref_)?;
        Ok(Self { array })
    }
}

impl ArrayLeaf<Option<bool>, ()> for OptionalBoolArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<Option<bool>> {
        let value = self.array.get(index + 1);
        if value == self.null_value() {
            return Ok(None);
        }

        Ok(Some(value == 1))
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
    ) -> anyhow::Result<Option<bool>> {
        let header = realm.header(ref_)?;
        let width = header.width();

        let value = read_array_value(realm.payload(ref_, header.payload_len()), width, index + 1);
        let null_value = read_array_value(realm.payload(ref_, header.payload_len()), width, 0);

        Ok(if value == null_value {
            None
        } else {
            Some(value == 1)
        })
    }
}

impl OptionalBoolArrayLeaf {
    fn null_value(&self) -> u64 {
        self.array.get(0)
    }
}

// Factory function for nullable bool columns
pub fn create_bool_null_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    index_ref: Option<RealmRef>,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(BoolNullColumn::new(
        realm,
        data_ref,
        index_ref,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type BoolNullColumn = ColumnImpl<BoolNullableColumnType>;
