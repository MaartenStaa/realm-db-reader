use crate::array::{IntegerArray, RealmRef};
use crate::column::{Column, ColumnImpl, ColumnType};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IntNullableColumnType;

impl ColumnType for IntNullableColumnType {
    type Value = Option<i64>;
    type LeafType = IntegerArray;
    type LeafContext = ();
}

// Factory function for integer columns
pub fn create_int_null_column(
    realm: Arc<Realm>,
    data_ref: RealmRef,
    index_ref: Option<RealmRef>,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(IntNullColumn::new(
        realm,
        data_ref,
        index_ref,
        attributes,
        Some(name),
        (),
    )?))
}

// Type alias for convenience
pub type IntNullColumn = ColumnImpl<IntNullableColumnType>;
