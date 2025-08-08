use crate::array::{RealmRef, ScalarArray};
use crate::column::{Column, ColumnImpl, ColumnType};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BoolNullableColumnType;

impl ColumnType for BoolNullableColumnType {
    type Value = Option<bool>;
    type LeafType = ScalarArray;
    type LeafContext = ();
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
