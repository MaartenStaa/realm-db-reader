use crate::array::{RealmRef, ScalarArray};
use crate::column::{Column, ColumnImpl, ColumnType};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use std::sync::Arc;

// Boolean column type implementation
#[derive(Debug, Clone)]
pub struct BoolColumnType;

impl ColumnType for BoolColumnType {
    type Value = bool;
    type LeafType = ScalarArray;
    type LeafContext = ();
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
