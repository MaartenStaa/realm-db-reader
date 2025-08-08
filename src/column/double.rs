use std::sync::Arc;

use crate::array::{RealmRef, ScalarArray};
use crate::column::{Column, ColumnImpl, ColumnType};
use crate::realm::Realm;
use crate::table::ColumnAttributes;

// Double column type implementation
#[derive(Debug, Clone)]
pub struct DoubleColumnType;

impl ColumnType for DoubleColumnType {
    type Value = f64;
    type LeafType = ScalarArray;
    type LeafContext = ();
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
