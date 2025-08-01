use crate::array::{
    Array, ArrayString, ArrayStringShort, Expectation, LongBlobsArray, RealmRef, SmallBlobsArray,
};
use crate::column::Column;
use crate::column::bptree::BpTreeNode;
use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use crate::table::ColumnAttributes;
use crate::value::Value;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StringColumn {
    root: Array,
    attributes: ColumnAttributes,
    name: String,
}

impl StringColumn {
    pub fn new(
        realm: Arc<Realm>,
        ref_: RealmRef,
        attributes: ColumnAttributes,
        name: String,
    ) -> anyhow::Result<Self> {
        Ok(StringColumn {
            root: Array::from_ref(realm, ref_)?,
            attributes,
            name,
        })
    }
}

impl Column for StringColumn {
    /// Get the value for this column for the row with the given index.
    fn get(&self, index: usize) -> anyhow::Result<Value> {
        if self.root_is_leaf() {
            let long_strings = self.root.node.header.has_refs();
            if !long_strings {
                return Ok(ArrayStringShort::<String>::get_static(
                    &self.root.node,
                    index,
                    if self.attributes.is_nullable() {
                        Expectation::Nullable
                    } else {
                        Expectation::NotNullable
                    },
                )
                .map(|s| s.to_owned())
                .unwrap_or_default()
                .into());
            }

            let is_big = self.root.node.header.context_flag();
            let bytes = if !is_big {
                // Medimum strings
                self.get_from_small_blob(self.root.node.ref_, index)?
            } else {
                self.get_from_long_blobs(self.root.node.ref_, index)?
            };

            return Ok(bytes.map(ArrayString::<String>::string_from_bytes).into());
        }

        // Non-leaf root
        let (leaf_ref, index_in_leaf) = BpTreeNode::new(&self.root).get_bptree_leaf(index)?;
        let leaf_node = RealmNode::from_ref(Arc::clone(&self.root.node.realm), leaf_ref)?;

        let long_strings = leaf_node.header.has_refs();
        if !long_strings {
            // Small strings
            return Ok(ArrayStringShort::<String>::get_static(
                &leaf_node,
                index_in_leaf,
                if self.attributes.is_nullable() {
                    Expectation::Nullable
                } else {
                    Expectation::NotNullable
                },
            )
            .map(|s| s.to_owned())
            .unwrap_or_default()
            .into());
        }
        let is_big = leaf_node.header.context_flag();
        let bytes = if !is_big {
            // Medimum strings
            self.get_from_small_blob(leaf_ref, index_in_leaf)?
        } else {
            self.get_from_long_blobs(leaf_ref, index_in_leaf)?
        };

        Ok(bytes.map(ArrayString::<String>::string_from_bytes).into())
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        Ok(self.nullable() && self.get(index)?.is_none())
    }

    /// Get the total number of values in this column.
    fn count(&self) -> anyhow::Result<usize> {
        if self.root_is_leaf() {
            let long_strings = self.root.node.header.has_refs();
            if !long_strings {
                return Ok(self.root.node.header.size as usize);
            }

            let is_big = self.root.node.header.context_flag();
            if !is_big {
                // Small strings
                return Ok(SmallBlobsArray::from_ref(
                    Arc::clone(&self.root.node.realm),
                    self.root.node.ref_,
                )?
                .element_count());
            }

            // Long strings
            return Ok(LongBlobsArray::from_ref(
                Arc::clone(&self.root.node.realm),
                self.root.node.ref_,
            )?
            .element_count());
        }

        // Non-leaf root
        Ok(BpTreeNode::new(&self.root).get_bptree_size())
    }

    /// Get whether this column is nullable.
    fn nullable(&self) -> bool {
        self.attributes.is_nullable()
    }

    fn is_indexed(&self) -> bool {
        self.attributes.is_indexed()
    }

    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }
}

impl StringColumn {
    fn root_is_leaf(&self) -> bool {
        !self.root.node.header.is_inner_bptree()
    }

    fn get_from_small_blob(&self, ref_: RealmRef, index: usize) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(
            SmallBlobsArray::from_ref(Arc::clone(&self.root.node.realm), ref_)?.get(
                index,
                if self.nullable() {
                    Expectation::Nullable
                } else {
                    Expectation::NotNullable
                },
            ),
        )
    }

    fn get_from_long_blobs(&self, ref_: RealmRef, index: usize) -> anyhow::Result<Option<Vec<u8>>> {
        LongBlobsArray::from_ref(Arc::clone(&self.root.node.realm), ref_)?.get(
            index,
            if self.nullable() {
                Expectation::Nullable
            } else {
                Expectation::NotNullable
            },
        )
    }
}

// Factory function for string columns
pub fn create_string_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<StringColumn>> {
    Ok(Box::new(StringColumn::new(realm, ref_, attributes, name)?))
}
