use std::fmt::Debug;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::{Array, Expectation, IntegerArray, RealmRef};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};

#[derive(Debug, Clone)]
pub struct SmallBlobsArray {
    lengths: IntegerArray,
    blobs: RealmNode,
    null: Option<IntegerArray>,
}

impl Node for SmallBlobsArray {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        let size = array.node.header.size as usize;
        assert!(size >= 2, "SmallBlobsArray size must be at least 2");
        assert!(size <= 3, "SmallBlobsArray size must be at most 3");

        let lengths_array: IntegerArray = array.get_node(0)?.unwrap();
        let blobs: RealmNode = array.get_node(1)?.unwrap();
        let null_array: Option<IntegerArray> = if size == 3 { array.get_node(2)? } else { None };

        if let Some(nullable_array) = &null_array {
            assert!(lengths_array.element_count() == nullable_array.element_count());
        }

        Ok(Self {
            lengths: lengths_array,
            blobs,
            null: null_array,
        })
    }
}

impl SmallBlobsArray {
    pub fn element_count(&self) -> usize {
        self.lengths.element_count()
    }

    #[instrument(target = "SmallBlobsArray", level = "debug")]
    pub fn get(&self, index: usize, expectation: Expectation) -> Option<Vec<u8>> {
        if let Some(null_array) = &self.null {
            let is_null = null_array.get(index);
            assert!(
                is_null == 0 || is_null == 1,
                "Invalid null value: {is_null}"
            );
            if is_null == 0 {
                return match expectation {
                    Expectation::Nullable => None,
                    Expectation::NotNullable => Some(vec![]),
                };
            }
        }

        let begin = if index == 0 {
            0
        } else {
            self.lengths.get(index - 1) as usize
        };
        let end = self.lengths.get(index) as usize;

        assert!(
            end > begin,
            "Invalid blob length: end ({end}) <= begin ({begin})"
        );

        assert!(
            end <= self.blobs.payload().len(),
            "Blob end index out of bounds: {end} >= {}",
            self.blobs.payload().len()
        );

        Some(self.blobs.payload()[begin..end].to_vec())
    }
}
