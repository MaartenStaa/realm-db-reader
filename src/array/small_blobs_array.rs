use std::fmt::Debug;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::{Array, ArrayBasic, RealmRef};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};

#[derive(Debug, Clone)]
pub struct SmallBlobsArray {
    lengths: Array<u64>,
    blobs: RealmNode,
    null: Option<Array<bool>>,
}

impl Node for SmallBlobsArray {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = ArrayBasic::from_ref(realm, ref_)?;

        let size = array.node.header.size as usize;
        assert!(size >= 2, "SmallBlobsArray size must be at least 2");
        assert!(size <= 3, "SmallBlobsArray size must be at most 3");

        let lengths_array: Array<u64> = array.get_node(0)?;
        let blobs: RealmNode = array.get_node(1)?;
        let null_array: Option<Array<bool>> = if size == 3 {
            Some(array.get_node(2)?)
        } else {
            None
        };

        if let Some(nullable_array) = &null_array {
            assert!(lengths_array.node.header.size == nullable_array.node.header.size);
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
        self.lengths.node.header.size as usize
    }

    #[instrument(target = "SmallBlobsArray")]
    pub fn get(&self, index: usize) -> Option<Vec<u8>> {
        if let Some(null_array) = &self.null {
            let is_null = null_array.get(index);
            assert!(
                is_null == 0 || is_null == 1,
                "Invalid null value: {is_null}"
            );
            if is_null == 1 {
                return None; // This blob is null
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
