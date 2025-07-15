use std::fmt::Debug;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::{Array, RealmRef};
use crate::node::Node;
use crate::realm::Realm;

#[derive(Debug)]
pub struct SmallBlobsArray {
    array: Array,
    lengths: Array,
    blobs: Array,
    null: Option<Array>,
}

impl Node for SmallBlobsArray {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        let size = array.node.header.size as usize;
        assert!(size >= 2);
        assert!(size <= 3);

        let lengths_array: Array = array.get_node(0)?;
        let blobs_array: Array = array.get_node(1)?;
        let null_array: Option<Array> = if size == 3 {
            Some(array.get_node(2)?)
        } else {
            None
        };

        if let Some(nullable_array) = &null_array {
            assert!(lengths_array.node.header.size == nullable_array.node.header.size);
        }

        Ok(Self {
            array,
            lengths: lengths_array,
            blobs: blobs_array,
            null: null_array,
        })
    }
}

impl SmallBlobsArray {
    pub fn blobs_count(&self) -> usize {
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

        warn!(target: "SmallBlobsArray", "get: index={index} begin={begin} end={end}");

        // if end >= self.blobs.node.payload.len() {
        //     warn!(
        //         "Blob end index out of bounds: {end} >= {}",
        //         self.blobs.node.payload.len()
        //     );
        //     // FIXME!
        //     return None; // Out of bounds
        // }

        // if end - 1 <= begin {
        //     warn!("Invalid blob length: end ({end}) <= begin ({begin})");
        //     // FIXME!
        //     return None; // Invalid length
        // }

        assert!(
            end > begin,
            "Invalid blob length: end ({end}) <= begin ({begin})"
        );

        assert!(
            end <= self.blobs.node.payload().len(),
            "Blob end index out of bounds: {end} >= {}",
            self.blobs.node.payload().len()
        );

        Some(self.blobs.node.payload()[begin..end].to_vec())
    }
}
