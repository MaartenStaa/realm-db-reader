use std::fmt::Debug;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::{Array, RealmRef};
use crate::node::Node;
use crate::realm::Realm;

#[derive(Debug)]
pub struct LongBlobsArray {
    array: Array,
}

impl Node for LongBlobsArray {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        assert!(
            array.node.header.has_refs(),
            "LongBlobsArray must have references"
        );
        assert!(
            array.node.header.context_flag(),
            "LongBlobsArray must have context flag set"
        );

        Ok(Self { array })
    }
}

impl LongBlobsArray {
    #[instrument(target = "LongBlobsArray")]
    pub fn get(&self, index: usize) -> anyhow::Result<Option<Vec<u8>>> {
        let Some(ref_) = self.array.get_ref(index) else {
            warn!("get: index={index} returned NULL");
            return Ok(None);
        };

        let item: Array = self.array.get_node_at_ref(ref_)?;
        let payload = item.node.payload();
        let size = item.node.header.size as usize;

        if size == 0 {
            return Ok(None);
        }

        assert!(
            size <= payload.len(),
            "LongBlobsArray: size ({size}) is greater than payload length ({})",
            payload.len()
        );

        // The payload is owned by item.node, which is dropped at the end of this function.
        // Returning a reference to its data is invalid. Instead, return an owned Vec<u8>.
        Ok(Some(payload[..size].to_vec()))
    }
}
