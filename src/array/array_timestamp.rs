use std::sync::Arc;

use chrono::{DateTime, Utc};
use tracing::instrument;

use crate::{
    array::{Array, ArrayBasic, RealmRef},
    node::Node,
    realm::Realm,
};

#[derive(Debug, Clone)]
pub struct ArrayTimestamp {
    seconds: Array<u64>,
    nanoseconds: Array<u32>,
}

impl Node for ArrayTimestamp {
    #[instrument(target = "ArrayTimestamp", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = ArrayBasic::from_ref(realm, ref_)?;

        assert_eq!(
            array.node.header.size, 2,
            "ArrayTimestamp size must be equal to least 2"
        );

        let seconds = array.get_node(0)?;
        let nanoseconds = array.get_node(1)?;

        Ok(Self {
            seconds,
            nanoseconds,
        })
    }
}

impl ArrayTimestamp {
    pub fn element_count(&self) -> usize {
        self.seconds.node.header.size as usize
    }

    #[instrument(target = "ArrayTimestamp", level = "debug")]
    pub fn get(&self, index: usize) -> anyhow::Result<Option<DateTime<Utc>>> {
        let seconds = self.seconds.get_integer(index)?;
        if seconds == 0 {
            return Ok(None);
        }

        let seconds = i64::from_le_bytes(seconds.to_le_bytes());
        let nanoseconds = self.nanoseconds.get_integer(index)?;

        Ok(DateTime::from_timestamp(seconds, nanoseconds))
    }
}
