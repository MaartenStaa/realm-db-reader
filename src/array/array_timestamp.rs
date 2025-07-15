use std::sync::Arc;

use chrono::{DateTime, Utc};
use tracing::instrument;

use crate::{
    array::{Array, RealmRef},
    node::Node,
    realm::Realm,
};

#[derive(Debug)]
pub struct ArrayTimestamp {
    seconds: Array,
    nanoseconds: Array,
}

impl Node for ArrayTimestamp {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;
        let seconds = array.get_node(0)?;
        let nanoseconds = array.get_node(1)?;

        Ok(Self {
            seconds,
            nanoseconds,
        })
    }
}

impl ArrayTimestamp {
    #[instrument(target = "ArrayTimestamp")]
    pub fn get(&self, index: usize) -> anyhow::Result<Option<DateTime<Utc>>> {
        let seconds = self.seconds.get(index);
        if seconds == 0 {
            return Ok(None);
        }

        let nanoseconds = self.nanoseconds.get(index);

        Ok(DateTime::from_timestamp(seconds as i64, nanoseconds as u32))
    }
}
