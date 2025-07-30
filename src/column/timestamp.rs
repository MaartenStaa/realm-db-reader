use crate::array::{Array, RealmRef};
use crate::column::integer::IntColumnType;
use crate::column::integer_optional::IntNullableColumnType;
use crate::column::{BpTree, Column};
use crate::node::Node;
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::value::Value;
use chrono::DateTime;
use std::sync::Arc;

#[derive(Debug)]
pub struct TimestampColumn {
    seconds: BpTree<IntNullableColumnType>,
    nanoseconds: BpTree<IntColumnType>,
    attributes: ColumnAttributes,
    name: String,
}

impl TimestampColumn {
    pub fn new(
        realm: Arc<Realm>,
        ref_: RealmRef,
        attributes: ColumnAttributes,
        name: String,
    ) -> anyhow::Result<Self> {
        let array = Array::from_ref(Arc::clone(&realm), ref_)?;
        let seconds = array.get_node(0)?.unwrap();
        let nanoseconds = array.get_node(1)?.unwrap();

        Ok(Self {
            seconds,
            nanoseconds,
            attributes,
            name,
        })
    }
}

impl Column for TimestampColumn {
    fn get(&self, index: usize) -> anyhow::Result<Value> {
        // Get seconds value
        let seconds = match self.seconds.get(index)? {
            Some(seconds) => seconds,
            None => return Ok(Value::None),
        };

        // If seconds is 0, the timestamp is null
        if seconds == 0 {
            return Ok(Value::Timestamp(Default::default()));
        }

        // Get nanoseconds value
        let nanoseconds = self.nanoseconds.get(index)?;

        // Convert to DateTime
        let seconds = i64::from_le_bytes(seconds.to_le_bytes());
        Ok(DateTime::from_timestamp(seconds, nanoseconds as u32).into())
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        self.seconds.is_null(index)
    }

    fn count(&self) -> anyhow::Result<usize> {
        self.seconds.count()
    }

    fn nullable(&self) -> bool {
        self.attributes.is_nullable()
    }

    fn is_indexed(&self) -> bool {
        todo!()
    }

    fn name(&self) -> Option<&str> {
        Some(&self.name)
    }
}

// Factory function for timestamp columns
pub fn create_timestamp_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    name: String,
) -> anyhow::Result<Box<TimestampColumn>> {
    Ok(Box::new(TimestampColumn::new(
        realm, ref_, attributes, name,
    )?))
}
