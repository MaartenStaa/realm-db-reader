use std::fmt::Debug;
use std::sync::Arc;

use tracing::instrument;

use crate::Realm;
use crate::realm::RealmNode;
use crate::traits::{ArrayLike, Node, NodeWithContext};
use crate::utils::read_array_value;

use super::RealmRef;

#[derive(Debug)]
pub(crate) struct ScalarArray {
    node: RealmNode,
}

impl NodeWithContext<()> for ScalarArray {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, _: ()) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node: RealmNode::from_ref(realm, ref_)?,
        })
    }
}

macro_rules! impl_scalar_bytewise {
    ($scalar:ty) => {
        impl ArrayLike<$scalar> for ScalarArray {
            #[instrument(level = "debug")]
            fn get(&self, index: usize) -> anyhow::Result<$scalar> {
                assert!(
                    index < self.node.header.size as usize,
                    "Index out of bounds: {index} >= {}",
                    self.node.header.size
                );

                let offset_start = index * std::mem::size_of::<$scalar>();
                let offset_end = offset_start + std::mem::size_of::<$scalar>();
                let bytes = &self.node.payload()[offset_start..offset_end];

                Ok(<$scalar>::from_le_bytes(bytes.try_into().unwrap()))
            }

            fn get_direct(
                realm: Arc<Realm>,
                ref_: RealmRef,
                index: usize,
                _: (),
            ) -> anyhow::Result<$scalar> {
                let header = realm.header(ref_)?;
                let payload = realm.payload(ref_, header.payload_len());
                let offset_start = index * std::mem::size_of::<$scalar>();
                let offset_end = offset_start + std::mem::size_of::<$scalar>();
                let bytes = &payload[offset_start..offset_end];

                Ok(<$scalar>::from_le_bytes(bytes.try_into().unwrap()))
            }

            fn is_null(&self, _: usize) -> anyhow::Result<bool> {
                Ok(false)
            }

            fn size(&self) -> usize {
                self.node.header.size as usize
            }
        }
    };
}

impl_scalar_bytewise!(f32);
impl_scalar_bytewise!(f64);

impl ArrayLike<bool> for ScalarArray {
    fn get(&self, index: usize) -> anyhow::Result<bool> {
        let value = read_array_value(self.node.payload(), self.node.header.width(), index);
        Ok(value != 0)
    }

    fn get_direct(realm: Arc<Realm>, ref_: RealmRef, index: usize, _: ()) -> anyhow::Result<bool> {
        let header = realm.header(ref_)?;
        let payload = realm.payload(ref_, header.payload_len());

        let value = read_array_value(payload, header.width(), index);
        Ok(value != 0)
    }

    fn is_null(&self, _: usize) -> anyhow::Result<bool> {
        Ok(false)
    }

    fn size(&self) -> usize {
        self.node.header.size as usize
    }
}

impl ArrayLike<Option<bool>> for ScalarArray {
    fn get(&self, index: usize) -> anyhow::Result<Option<bool>> {
        let value = read_array_value(self.node.payload(), self.node.header.width(), index + 1);
        let null_value = read_array_value(self.node.payload(), self.node.header.width(), 0);

        Ok(if value == null_value {
            None
        } else {
            Some(value != 0)
        })
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        _: (),
    ) -> anyhow::Result<Option<bool>> {
        let header = realm.header(ref_)?;
        let payload = realm.payload(ref_, header.payload_len());

        let value = read_array_value(payload, header.width(), index + 1);
        let null_value = read_array_value(payload, header.width(), 0);

        Ok(if value == null_value {
            None
        } else {
            Some(value != 0)
        })
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        let value = read_array_value(self.node.payload(), self.node.header.width(), index + 1);
        let null_value = read_array_value(self.node.payload(), self.node.header.width(), 0);

        Ok(value == null_value)
    }

    fn size(&self) -> usize {
        self.node.header.size as usize
    }
}
