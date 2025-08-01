use std::fmt::Debug;
use std::marker::PhantomData;

use tracing::instrument;

use crate::Realm;
use crate::node::Node;
use crate::realm::RealmNode;

use super::RealmRef;

#[derive(Debug)]
pub struct ScalarArray<T> {
    pub(crate) node: RealmNode,
    phantom: PhantomData<T>,
}

impl<T> Node for ScalarArray<T> {
    fn from_ref(realm: std::sync::Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            node: RealmNode::from_ref(realm, ref_)?,
            phantom: PhantomData,
        })
    }
}

macro_rules! impl_scalar {
    ($scalar:ty) => {
        impl ScalarArray<$scalar> {
            #[instrument(target = "ScalarArray", level = "debug")]
            pub fn get(&self, index: usize) -> $scalar {
                assert!(
                    index < self.node.header.size as usize,
                    "Index out of bounds: {index} >= {}",
                    self.node.header.size
                );

                let offset_start = index * std::mem::size_of::<$scalar>();
                let offset_end = offset_start + std::mem::size_of::<$scalar>();
                let bytes = &self.node.payload()[offset_start..offset_end];

                <$scalar>::from_le_bytes(bytes.try_into().unwrap())
            }
        }
    };
}

impl_scalar!(f32);
impl_scalar!(f64);
