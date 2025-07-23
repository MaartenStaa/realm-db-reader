use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::{Array, RealmRef};
use crate::build::Build;
use crate::node::Node;
use crate::realm::Realm;

pub struct GenericArray<T> {
    array: Array,
    phantom: PhantomData<T>,
}

impl<T> Debug for GenericArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericArray")
            .field("array", &self.array)
            .finish()
    }
}

impl<T> Node for GenericArray<T> {
    #[instrument(target = "GenericArray", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        Ok(Self {
            array,
            phantom: PhantomData,
        })
    }
}

impl<T> GenericArray<T>
where
    T: Build + std::fmt::Debug,
{
    #[instrument(target = "GenericArray", level = "debug")]
    pub fn get_elements(&self) -> anyhow::Result<Vec<T>> {
        let mut result = Vec::with_capacity(self.array.node.header.size as usize);
        for i in 0..self.array.node.header.size as usize {
            let element_node: Array = self.array.get_node(i)?;
            warn!(target: "GenericArray", "element_node {i}: {:?}", element_node);
            result.push(T::build(element_node)?);
        }

        Ok(result)
    }
}
