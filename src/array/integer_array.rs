use std::fmt::Debug;
use std::sync::Arc;

use crate::array::{Array, RealmRef};
use crate::node::Node;
use crate::realm::Realm;

pub trait FromU64 {
    fn from_u64(value: u64) -> Self;
}

#[derive(Debug, Clone)]
pub struct IntegerArray {
    array: Array,
}

impl Node for IntegerArray {
    // #[instrument(target = "IntegerArray", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        Ok(Self::from_array(array))
    }
}

impl IntegerArray {
    pub fn from_array(array: Array) -> Self {
        Self { array }
    }

    pub fn element_count(&self) -> usize {
        self.array.node.header.size as usize
    }
}

impl IntegerArray {
    pub fn get(&self, index: usize) -> u64 {
        self.array.get(index)
    }

    pub fn get_integers(&self) -> Vec<u64> {
        (0..self.array.node.header.size as usize)
            .map(|i| self.array.get(i))
            .collect()
    }
}
