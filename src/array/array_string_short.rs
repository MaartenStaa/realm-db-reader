use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::array::{Expectation, RealmRef};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use log::debug;
use std::str;
use tracing::instrument;

#[derive(Clone)]
pub struct ArrayStringShort<T> {
    node: RealmNode,
    str_type: PhantomData<T>,
}

impl<T> Debug for ArrayStringShort<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayStringShort")
            .field("node", &self.node)
            .finish()
    }
}

impl<T> Node for ArrayStringShort<T> {
    // #[instrument(target = "ArrayStringShort", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(realm, ref_)?;

        Ok(Self {
            node,
            str_type: PhantomData,
        })
    }
}

impl<T> ArrayStringShort<T> {
    pub fn element_count(&self) -> usize {
        self.node.header.size as usize
    }

    #[instrument(target = "ArrayStringShort", level = "debug")]
    pub fn get(&self, index: usize, expectation: Expectation) -> Option<&str> {
        Self::get_static(&self.node, index, expectation)
    }

    #[instrument(target = "ArrayStringShort", level = "debug")]
    pub fn get_static(node: &RealmNode, index: usize, expectation: Expectation) -> Option<&str> {
        let width = node.header.width() as usize;
        if width == 0 {
            debug!(target: "ArrayStringShort", "get: width is 0, returning None");
            return match expectation {
                Expectation::Nullable => None,
                Expectation::NotNullable => Some(""),
            };
        }

        let element_data = &node.payload()[index * width..(index + 1) * width];
        let zeroes = element_data[width - 1] as usize;
        if zeroes == width {
            return None;
        }

        debug!(
            target: "ArrayStringShort",
            "get: index={index} width={width} zeroes={zeroes} element_data=0x{}",
            hex::encode(element_data)
        );

        // e.g. width = 4, zeroes = 1, element_data = [xx, xx, 00, 01]
        Some(unsafe { str::from_utf8_unchecked(&element_data[..width - 1 - zeroes]) })
    }
}

impl ArrayStringShort<String> {
    #[instrument(target = "ArrayStringShort", level = "debug")]
    pub fn get_strings(&self, expectation: Expectation) -> Vec<String> {
        (0..self.node.header.size as usize)
            .map(|i| {
                self.get(i, expectation)
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            })
            .collect()
    }
}

#[allow(unused)]
impl ArrayStringShort<Option<String>> {
    #[instrument(target = "ArrayStringShort", level = "debug")]
    pub fn get_strings(&self, expectation: Expectation) -> Vec<Option<String>> {
        (0..self.node.header.size as usize)
            .map(|i| self.get(i, expectation).map(|s| s.to_string()))
            .collect()
    }
}

#[allow(unused)]
impl ArrayStringShort<&str> {
    #[instrument(target = "ArrayStringShort", level = "debug")]
    pub fn get_strings(&self, expectation: Expectation) -> Vec<&str> {
        (0..self.node.header.size as usize)
            .map(|i| self.get(i, expectation).unwrap_or_default())
            .collect()
    }
}
