use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::array::{Array, RealmRef};
use crate::node::Node;
use crate::realm::Realm;
use log::debug;
use std::str;
use tracing::instrument;

pub struct ArrayStringShort<T> {
    array: Array,
    str_type: PhantomData<T>,
}

impl<T> Debug for ArrayStringShort<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayStringShort")
            .field("array", &self.array)
            .finish()
    }
}

impl<T> Node for ArrayStringShort<T> {
    // #[instrument(target = "ArrayStringShort")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        Ok(Self {
            array,
            str_type: PhantomData,
        })
    }
}

impl<T> ArrayStringShort<T> {
    pub fn element_count(&self) -> usize {
        self.array.node.header.size as usize
    }

    #[instrument(target = "ArrayStringShort")]
    pub fn get(&self, index: usize) -> Option<&str> {
        Self::get_static(&self.array, index)
    }

    #[instrument(target = "ArrayStringShort")]
    pub fn get_static(array: &Array, index: usize) -> Option<&str> {
        let width = array.node.header.width() as usize;
        if width == 0 {
            debug!("get: width is 0, returning None");
            return None;
        }

        let element_data = &array.node.payload()[index * width..(index + 1) * width];
        let zeroes = element_data[width - 1] as usize;
        if zeroes == width {
            return None;
        }

        debug!(
            "get: index={index} width={width} zeroes={zeroes} element_data=0x{}",
            hex::encode(element_data)
        );

        // e.g. width = 4, zeroes = 1, element_data = [xx, xx, 00, 01]
        Some(unsafe { str::from_utf8_unchecked(&element_data[..width - 1 - zeroes]) })
    }
}

impl ArrayStringShort<String> {
    #[instrument(target = "ArrayStringShort")]
    pub fn get_strings(&self) -> Vec<String> {
        (0..self.array.node.header.size as usize)
            .map(|i| self.get(i).map(|s| s.to_string()).unwrap_or_default())
            .collect()
    }
}

#[allow(unused)]
impl ArrayStringShort<Option<String>> {
    #[instrument(target = "ArrayStringShort")]
    pub fn get_strings(&self) -> Vec<Option<String>> {
        (0..self.array.node.header.size as usize)
            .map(|i| self.get(i).map(|s| s.to_string()))
            .collect()
    }
}

#[allow(unused)]
impl ArrayStringShort<&str> {
    #[instrument(target = "ArrayStringShort")]
    pub fn get_strings(&self) -> Vec<&str> {
        (0..self.array.node.header.size as usize)
            .map(|i| self.get(i).unwrap_or_default())
            .collect()
    }
}
