use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use tracing::instrument;

use crate::array::{Array, RealmRef};
use crate::node::Node;
use crate::realm::Realm;

pub trait FromU64 {
    fn from_u64(value: u64) -> Self;
}

#[derive(Debug)]
pub struct IntegerArray<T> {
    array: Array,
    phantom: PhantomData<T>,
}

impl<T> Node for IntegerArray<T> {
    // #[instrument(target = "IntegerArray")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm, ref_)?;

        Ok(Self::from_array(array))
    }
}

impl<T> IntegerArray<T> {
    pub fn from_array(array: Array) -> Self {
        Self {
            array,
            phantom: PhantomData,
        }
    }

    pub fn element_count(&self) -> usize {
        self.array.node.header.size as usize
    }
}

macro_rules! integer_array_impl {
    ($type:ty) => {
        impl<T> IntegerArray<T> {
            pub fn get(&self, index: usize) -> $type {
                self.array.get(index) as $type
            }

            pub fn get_integers(&self) -> Vec<$type> {
                (0..self.array.node.header.size as usize)
                    .map(|i| self.array.get(i) as $type)
                    .collect()
            }
        }
    };
}

impl<T> IntegerArray<T> {
    pub fn get(&self, index: usize) -> u64 {
        self.array.get(index)
    }

    pub fn get_integers(&self) -> Vec<u64> {
        (0..self.array.node.header.size as usize)
            .map(|i| self.array.get(i))
            .collect()
    }
}

// integer_array_impl!(u8);
// integer_array_impl!(u16);
// integer_array_impl!(u32);
// integer_array_impl!(u64);
// integer_array_impl!(usize);
// integer_array_impl!(i8);
// integer_array_impl!(i16);
// integer_array_impl!(i32);
// integer_array_impl!(i64);

impl<T> IntegerArray<T>
where
    T: FromU64 + Debug,
{
    #[instrument(target = "IntegerArray")]
    pub fn get_integers_generic(&self) -> Vec<T> {
        (0..self.array.node.header.size as usize)
            .map(|i| T::from_u64(self.array.get(i)))
            .collect()
    }
}
