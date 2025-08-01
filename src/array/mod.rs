mod array_string;
mod array_string_short;
mod integer_array;
mod long_blobs_array;
mod small_blobs_array;

pub use array_string::ArrayString;
pub use array_string_short::ArrayStringShort;
#[allow(unused_imports)]
pub use integer_array::{FromU64, IntegerArray};
pub use long_blobs_array::LongBlobsArray;
pub use small_blobs_array::SmallBlobsArray;

use std::fmt::Debug;
use std::ops::Add;
use std::sync::Arc;

use log::debug;
use tracing::instrument;

use crate::node::{Node, NodeWithContext};
use crate::realm::{Realm, RealmNode};
use crate::utils::read_array_value;

#[derive(Copy, Clone, Hash, PartialEq, Eq)]
pub struct RealmRef(usize);

impl Debug for RealmRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{:X}", self.0)
    }
}

impl RealmRef {
    pub fn new(ref_: usize) -> Self {
        assert!(ref_ % 8 == 0, "RealmRef must be a multiple of 8");

        Self(ref_)
    }

    pub fn to_offset(self) -> usize {
        self.0
    }
}

impl Add<usize> for RealmRef {
    type Output = Self;

    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

#[derive(Debug, Copy, Clone)]
pub enum RefOrTaggedValue {
    Ref(RealmRef),
    TaggedValue(u64),
}

impl RefOrTaggedValue {
    pub fn from_raw(value: u64) -> Self {
        if value & 1 == 0 {
            Self::Ref(RealmRef(value as usize))
        } else {
            Self::TaggedValue(value >> 1)
        }
    }

    pub fn from_ref(ref_: RealmRef) -> Self {
        Self::Ref(ref_)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Expectation {
    Nullable,
    NotNullable,
}

#[derive(Debug, Clone)]
pub struct Array {
    pub node: RealmNode,
    width: u8,
}

impl Node for Array {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;
        let width = node.header.width();

        Ok(Self { node, width })
    }
}

impl Array {
    #[instrument(target = "Array", level = "debug")]
    pub fn get(&self, index: usize) -> u64 {
        assert!(
            index < self.node.header.size as usize,
            "Index out of bounds: {index} >= {}",
            self.node.header.size
        );

        self.get_direct(self.width, index)
    }

    #[instrument(target = "Array", level = "debug")]
    pub fn get_ref(&self, index: usize) -> Option<RealmRef> {
        assert!(
            index < self.node.header.size as usize,
            "Index out of bounds: {index} >= {}",
            self.node.header.size
        );

        let ref_ = self.get_direct(self.width, index);

        if ref_ == 0 {
            return None;
        }

        assert!(ref_ % 8 == 0);

        Some(RealmRef(ref_ as usize))
    }

    #[instrument(target = "Array", level = "debug")]
    pub fn get_ref_or_tagged_value(&self, index: usize) -> Option<RefOrTaggedValue> {
        assert!(
            index < self.node.header.size as usize,
            "Index out of bounds: {index} >= {}",
            self.node.header.size
        );

        let value = self.get_direct(self.width, index);

        if value == 0 {
            return None;
        }

        Some(RefOrTaggedValue::from_raw(value))
    }

    #[instrument(target = "Array", level = "debug")]
    pub fn get_node<N>(&self, index: usize) -> anyhow::Result<Option<N>>
    where
        N: Node,
    {
        let Some(ref_) = self.get_ref(index) else {
            return Ok(None);
        };

        debug!(
            target: "Array",
            "get_node: offset={ref_:?} payload=0x{}",
            hex::encode(self.node.payload())
        );

        N::from_ref(self.node.realm.clone(), ref_).map(Some)
    }

    #[instrument(target = "Array", level = "debug")]
    pub fn get_node_with_context<N: NodeWithContext<T>, T: Debug>(
        &self,
        index: usize,
        context: T,
    ) -> anyhow::Result<N> {
        let ref_ = self.get_ref(index);

        // TODO: Don't unwrap here
        let ref_ = ref_.unwrap();

        debug!(
            target: "Array",
            "get_node: offset={ref_:?} payload=0x{}",
            hex::encode(self.node.payload())
        );

        N::from_ref_with_context(self.node.realm.clone(), ref_, context)
    }

    pub fn front(&self) -> u64 {
        assert!(self.node.header.size > 0, "Array is empty");

        read_array_value(self.node.payload(), self.width, 0)
    }

    pub fn back(&self) -> u64 {
        let size = self.node.header.size as usize;
        if size == 0 {
            return 0;
        }

        read_array_value(self.node.payload(), self.width, size - 1)
    }

    #[instrument(target = "Array", level = "debug")]
    fn get_direct(&self, width: u8, index: usize) -> u64 {
        read_array_value(self.node.payload(), self.width, index)
    }
}
