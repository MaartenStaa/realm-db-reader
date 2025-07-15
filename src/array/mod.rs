mod array_link_list;
mod array_string;
mod array_string_short;
mod array_timestamp;
mod generic_array;
mod integer_array;
mod long_blobs_array;
mod small_blobs_array;

pub use array_link_list::ArrayLinkList;
pub use array_string::ArrayString;
pub use array_string_short::ArrayStringShort;
pub use array_timestamp::ArrayTimestamp;
#[allow(unused_imports)]
pub use generic_array::GenericArray;
pub use integer_array::{FromU64, IntegerArray};

use std::fmt::Debug;
use std::ops::Add;
use std::sync::Arc;

use log::debug;
use tracing::instrument;

use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use crate::utils::read_array_value;

#[derive(Debug, Clone)]
pub struct Array {
    pub node: RealmNode,
}

#[derive(Copy, Clone)]
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
    TaggedRef(u64),
}

impl RefOrTaggedValue {
    pub fn from_raw(value: u64) -> Self {
        if value & 1 == 0 {
            Self::Ref(RealmRef(value as usize))
        } else {
            Self::TaggedRef(value >> 1)
        }
    }

    pub fn from_ref(ref_: RealmRef) -> Self {
        Self::Ref(ref_)
    }
}

impl Node for Array {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(realm, ref_)?;

        Ok(Self { node })
    }
}

impl Array {
    #[instrument(target = "Array")]
    pub fn get(&self, index: usize) -> u64 {
        let width = self.node.header.width();

        self.get_direct(width, index)
    }

    #[instrument(target = "Array")]
    pub fn get_ref(&self, index: usize) -> Option<RealmRef> {
        let width = self.node.header.width();
        let ref_ = self.get_direct(width, index);

        if ref_ == 0 {
            return None;
        }

        assert!(ref_ % 8 == 0);

        Some(RealmRef(ref_ as usize))
    }

    #[instrument(target = "Array")]
    pub fn get_ref_or_tagged_value(&self, index: usize) -> Option<RefOrTaggedValue> {
        let width = self.node.header.width();
        let value = self.get_direct(width, index);

        if value == 0 {
            return None;
        }

        Some(RefOrTaggedValue::from_raw(value))
    }

    #[instrument(target = "Array")]
    pub fn get_node<T>(&self, index: usize) -> anyhow::Result<T>
    where
        T: Node,
    {
        let ref_ = self.get_ref(index);

        // TODO
        self.get_node_at_ref(ref_.unwrap())
    }

    #[instrument(target = "Array")]
    pub fn get_node_at_ref<T>(&self, ref_: RealmRef) -> anyhow::Result<T>
    where
        T: Node,
    {
        debug!(
            target: "Array",
            "get_node_at_offset: offset={ref_:?} payload=0x{}",
            hex::encode(self.node.payload())
        );

        T::from_ref(self.node.realm.clone(), ref_)
    }

    #[instrument(target = "Array")]
    fn get_direct(&self, width: u8, index: usize) -> u64 {
        read_array_value(self.node.payload(), width, index)
    }
}
