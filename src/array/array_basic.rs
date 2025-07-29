use std::fmt::Debug;
use std::sync::Arc;

use log::debug;
use tracing::instrument;

use crate::array::{RealmRef, RefOrTaggedValue};
use crate::node::{Node, NodeWithContext};
use crate::realm::{Realm, RealmNode};
use crate::utils::read_array_value;

#[derive(Debug, Clone)]
pub struct ArrayBasic {
    pub node: RealmNode,
    width: u8,
}

impl Node for ArrayBasic {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;
        let width = node.header.width();

        // assert!(
        //     !node.header.is_inner_bptree(),
        //     "cannot use ArrayBasic with B+Tree inner nodes (node {node:?})"
        // );

        Ok(Self { node, width })
    }
}

impl ArrayBasic {
    pub unsafe fn from_ref_bypass_bptree(
        realm: Arc<Realm>,
        ref_: RealmRef,
    ) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;
        let width = node.header.width();

        Ok(Self { node, width })
    }

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
    pub fn get_node<N>(&self, index: usize) -> anyhow::Result<N>
    where
        N: Node,
    {
        let ref_ = self.get_ref(index);

        // TODO: Don't unwrap here
        let ref_ = ref_.unwrap();

        debug!(
            target: "Array",
            "get_node: offset={ref_:?} payload=0x{}",
            hex::encode(self.node.payload())
        );

        N::from_ref(self.node.realm.clone(), ref_)
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
