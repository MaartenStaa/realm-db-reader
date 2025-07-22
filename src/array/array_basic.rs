use std::fmt::Debug;
use std::sync::Arc;

use log::debug;
use tracing::instrument;

use crate::array::{RealmRef, RefOrTaggedValue};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use crate::utils::read_array_value;

#[derive(Debug, Clone)]
pub struct ArrayBasic {
    pub node: RealmNode,
}

impl Node for ArrayBasic {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;

        assert!(
            !node.header.is_inner_bptree(),
            "cannot use ArrayBasic with B+Tree inner nodes (node {node:?})"
        );

        Ok(Self { node })
    }
}

impl ArrayBasic {
    pub unsafe fn from_ref_bypass_bptree(
        realm: Arc<Realm>,
        ref_: RealmRef,
    ) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;

        Ok(Self { node })
    }

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
    pub fn get_node<N>(&self, index: usize) -> anyhow::Result<N>
    where
        N: Node,
    {
        let ref_ = self.get_ref(index);

        // TODO
        self.get_node_at_ref(ref_.unwrap())
    }

    #[instrument(target = "Array")]
    pub fn get_node_at_ref<N>(&self, ref_: RealmRef) -> anyhow::Result<N>
    where
        N: Node,
    {
        debug!(
            target: "Array",
            "get_node_at_offset: offset={ref_:?} payload=0x{}",
            hex::encode(self.node.payload())
        );

        N::from_ref(self.node.realm.clone(), ref_)
    }

    #[instrument(target = "Array")]
    fn get_direct(&self, width: u8, index: usize) -> u64 {
        read_array_value(self.node.payload(), width, index)
    }
}
