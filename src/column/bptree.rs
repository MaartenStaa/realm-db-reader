use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{
    array::{Array, RealmRef},
    column::{ArrayLeaf, ColumnType},
    node::{Node, NodeWithContext},
    realm::Realm,
    utils,
};

pub struct BpTree<T: ColumnType> {
    root: Array,
    context: T::LeafContext,
    column_type: PhantomData<T>,
}

impl<T: ColumnType> Debug for BpTree<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BpTree")
            .field("root", &self.root)
            .field("context", &self.context)
            .finish()
    }
}

impl<T: ColumnType> NodeWithContext<T::LeafContext> for BpTree<T> {
    fn from_ref_with_context(
        realm: Arc<Realm>,
        ref_: RealmRef,
        context: T::LeafContext,
    ) -> anyhow::Result<Self> {
        let root = Array::from_ref(realm, ref_)?;

        Ok(Self {
            root,
            column_type: PhantomData,
            context,
        })
    }
}

mod sealed {
    pub trait EmptyContext {
        fn make() -> Self;
    }

    impl EmptyContext for () {
        fn make() -> Self {}
    }
}

use sealed::EmptyContext;

impl<T: ColumnType> Node for BpTree<T>
where
    T::LeafContext: sealed::EmptyContext,
{
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let root = Array::from_ref(realm, ref_)?;

        Ok(Self {
            root,
            column_type: PhantomData,
            context: T::LeafContext::make(),
        })
    }
}

impl<T: ColumnType> BpTree<T> {
    pub(crate) fn get(&self, index: usize) -> anyhow::Result<T::Value> {
        if self.root_is_leaf() {
            let leaf = T::LeafType::from_ref_with_context(
                self.root.node.realm.clone(),
                self.root.node.ref_,
                self.context,
            )?;
            return leaf.get(index);
        }

        let (leaf_ref, index_in_leaf) = self.root_as_node().get_bptree_leaf(index)?;
        T::LeafType::get_direct(
            Arc::clone(&self.root.node.realm),
            leaf_ref,
            index_in_leaf,
            self.context,
        )
    }

    pub(crate) fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        if self.root_is_leaf() {
            let leaf = T::LeafType::from_ref_with_context(
                self.root.node.realm.clone(),
                self.root.node.ref_,
                self.context,
            )
            .unwrap();
            return Ok(leaf.is_null(index));
        }

        let (leaf_ref, index_in_leaf) = self.root_as_node().get_bptree_leaf(index)?;
        let leaf = T::LeafType::from_ref_with_context(
            self.root.node.realm.clone(),
            leaf_ref,
            self.context,
        )?;

        Ok(leaf.is_null(index_in_leaf))
    }

    pub fn count(&self) -> anyhow::Result<usize> {
        Ok(if self.root_is_leaf() {
            self.root_as_leaf()?.size()
        } else {
            self.root_as_node().get_bptree_size()
        })
    }
}

impl<T: ColumnType> BpTree<T> {
    fn root_is_leaf(&self) -> bool {
        !self.root.node.header.is_inner_bptree()
    }

    fn root_as_leaf(&self) -> anyhow::Result<T::LeafType> {
        assert!(self.root_is_leaf(), "Root is not a leaf node");

        T::LeafType::from_ref_with_context(
            Arc::clone(&self.root.node.realm),
            self.root.node.ref_,
            self.context,
        )
    }

    fn root_as_node<'a>(&'a self) -> BpTreeNode<'a> {
        assert!(!self.root_is_leaf(), "Root is not a B+Tree node");

        BpTreeNode { root: &self.root }
    }
}

/// A B+Tree node that holds a reference to the root node of the B+Tree.
/// Root is not allowed to be a leaf node.
pub(crate) struct BpTreeNode<'a> {
    root: &'a Array,
}

impl<'a> BpTreeNode<'a> {
    pub(crate) fn new(root: &'a Array) -> Self {
        assert!(
            root.node.header.is_inner_bptree(),
            "Root must be a B+Tree node"
        );

        Self { root }
    }

    pub(crate) fn get_bptree_leaf(&self, mut index: usize) -> anyhow::Result<(RealmRef, usize)> {
        let mut width = self.root.node.header.width();
        let mut payload = self.root.node.payload();

        loop {
            let (child_ref, index_in_child) = utils::find_bptree_child_in_payload(
                Arc::clone(&self.root.node.realm),
                payload,
                width,
                index,
            )?;
            let child_header = self.root.node.realm.header(child_ref)?;
            let child_is_leaf = !child_header.is_inner_bptree();
            if child_is_leaf {
                return Ok((child_ref, index_in_child));
            }

            index = index_in_child;
            width = child_header.width();
            payload = self
                .root
                .node
                .realm
                .payload(child_ref, child_header.payload_len());
        }
    }

    pub(crate) fn get_bptree_size(&self) -> usize {
        assert!(self.root.node.header.is_inner_bptree());
        let v = self.root.back();

        (v / 2) as usize
    }
}
