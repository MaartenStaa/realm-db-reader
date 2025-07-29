mod array_basic;
mod array_string;
mod array_string_short;
mod integer_array;
mod long_blobs_array;
mod small_blobs_array;

pub use array_basic::ArrayBasic;
pub use array_string::ArrayString;
pub use array_string_short::ArrayStringShort;
#[allow(unused_imports)]
pub use integer_array::{FromU64, IntegerArray};
pub use long_blobs_array::LongBlobsArray;
pub use small_blobs_array::SmallBlobsArray;

use std::fmt::Debug;
use std::ops::Add;
use std::sync::Arc;

use anyhow::bail;
use log::debug;
use tracing::instrument;

use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use crate::utils::read_array_value;

#[derive(Debug, Clone)]
pub struct Array<T> {
    pub node: RealmNode,
    inner: ArrayInner<T>,
}

#[derive(Debug, Clone)]
enum ArrayInner<T> {
    BPTree(BPTreeArray),
    String(Box<ArrayString<T>>),
    Integer(Box<IntegerArray>),
    Bool(Box<IntegerArray>),
    Blob(Box<LongBlobsArray>),
}

#[derive(Debug, Clone)]
struct BPTreeArray {
    form: BPTreeForm,
    total_elements: usize,
}

#[derive(Debug, Clone)]
enum BPTreeForm {
    Compact { elements_per_child: usize },
    Regular { offsets: Box<Array<u64>> },
}

impl BPTreeArray {
    /// The first element of a B+Tree node is either the number of elements per child, or the
    /// offsets of the children. So child 0 is at index 1.
    const CHILD_OFFSET: usize = 1;

    #[instrument(target = "BPTreeArray", level = "debug")]
    fn find_bptree_child(&self, index: usize) -> (usize, usize) {
        assert!(
            index < self.total_elements,
            "Index out of bounds for B+Tree array"
        );

        let result = match &self.form {
            BPTreeForm::Compact { elements_per_child } => (
                (index / elements_per_child) + Self::CHILD_OFFSET,
                index % elements_per_child,
            ),
            BPTreeForm::Regular { offsets } => todo!(),
        };

        tracing::debug!(
            target: "BPTreeArray",
            "find_bptree_child: index={index} result={result:?}"
        );

        result
    }
}

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

macro_rules! array_node_impl {
    ($T:ty, $inner_variant:ident, $inner_class:ty) => {
        impl Node for Array<$T> {
            #[instrument(target = "Array", level = "debug")]
            fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
                let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;
                let inner = if node.header.is_inner_bptree() {
                    assert!(node.header.has_refs(), "invariant: b+tree nodes have refs");
                    assert!(
                        node.header.size >= 2,
                        "invariant: b+tree nodes have a size of at least 2"
                    );

                    let payload = node.payload();
                    let head = read_array_value(payload, node.header.width(), 0) as usize;
                    let is_compact_form = head % 2 != 0;
                    let total_elements = read_array_value(
                        payload,
                        node.header.width(),
                        node.header.size as usize - 1,
                    ) as usize
                        / 2;

                    ArrayInner::BPTree(BPTreeArray {
                        total_elements,
                        form: if is_compact_form {
                            BPTreeForm::Compact {
                                elements_per_child: head / 2,
                            }
                        } else {
                            BPTreeForm::Regular {
                                offsets: Box::new(Array::<u64>::from_ref(
                                    realm,
                                    RealmRef::new(head),
                                )?),
                            }
                        },
                    })
                } else {
                    ArrayInner::$inner_variant(Box::new(<$inner_class>::from_ref(realm, ref_)?))
                };

                Ok(Self { node, inner })
            }
        }
    };
}

array_node_impl!(bool, Bool, IntegerArray);
array_node_impl!(u8, Integer, IntegerArray);
array_node_impl!(u16, Integer, IntegerArray);
array_node_impl!(u32, Integer, IntegerArray);
array_node_impl!(u64, Integer, IntegerArray);
array_node_impl!(i8, Integer, IntegerArray);
array_node_impl!(i16, Integer, IntegerArray);
array_node_impl!(i32, Integer, IntegerArray);
array_node_impl!(i64, Integer, IntegerArray);
array_node_impl!(f32, Integer, IntegerArray);
array_node_impl!(f64, Integer, IntegerArray);
array_node_impl!(usize, Integer, IntegerArray);
array_node_impl!(String, String, ArrayString::<String>);
array_node_impl!(Vec<u8>, Blob, LongBlobsArray);

impl<T> Array<T>
where
    T: Debug,
{
    #[instrument(target = "Array", level = "debug")]
    fn get(&self, index: usize) -> u64 {
        let width = self.node.header.width();

        self.get_direct(width, index)
    }

    #[instrument(target = "Array", level = "debug")]
    fn get_ref(&self, index: usize) -> Option<RealmRef> {
        let width = self.node.header.width();
        let ref_ = self.get_direct(width, index);

        if ref_ == 0 {
            return None;
        }

        assert!(ref_ % 8 == 0);

        Some(RealmRef(ref_ as usize))
    }

    #[instrument(target = "Array", level = "debug")]
    fn get_ref_or_tagged_value(&self, index: usize) -> Option<RefOrTaggedValue> {
        let width = self.node.header.width();
        let value = self.get_direct(width, index);

        if value == 0 {
            return None;
        }

        Some(RefOrTaggedValue::from_raw(value))
    }

    #[instrument(target = "Array", level = "debug")]
    fn get_node<N>(&self, index: usize) -> anyhow::Result<N>
    where
        N: Node,
    {
        let ref_ = self.get_ref(index);

        // TODO
        self.get_node_at_ref(ref_.unwrap())
    }

    #[instrument(target = "Array", level = "debug")]
    fn get_node_at_ref<N>(&self, ref_: RealmRef) -> anyhow::Result<N>
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

    #[instrument(target = "Array", level = "debug")]
    fn get_direct(&self, width: u8, index: usize) -> u64 {
        read_array_value(self.node.payload(), width, index)
    }

    pub fn element_count(&self) -> usize {
        match &self.inner {
            ArrayInner::BPTree(bptree) => bptree.total_elements,
            ArrayInner::String(array_string) => array_string.element_count(),
            _ => self.node.header.size as usize,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Expectation {
    Nullable,
    NotNullable,
}

impl Array<String> {
    pub fn get_string(
        &self,
        index: usize,
        expectation: Expectation,
    ) -> anyhow::Result<Option<String>> {
        match &self.inner {
            ArrayInner::BPTree(bptree) => {
                let (child_index, index_in_child) = bptree.find_bptree_child(index);
                let child: Self = self.get_node(child_index)?;

                child.get_string(index_in_child, expectation)
            }
            ArrayInner::String(array_string) => array_string.get_string(index, expectation),
            _ => unreachable!("get_string called on non-string array"),
        }
    }
}

macro_rules! array_impl_numeric {
    ($type:ty) => {
        impl Array<$type> {
            pub fn get_integer(&self, index: usize) -> anyhow::Result<$type> {
                match &self.inner {
                    ArrayInner::BPTree(bptree) => {
                        let (child_index, index_in_child) = bptree.find_bptree_child(index);
                        let child: Self = self.get_node(child_index)?;

                        child.get_integer(index_in_child)
                    }
                    ArrayInner::Integer(_) => {
                        let value = self.get(index);
                        Ok(value as $type)
                    }
                    _ => unreachable!("get_integer called on non-integer array"),
                }
            }

            pub fn get_tagged_integer(&self, index: usize) -> anyhow::Result<Option<$type>> {
                match &self.inner {
                    ArrayInner::BPTree(bptree) => {
                        let (child_index, index_in_child) = bptree.find_bptree_child(index);
                        let child: Self = self.get_node(child_index)?;

                        child.get_tagged_integer(index_in_child)
                    }
                    ArrayInner::Integer(_) => match self.get_ref_or_tagged_value(index) {
                        Some(RefOrTaggedValue::Ref(ref_)) => {
                            bail!("ref found in get_tagged_integer: {ref_:?}");
                        }
                        Some(RefOrTaggedValue::TaggedValue(value)) => Ok(Some(value as $type)),
                        None => Ok(None),
                    },
                    _ => unreachable!("get_tagged_integer called on non-integer array"),
                }
            }
        }
    };
}

array_impl_numeric!(u8);
array_impl_numeric!(u16);
array_impl_numeric!(u32);
array_impl_numeric!(u64);
array_impl_numeric!(i8);
array_impl_numeric!(i16);
array_impl_numeric!(i32);
array_impl_numeric!(i64);

impl Array<bool> {
    pub fn get_bool(&self, index: usize) -> anyhow::Result<bool> {
        match &self.inner {
            ArrayInner::BPTree(bptree) => {
                let (child_index, index_in_child) = bptree.find_bptree_child(index);
                let child: Self = self.get_node(child_index)?;

                child.get_bool(index_in_child)
            }
            ArrayInner::Bool(_) => {
                let value = self.get(index);
                Ok(value != 0)
            }
            _ => unreachable!("get_bool called on non-bool array"),
        }
    }
}
