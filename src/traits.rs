use std::fmt::Debug;
use std::sync::Arc;

use crate::Realm;
use crate::array::RealmRef;

/// Trait for nodes in the realm. A node is a struct that can be created from a
/// reference to its realm and reference.
pub trait Node {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized;
}

/// Blanket implementation: any node with an empty context implements Node
impl<T> Node for T
where
    T: NodeWithContext<()>,
{
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Self::from_ref_with_context(realm, ref_, ())
    }
}

/// Trait for nodes in the realm, holding a context. A node is a struct that can
/// be created from a reference to its realm and reference.
pub trait NodeWithContext<T> {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, context: T) -> anyhow::Result<Self>
    where
        Self: Sized;
}

/// Array-like trait for nodes in the realm, holding a context. These allow
/// fetching elements of the given type from the array.
pub(crate) trait ArrayLike<T, Context = ()>: NodeWithContext<Context> + Debug {
    /// Get the value at the given index.
    fn get(&self, index: usize) -> anyhow::Result<T>;

    /// Get the value at the given index directly from the realm, without
    /// allocating an instance of the array.
    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        context: Context,
    ) -> anyhow::Result<T>
    where
        Self: Sized;

    /// Get all values from the array.
    fn get_all(&self) -> anyhow::Result<Vec<T>> {
        (0..self.size()).map(|i| self.get(i)).collect()
    }

    /// Check if the value at the given index is null.
    fn is_null(&self, index: usize) -> anyhow::Result<bool>;

    /// Get the size of the array, indicating the number of elements it contains.
    fn size(&self) -> usize;
}
