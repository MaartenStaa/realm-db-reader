use std::sync::Arc;

use crate::{array::RealmRef, realm::Realm};

pub trait Node {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized;
}

pub trait NodeWithContext<T> {
    fn from_ref_with_context(realm: Arc<Realm>, ref_: RealmRef, context: T) -> anyhow::Result<Self>
    where
        Self: Sized;
}
