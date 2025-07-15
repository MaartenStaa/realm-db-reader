use std::sync::Arc;

use crate::{array::RealmRef, realm::Realm};

#[allow(unused)]
pub trait Node {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized;
}
