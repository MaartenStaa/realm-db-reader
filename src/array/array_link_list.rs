use crate::array::{Array, IntegerArray, RealmRef, RefOrTaggedValue};
use crate::node::Node;
use crate::realm::Realm;

use std::sync::Arc;

pub struct ArrayLinkList {
    array: Array,
}

impl Node for ArrayLinkList {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        Ok(Self {
            array: Array::from_ref(realm, ref_)?,
        })
    }
}

impl ArrayLinkList {
    pub fn get(&self, index: usize) -> anyhow::Result<Option<Vec<usize>>> {
        let sub_array = match self.array.get_ref_or_tagged_value(index) {
            Some(RefOrTaggedValue::Ref(ref_)) => {
                Array::from_ref(self.array.node.realm.clone(), ref_)?
            }
            _ => return Ok(None),
        };

        if sub_array.node.header.is_inner_btree() {
            unimplemented!();
        }

        let integer_array: IntegerArray<usize> = IntegerArray::from_array(sub_array);

        Ok(Some(
            integer_array
                .get_integers()
                .into_iter()
                .map(|x| x as usize)
                .collect(),
        ))
    }
}
