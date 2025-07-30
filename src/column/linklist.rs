use crate::array::{Array, IntegerArray, RealmRef, RefOrTaggedValue};
use crate::column::{ArrayLeaf, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

pub struct LinkListColumnType;

#[derive(Debug, Copy, Clone)]
pub struct LinkListColumnContext {
    target_table_index: usize,
}

impl ColumnType for LinkListColumnType {
    type Value = Vec<usize>;
    type LeafType = LinkListLeaf;
    type LeafContext = LinkListColumnContext;

    const IS_NULLABLE: bool = false;
}

pub struct LinkListLeaf {
    root: Array,
}

impl NodeWithContext<LinkListColumnContext> for LinkListLeaf {
    fn from_ref_with_context(
        realm: Arc<Realm>,
        ref_: RealmRef,
        // TODO: Should this be part of the returned value like with the backlinks?
        _: LinkListColumnContext,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let root = Array::from_ref(realm, ref_)?;
        Ok(Self { root })
    }
}

impl ArrayLeaf<Vec<usize>, LinkListColumnContext> for LinkListLeaf {
    fn get(&self, index: usize) -> anyhow::Result<Vec<usize>> {
        let sub_array = match self.root.get_ref_or_tagged_value(index) {
            Some(RefOrTaggedValue::Ref(ref_)) => {
                Array::from_ref(Arc::clone(&self.root.node.realm), ref_)?
            }
            _ => return Ok(vec![]),
        };

        Ok(Self::get_from_sub_array(sub_array))
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        // TODO: Should this be part of the returned value like with the backlinks?
        _: LinkListColumnContext,
    ) -> anyhow::Result<Vec<usize>> {
        let header = realm.header(ref_)?;
        let payload = realm.payload(ref_, header.payload_len());

        let sub_array = match read_array_value(payload, header.width(), index) {
            0 => return Ok(vec![]),
            n => match RefOrTaggedValue::from_raw(n) {
                RefOrTaggedValue::Ref(ref_) => Array::from_ref(Arc::clone(&realm), ref_)?,
                _ => return Ok(vec![]),
            },
        };

        Ok(Self::get_from_sub_array(sub_array))
    }

    fn is_null(&self, _: usize) -> bool {
        false
    }

    fn size(&self) -> usize {
        self.root.node.header.size as usize
    }
}

impl LinkListLeaf {
    fn get_from_sub_array(sub_array: Array) -> Vec<usize> {
        assert!(!sub_array.node.header.is_inner_bptree());

        IntegerArray::from_array(sub_array)
            .get_integers()
            .into_iter()
            .map(|x| x as usize)
            .collect::<Vec<_>>()
    }
}

// Factory function for link list columns
pub fn create_linklist_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    target_table_index: usize,
    name: String,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(LinkListColumn::new(
        realm,
        ref_,
        attributes,
        // target_table_index,
        Some(name),
        LinkListColumnContext { target_table_index },
    )?))
}

pub type LinkListColumn = ColumnImpl<LinkListColumnType>;
