use crate::array::{ArrayBasic, IntegerArray, RealmRef, RefOrTaggedValue};
use crate::column::{ArrayLeaf, BpTree, Column, ColumnImpl, ColumnType};
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use crate::value::{Backlink, Value};
use std::sync::Arc;

#[derive(Debug, Copy, Clone)]
struct BacklinkContext {
    target_table_index: usize,
    target_table_column_index: usize,
}

struct BacklinkColumnType;

impl ColumnType for BacklinkColumnType {
    type Value = Option<Backlink>;
    type LeafType = BacklinkArrayLeaf;
    type LeafContext = BacklinkContext;

    const IS_NULLABLE: bool = false;
}

struct BacklinkArrayLeaf {
    root: ArrayBasic,
    context: BacklinkContext,
}

impl NodeWithContext<BacklinkContext> for BacklinkArrayLeaf {
    fn from_ref_with_context(
        realm: Arc<Realm>,
        ref_: RealmRef,
        context: BacklinkContext,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            root: ArrayBasic::from_ref(realm, ref_)?,
            context,
        })
    }
}

impl ArrayLeaf<Option<Backlink>, BacklinkContext> for BacklinkArrayLeaf {
    fn get(&self, index: usize) -> anyhow::Result<Option<Backlink>> {
        let Some(ref_or_tagged) = self.root.get_ref_or_tagged_value(index) else {
            return Ok(None);
        };

        Ok(Some(Self::get_from_ref_or_tagged_value(
            &self.root.node.realm,
            ref_or_tagged,
            self.context,
        )?))
    }

    fn get_direct(
        realm: Arc<Realm>,
        ref_: RealmRef,
        index: usize,
        context: BacklinkContext,
    ) -> anyhow::Result<Option<Backlink>> {
        let header = realm.header(ref_)?;
        let payload = realm.payload(ref_, header.payload_len());

        let ref_or_tagged = match read_array_value(payload, header.width(), index) {
            0 => return Ok(None),
            n => RefOrTaggedValue::from_raw(n),
        };

        Ok(Some(Self::get_from_ref_or_tagged_value(
            &realm,
            ref_or_tagged,
            context,
        )?))
    }

    fn is_null(&self, _: usize) -> bool {
        false
    }

    fn size(&self) -> usize {
        self.root.node.header.size as usize
    }
}

impl BacklinkArrayLeaf {
    fn get_from_ref_or_tagged_value(
        realm: &Arc<Realm>,
        value: RefOrTaggedValue,
        context: BacklinkContext,
    ) -> anyhow::Result<Backlink> {
        match value {
            RefOrTaggedValue::Ref(ref_) => {
                let backlink_list = IntegerArray::from_ref(Arc::clone(&realm), ref_)?;
                let values = backlink_list
                    .get_integers()
                    .into_iter()
                    .map(|n| n as usize)
                    .collect();
                Ok(Backlink::new(
                    context.target_table_index,
                    context.target_table_column_index,
                    values,
                ))
            }
            RefOrTaggedValue::TaggedValue(value) => Ok(Backlink::new(
                context.target_table_index,
                context.target_table_column_index,
                vec![value as usize],
            )),
        }
    }
}

// Factory function for boolean columns
pub fn create_backlink_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    attributes: ColumnAttributes,
    target_table_index: usize,
    target_table_column_index: usize,
) -> anyhow::Result<Box<dyn Column>> {
    Ok(Box::new(BacklinkColumn::new(
        realm,
        ref_,
        attributes,
        None,
        BacklinkContext {
            target_table_index,
            target_table_column_index,
        },
    )?))
}

pub type BacklinkColumn = ColumnImpl<BacklinkColumnType>;
