use std::fmt::Debug;

pub use crate::column::backlink::create_backlink_column;
pub use crate::column::bool::create_bool_column;
use crate::column::bptree::BpTree;
pub use crate::column::integer::create_int_column;
pub use crate::column::linklist::create_linklist_column;
pub use crate::column::string::create_string_column;
pub use crate::column::subtable::create_subtable_column;
pub use crate::column::timestamp::create_timestamp_column;
use crate::node::NodeWithContext;
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::{array::RealmRef, value::Value};
use std::sync::Arc;

mod backlink;
mod bool;
mod bptree;
// mod float;
mod integer;
mod integer_optional;
mod linklist;
mod string;
mod subtable;
mod timestamp;

/// A column for a table.
pub trait Column: Debug + Send {
    /// Get the value for this column for the row with the given index.
    fn get(&self, index: usize) -> anyhow::Result<Value>;

    /// Check whether the value at the given index is null.
    fn is_null(&self, index: usize) -> anyhow::Result<bool>;

    /// Get the total number of values in this column.
    fn count(&self) -> anyhow::Result<usize>;

    /// Get whether this column is nullable.
    fn nullable(&self) -> bool;

    fn is_indexed(&self) -> bool;

    fn name(&self) -> Option<&str>;
}

/// B+Tree leaf array.
pub trait ArrayLeaf<T, C>: NodeWithContext<C> {
    fn get(&self, index: usize) -> anyhow::Result<T>;
    fn get_direct(realm: Arc<Realm>, ref_: RealmRef, index: usize, context: C)
    -> anyhow::Result<T>;
    fn is_null(&self, index: usize) -> bool;
    fn size(&self) -> usize;
}

/// The definition of a column type, which includes the value type, leaf type, and B+Tree type.
pub trait ColumnType {
    type Value: Into<Value>;
    type LeafType: ArrayLeaf<Self::Value, Self::LeafContext>;
    type LeafContext: Copy + Debug;

    const IS_NULLABLE: bool;
}

struct ColumnImpl<T: ColumnType> {
    tree: BpTree<T>,
    attributes: ColumnAttributes,
    name: Option<String>,
}

impl<T: ColumnType> Debug for ColumnImpl<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnImpl")
            .field("tree", &self.tree)
            .field("attributes", &self.attributes)
            .field("name", &self.name)
            .finish()
    }
}

impl<T: ColumnType + Send> Column for ColumnImpl<T>
where
    Value: From<T::Value>,
    <T as ColumnType>::LeafContext: std::marker::Send,
{
    fn get(&self, index: usize) -> anyhow::Result<Value> {
        Ok(Value::from(self.tree.get(index)?))
    }

    fn is_null(&self, index: usize) -> anyhow::Result<bool> {
        self.tree.is_null(index)
    }

    fn count(&self) -> anyhow::Result<usize> {
        self.tree.count()
    }

    fn nullable(&self) -> bool {
        self.attributes.is_nullable()
    }

    fn is_indexed(&self) -> bool {
        todo!()
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl<T: ColumnType> ColumnImpl<T> {
    pub fn new(
        realm: Arc<Realm>,
        ref_: RealmRef,
        attributes: ColumnAttributes,
        name: Option<String>,
        context: T::LeafContext,
    ) -> anyhow::Result<Self> {
        let tree = BpTree::from_ref_with_context(realm, ref_, context)?;

        Ok(Self {
            tree,
            attributes,
            name,
        })
    }
}
