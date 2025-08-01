use std::fmt::Debug;

pub use crate::column::backlink::create_backlink_column;
pub use crate::column::bool::create_bool_column;
pub use crate::column::bool_optional::create_bool_null_column;
use crate::column::bptree::BpTree;
pub use crate::column::integer::create_int_column;
pub use crate::column::integer_optional::create_int_null_column;
pub use crate::column::linklist::create_linklist_column;
pub use crate::column::string::create_string_column;
pub use crate::column::subtable::create_subtable_column;
pub use crate::column::timestamp::create_timestamp_column;
use crate::index::Index;
use crate::node::{Node, NodeWithContext};
use crate::realm::Realm;
use crate::table::ColumnAttributes;
use crate::{array::RealmRef, value::Value};
use std::sync::Arc;

mod backlink;
mod bool;
mod bool_optional;
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

    /// Is table indexed?
    fn is_indexed(&self) -> bool;

    /// Look up a value for this column in the index.
    ///
    /// Panics if this column is not indexed.
    fn get_row_number_by_index(&self, lookup_value: &Value) -> anyhow::Result<Option<usize>>;

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
    index: Option<Index>,
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
        self.attributes.is_indexed()
    }

    fn get_row_number_by_index(&self, lookup_value: &Value) -> anyhow::Result<Option<usize>> {
        let Some(index) = &self.index else {
            panic!("Column {:?} is not indexed", self.name());
        };

        index.find_first(lookup_value)
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

impl<T: ColumnType> ColumnImpl<T> {
    pub fn new(
        realm: Arc<Realm>,
        data_ref: RealmRef,
        index_ref: Option<RealmRef>,
        attributes: ColumnAttributes,
        name: Option<String>,
        context: T::LeafContext,
    ) -> anyhow::Result<Self> {
        let tree = BpTree::from_ref_with_context(Arc::clone(&realm), data_ref, context)?;
        let index = index_ref
            .map(|ref_| Index::from_ref(realm, ref_))
            .transpose()?;

        Ok(Self {
            tree,
            index,
            attributes,
            name,
        })
    }
}
