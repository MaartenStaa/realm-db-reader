use crate::array::{ArrayBasic, RealmRef};
use crate::column::{ArrayLeaf, BpTree, Column_, ColumnType, LeafCache};
use crate::node::Node;
use crate::realm::Realm;
use crate::spec::ThinColumnType;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

// Float column type implementation
#[derive(Debug, Clone)]
pub struct FloatColumnType;

impl ColumnType for FloatColumnType {
    type Value = f32;
    type LeafType = FloatArrayLeaf;
    type BpTreeType = FloatBpTree;

    const IS_NULLABLE: bool = false;
    const COLUMN_TYPE: ThinColumnType = ThinColumnType::Float;

    fn find_first(leaf: &Self::LeafType, value: &Self::Value) -> Option<usize> {
        leaf.find_first(value)
    }

    fn compare_values(a: &Self::Value, b: &Self::Value) -> std::cmp::Ordering {
        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
    }
}

// Float leaf implementation - wraps Array<f32>
#[derive(Debug)]
pub struct FloatArrayLeaf {
    array: ArrayBasic,
}

impl ArrayLeaf<f32> for FloatArrayLeaf {
    fn get(&self, index: usize) -> f32 {
        // Delegate to the underlying Array<f32>
        // Note: This assumes Array<f32> has a get_float method
        // For now, we'll use a placeholder implementation
        // In a real implementation, you'd need to add get_float to Array<f32>
        let value = self.array.get(index);
        f32::from_le_bytes((value as u32).to_le_bytes())
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Floats are never null in Realm
    }

    fn size(&self) -> usize {
        self.array.node.header.size as usize
    }

    fn find_first(&self, value: &f32) -> Option<usize> {
        // Linear search for float values
        for i in 0..self.size() {
            if (self.get(i) - *value).abs() < f32::EPSILON {
                return Some(i);
            }
        }
        None
    }
}

// Float B+Tree implementation - handles B+Tree traversal for floats
pub struct FloatBpTree {
    root: ArrayBasic,
}

impl BpTree<f32> for FloatBpTree {
    type LeafType = FloatArrayLeaf;

    fn get(&self, index: usize) -> f32 {
        if self.root.node.header.is_inner_bptree() {
            // Handle B+Tree traversal
            let (child_index, index_in_child) = self.find_bptree_child(index);
            let child: ArrayBasic = self.root.get_node(child_index).unwrap();
            let leaf = FloatArrayLeaf {
                array: ArrayBasic::from_ref(self.root.node.realm.clone(), child.node.ref_).unwrap(),
            };
            leaf.get(index_in_child)
        } else {
            // Direct leaf access
            let leaf = FloatArrayLeaf {
                array: ArrayBasic::from_ref(self.root.node.realm.clone(), self.root.node.ref_)
                    .unwrap(),
            };
            leaf.get(index)
        }
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Floats are never null
    }

    fn size(&self) -> usize {
        if self.root.node.header.is_inner_bptree() {
            // Extract total elements from B+Tree header
            let payload = self.root.node.payload();
            read_array_value(
                payload,
                self.root.node.header.width(),
                self.root.node.header.size as usize - 1,
            ) as usize
                / 2
        } else {
            self.root.node.header.size as usize
        }
    }

    fn get_leaf_cached<'a>(
        &self,
        index: usize,
        cache: &'a mut LeafCache<Self::LeafType>,
    ) -> (&'a Self::LeafType, usize) {
        if self.root.node.header.is_inner_bptree() {
            let (child_index, index_in_child) = self.find_bptree_child(index);
            let leaf = cache.get_or_insert(child_index, || {
                let child: ArrayBasic = self.root.get_node(child_index).unwrap();
                FloatArrayLeaf {
                    array: ArrayBasic::from_ref(self.root.node.realm.clone(), child.node.ref_)
                        .unwrap(),
                }
            });
            (leaf, index_in_child)
        } else {
            let leaf = cache.get_or_insert(0, || FloatArrayLeaf {
                array: ArrayBasic::from_ref(self.root.node.realm.clone(), self.root.node.ref_)
                    .unwrap(),
            });
            (leaf, index)
        }
    }

    fn get_leaf(&self, index: usize) -> (Self::LeafType, usize) {
        if self.root.node.header.is_inner_bptree() {
            let (child_index, index_in_child) = self.find_bptree_child(index);
            let child: ArrayBasic = self.root.get_node(child_index).unwrap();
            let leaf = FloatArrayLeaf {
                array: ArrayBasic::from_ref(self.root.node.realm.clone(), child.node.ref_).unwrap(),
            };
            (leaf, index_in_child)
        } else {
            let leaf = FloatArrayLeaf {
                array: ArrayBasic::from_ref(self.root.node.realm.clone(), self.root.node.ref_)
                    .unwrap(),
            };
            (leaf, index)
        }
    }
}

impl FloatBpTree {
    fn find_bptree_child(&self, index: usize) -> (usize, usize) {
        // Same logic as in current Array<T> implementation
        let payload = self.root.node.payload();
        let head = read_array_value(payload, self.root.node.header.width(), 0) as usize;
        let is_compact_form = head % 2 != 0;

        if is_compact_form {
            let elements_per_child = head / 2;
            ((index / elements_per_child) + 1, index % elements_per_child)
        } else {
            unimplemented!("Regular B+Tree form not implemented")
        }
    }
}

impl Node for FloatBpTree {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = ArrayBasic::from_ref(realm, ref_)?;
        Ok(Self { root: array })
    }
}

// Factory function for float columns
pub fn create_float_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    column_index: usize,
    attributes: ColumnAttributes,
) -> anyhow::Result<Column_<FloatColumnType>> {
    let tree = FloatBpTree::from_ref(realm, ref_)?;
    Ok(Column_::new(tree, column_index, attributes))
}

// Type alias for convenience
pub type FloatColumn = Column_<FloatColumnType>;
