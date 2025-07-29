use crate::array::{Array, ArrayBasic, RealmRef};
use crate::column::{ArrayLeaf, BpTree, Column, ColumnType, LeafCache};
use crate::node::Node;
use crate::realm::Realm;
use crate::spec::ThinColumnType;
use crate::table::ColumnAttributes;
use crate::utils::read_array_value;
use std::sync::Arc;

// Double column type implementation
pub struct DoubleColumnType;

impl ColumnType for DoubleColumnType {
    type Value = f64;
    type LeafType = DoubleArrayLeaf;
    type BpTreeType = DoubleBpTree;

    const IS_NULLABLE: bool = false;
    const COLUMN_TYPE: ThinColumnType = ThinColumnType::Double;

    fn find_first(leaf: &Self::LeafType, value: &Self::Value) -> Option<usize> {
        leaf.find_first(value)
    }

    fn compare_values(a: &Self::Value, b: &Self::Value) -> std::cmp::Ordering {
        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
    }
}

// Double leaf implementation - wraps Array<f64>
pub struct DoubleArrayLeaf {
    array: Array<f64>,
}

impl ArrayLeaf<f64> for DoubleArrayLeaf {
    fn get(&self, index: usize) -> f64 {
        // Delegate to the underlying Array<f64>
        // Note: This assumes Array<f64> has a get_double method
        // For now, we'll use a placeholder implementation
        // In a real implementation, you'd need to add get_double to Array<f64>
        match self.array.get_integer(index) {
            Ok(value) => f64::from_bits(value),
            Err(_) => 0.0, // Default value on error
        }
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Doubles are never null in Realm
    }

    fn size(&self) -> usize {
        self.array.element_count()
    }

    fn find_first(&self, value: &f64) -> Option<usize> {
        // Linear search for double values
        for i in 0..self.size() {
            if (self.get(i) - *value).abs() < f64::EPSILON {
                return Some(i);
            }
        }
        None
    }
}

// Double B+Tree implementation - handles B+Tree traversal for doubles
pub struct DoubleBpTree {
    root: ArrayBasic,
}

impl BpTree<f64> for DoubleBpTree {
    type LeafType = DoubleArrayLeaf;

    fn get(&self, index: usize) -> f64 {
        if self.root.node.header.is_inner_bptree() {
            // Handle B+Tree traversal
            let (child_index, index_in_child) = self.find_bptree_child(index);
            let child: ArrayBasic = self.root.get_node(child_index).unwrap();
            let leaf = DoubleArrayLeaf {
                array: Array::from_ref(self.root.node.realm.clone(), child.node.ref_).unwrap(),
            };
            leaf.get(index_in_child)
        } else {
            // Direct leaf access
            let leaf = DoubleArrayLeaf {
                array: Array::from_ref(self.root.node.realm.clone(), self.root.node.ref_).unwrap(),
            };
            leaf.get(index)
        }
    }

    fn is_null(&self, _index: usize) -> bool {
        false // Doubles are never null
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
                DoubleArrayLeaf {
                    array: Array::from_ref(self.root.node.realm.clone(), child.node.ref_).unwrap(),
                }
            });
            (leaf, index_in_child)
        } else {
            let leaf = cache.get_or_insert(0, || DoubleArrayLeaf {
                array: Array::from_ref(self.root.node.realm.clone(), self.root.node.ref_).unwrap(),
            });
            (leaf, index)
        }
    }

    fn get_leaf(&self, index: usize) -> (Self::LeafType, usize) {
        if self.root.node.header.is_inner_bptree() {
            let (child_index, index_in_child) = self.find_bptree_child(index);
            let child: ArrayBasic = self.root.get_node(child_index).unwrap();
            let leaf = DoubleArrayLeaf {
                array: Array::from_ref(self.root.node.realm.clone(), child.node.ref_).unwrap(),
            };
            (leaf, index_in_child)
        } else {
            let leaf = DoubleArrayLeaf {
                array: Array::from_ref(self.root.node.realm.clone(), self.root.node.ref_).unwrap(),
            };
            (leaf, index)
        }
    }
}

impl DoubleBpTree {
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

impl Node for DoubleBpTree {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let array = ArrayBasic::from_ref(realm, ref_)?;
        Ok(Self { root: array })
    }
}

// Factory function for double columns
pub fn create_double_column(
    realm: Arc<Realm>,
    ref_: RealmRef,
    column_index: usize,
    attributes: ColumnAttributes,
) -> anyhow::Result<Column<DoubleColumnType>> {
    let tree = DoubleBpTree::from_ref(realm, ref_)?;
    Ok(Column::new(tree, column_index, attributes))
}

// Type alias for convenience
pub type DoubleColumn = Column<DoubleColumnType>;
