use std::borrow::Cow;
use std::sync::Arc;

use anyhow::bail;
use tracing::instrument;

use crate::array::{ArrayBasic, RealmRef, RefOrTaggedValue};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};
use crate::utils;
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct Index {
    array: ArrayBasic,
    offsets: ArrayBasic,
    // keys: Vec<u8>,
    // offsets_size: usize,
    // is_inner: bool,
    // components: Vec<IndexComponent>,
}

#[derive(Debug, Clone)]
pub enum IndexComponent {
    RowIndex(usize),
    RowIndexes(Vec<usize>),
    SubIndex(Box<Index>),
}

impl Node for Index {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = unsafe { ArrayBasic::from_ref_bypass_bptree(Arc::clone(&realm), ref_)? };
        assert!(array.node.header.size >= 1);

        let (keys, offsets_size) = {
            let keys_node: ArrayBasic = array.get_node(0)?;
            (
                // keys_node.node.payload().to_vec(),
                // keys_node.node.header.size as usize,
                keys_node, 0,
            )
        };

        // let is_inner_bptree = array.node.header.is_inner_bptree();
        // let components = if is_inner_bptree {
        //     assert!(
        //         array.node.header.context_flag(),
        //         "Inner B+Tree nodes should have context flag set"
        //     );
        //
        //     let size = array.node.header.size as usize;
        //     (1..size)
        //         .map(|i| -> anyhow::Result<_> {
        //             let Some(sub_index_ref) = array.get_ref(i) else {
        //                 bail!("Sub index ref is None");
        //             };
        //             let sub_index = Index::from_ref(Arc::clone(&realm), sub_index_ref)?;
        //             Ok(IndexComponent::SubIndex(Box::new(sub_index)))
        //         })
        //         .collect::<anyhow::Result<Vec<_>>>()?
        // } else {
        //     let size = array.node.header.size as usize;
        //     (1..size)
        //         .map(|i| {
        //             match array.get_ref_or_tagged_value(i) {
        //                 Some(RefOrTaggedValue::Ref(ref_)) => {
        //                     // If it's a reference to a node with refs, then it's a sub-index,
        //                     // otherwise it's an array of row indexes.
        //                     let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;
        //                     if node.header.has_refs() {
        //                         let sub_index = Index::from_ref(Arc::clone(&realm), ref_)?;
        //                         Ok(IndexComponent::SubIndex(Box::new(sub_index)))
        //                     } else {
        //                         let array = ArrayBasic::from_ref(Arc::clone(&realm), ref_)?;
        //                         let row_indexes: Vec<usize> = (0..array.node.header.size)
        //                             .map(|i| array.get(i as usize) as usize)
        //                             .collect();
        //
        //                         Ok(IndexComponent::RowIndexes(row_indexes))
        //                     }
        //                 }
        //                 Some(RefOrTaggedValue::TaggedRef(value)) => {
        //                     // If it's a tagged value, then it's a single row index.
        //                     Ok(IndexComponent::RowIndex(value as usize))
        //                 }
        //                 None => {
        //                     bail!("Index component at index {i} is None (in node {array:?})");
        //                 }
        //             }
        //         })
        //         .collect::<anyhow::Result<Vec<_>>>()?
        // };

        Ok(Self {
            array,
            offsets: keys,
            // keys,
            // offsets_size,
            // is_inner: is_inner_bptree,
            // components,
        })
    }
}

type KeyType = u32;

impl Index {
    const KEY_SIZE: u8 = 4; // 32 bits for the key
    const KEY_SIZE_BITS: u8 = Self::KEY_SIZE * 8;

    #[instrument(target = "Index", skip(self))]
    pub fn find_first(&self, value: &Value) -> anyhow::Result<Option<usize>> {
        let value = Self::coerce_to_string(value);

        let mut value_offset: usize = 0;
        let mut key = Self::create_key(&value);

        log::debug!(target: "Index", "finding first occurrence of '{value}', key = {key:?}");

        let mut current_index = Cow::Borrowed(self);
        loop {
            log::debug!(
                target: "Index", "current_index: {current_index:?}, value_offset = {value_offset}, key = {key:?}"
            );

            // Find the position matching the key
            let pos = Self::lower_bound(
                current_index.offsets.node.payload(),
                current_index.offsets.node.header.size as usize,
                key as u64,
            );
            log::debug!(target: "Index", "lower_bound: value = {value:?}, key = {key:?}, pos = {pos}",);

            // If key is outside range, we know there can be no match.
            if pos == current_index.offsets.node.header.size as usize {
                return Ok(None);
            }

            // assert!(pos <= self.components.len());
            assert!(pos < current_index.array.node.header.size as usize);

            let pos_refs = pos + 1;
            let ref_ = current_index.array.get(pos_refs);

            if current_index.array.node.header.is_inner_bptree() {
                current_index = Cow::Owned(Self::from_ref(
                    Arc::clone(&self.array.node.realm),
                    RealmRef::new(ref_ as usize),
                )?);
                continue;
            }

            // let stored_key = current_index.offsets.get(pos) as KeyType;
            // if stored_key != key {
            //     log::warn!(
            //         target: "Index", "Key mismatch: stored_key = {stored_key:?}, expected key = {key:?} at pos = {pos}"
            //     );
            //
            //     return Ok(None);
            // }

            match RefOrTaggedValue::from_raw(ref_) {
                RefOrTaggedValue::TaggedRef(row_index) => {
                    return Ok(Some(row_index as usize));
                }
                RefOrTaggedValue::Ref(ref_) => {
                    let array = ArrayBasic::from_ref(Arc::clone(&self.array.node.realm), ref_)?;
                    let is_sub_index = array.node.header.context_flag();

                    if !is_sub_index {
                        return Ok(Some(array.get(0) as usize));
                    }

                    // Otherwise, go into the sub-index.
                    current_index =
                        Cow::Owned(Self::from_ref(Arc::clone(&self.array.node.realm), ref_)?);

                    // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
                    value_offset += Self::KEY_SIZE as usize;

                    // Update 4 byte index key
                    key = Self::create_key_with_offset(&value, value_offset);
                }
            }

            // Get entry under key
            // match &self.components[pos] {
            //     IndexComponent::RowIndex(n) => {
            //         return Some(*n);
            //     }
            //     IndexComponent::RowIndexes(ns) => {
            //         return Some(ns[0]);
            //     }
            //     IndexComponent::SubIndex(sub_index) => {
            //         // Update key if we're not in an inner node
            //         if !current_index.is_inner {
            //             value_offset += Self::KEY_SIZE as usize;
            //             key = Self::create_key_with_offset(&value, value_offset);
            //         }
            //
            //         current_index = sub_index;
            //     }
            // }
        }
    }

    fn create_key(value: &str) -> KeyType {
        let mut key: KeyType = 0;

        for (i, c) in value.char_indices().take(Self::KEY_SIZE as usize) {
            key |= (c as u32) << (i * 8);
        }

        key
    }

    /// Index works as follows: All non-NULL values are stored as if they had appended an 'X'
    /// character at the end. So "foo" is stored as if it was "fooX", and "" (empty string) is
    /// stored as "X". And NULLs are stored as empty strings.
    fn create_key_with_offset(value: &str, offset: usize) -> u32 {
        if offset > value.len() {
            return 0;
        }

        // For very short strings
        let tail = value.len() - offset;
        if tail < Self::KEY_SIZE as usize {
            let mut buf = [b'\0'; Self::KEY_SIZE as usize];
            buf[tail] = b'X';
            for (i, c) in value.char_indices() {
                buf[i] = c as u8;
            }
            return Self::create_key(&unsafe { String::from_utf8_unchecked(buf.to_vec()) });
        }

        Self::create_key(&value[offset..])
    }

    fn coerce_to_string(value: &Value) -> Cow<'_, str> {
        match value {
            Value::String(s) => Cow::Borrowed(s),
            Value::Int(n) => Cow::Owned(n.to_string()),
            Value::Bool(b) => Cow::Owned(b.to_string()),
            _ => unimplemented!("Unsupported value type for coercion to string"),
        }
    }

    // Lower/upper bound in sorted sequence
    // ------------------------------------
    //
    //   3 3 3 4 4 4 5 6 7 9 9 9
    //   ^     ^     ^     ^     ^
    //   |     |     |     |     |
    //   |     |     |     |      -- Lower and upper bound of 15
    //   |     |     |     |
    //   |     |     |      -- Lower and upper bound of 8
    //   |     |     |
    //   |     |      -- Upper bound of 4
    //   |     |
    //   |      -- Lower bound of 4
    //   |
    //    -- Lower and upper bound of 1
    //
    // These functions are semantically identical to std::lower_bound() and
    // std::upper_bound().
    //
    // We currently use binary search. See for example
    // http://www.tbray.org/ongoing/When/200x/2003/03/22/Binary.
    fn lower_bound(keys: &[u8], mut size: usize, value: u64) -> usize {
        // The binary search used here is carefully optimized. Key trick is to use a single
        // loop controlling variable (size) instead of high/low pair, and to keep updates
        // to size done inside the loop independent of comparisons. Further key to speed
        // is to avoid branching inside the loop, using conditional moves instead. This
        // provides robust performance for random searches, though predictable searches
        // might be slightly faster if we used branches instead. The loop unrolling yields
        // a final 5-20% speedup depending on circumstances.

        let mut low = 0;

        while size >= 8 {
            // The following code (at X, Y and Z) is 3 times manually unrolled instances of (A) below.
            // These code blocks must be kept in sync. Meassurements indicate 3 times unrolling to give
            // the best performance. See (A) for comments on the loop body.
            // (X)
            let mut half = size / 2;
            let mut other_half = size - half;
            let mut probe = low + half;
            let mut other_low = low + other_half;
            let mut v = utils::read_array_value(keys, Self::KEY_SIZE_BITS, probe);
            size = half;
            low = if v < value { other_low } else { low };

            // (Y)
            half = size / 2;
            other_half = size - half;
            probe = low + half;
            other_low = low + other_half;
            v = utils::read_array_value(keys, Self::KEY_SIZE_BITS, probe);
            size = half;
            low = if v < value { other_low } else { low };

            // (Z)
            half = size / 2;
            other_half = size - half;
            probe = low + half;
            other_low = low + other_half;
            v = utils::read_array_value(keys, Self::KEY_SIZE_BITS, probe);
            size = half;
            low = if v < value { other_low } else { low };
        }
        while size > 0 {
            // (A)
            // To understand the idea in this code, please note that
            // for performance, computation of size for the next iteration
            // MUST be INDEPENDENT of the conditional. This allows the
            // processor to unroll the loop as fast as possible, and it
            // minimizes the length of dependence chains leading up to branches.
            // Making the unfolding of the loop independent of the data being
            // searched, also minimizes the delays incurred by branch
            // mispredictions, because they can be determined earlier
            // and the speculation corrected earlier.

            // Counterintuitive:
            // To make size independent of data, we cannot always split the
            // range at the theoretical optimal point. When we determine that
            // the key is larger than the probe at some index K, and prepare
            // to search the upper part of the range, you would normally start
            // the search at the next index, K+1, to get the shortest range.
            // We can only do this when splitting a range with odd number of entries.
            // If there is an even number of entries we search from K instead of K+1.
            // This potentially leads to redundant comparisons, but in practice we
            // gain more performance by making the changes to size predictable.

            // if size is even, half and other_half are the same.
            // if size is odd, half is one less than other_half.
            let half = size / 2;
            let other_half = size - half;
            let probe = low + half;
            let other_low = low + other_half;
            let v = utils::read_array_value(keys, Self::KEY_SIZE_BITS, probe);
            size = half;
            // for max performance, the line below should compile into a conditional
            // move instruction. Not all compilers do this. To maximize chance
            // of succes, no computation should be done in the branches of the
            // conditional.
            low = if v < value { other_low } else { low };
        }

        low
    }
}
