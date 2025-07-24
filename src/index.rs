use std::borrow::Cow;
use std::sync::Arc;

use tracing::instrument;

use crate::array::{ArrayBasic, RealmRef, RefOrTaggedValue};
use crate::node::Node;
use crate::realm::Realm;
use crate::utils;
use crate::value::Value;

#[derive(Debug, Clone)]
pub struct Index {
    array: ArrayBasic,
    offsets: ArrayBasic,
}

impl Node for Index {
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = unsafe { ArrayBasic::from_ref_bypass_bptree(Arc::clone(&realm), ref_)? };
        assert!(array.node.header.size >= 1);

        let keys = array.get_node(0)?;

        Ok(Self {
            array,
            offsets: keys,
        })
    }
}

type KeyType = u32;

impl Index {
    const KEY_SIZE: u8 = 4; // 32 bits for the key
    const KEY_SIZE_BITS: u8 = Self::KEY_SIZE * 8;

    #[instrument(target = "Index", level = "debug", skip(self))]
    pub fn find_first(&self, value: &Value) -> anyhow::Result<Option<usize>> {
        let value = Self::coerce_to_string(value);

        let mut value_offset: usize = 0;
        let mut key = Self::create_key(&value);

        log::debug!(target: "Index", "finding first occurrence of '{value:?}', key = {key:?}");

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
            log::debug!(target: "Index", "lower_bound: value = {value:?}, key = {key:?}, pos = {pos}");

            // If key is outside range, we know there can be no match.
            if pos == current_index.offsets.node.header.size as usize {
                log::info!(target: "Index", "No match found for key = {key:?} in current_index");

                return Ok(None);
            }

            // assert!(pos <= self.components.len());
            assert!(pos < current_index.array.node.header.size as usize);

            let pos_refs = pos + 1;
            let ref_ = current_index.array.get(pos_refs);

            if current_index.array.node.header.is_inner_bptree() {
                let ref_ = RealmRef::new(ref_ as usize);
                current_index =
                    Cow::Owned(Self::from_ref(Arc::clone(&self.array.node.realm), ref_)?);

                log::info!(target: "Index", "Going to sub-index at {ref_:?} (current was inner B+Tree)");

                continue;
            }

            let stored_key = current_index.offsets.get(pos) as KeyType;
            if stored_key != key {
                log::warn!(
                    target: "Index", "Key mismatch: stored_key = {stored_key:?}, expected key = {key:?} at pos = {pos}",
                );

                return Ok(None);
            }

            match RefOrTaggedValue::from_raw(ref_) {
                RefOrTaggedValue::TaggedRef(row_index) => {
                    return Ok(Some(row_index as usize));
                }
                RefOrTaggedValue::Ref(ref_) => {
                    let array = unsafe {
                        ArrayBasic::from_ref_bypass_bptree(
                            Arc::clone(&self.array.node.realm),
                            ref_,
                        )?
                    };
                    let is_sub_index = array.node.header.context_flag();

                    if !is_sub_index {
                        log::info!(
                            target: "Index",
                            "Found row index at pos {pos}: {ref_:?}, value = {:?}",
                            value
                        );
                        return Ok(Some(array.get(0) as usize));
                    }

                    // Otherwise, go into the sub-index.
                    current_index =
                        Cow::Owned(Self::from_ref(Arc::clone(&self.array.node.realm), ref_)?);

                    log::info!(target: "Index", "going to sub-index at {ref_:?}");

                    // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
                    value_offset += Self::KEY_SIZE as usize;

                    // Update 4 byte index key
                    key = Self::create_key_with_offset(&value, value_offset);
                }
            }
        }
    }

    fn create_key(value: &[u8]) -> KeyType {
        let mut key: KeyType = 0;

        for (i, c) in value.iter().enumerate().take(Self::KEY_SIZE as usize) {
            // Index 0 shift left by 24, index 1 by 16...
            let shl = (Self::KEY_SIZE - 1 - i as u8) * 8;
            key |= (*c as u32) << shl;
        }

        key
    }

    /// Index works as follows: All non-NULL values are stored as if they had appended an 'X'
    /// character at the end. So "foo" is stored as if it was "fooX", and "" (empty string) is
    /// stored as "X". And NULLs are stored as empty strings.
    fn create_key_with_offset(value: &[u8], offset: usize) -> u32 {
        if offset > value.len() {
            return 0;
        }

        // For very short strings
        let tail = value.len() - offset;
        if tail < Self::KEY_SIZE as usize {
            let mut buf = [b'\0'; Self::KEY_SIZE as usize];
            buf[tail] = b'X';
            for (i, c) in value.iter().skip(offset).enumerate() {
                buf[i] = *c;
            }
            return Self::create_key(&buf);
        }

        Self::create_key(&value[offset..])
    }

    fn coerce_to_string(value: &Value) -> Cow<'_, [u8]> {
        match value {
            Value::String(s) => Cow::Borrowed(s.as_bytes()),
            Value::Int(n) => {
                let mut str = Vec::with_capacity(std::mem::size_of_val(n));
                str.extend_from_slice(&n.to_le_bytes());
                Cow::Owned(str)
            }
            Value::Bool(b) => Cow::Owned(vec![if *b { 1 } else { 0 }]),
            Value::Timestamp(dt) => {
                let s = dt.timestamp() as u64;
                let ns = dt.timestamp_subsec_nanos();
                log::debug!(target: "Index", "coercing timestamp {dt:?} to string, value = {s} . {ns}");
                let mut str =
                    Vec::with_capacity(std::mem::size_of_val(&s) + std::mem::size_of_val(&ns));
                str.extend_from_slice(&s.to_le_bytes());
                str.extend_from_slice(&ns.to_le_bytes());
                Cow::Owned(str)
            }
            _ => unimplemented!("Unsupported value type for coercion to string: {value:?}"),
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
