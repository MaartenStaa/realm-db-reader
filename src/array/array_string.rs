use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::long_blobs_array::LongBlobsArray;
use crate::array::small_blobs_array::SmallBlobsArray;
use crate::array::{Array, ArrayStringShort, RealmRef};
use crate::node::Node;
use crate::realm::Realm;

pub struct ArrayString<T> {
    array: Array,
    inner: ArrayStringInner<T>,
    phantom: PhantomData<T>,
}

impl<T> Debug for ArrayString<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayString")
            .field("array", &self.array)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T> Node for ArrayString<T> {
    // #[instrument(target = "ArrayString")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let array = Array::from_ref(realm.clone(), ref_)?;

        let has_long_strings = array.node.header.has_refs();
        let inner: ArrayStringInner<T>;
        if !has_long_strings {
            warn!(
                target: "ArrayString",
                "has_long_strings is false, treating as a short string array."
            );

            let is_small = array.node.header.width_scheme() == 1;
            if !is_small {
                unimplemented!("width_scheme is not 1 for not-long strings");
            }

            inner = ArrayStringInner::Short(ArrayStringShort::from_ref(realm, ref_)?);
        } else {
            let use_big_blobs = array.node.header.context_flag();
            if !use_big_blobs {
                warn!(
                    target: "ArrayString",
                    "has_long_string is true, use_big_blobs is false, treating as a small blobs array."
                );

                inner = ArrayStringInner::SmallBlobs(SmallBlobsArray::from_ref(realm, ref_)?);
            } else {
                warn!(
                    target: "ArrayString",
                    "has_long_string is true, use_big_blobs is true, treating as a long blobs array."
                );

                inner = ArrayStringInner::LongBlobs(LongBlobsArray::from_ref(realm, ref_)?);
            }
        }

        Ok(Self {
            array,
            inner,
            phantom: PhantomData,
        })
    }
}

impl<T> ArrayString<T> {
    #[instrument(target = "ArrayString")]
    fn get_inner(&self, index: usize) -> anyhow::Result<Option<String>> {
        match &self.inner {
            ArrayStringInner::Short(short) => Ok(short.get(index).map(|s| s.to_string())),
            ArrayStringInner::SmallBlobs(small_blobs) => {
                Ok(small_blobs.get(index).map(Self::string_from_bytes))
            }
            ArrayStringInner::LongBlobs(long_blobs) => {
                Ok(long_blobs.get(index)?.map(Self::string_from_bytes))
            }
        }
    }

    fn string_from_bytes(mut bytes: Vec<u8>) -> String {
        assert!(
            !bytes.is_empty(),
            "string cannot be empty (should have a trailing \\0"
        );
        assert!(
            bytes[bytes.len() - 1] == 0,
            "string must end with a \\0 byte"
        );

        bytes.pop();

        unsafe { String::from_utf8_unchecked(bytes) }
    }

    #[instrument(target = "ArrayString")]
    fn get_strings_internal(&self) -> anyhow::Result<Vec<Option<String>>> {
        let size = self.array.node.header.size as usize;
        (0..size)
            .map(|index| self.get_inner(index))
            .collect::<anyhow::Result<Vec<_>>>()
    }
}

impl ArrayString<String> {
    #[instrument(target = "ArrayString")]
    pub fn get_string(&self, index: usize) -> anyhow::Result<Option<String>> {
        self.get_inner(index)
    }

    #[instrument(target = "ArrayString")]
    pub fn get_strings(&self) -> anyhow::Result<Vec<String>> {
        Ok(self
            .get_strings_internal()?
            .into_iter()
            .map(|s| s.unwrap_or_default())
            .collect())
    }
}

impl ArrayString<Option<String>> {
    #[instrument(target = "ArrayString")]
    pub fn get_strings(&self) -> anyhow::Result<Vec<Option<String>>> {
        self.get_strings_internal()
    }
}

enum ArrayStringInner<T> {
    Short(ArrayStringShort<T>),
    SmallBlobs(SmallBlobsArray),
    LongBlobs(LongBlobsArray),
}

impl<T> Debug for ArrayStringInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrayStringInner::Short(short) => write!(f, "Short({:?})", short),
            ArrayStringInner::SmallBlobs(small_blobs) => write!(f, "SmallBlobs({:?})", small_blobs),
            ArrayStringInner::LongBlobs(long_blobs) => write!(f, "LongBlobs({:?})", long_blobs),
        }
    }
}
