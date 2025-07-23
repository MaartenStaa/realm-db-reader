use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use log::warn;
use tracing::instrument;

use crate::array::long_blobs_array::LongBlobsArray;
use crate::array::small_blobs_array::SmallBlobsArray;
use crate::array::{ArrayStringShort, Expectation, RealmRef};
use crate::node::Node;
use crate::realm::{Realm, RealmNode};

#[derive(Clone)]
pub struct ArrayString<T> {
    size: usize,
    inner: ArrayStringInner<T>,
    phantom: PhantomData<T>,
}

impl<T> Debug for ArrayString<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayString")
            .field("size", &self.size)
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T> Node for ArrayString<T> {
    // #[instrument(target = "ArrayString", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let node = RealmNode::from_ref(Arc::clone(&realm), ref_)?;

        let has_long_strings = node.header.has_refs();
        let inner: ArrayStringInner<T>;
        if !has_long_strings {
            warn!(
                target: "ArrayString",
                "has_long_strings is false, treating as a short string array."
            );

            let is_small = node.header.width_scheme() == 1;
            if !is_small {
                unimplemented!("width_scheme is not 1 for not-long strings");
            }

            inner = ArrayStringInner::Short(ArrayStringShort::from_ref(realm, ref_)?);
        } else {
            let use_big_blobs = node.header.context_flag();
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
            size: node.header.size as usize,
            inner,
            phantom: PhantomData,
        })
    }
}

impl<T> ArrayString<T> {
    pub fn element_count(&self) -> usize {
        match &self.inner {
            ArrayStringInner::Short(short) => short.element_count(),
            ArrayStringInner::SmallBlobs(small_blobs) => small_blobs.element_count(),
            ArrayStringInner::LongBlobs(long_blobs) => long_blobs.element_count(),
        }
    }

    #[instrument(target = "ArrayString", level = "debug")]
    fn get_inner(&self, index: usize, expectation: Expectation) -> anyhow::Result<Option<String>> {
        match &self.inner {
            ArrayStringInner::Short(short) => {
                Ok(short.get(index, expectation).map(|s| s.to_string()))
            }
            ArrayStringInner::SmallBlobs(small_blobs) => Ok(small_blobs
                .get(index, expectation)
                .map(Self::string_from_bytes)),
            ArrayStringInner::LongBlobs(long_blobs) => Ok(long_blobs
                .get(index, expectation)?
                .map(Self::string_from_bytes)),
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

    #[instrument(target = "ArrayString", level = "debug")]
    fn get_strings_internal(
        &self,
        expectation: Expectation,
    ) -> anyhow::Result<Vec<Option<String>>> {
        (0..self.size)
            .map(|index| self.get_inner(index, expectation))
            .collect::<anyhow::Result<Vec<_>>>()
    }
}

impl ArrayString<String> {
    #[instrument(target = "ArrayString", level = "debug")]
    pub fn get_string(
        &self,
        index: usize,
        expectation: Expectation,
    ) -> anyhow::Result<Option<String>> {
        self.get_inner(index, expectation)
    }

    #[instrument(target = "ArrayString", level = "debug")]
    pub fn get_strings(&self, expectation: Expectation) -> anyhow::Result<Vec<String>> {
        Ok(self
            .get_strings_internal(expectation)?
            .into_iter()
            .map(|s| s.unwrap_or_default())
            .collect())
    }
}

impl ArrayString<Option<String>> {
    #[instrument(target = "ArrayString", level = "debug")]
    pub fn get_strings(&self, expectation: Expectation) -> anyhow::Result<Vec<Option<String>>> {
        self.get_strings_internal(expectation)
    }
}

#[derive(Clone)]
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
