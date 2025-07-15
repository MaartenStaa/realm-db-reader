use std::fmt::Debug;

use crate::array::FromU64;

#[derive(Copy, Clone)]
pub struct ColumnAttributes(u64);

impl ColumnAttributes {
    const INDEXED: u64 = 1 << 0;
    const UNIQUE: u64 = 1 << 1;
    const RESERVED: u64 = 1 << 2;
    const STRONG_LINKS: u64 = 1 << 3;
    const NULLABLE: u64 = 1 << 4;
    const LIST: u64 = 1 << 5;
    const DICTIONARY: u64 = 1 << 6;
    const SET: u64 = 1 << 7;
    const FULLTEXT_INDEXED: u64 = 1 << 8;

    /// Mask for the collection type (list, dictionary, set)
    const COLLECTION_TYPE_MASK: u64 = Self::LIST | Self::DICTIONARY | Self::SET;

    pub fn new(attributes: u64) -> Self {
        Self(attributes)
    }

    pub fn is_indexed(&self) -> bool {
        self.0 & Self::INDEXED != 0
    }

    pub fn is_unique(&self) -> bool {
        self.0 & Self::UNIQUE != 0
    }

    pub fn is_reserved(&self) -> bool {
        self.0 & Self::RESERVED != 0
    }

    pub fn is_strong_links(&self) -> bool {
        self.0 & Self::STRONG_LINKS != 0
    }

    pub fn is_nullable(&self) -> bool {
        self.0 & Self::NULLABLE != 0
    }

    pub fn is_list(&self) -> bool {
        self.0 & Self::LIST != 0
    }

    pub fn is_dictionary(&self) -> bool {
        self.0 & Self::DICTIONARY != 0
    }

    pub fn is_set(&self) -> bool {
        self.0 & Self::SET != 0
    }

    pub fn is_fulltext_indexed(&self) -> bool {
        self.0 & Self::FULLTEXT_INDEXED != 0
    }

    pub fn is_collection(&self) -> bool {
        self.0 & Self::COLLECTION_TYPE_MASK != 0
    }
}

impl Debug for ColumnAttributes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("ColumnAttributes");
        if self.is_indexed() {
            s.field("indexed", &true);
        }
        if self.is_unique() {
            s.field("unique", &true);
        }
        if self.is_reserved() {
            s.field("reserved", &true);
        }
        if self.is_strong_links() {
            s.field("strong_links", &true);
        }
        if self.is_nullable() {
            s.field("nullable", &true);
        }
        if self.is_list() {
            s.field("list", &true);
        }
        if self.is_dictionary() {
            s.field("dictionary", &true);
        }
        if self.is_set() {
            s.field("set", &true);
        }
        if self.is_fulltext_indexed() {
            s.field("fulltext_indexed", &true);
        }
        s.finish()
    }
}

impl FromU64 for ColumnAttributes {
    fn from_u64(attributes: u64) -> Self {
        Self(attributes)
    }
}
