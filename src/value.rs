use chrono::{DateTime, Utc};

use crate::{array::RealmRef, table::Table};

/// Should match `crate::spec::ColumnType`
#[allow(unused)]
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Value {
    Int(u64),
    Bool(bool),
    String(String),
    OldStringEnum(String),
    Binary(Vec<u8>),
    // Table(Table),
    Table(RealmRef),
    OldMixed,
    OldDateTime,
    Timestamp(DateTime<Utc>),
    // Float(f32),
    // Double(f64),
    Float,
    Double,
    Reserved4,
    Link,
    LinkList(Vec<usize>),
    BackLink(Backlink),

    // Nullable
    None,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Backlink {
    pub origin_table_index: usize,
    pub origin_column_index: usize,
    pub row_index: usize,
}

impl Backlink {
    pub fn new(origin_table_index: usize, origin_column_index: usize, row_index: usize) -> Self {
        Self {
            origin_table_index,
            origin_column_index,
            row_index,
        }
    }
}
