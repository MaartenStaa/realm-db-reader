use chrono::{DateTime, Utc};

use crate::table::Table;

/// Should match `crate::spec::ColumnType`
#[allow(unused)]
#[derive(Debug, Clone)]
pub enum Value {
    Int(u64),
    Bool(bool),
    String(String),
    OldStringEnum(String),
    Binary(Vec<u8>),
    Table(Table),
    OldMixed,
    OldDateTime,
    Timestamp(DateTime<Utc>),
    Float(f32),
    Double(f64),
    Reserved4,
    Link,
    LinkList(Vec<usize>),
    BackLink,

    // Nullable
    None,
}
