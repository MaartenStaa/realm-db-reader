use chrono::{DateTime, Utc};

use crate::array::RealmRef;

/// Should match `crate::spec::ColumnType`
#[allow(unused)]
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Value {
    Int(i64),
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

impl Value {
    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Backlink {
    pub origin_table_index: usize,
    pub origin_column_index: usize,
    pub row_indexes: Vec<usize>,
}

impl Backlink {
    pub fn new(
        origin_table_index: usize,
        origin_column_index: usize,
        row_indexes: Vec<usize>,
    ) -> Self {
        Self {
            origin_table_index,
            origin_column_index,
            row_indexes,
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<chrono::DateTime<Utc>> for Value {
    fn from(value: DateTime<Utc>) -> Self {
        Value::Timestamp(value)
    }
}

impl From<Vec<usize>> for Value {
    fn from(value: Vec<usize>) -> Self {
        Value::LinkList(value)
    }
}

impl From<Backlink> for Value {
    fn from(value: Backlink) -> Self {
        Value::BackLink(value)
    }
}

impl<T> From<Option<T>> for Value
where
    T: Into<Value>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Value::None,
        }
    }
}
