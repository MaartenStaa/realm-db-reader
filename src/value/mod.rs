use chrono::{DateTime, Utc};

use crate::table::Row;

mod from;
mod into;

pub const ARRAY_VALUE_KEY: &str = "!ARRAY_VALUE";

/// Should match `crate::spec::ColumnType`
#[allow(unused)]
#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Bool(bool),
    String(String),
    OldStringEnum(String),
    Binary(Vec<u8>),
    Table(Vec<Row<'static>>),
    OldMixed,
    OldDateTime,
    Timestamp(DateTime<Utc>),
    Float(f32),
    Double(f64),
    Reserved4,
    Link,
    LinkList(Vec<Link>),
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
pub struct Link {
    pub target_table_index: usize,
    pub row_index: usize,
}

impl Link {
    pub fn new(target_table_index: usize, row_index: usize) -> Self {
        Self {
            target_table_index,
            row_index,
        }
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
