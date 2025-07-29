use crate::array::FromU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(unused)]
pub enum ColumnType {
    Int = 0,
    Bool = 1,
    String = 2,
    OldStringEnum = 3, // double refs
    Binary = 4,
    Table = 5,
    OldMixed = 6,
    OldDateTime = 7,
    Timestamp = 8,
    Float = 9,
    Double = 10,
    Reserved4 = 11, // Decimal
    Link = 12,
    LinkList = 13,
    BackLink = 14,
}

impl FromU64 for ColumnType {
    fn from_u64(value: u64) -> Self {
        unsafe { std::mem::transmute(value as u8) }
    }
}

impl ColumnType {
    pub fn has_sub_spec(&self) -> bool {
        matches!(
            self,
            ColumnType::Table | ColumnType::Link | ColumnType::LinkList | ColumnType::BackLink
        )
    }

    pub fn sub_spec_entries_count(&self) -> usize {
        match self {
            ColumnType::Table | ColumnType::Link | ColumnType::LinkList => 1,
            ColumnType::BackLink => 2,
            _ => 0,
        }
    }

    pub fn as_thin_column_type(self) -> anyhow::Result<ThinColumnType> {
        match self {
            ColumnType::Table | ColumnType::Link | ColumnType::LinkList | ColumnType::BackLink => {
                anyhow::bail!("{self:?} is not a thin column type")
            }
            _ => Ok(ThinColumnType::from_u64(self as u64)),
        }
    }
}

/// Same as [`ColumnType`], but without the variants that have sub-specs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[allow(unused)]
pub enum ThinColumnType {
    Int = 0,
    Bool = 1,
    String = 2,
    OldStringEnum = 3, // double refs
    Binary = 4,
    OldMixed = 6,
    OldDateTime = 7,
    Timestamp = 8,
    Float = 9,
    Double = 10,
    Reserved4 = 11, // Decimal
}

impl FromU64 for ThinColumnType {
    fn from_u64(value: u64) -> Self {
        unsafe { std::mem::transmute(value as u8) }
    }
}

impl ThinColumnType {
    pub fn as_column_type(self) -> ColumnType {
        unsafe { std::mem::transmute(self as u8) }
    }
}
