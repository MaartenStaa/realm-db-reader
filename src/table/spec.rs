use crate::spec::{ColumnType, ThinColumnType};
use crate::table::column::ColumnAttributes;
use crate::table::header::TableHeader;

#[derive(Debug)]
pub enum FatColumnType {
    Thin(ThinColumnType),
    Table(TableHeader),
    // TODO: Payload for these
    Link { target_table_index: usize },
    LinkList { target_table_index: usize },
}

impl FatColumnType {
    fn as_column_type(&self) -> ColumnType {
        match self {
            FatColumnType::Thin(type_) => type_.as_column_type(),
            FatColumnType::Table(_) => ColumnType::Table,
            FatColumnType::Link { .. } => ColumnType::Link,
            FatColumnType::LinkList { .. } => ColumnType::LinkList,
        }
    }
}

// #[derive(Debug, Clone)]
// pub enum ColumnSpec {
//     Regular {
//         type_: FatColumnType,
//         data_array_index: usize,
//         name: String,
//         attributes: ColumnAttributes,
//     },
//     /// Backlink columns don't have a name, so acount for this
//     /// as a separate column spec variant.
//     BackLink {
//         data_array_index: usize,
//         attributes: ColumnAttributes,
//         origin_table_index: usize,
//         origin_column_index: usize,
//     },
// }

// impl ColumnSpec {
//     pub(crate) fn as_column_type(&self) -> ColumnType {
//         match self {
//             ColumnSpec::Regular { type_, .. } => type_.as_column_type(),
//             ColumnSpec::BackLink { .. } => ColumnType::BackLink,
//         }
//     }
//
//     pub(crate) fn get_attributes(&self) -> ColumnAttributes {
//         match self {
//             ColumnSpec::Regular { attributes, .. } => *attributes,
//             ColumnSpec::BackLink { attributes, .. } => *attributes,
//         }
//     }
//
//     pub(crate) fn get_data_array_index(&self) -> usize {
//         match self {
//             ColumnSpec::Regular {
//                 data_array_index: data_index,
//                 ..
//             } => *data_index,
//             ColumnSpec::BackLink {
//                 data_array_index: data_index,
//                 ..
//             } => *data_index,
//         }
//     }
// }
