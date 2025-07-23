use std::collections::HashMap;

use crate::{
    table::spec::ColumnSpec,
    value::{Backlink, Value},
};

#[derive(Debug)]
pub struct Row<'a> {
    columns: HashMap<&'a str, usize>,
    values: &'a [Value],
}

impl<'a> Row<'a> {
    pub fn new(row: &'a [Value], columns: &'a [ColumnSpec]) -> Self {
        Self {
            columns: columns
                .iter()
                .enumerate()
                .filter_map(|(index, spec)| match spec {
                    ColumnSpec::Regular { name, .. } => Some((name.as_str(), index)),
                    ColumnSpec::BackLink { .. } => None,
                })
                .collect(),
            values: row,
        }
    }

    pub fn values(&self) -> &'a [Value] {
        self.values
    }

    pub fn get(&self, column_name: &str) -> Option<&Value> {
        self.columns
            .get(column_name)
            .and_then(|&index| self.values.get(index))
    }

    pub fn backlinks(&self) -> impl Iterator<Item = Backlink> {
        self.values.iter().filter_map(|value| {
            if let Value::BackLink(backlink) = value {
                Some(*backlink)
            } else {
                None
            }
        })
    }
}
