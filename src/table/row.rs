use std::collections::HashMap;

use crate::{
    column::Column,
    value::{Backlink, Value},
};

#[derive(Debug)]
pub struct Row<'a> {
    pub columns: HashMap<&'a str, usize>,
    values: &'a [Value],
}

impl<'a> Row<'a> {
    pub fn new(row: &'a [Value], columns: &'a [Box<dyn Column>]) -> Self {
        Self {
            columns: columns
                .iter()
                .enumerate()
                .filter_map(|(index, spec)| spec.name().map(|name| (name, index)))
                .collect(),
            values: row,
        }
    }

    pub fn value(&self, index: usize) -> &'a Value {
        &self.values[index]
    }

    pub fn values(&self) -> &'a [Value] {
        self.values
    }

    pub fn get(&self, column_name: &str) -> Option<&Value> {
        self.columns
            .get(column_name)
            .and_then(|&index| self.values.get(index))
    }

    pub fn backlinks(&self) -> impl Iterator<Item = &Backlink> {
        self.values.iter().filter_map(|value| {
            if let Value::BackLink(backlink) = value {
                Some(backlink)
            } else {
                None
            }
        })
    }
}
