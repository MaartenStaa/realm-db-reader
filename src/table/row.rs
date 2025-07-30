use std::{borrow::Cow, collections::HashMap};

use crate::value::{Backlink, Value};

#[derive(Debug, Clone)]
pub struct Row<'a> {
    values: HashMap<Cow<'a, str>, Value>,
    backlinks: Vec<Backlink>,
}

impl<'a> Row<'a> {
    pub fn new(mut row: Vec<Value>, column_names: Vec<Cow<'a, str>>) -> Self {
        let backlinks = row
            .extract_if(.., |v| matches!(v, Value::BackLink(_)))
            .map(|v| {
                v.try_into()
                    .expect("already matched the right value variant")
            })
            .collect();
        let values = column_names
            .into_iter()
            .enumerate()
            .rev()
            .map(|(index, name)| (name, row.remove(index)))
            .collect();

        Self { values, backlinks }
    }

    pub fn entries(&self) -> impl Iterator<Item = (&Cow<'a, str>, &Value)> {
        self.values.iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.values.values()
    }

    pub fn get(&self, column_name: &str) -> Option<&Value> {
        self.values.get(column_name)
    }

    pub fn take(&mut self, column_name: &str) -> Option<Value> {
        self.values.remove(column_name)
    }

    pub fn backlinks(&self) -> impl Iterator<Item = &Backlink> {
        self.backlinks.iter()
    }

    pub fn has_field(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }
}
