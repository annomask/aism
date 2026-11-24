use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

use crate::models::{Val, inline_fnv::new_inline_fnv_64a};

/// Row represents a single row returned from the execution of a statement.
#[derive(Default, PartialEq, Eq)]
pub struct Row {
    pub name: String,
    pub tags: HashMap<String, String>,
    pub columns: Vec<String>,
    pub values: Vec<Val>,
    pub partial: bool,
}

impl Row {
    /// Returns true if self contains values for the same series as other.
    pub fn same_series(&self, other: &Row) -> bool {
        self.tags_hash() == other.tags_hash() && self.name == other.name
    }

    /// Returns a hash of tag key/value pairs.
    fn tags_hash(&self) -> u64 {
        let mut h = new_inline_fnv_64a();
        let keys = self.tags_keys();
        for k in keys.iter() {
            h.write(k.as_bytes());
            h.write(self.tags.get(k).unwrap().as_bytes());
        }

        h.sum64()
    }

    /// Returns a sorted list of tag keys.
    fn tags_keys(&self) -> Vec<String> {
        let mut a = Vec::with_capacity(self.tags.len());
        for k in self.tags.keys() {
            a.push(k.clone());
        }
        a.sort();

        a
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by name first.
        if self.name != other.name {
            return self.name.cmp(&other.name);
        }

        // Sort by tag set hash. Tags don't have a meaningful sort order so we
        // just compute a hash and sort by that instead. This allows the tests
        // to receive rows in a predictable order every time.
        self.tags_hash().cmp(&other.tags_hash())
    }
}

/// Rows represents a collection of rows.
pub struct Rows(Vec<Row>);

impl Deref for Rows {
    type Target = Vec<Row>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
