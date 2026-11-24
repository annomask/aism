//! Statistic is the representation of a statistic used by the monitoring service.

use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};

/// Used by the monitoring service
pub struct Statistic {
    pub name: String,
    pub tags: HashMap<String, String>,
    pub values: HashMap<String, i64>,
}

impl Statistic {
    pub fn new(name: String, tags: HashMap<String, String>, values: HashMap<String, i64>) -> Self {
        Self { name, tags, values }
    }
}

/// StatisticTags is a map that can be merged with others without causing
/// mutations to either map.
#[derive(Default)]
pub struct StatisticTags(HashMap<String, String>);

impl StatisticTags {
    /// merge creates a new map containing the merged contents of tags and t.
    /// If both tags and the receiver map contain the same key, the value in tags
    /// is used in the resulting map.
    ///
    /// merge always returns a usable map.
    pub fn merge(&self, tags: HashMap<String, String>) -> HashMap<String, String> {
        // Add everything in tags to the result.
        let mut out = HashMap::<String, String>::new();
        for (k, v) in tags {
            out.insert(k, v);
        }

        // Only add values from self that don't appear in tags.
        for (k, v) in &self.0 {
            out.entry(k.clone()).or_insert(v.clone());
        }

        out
    }
}

impl Deref for StatisticTags {
    type Target = HashMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StatisticTags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn test_tags_merge() {
        struct Example {
            base: HashMap<String, String>,
            arg: HashMap<String, String>,
            result: HashMap<String, String>,
        }
        let examples = vec![
            Example {
                base: HashMap::new(),
                arg: HashMap::new(),
                result: HashMap::new(),
            },
            Example {
                base: HashMap::new(),
                arg: HashMap::from([("foo".to_string(), "foo".to_string())]),
                result: HashMap::from([("foo".to_string(), "foo".to_string())]),
            },
            Example {
                base: HashMap::from([("foo".to_string(), "foo".to_string())]),
                arg: HashMap::new(),
                result: HashMap::from([("foo".to_string(), "foo".to_string())]),
            },
            Example {
                base: HashMap::from([("foo".to_string(), "foo".to_string())]),
                arg: HashMap::from([("bar".to_string(), "bar".to_string())]),
                result: HashMap::from([
                    ("foo".to_string(), "foo".to_string()),
                    ("bar".to_string(), "bar".to_string()),
                ]),
            },
            Example {
                base: HashMap::from([
                    ("foo".to_string(), "foo".to_string()),
                    ("bar".to_string(), "bar".to_string()),
                ]),
                arg: HashMap::from([("zoo".to_string(), "zoo".to_string())]),
                result: HashMap::from([
                    ("foo".to_string(), "foo".to_string()),
                    ("bar".to_string(), "bar".to_string()),
                    ("zoo".to_string(), "zoo".to_string()),
                ]),
            },
            Example {
                base: HashMap::from([
                    ("foo".to_string(), "foo".to_string()),
                    ("bar".to_string(), "bar".to_string()),
                ]),
                arg: HashMap::from([("bar".to_string(), "newbar".to_string())]),
                result: HashMap::from([
                    ("foo".to_string(), "foo".to_string()),
                    ("bar".to_string(), "newbar".to_string()),
                ]),
            },
        ];

        for (i, example) in examples.into_iter().enumerate() {
            let res = super::StatisticTags(example.base).merge(example.arg);
            assert_eq!(
                res, example.result,
                "[Example {}] got {:?}, expected {:?}",
                i, res, example.result
            );
        }
    }
}
