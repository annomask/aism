use std::{
    cmp::Ordering,
    collections::HashMap,
    hash::Hash,
    ops::{Deref, DerefMut},
};

use crate::models::points::Val;
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeStruct, Serializer},
};

/// IntegerPoint represents a point with a i64 value.
#[derive(Clone, Default, PartialEq)]
pub struct IntegerPoint {
    pub(crate) name: String,
    pub(crate) tags: Tags,

    pub(crate) time: i64,
    pub(crate) value: i64,
    pub(crate) aux: Vec<Val>, // TODO: i64 ?

    // Total number of points that were combined into this point from an aggregate.
    // If this is zero, the point is not the result of an aggregate function.
    pub(crate) aggregated: u32,
    pub(crate) none: bool,
}

impl IntegerPoint {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tags(&self) -> Tags {
        self.tags.clone()
    }

    pub fn time(&self) -> i64 {
        self.time
    }

    pub fn value(&self) -> i64 {
        self.value
    }
}

impl Eq for IntegerPoint {}

impl Ord for IntegerPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare timestamp first, if timestamp is equal, then compare value.
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => self
                .value
                .partial_cmp(&other.value)
                .unwrap_or(Ordering::Equal),
            non_eq => non_eq,
        }
    }
}

impl PartialOrd for IntegerPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for IntegerPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("IntegerPoint", 3)?;
        s.serialize_field("name", &self.name)?;
        // s.serialize_field("tags", &self.tags)?;
        s.serialize_field("time", &self.time)?;
        s.serialize_field("value", &self.value)?;
        s.end()
    }
}

/// IntegerPoints represents a slice of points sortable by value.
pub struct IntegerPoints(Vec<IntegerPoint>);

impl Deref for IntegerPoints {
    type Target = Vec<IntegerPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IntegerPoints {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

//////////////////////////////////////////////////////////////////

/// UnsignedPoint represents a point with a u64 value.
#[derive(Clone, Default, PartialEq)]
pub struct UnsignedPoint {
    pub(crate) name: String,
    pub(crate) tags: Tags,

    pub(crate) time: i64,
    pub(crate) value: u64,
    pub(crate) aux: Vec<Val>,

    // Total number of points that were combined into this point from an aggregate.
    // If this is zero, the point is not the result of an aggregate function.
    pub(crate) aggregated: u32,
    pub(crate) none: bool,
}

impl UnsignedPoint {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tags(&self) -> Tags {
        self.tags.clone()
    }

    pub fn time(&self) -> i64 {
        self.time
    }

    pub fn value(&self) -> u64 {
        self.value
    }
}

impl Eq for UnsignedPoint {}

impl Ord for UnsignedPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare timestamp first, if timestamp is equal, then compare value.
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => self
                .value
                .partial_cmp(&other.value)
                .unwrap_or(Ordering::Equal),
            non_eq => non_eq,
        }
    }
}

impl PartialOrd for UnsignedPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for UnsignedPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("UnsignedPoint", 3)?;
        s.serialize_field("name", &self.name)?;
        // s.serialize_field("tags", &self.tags)?;
        s.serialize_field("time", &self.time)?;
        s.serialize_field("value", &self.value)?;
        s.end()
    }
}

/// UnsignedPoints represents a slice of points sortable by value.
pub struct UnsignedPoints(Vec<UnsignedPoint>);

impl Deref for UnsignedPoints {
    type Target = Vec<UnsignedPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UnsignedPoints {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// FloatPoint represents a point with a f64 value.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct FloatPoint {
    pub(crate) name: String,
    pub(crate) tags: Tags,

    pub(crate) time: i64,
    pub value: f64,
    // TODO: aux ?
    //       可能有多个值，这里保存了多个值
    // for i, v := range p.Aux {
    // 	k := s.keys[i+1]
    // 	switch v.(type) {
    // 	case float64, int64, uint64, string, bool:
    // 		m[k.Val] = v
    // 	default:
    // 		// Insert the fill value if one was specified.
    // 		if s.defaultValue != SkipDefault {
    // 			m[k.Val] = castToType(s.defaultValue, k.Type)
    // 		}
    // 	}
    // }
    pub aux: Vec<Val>,

    // Total number of points that were combined into this point from an aggregate.
    // If this is zero, the point is not the result of an aggregate function.
    pub(crate) aggregated: u32,
    pub(crate) none: bool,
}

impl FloatPoint {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tags(&self) -> Tags {
        self.tags.clone()
    }

    pub fn time(&self) -> i64 {
        self.time
    }

    pub fn value(&self) -> f64 {
        self.value
    }

    /// The auxiliary field values for this point, positionally matching the
    /// `IteratorOptions.aux` cursors that produced them (`Val::Empty` where a
    /// field has no value at this time). Used by the DataFusion bridge to read
    /// every field of a measurement from a single aux-driven iterator instead
    /// of one merged iterator per field.
    pub fn aux(&self) -> &[Val] {
        &self.aux
    }
}

impl Eq for FloatPoint {}

impl Ord for FloatPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare timestamp first, if timestamp is equal, then compare value.
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => self
                .value
                .partial_cmp(&other.value)
                .unwrap_or(Ordering::Equal),
            non_eq => non_eq,
        }
    }
}

impl PartialOrd for FloatPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for FloatPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("FloatPoint", 3)?;
        s.serialize_field("name", &self.name)?;
        // s.serialize_field("tags", &self.tags)?;
        s.serialize_field("time", &self.time)?;

        if self.aux.is_empty() {
            s.serialize_field("value", &self.value)?;
        } else {
            for a in &self.aux {
                match a {
                    Val::I64(v) => {
                        s.serialize_field("value", &v)?;
                        break;
                    }
                    Val::U64(v) => {
                        s.serialize_field("value", &v)?;
                        break;
                    }
                    Val::F64(v) => {
                        s.serialize_field("value", &v)?;
                        break;
                    }
                    Val::Bool(v) => {
                        s.serialize_field("value", &v)?;
                        break;
                    }
                    Val::String(v) => {
                        s.serialize_field("value", &v)?;
                        break;
                    }
                    _ => {}
                }
            }
        }

        s.end()
    }
}

/// FloatPoints represents a slice of points sortable by value.
pub struct FloatPoints(Vec<FloatPoint>);

impl Deref for FloatPoints {
    type Target = Vec<FloatPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FloatPoints {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// BoolPoint represents a point with a bool value.
#[derive(Clone, Default, PartialEq)]
pub struct BoolPoint {
    pub(crate) name: String,
    pub(crate) tags: Tags,

    pub(crate) time: i64,
    pub(crate) value: bool,
    pub(crate) aux: Vec<Val>,

    // Total number of points that were combined into this point from an aggregate.
    // If this is zero, the point is not the result of an aggregate function.
    pub(crate) aggregated: u32,
    pub(crate) none: bool,
}

impl BoolPoint {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tags(&self) -> Tags {
        self.tags.clone()
    }

    pub fn time(&self) -> i64 {
        self.time
    }

    pub fn value(&self) -> bool {
        self.value
    }
}

impl Eq for BoolPoint {}

impl Ord for BoolPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare timestamp first, if timestamp is equal, then compare value.
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => self
                .value
                .partial_cmp(&other.value)
                .unwrap_or(Ordering::Equal),
            non_eq => non_eq,
        }
    }
}

impl PartialOrd for BoolPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for BoolPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("BoolPoint", 3)?;
        s.serialize_field("name", &self.name)?;
        // s.serialize_field("tags", &self.tags)?;
        s.serialize_field("time", &self.time)?;
        s.serialize_field("value", &self.value)?;
        s.end()
    }
}

/// BoolPoints represents a slice of points sortable by value.
pub struct BoolPoints(Vec<BoolPoint>);

impl Deref for BoolPoints {
    type Target = Vec<BoolPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BoolPoints {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// StringPoint represents a point with a string value.
#[derive(Clone, Default, PartialEq)]
pub struct StringPoint {
    pub(crate) name: String,
    pub(crate) tags: Tags,

    pub(crate) time: i64,
    pub(crate) value: String,
    pub(crate) aux: Vec<Val>,

    // Total number of points that were combined into this point from an aggregate.
    // If this is zero, the point is not the result of an aggregate function.
    pub(crate) aggregated: u32,
    pub(crate) none: bool,
}

impl StringPoint {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn tags(&self) -> Tags {
        self.tags.clone()
    }

    pub fn time(&self) -> i64 {
        self.time
    }

    pub fn value(&self) -> String {
        self.value.clone()
    }
}

impl Eq for StringPoint {}

impl Ord for StringPoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare timestamp first, if timestamp is equal, then compare value.
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => self
                .value
                .partial_cmp(&other.value)
                .unwrap_or(Ordering::Equal),
            non_eq => non_eq,
        }
    }
}

impl PartialOrd for StringPoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for StringPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("StringPoint", 3)?;
        s.serialize_field("name", &self.name)?;
        // s.serialize_field("tags", &self.tags)?;
        s.serialize_field("time", &self.time)?;
        s.serialize_field("value", &self.value)?;
        s.end()
    }
}

/// StringPoints represents a slice of points sortable by value.
pub struct StringPoints(Vec<StringPoint>);

impl Deref for StringPoints {
    type Target = Vec<StringPoint>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StringPoints {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Tags represent a map of keys and values.
/// It memorizes its key so it can be used efficiently during query execution.
#[derive(Default, Clone, Debug)]
pub struct Tags {
    id: String,
    m: HashMap<String, String>,
}

// impl Serialize for Tags {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//         where
//             S: Serializer {
//         let mut map = serializer.serialize_map(Some(self.m.len() + 1))?;
//         map.serialize_entry("id", &self.id)?;
//         for (k, v) in &self.m {
//             map.serialize_entry(k, v)?;
//         }
//         map.end()
//     }
// }

impl Tags {
    pub fn new(m: HashMap<String, String>) -> Self {
        if m.is_empty() {
            return Tags::default();
        }

        Self {
            id: unsafe { String::from_utf8_unchecked(encode_tags(&m)) },
            m,
        }
    }

    /// Returns a new instance of Tags by parsing the given tag ID.
    pub fn from_id(id: String) -> Self {
        {
            let m = decode_tags(id.as_bytes());
            if m.is_empty() {
                return Tags::default();
            }

            Self { id, m }
        }
    }

    /// Returns the string identifier for the tags.
    pub fn id(&self) -> String {
        self.id.clone()
    }

    /// Returns the underlying map for the tags.
    pub fn key_values(&self) -> HashMap<String, String> {
        self.m.clone()
    }

    /// Returns a sorted list of all keys on the tag.
    pub fn keys(&self) -> Vec<String> {
        let mut a = vec![];
        for k in self.m.keys() {
            a.push(k.clone());
        }
        a.sort();

        a
    }

    /// Returns a sorted list of all values on the tag.
    pub fn values(&self) -> Vec<String> {
        let mut a = vec![];
        for k in self.m.values() {
            a.push(k.clone());
        }
        a.sort();

        a
    }

    /// Returns the value for a given key.
    pub fn value(&self, key: &str) -> Option<String> {
        self.m.get(key).cloned()
    }

    pub(crate) fn value_ref(&self, key: &str) -> Option<&str> {
        self.m.get(key).map(String::as_str)
    }

    /// Returns a new tags object with a subset of the keys.
    pub fn subset(&self, keys: &[String]) -> Self {
        if keys.is_empty() {
            return Tags::default();
        }

        // If keys match existing keys, simply return this tagset.
        if keys_match(&self.m, keys) {
            return self.clone();
        }

        // Otherwise create new tag set. Skip keys this tag set does not have: a
        // series missing a group-by key simply contributes nothing for it
        // (InfluxDB semantics). The previous `.unwrap()` panicked on
        // heterogeneous/sparse tag sets (e.g. grouping by a union of tag keys
        // where some series lack one of them).
        let mut m = HashMap::new();
        for k in keys.iter() {
            if let Some(v) = self.m.get(k) {
                m.insert(k.clone(), v.clone());
            }
        }

        Self::new(m)
    }

    /// Returns true if t equals other.
    pub fn equals(&self, other: &Tags) -> bool {
        self.id == other.id
    }
}

impl PartialEq for Tags {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for Tags {}

fn keys_match(m: &HashMap<String, String>, keys: &[String]) -> bool {
    if keys.len() != m.len() {
        return false;
    }

    for k in keys.iter() {
        if !m.contains_key(k) {
            return false;
        }
    }

    true
}

/// Converts a map of strings to an identifier.
fn encode_tags(m: &HashMap<String, String>) -> Vec<u8> {
    // Empty maps marshal to empty bytes.
    if m.is_empty() {
        return vec![];
    }

    // Extract keys and determine final size.
    let mut sz = (m.len() * 2) - 1; // separators
    let mut keys = vec![];
    for (k, v) in m.iter() {
        keys.push(k.clone());
        sz += k.len() + v.len();
    }
    keys.sort();

    // Generate marshaled bytes.
    let mut b = vec![0u8; sz];
    let buf = &mut b;
    let mut n = 0;
    for k in keys.iter() {
        let klen = k.len();
        buf[n..n + klen].copy_from_slice(k.as_bytes());
        n += klen;
        buf[n] = b' ';
        n += 1;
    }

    for (i, k) in keys.iter().enumerate() {
        let v = m.get(k).unwrap();
        let vlen = v.len();
        buf[n..n + vlen].copy_from_slice(v.as_bytes());
        n += vlen;
        if i < keys.len() - 1 {
            buf[n] = b' ';
            n += 1;
        }
    }

    b.truncate(n);

    b
}

/// Parses an identifier into a map of tags.
fn decode_tags(id: &[u8]) -> HashMap<String, String> {
    let mut a = id.split(|&item| item == b' ').collect::<Vec<&[u8]>>();

    // There must be an even number of segments.
    if a.len() > 0 && a.len() % 2 == 1 {
        let _ = a.pop();
    }

    // Return nil if there are no segments.
    if a.is_empty() {
        return HashMap::new();
    }
    let mid = a.len() / 2;

    // Decode key/value tags.
    let mut m = HashMap::new();
    for i in 0..mid {
        unsafe {
            m.insert(
                String::from_utf8_unchecked(a[i].to_vec()),
                String::from_utf8_unchecked(a[i + mid].to_vec()),
            );
        }
    }

    m
}
