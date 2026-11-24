//! Points
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::HashMap,
    default,
    fmt::{self, Display},
    ops::{Deref, DerefMut},
    str,
};

use crate::common::{
    time::{self, Time},
    to_str, to_string,
};
use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use serde::Serialize;
use snafu::{Snafu, ensure};

use crate::models::{
    TIME_COLUMN_NAME,
    escape::bytes,
    time::{MAX_NANO_TIME_I64, MIN_NANO_TIME_I64, ZERO_TIME, safe_calc_time},
    utf8,
};

pub use crate::models::field_type::{BOOL, EMPTY, FLOAT, FieldType, INTEGER, STRING, UNSIGNED};

#[allow(dead_code)]
const FIELD_KEY_DELIMITER: &[char] = &[',', '=', ' ']; // byte array
#[allow(dead_code)]
const FIELD_VALUE_STRING_DELIMITER: &[char] = &['"'];
// values used to store the field key and measurement name as special interval tags.
const FIELD_KEY_TAG_KEY: &str = "\u{ff}";
const MEASURE_TAG_KEY: &str = "\u{00}";
// reserved tag keys which when present cause the Point to be discarded and an error return.
const RESERVED_FIELD_TAG_KEY: &str = "_field";
const RESERVED_MEASUREMENT_TAG_KEY: &str = "_measurement";

// predifined byte representations of special tag keys.
const FIELD_KEY_TAG_KEY_BYTES: &str = FIELD_KEY_TAG_KEY;
const MEASURE_TAG_KEY_BYTES: &str = MEASURE_TAG_KEY;

pub(crate) struct EscapeSet {
    pub k: u8,
    pub esc: [u8; 2],
}

const MEASUREMENT_ESCAPE_CODES: [EscapeSet; 2] = [
    EscapeSet {
        k: b',',
        esc: [b'\\', b','],
    },
    EscapeSet {
        k: b' ',
        esc: [b'\\', b' '],
    },
];

pub(crate) const TAG_ESCAPE_CODES: [EscapeSet; 3] = [
    EscapeSet {
        k: b',',
        esc: [b'\\', b','],
    },
    EscapeSet {
        k: b' ',
        esc: [b'\\', b' '],
    },
    EscapeSet {
        k: b'=',
        esc: [b'\\', b'='],
    },
];

// set of reserved tag keys which can't be present when a Point is being parsed.
const RESERVED_TAG_KEYS: &[&str] = &[
    FIELD_KEY_TAG_KEY,
    MEASURE_TAG_KEY,
    RESERVED_FIELD_TAG_KEY,
    RESERVED_MEASUREMENT_TAG_KEY,
    TIME_COLUMN_NAME,
];

// MAX_KEY_LENGTH is the largestest allowed byte size of the combined measurement and tag keys.
const MAX_KEY_LENGTH: usize = 65535;
// for parse float/int/uint field value
// the number of characters for the largest possible int64 (9223372036854775807)
const MAX_INT64_DIGITS: usize = 19;
// the number of characters for the smallest possible int64 (-9223372036854775808)
const MIN_INT64_DIGITS: usize = 20;
// the number of characters for the largest possible uint64 (18446744073709551615)
const MAX_UINT64_DIGITS: usize = 20;
// the number of characters required for the largest float64 before a range check
// would occur during parsing
const MAX_FLOAT64_DIGITS: usize = 25;
// the number of characters required for smallest float64 before a range check occur
// would occur during parsing
const MIN_FLOAT_DIGITS: usize = 27;

/// Error type for points.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Missing measurement.
    #[snafu(display("Missing measurement"))]
    MissingMeasurement,

    /// Missing tag key.
    #[snafu(display("Missing tag key"))]
    MissingTagKey,

    /// Missing tag value.
    #[snafu(display("Missing tag value"))]
    MissingTagValue,

    /// Invalid tag format.
    #[snafu(display("Invalid tag format"))]
    InvalidTagFormat,

    /// Max key length exceed.
    #[snafu(display("Max key length exceeds: {} > {}", key_length, MAX_KEY_LENGTH))]
    MaxKeyLengthExceed { key_length: usize },

    /// Use reserved tag key.
    #[snafu(display("Can't use reserved tag key: '{}'", reserved))]
    UseReservedTagKey { reserved: String },

    /// Duplicate tags.
    #[snafu(display("Duplicate tags: {}", duplicate))]
    DuplicateTags { duplicate: String },

    /// Missing field.
    // #[snafu(display("Missing Field, current point is: {:?}", point))]
    // MissingField {
    //     point: Point,
    // },
    #[snafu(display("Missing Field, at least one field required"))]
    MissingField,

    /// Missing field key.
    #[snafu(display("Missing field key"))]
    MissingFieldKey,

    /// Missing field value.
    #[snafu(display("Missing field value"))]
    MissingFieldValue,

    /// Invalid field format.
    #[snafu(display("Invalid field format"))]
    InvalidFieldFormat,

    /// Unbalanced quotes.
    #[snafu(display("Unbalanced quotes"))]
    UnbalancedQuotes,

    /// Invalid bool value.
    #[snafu(display("Invalid bool value"))]
    InvalidBoolValue,

    /// Invalid number value.
    #[snafu(display("Invalid number value"))]
    InvalidNumberValue,

    /// Invalid timestamp.
    #[snafu(display("Invalid timestamp"))]
    InvalidTimestamp,

    /// Parse bool failed.
    #[snafu(display("Parse bool failed: {}", bool))]
    ParseBoolFailed { bool: String },

    /// Parse integer failed.
    #[snafu(display("Parse integer failed: {}", integer))]
    ParseIntegerFailed { integer: String },

    /// Parse unsigned failed.
    #[snafu(display("Parse unsigned failed: {}", unsigned))]
    ParseUnsignedFailed { unsigned: String },

    /// Parse float failed.
    #[snafu(display("Parse float failed: {}", float))]
    ParseFloatFailed { float: String },

    /// Parse string failed.
    #[snafu(display("Parse string failed: {}", string))]
    ParseStringFailed { string: String },

    /// Time outside range.
    #[snafu(display("Time outside range: {} > {}", MIN_NANO_TIME_I64, MAX_NANO_TIME_I64))]
    TimeOutsideRange,

    /// Invalid point format.
    #[snafu(display("Invalid point format"))]
    InvalidPointFormat,

    /// PointMustHaveAField is returned when operating on a point that does not have any fields.
    #[snafu(display("point without fields is unsupported"))]
    PointMustHaveAField,

    /// InvalidNumber is returned when a number is expected but not provided.
    #[snafu(display("invalid number"))]
    InvalidNumber,

    /// InvalidPoint is returned when a point cannot be parsed correctly.
    #[snafu(display("point is invalid"))]
    InvalidPoint,

    /// InvalidKevValuePairs is returned when the number of key, value pairs
    /// is odd, indicating a missing value.
    #[snafu(display("key/value pairs is an odd length"))]
    InvalidKevValuePairs,

    /// String error.
    #[snafu(display("{reason}"))]
    StringErr { reason: String },
}

/// Result type for parsed Points.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// Points represents a sortable list of points by timestamp.
// pub type Points Vec<Point>;

// impl Points {
//     // Len implements sort.Interface.
//     fn (a Points) Len() int { return len(a) }

//     // Less implements sort.Interface.
//     fn (a Points) Less(i, j int) bool { return a[i].Time().Before(a[j].Time()) }

//     // Swap implements sort.Interface.
//     fn (a Points) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
// }

/// Point from line protocol(e.g. "mem,region=beijing,host=server1 used=0.8,free=0.124 1646064000000000000")
#[derive(Debug, Clone)]
pub struct Point {
    pub time: time::Time, // 2022-02-28 16:00:00 UTC

    // text encoding of measurement and tags, key must always be stored sorted by tags,
    // if the original line was not sorted, we should sort them first.
    pub key: Vec<u8>,    // "mem,host=server1,region=beijing"
    pub fields: Vec<u8>, // "used=0.8,free=0.124"
    pub ts: Vec<u8>,     // 1646064000000000000

    // cached version of parsed fields from data

    // cached version of parsed tags
    pub cached_tags: Tags,
}

impl Point {
    /// new returns a new Point instance with specified key(measurement and tags), fields and timestamp.
    pub fn new(key: &[u8], fields: &[u8], timestamp: &[u8]) -> Self {
        Self {
            time: Time::from_timestamp_nanos(0),
            key: key.to_vec(),
            fields: fields.to_vec(),
            ts: timestamp.to_vec(),
            cached_tags: Tags::default(),
        }
    }

    /// key returns the key (measurement joined with tags) of the point.
    pub fn key(&self) -> &[u8] {
        self.key.as_slice()
    }

    /// fields_string returns the fields of the point.
    pub fn fields_str(&self) -> &str {
        to_str!(&self.fields)
    }

    /// name returns the measurement name for the point.
    pub fn name(&self) -> Vec<u8> {
        let (_pos, buf) = scan_to(self.key.as_slice(), 0, b',');
        bytes::unescape(buf)
    }

    /// set_name updates the measurement name for the point.
    pub fn set_name(&mut self, buf: &[u8]) {
        self.key = buf.to_vec();
    }

    /// tags returns the tag set for the point.
    pub fn tags(&mut self) -> Tags {
        if self.cached_tags.len() != 0 {
            return self.cached_tags.clone();
        }

        let tags = parse_tags(&self.key, None);
        self.cached_tags = tags.clone();
        tags
    }

    /// for_each_tag iterates over each tag invoking fn, if fn returns false, iteration stops.
    pub fn for_each_tag<F>(&self, f: F)
    where
        F: FnMut(&[u8], &[u8]) -> bool,
    {
        walk_tags(&self.key, f)
    }

    // add_tag adds or replaces a tag value for a point.

    /// set_tags replaces the tags for the point.
    pub fn set_tags(&mut self, tags: &Tags) {
        self.key = make_key(self.name().as_slice(), tags);
        self.cached_tags = tags.clone();
    }

    /// hash_tag returns true if the tag exists for the point.
    pub fn has_tag(&self, tag: &[u8]) -> bool {
        if self.key.is_empty() {
            return false;
        }

        let mut exists = false;
        walk_tags(self.key.as_slice(), |key: &[u8], _value: &[u8]| -> bool {
            if tag == key {
                exists = true;
                return false;
            }
            true
        });

        exists
    }

    // fields returns the fields for the point.

    /// time return a copy timestamp for the point.
    pub fn time(&self) -> Time {
        self.time
    }

    /// set_time updates the timestamp for the point.
    pub fn set_time(&mut self, t: Time) {
        self.time = t;
    }

    /// set_precision will round a time to the specified precision.
    pub fn set_precision(&mut self, precision: &str) {
        match precision {
            "n" | "ns" => {
                // do nothing
                // self.time().duration_round(Duration::NANOSECOND)
            }
            "u" | "us" => {
                self.time = self
                    .time
                    .duration_round(Duration::microseconds(1))
                    .unwrap()
                    .into();
            }
            "ms" => {
                self.time = self
                    .time
                    .duration_round(Duration::milliseconds(1))
                    .unwrap()
                    .into();
            }
            "s" => {
                self.time = self
                    .time
                    .duration_round(Duration::seconds(1))
                    .unwrap()
                    .into();
            }
            "m" => {
                self.time = self
                    .time
                    .duration_round(Duration::minutes(1))
                    .unwrap()
                    .into();
            }
            "h" => {
                self.time = self.time.duration_round(Duration::hours(1)).unwrap().into();
            }
            _ => {}
        }
    }

    /// unix_nano returns the timestamp of the point as nanoseconds since Unix epoch.
    pub fn unix_nano(&self) -> i64 {
        self.time.timestamp_nanos()
    }

    // hash_id returns a non-cryptogrphic checksum of the point's key.

    /// string returns a string representation of the point. If there is a
    /// timestamp associated with the point then it will be specified with the default
    /// precision of nanoseconds.
    pub fn string(&self) -> String {
        // TODO: check if time is zero first ?
        format!(
            "{} {} {}",
            to_str!(&self.key),
            to_str!(&self.fields),
            self.time.timestamp_nanos()
        )
    }

    /// precision_tring returns a string representation of the point. If there
    /// is a timestamp associated with the point then it will be specified in the
    /// given unit.
    pub fn precision_tring(&self, precision: &str) -> String {
        format!(
            "{} {} {}",
            to_str!(&self.key),
            to_str!(&self.fields),
            self.time.timestamp_nanos() / get_precision_multiplier(precision)
        )
    }

    /// round will round the timestamp of the point to the given duration.
    pub fn round(&mut self, round: Duration) {
        self.time = self.time.duration_round(round).unwrap().into();
    }

    /// string_size returns the length of the string that would be returned by String().
    pub fn string_size(&self) -> usize {
        let mut size = self.key.len() + self.fields.len() + 1;

        // TODO: check if time is zero first ?
        let mut digits = 1; // even "0" has one digit
        let mut t = self.time.timestamp_nanos();
        if t < 0 {
            // account for negative sign, then negate
            digits += 1;
            t = -t;
        }
        while t > 9 {
            // already accounted for one digit
            digits += 1;
            t /= 10;
        }
        size += digits + 1; // digits and a space

        size
    }

    /// field_iter returns a FieldIterator that can be used to traverse the
    /// fields of a point without constructing the in-memory map.
    pub fn field_iter(&self) -> FieldIterator {
        FieldIterator {
            start: 0,
            end: 0,
            fields: &self.fields,
        }
    }
}

/// Returns a new point with the given measurement name, tags, fields and timestamp.  If
/// an unsupported field value (NaN) is passed, this function panics.
pub fn must_new_point(name: String, tags: Tags, fields: Fields, t: time::Time) -> Point {
    match new_point(name, tags, fields, t) {
        Ok(p) => p,
        Err(e) => panic!("{:?}", e),
    }
}

/// Returns a new point with the given measurement name, tags, fields and timestamp.  If
/// an unsupported field value (NaN, or +/-Inf) or out of range time is passed, this function
/// returns an error.
pub fn new_point(name: String, tags: Tags, fields: Fields, t: time::Time) -> Result<Point> {
    let key = point_key(name, tags, &fields, t)?;

    Ok(Point {
        time: t,
        key,
        fields: fields.marshal_binary(),
        ts: vec![],
        cached_tags: Tags::default(),
    })
}

fn point_key(measurement: String, tags: Tags, fields: &Fields, t: time::Time) -> Result<Vec<u8>> {
    ensure!(!fields.is_empty(), PointMustHaveAFieldSnafu);

    if !t.is_zero() {
        super::time::check_time(&t)?;
    }

    for (key, value) in fields.iter() {
        match value {
            RawVal::F64(v) => {
                // Ensure the caller validates and handles invalid field values
                if v.is_infinite() {
                    return Err(Error::StringErr {
                        reason: format!("+/-Inf is an unsupported value for field {}", key),
                    });
                }
                if v.is_nan() {
                    return Err(Error::StringErr {
                        reason: format!("Nan is an unsupported value for field {}", key),
                    });
                }
            }
            RawVal::F32(v) => {
                let v = *v as f64;
                // Ensure the caller validates and handles invalid field values
                if v.is_infinite() {
                    return Err(Error::StringErr {
                        reason: format!("+/-Inf is an unsupported value for field {}", key),
                    });
                }
                if v.is_nan() {
                    return Err(Error::StringErr {
                        reason: format!("Nan is an unsupported value for field {}", key),
                    });
                }
            }
            _ => {}
        }
    }

    let key = make_key(measurement.as_bytes(), &tags);
    for field in fields.keys() {
        let sz = series_key_size(key.as_slice(), field.as_bytes());
        ensure!(
            sz <= MAX_KEY_LENGTH,
            StringErrSnafu {
                reason: format!("max key length exceeded: {} > {}", sz, MAX_KEY_LENGTH),
            }
        );
    }

    Ok(key)
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "key: {}, fields: {}, unix_timestamp: {}({})",
            to_str!(&self.key),
            to_str!(&self.fields),
            to_str!(&self.ts),
            self.time,
        )
    }
}

// //////////////////////////////////////////////
// ////////////// Tag ///////////////////////////
// //////////////////////////////////////////////
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tag {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Tag {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    pub fn size(&self) -> i64 {
        (self.key.len() + self.value.len()) as i64
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{} {}}}", to_str!(&self.key), to_str!(&self.value),)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Tags(Vec<Tag>);

// implement Deref / DerefMut, so we can use Vunsafe{str::from_utf8_unchecked(&self.key)},ec methods directly.
impl Deref for Tags {
    type Target = Vec<Tag>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Tags {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<Vec<u8>, Vec<u8>>> for Tags {
    fn from(m: HashMap<Vec<u8>, Vec<u8>>) -> Self {
        if m.is_empty() {
            return Tags::default();
        }

        let mut tags = Tags(Vec::<Tag>::with_capacity(m.len()));
        for (k, v) in m {
            tags.push(Tag { key: k, value: v });
        }
        // sort Tags by key
        tags.sort_by(|x, y| x.key.cmp(&y.key));
        tags
    }
}

impl Tags {
    /// new returns a new Tags.
    pub fn new() -> Self {
        Tags::default()
    }

    /// returns a new tags with capacity.
    pub fn with_capacity(size: usize) -> Tags {
        Tags(Vec::<Tag>::with_capacity(size))
    }

    /// tags returns the reference to Vec<Tag>
    pub fn tags(&self) -> &Vec<Tag> {
        self
    }

    // /// new_tags_key_values returns a new Tags from a list of key, value pairs,
    // /// ensuring the returned result is correctly sorted. Duplicate keys are removed,
    // /// however, it which duplicate that remains is undefined.
    // /// NewTagsKeyValues will return ErrInvalidKevValuePairs if len(kvs) is not even.
    // /// If the input is guaranteed to be even, the error can be safely ignored.
    // /// If a has enough capacity, it will be reused.
    // pub fn new_tags_key_values(a: &Tags, Vec<>)

    /// keys returns the list of keys for a tag set.
    pub fn keys(&self) -> Option<Vec<Vec<u8>>> {
        if self.len() == 0 {
            return None;
        }

        let mut keys = Vec::<Vec<u8>>::with_capacity(self.len());
        for tag in &self.0 {
            keys.push(tag.key.clone());
        }
        Some(keys)
    }

    /// values returns the string representationof the tags.
    pub fn values(&self) -> Option<Vec<Vec<u8>>> {
        if self.len() == 0 {
            return None;
        }
        let mut values = Vec::<Vec<u8>>::with_capacity(self.len());
        for tag in &self.0 {
            values.push(tag.value.clone());
        }
        Some(values)
    }

    /// size returns the number of bytes needed to store all tags. Note, this is
    /// the number of bytes needed to store all keys and values and does not account
    /// for data structures or delimiters for example.
    pub fn size(&self) -> i64 {
        let mut total = 0;
        for tag in self.iter() {
            total += tag.size();
        }
        total
    }

    #[allow(dead_code)]
    /// sorted returns true if a is sorted.
    fn sroted(&self) -> bool {
        let mut i = self.len() - 1;
        while i > 0 {
            if self[i].key < self[i - 1].key {
                return false;
            }
            i -= 1;
        }
        true
    }

    /// hash_key hashes all of a tag's keys.
    pub fn hash_key(&self) -> Vec<u8> {
        self.append_hash_key(None)
    }

    fn needs_escape(&self) -> bool {
        for tag in self.iter() {
            for e in TAG_ESCAPE_CODES.iter() {
                if tag.key.as_slice().iter().any(|&x| x == e.k)
                    || tag.value.as_slice().iter().any(|&x| x == e.k)
                {
                    return true;
                }
            }
        }
        false
    }

    /// append_hash_key appends the result of hashing all of a tag's keys and values to dst
    /// and returns the extended buffer.
    pub fn append_hash_key(&self, buf: Option<&[u8]>) -> Vec<u8> {
        if self.len() == 0 {
            if let Some(dst) = buf {
                return dst.to_vec();
            }
            return vec![];
        }

        // type invariant: Tags are sorted
        let mut sz = 0;
        let mut escaped: Tags;
        if self.needs_escape() {
            let mut tmp = Tags::with_capacity(20);
            if self.capacity() < tmp.capacity() {
                tmp.shrink_to(self.capacity());
                escaped = tmp; // derive(Clone) for Tags
            } else {
                escaped = Tags::with_capacity(self.capacity());
            }

            for (i, tag) in self.iter().enumerate() {
                escaped.push(Tag {
                    key: escape_tag(tag.key.as_slice()),
                    value: escape_tag(tag.value.as_slice()),
                });
                sz += escaped[i].key.len() + escaped[i].value.len();
            }
        } else {
            sz = self.size() as usize;
            escaped = self.clone();
        }

        sz += escaped.len() + escaped.len() * 2; // separators

        let mut buf_len = 0usize;
        let mut out: Vec<u8>;
        if let Some(bf) = buf {
            out = vec![0; bf.len() + sz];
            out.as_mut_slice()[..bf.len()].copy_from_slice(bf);
            buf_len = bf.len();
        } else {
            out = vec![0; sz];
        }

        let b = if let Some(bf) = buf {
            &mut out.as_mut_slice()[bf.len()..]
        } else {
            out.as_mut_slice()
        };

        let mut idx = 0;
        for tag in escaped.iter() {
            if tag.value.is_empty() {
                continue;
            }

            b[idx] = b',';
            idx += 1;
            b[idx..idx + tag.key.len()].copy_from_slice(tag.key.as_slice());
            idx += tag.key.len();
            b[idx] = b'=';
            idx += 1;
            b[idx..idx + tag.value.len()].copy_from_slice(tag.value.as_slice());
            idx += tag.value.len();
        }

        out.as_slice()[..buf_len + idx].to_vec()
    }

    /// Set the value for a key.
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        for tag in &mut self.0 {
            if tag.key.as_slice() == key {
                tag.value = value.to_vec();
                return;
            }
        }
        self.0.push(Tag {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self.sort_by(|x, y| x.key.cmp(&y.key));
    }

    /// Returns the value for a key.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        // OPTIMIZE: Use sort.Search if tagset is large.
        for t in self.iter() {
            if t.key == key {
                return Some(&t.value);
            }
        }
        None
    }

    /// Returns a map representation of the tags.
    pub fn map(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();
        for t in self.iter() {
            m.insert(to_string!(&t.key), to_string!(&t.value));
        }
        m
    }
}

impl Display for Tags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = "[".to_string();
        let mut out = self.iter().enumerate().fold(s, |mut acc, (i, tag)| {
            acc += tag.to_string().as_str();
            if i < self.len() - 1 {
                acc += " ";
            }
            acc
        });
        out += "]";

        write!(f, "{}", out)
    }
}
/// Returns the parsed points and failed strings.
pub fn parse_points_string(buf: &str) -> Result<(Vec<Point>, Option<String>)> {
    parse_points(buf.as_bytes())
}

pub fn parse_points(buf: &[u8]) -> Result<(Vec<Point>, Option<String>)> {
    parse_points_with_precision(buf, Time::now(), "ns")
}

pub fn parse_points_with_precision(
    buf: &[u8],
    default_time: Time,
    precision: &str,
) -> Result<(Vec<Point>, Option<String>)> {
    // Preserve partial success by returning failed lines separately.
    let point_count = buf.iter().filter(|x| **x == b'\n').count() + 1;
    let mut points = Vec::with_capacity(point_count);

    let mut pos = 0;
    let mut block; // = Vec::<u8>::new();  // if a: Vec<u8>, then &a -> &[u8], and a.as_ref() -> &[u8]
    let mut failed = Vec::<String>::new();

    while pos < buf.len() {
        (pos, block) = scan_line(buf, pos);
        pos += 1;

        if block.is_empty() {
            continue;
        }

        let start = skip_white_space(block, 0);
        // check if line is all whitespaces
        if start >= block.len() {
            continue;
        }

        // lines start with '#' are comments
        if block[start] == b'#' {
            continue;
        }

        // skip the newline if one is present
        if block[block.len() - 1] == b'\n' {
            block = &block[0..block.len() - 1];
        }

        match parse_point(&block[start..], default_time, precision) {
            Ok(pt) => points.push(pt),
            Err(e) => failed.push(format!(
                "unable to parse '{}': {}",
                to_str!(&block[start..]),
                e
            )),
        }
    }

    if !failed.is_empty() {
        return Ok((points, Some(failed.join("\n"))));
    }

    Ok((points, None))
}

pub fn parse_key(buf: &[u8]) -> (Vec<u8>, Tags) {
    parse_key_bytes(buf)
}

pub fn parse_key_bytes(buf: &[u8]) -> (Vec<u8>, Tags) {
    parse_key_bytes_with_tags(buf, None)
}

pub fn parse_key_bytes_with_tags(buf: &[u8], tags: Option<&Tags>) -> (Vec<u8>, Tags) {
    // ignore the error because scanMeasurement returns "missing fields" which we ignore when just parsing a key.
    let state;
    let i;
    match scan_measurement(buf, 0) {
        (s, idx, Some(_e)) => {
            state = s;
            i = idx;
        }
        (s, idx, None) => {
            state = s;
            i = idx;
        }
    }

    let name: &[u8];
    let mut out = match tags {
        Some(tags) => tags.clone(),
        None => Tags::new(),
    };

    if state == TAG_KEY_STATE {
        out = parse_tags(buf, tags);
        // scan_measurement returns the location of the comma if there are tags, strip that off
        name = &buf[..i - 1];
    } else {
        name = &buf[..i];
    }

    (unescape_measurement(name), out)
}

// make_key creates a key for a set of tags.
pub fn make_key(name: &[u8], tags: &Tags) -> Vec<u8> {
    append_make_key(None, name, tags)
}

// append_make_key appends the key derived from name and tags to dst and returns the extended buffer.
pub fn append_make_key(dst: Option<&[u8]>, name: &[u8], tags: &Tags) -> Vec<u8> {
    // unescape the name and then re-escape it to avoid double escaping.
    // The key should always be stored in escaped form.
    let mut out = vec![];
    if let Some(dst) = dst {
        if !dst.is_empty() {
            out.extend_from_slice(dst);
            out.append(&mut escape_measurement(&unescape_measurement(name)));
        } else {
            out.append(&mut escape_measurement(&unescape_measurement(name)));
        }
    } else {
        out.append(&mut escape_measurement(&unescape_measurement(name)));
    }

    tags.append_hash_key(Some(out.as_slice()))
}

/// Converts a tag set to bytes for use as a lookup key.
pub fn make_tags_key(keys: &[String], tags: &Tags) -> Vec<u8> {
    // precondition: keys is sorted
    // precondition: models.Tags is sorted

    // Empty maps marshal to empty bytes.
    if keys.is_empty() || tags.is_empty() {
        return vec![];
    }

    let mut sel = Vec::with_capacity(keys.len());

    let mut sz = 0;
    let (mut i, mut j) = (0, 0);
    while i < keys.len() && j < tags.len() {
        if keys[i].as_bytes() < tags[j].key.as_slice() {
            i += 1;
        } else if keys[i].as_bytes() > tags[j].key.as_slice() {
            j += 1;
        } else {
            sel.push(j);
            sz += keys[i].len() + tags[j].value.len();
            i += 1;
            j += 1;
        }
    }

    if sel.is_empty() {
        // no tags matched the requested keys
        return vec![];
    }

    sz += sel.len() * 2 - 1; // selected tags, add separators

    // Generate marshaled bytes.
    let mut b = vec![0u8; sz];
    let mut n = 0;
    for k in &sel {
        b[n..n + tags[*k].key.len()].copy_from_slice(&tags[*k].key);
        n += tags[*k].key.len();
        b[n] = b'|';
        n += 1;
    }

    for (i, k) in sel.iter().enumerate() {
        b[n..n + tags[*k].value.len()].copy_from_slice(&tags[*k].value);
        n += tags[*k].value.len();
        if i < sel.len() - 1 {
            b[n] = b'|';
            n += 1;
        }
    }

    b.truncate(n);

    b
}

// Parse buf into the provided destination tags, returning destination
// Tags, which may have a different length and capacity.
pub fn parse_tags(buf: &[u8], dst: Option<&Tags>) -> Tags {
    if buf.is_empty() {
        return Tags::new();
    }

    let n = buf.iter().filter(|x| **x == b',').count();
    let mut parsed: Tags;
    if let Some(tags) = dst {
        parsed = Tags::with_capacity(n);
        for (i, tag) in tags.iter().enumerate() {
            if i == n {
                break;
            }
            parsed.push(tag.clone());
        }
    } else {
        parsed = Tags::new(); // length and capacity are zero
    }

    // Series keys can contain escaped commas, therefore the number of commas in a series key only
    // gives an estimation of the upper bound on the number of tags.
    let mut i = 0;
    walk_tags(buf, |key: &[u8], value: &[u8]| -> bool {
        // (tags[i].key, tags[i].value) = (to_string!(key.to_vec()).unwrap(), to_string!(value.to_vec()).unwrap());
        parsed.push(Tag {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        i += 1;

        true
    });

    if i == 0 {
        return Tags::new();
    }

    parsed.truncate(i);
    parsed
}

pub fn parse_tags_with_tags(buf: &[u8], tags: &Tags) -> Tags {
    parse_tags(buf, Some(tags))
}

pub fn parse_name(buf: &[u8]) -> Vec<u8> {
    let state;
    let i;
    match scan_measurement(buf, 0) {
        (s, idx, Some(_e)) => {
            state = s;
            i = idx;
        }
        (s, idx, None) => {
            state = s;
            i = idx;
        }
    }

    let name = if state == TAG_KEY_STATE {
        &buf[..i - 1]
    } else {
        &buf[..i]
    };

    unescape_measurement(name)
}

pub fn valid_precision(precision: &str) -> bool {
    matches!(precision, "ns" | "us" | "ms" | "s")
}

// scan_line returns the end position in buf and the next line found within buf.
fn scan_line(buf: &[u8], mut i: usize) -> (usize, &[u8]) {
    let start = i;
    let (mut quoted, mut fields) = (false, false);
    // let mut fields = false;

    // tracks how many '=' and commas we've seen this duplicates some of the functionality in scan_fields.
    let (mut equals, mut commas) = (0, 0);
    // let mut commas = 0;

    //  check if reached the end of buf
    while i < buf.len() {
        // // if reached the end of buf
        // if i >= buf.len() {
        //     break;
        // }

        // skip past escaped cahracters
        if buf[i] == b'\\' && i + 2 < buf.len() {
            i += 2;
            continue;
        }

        if buf[i] == b' ' {
            fields = true;
        }

        // make sure a duble quote is not escaped
        if fields {
            if !quoted && buf[i] == b'=' {
                i += 1;
                equals += 1;
                continue;
            } else if !quoted && buf[i] == b',' {
                i += 1;
                commas += 1;
                continue;
            } else if buf[i] == b'"' && equals > commas {
                i += 1;
                quoted = !quoted;
                continue;
            }
        }

        if buf[i] == b'\n' && !quoted {
            break;
        }
        i += 1;
    }

    (i, &buf[start..i])
}

// scan_to returns the end position in buf and the next consecutive block of bytes,
// starting from i and ending with stop byte, where stop byte has not been escaped.
//
// if there are leading spaces, they are skipped.
fn scan_to(buf: &[u8], i: usize, stop: u8) -> (usize, &[u8]) {
    // let start = i;
    let (mut i, start) = (i, i);
    while i < buf.len() {
        // reached unescaped stop value?
        if buf[i] == stop && (i == 0 || buf[i - 1] != b'\\') {
            break;
        }
        i += 1;
    }

    (i, &buf[start..i])
}

// scan_to_space_or returns the position in buf and the next consecutive block of bytes,
// starting from i adn ending with stop byte. If there are leading spaces, they are skipped.
fn scan_to_space_or(buf: &[u8], i: usize, stop: u8) -> (usize, &[u8]) {
    let (mut i, start) = (i, i);
    // let start = i;
    if buf[i] == stop || buf[i] == b' ' {
        return (i, &buf[start..i]);
    }

    loop {
        i += 1;
        if buf[i - 1] == b'\\' {
            continue;
        }

        // check if reached the end of buf
        if i >= buf.len() {
            return (i, &buf[start..i]);
        }

        // check if reached the end of block
        if buf[i] == stop || buf[i] == b' ' {
            return (i, &buf[start..i]);
        }
    }
}

// skip_white_space returns the end postion within buf, starting at i after scanning over spaces in tags.
fn skip_white_space(buf: &[u8], mut i: usize) -> usize {
    while i < buf.len() {
        if buf[i] != b' ' && buf[i] != b'\t' && buf[i] != 0 {
            break;
        }
        i += 1;
    }
    i
}

const TAG_KEY_STATE: i32 = 0;
const TAG_VALUE_STATE: i32 = 1;
const FIELD_STATE: i32 = 2;
#[allow(dead_code)]
const INVALID_STATE: i32 = -1;

fn parse_point(buf: &[u8], default_time: Time, precision: &str) -> Result<Point> {
    // scan the first block which is measurement[,tag1=valu1,tag=value2...]
    let (pos, key) = scan_key(buf, 0)?;

    // check if measurement is exist
    ensure!(!key.is_empty(), MissingMeasurementSnafu);

    ensure!(
        key.len() <= MAX_KEY_LENGTH,
        MaxKeyLengthExceedSnafu {
            key_length: key.len(),
        }
    );

    // scan the second block which is field1=value1[,field2=value2,...]
    let (mut pos, fields) = scan_fields(buf, pos)?;

    // at least one field is required
    ensure!(!fields.is_empty(), MissingFieldSnafu);

    let mut max_key_len_err: Option<Error> = None;
    match walk_fields(fields, |k: &[u8], _v: &[u8]| -> bool {
        let sz = series_key_size(&key, k);
        if sz > MAX_KEY_LENGTH {
            max_key_len_err = Some(Error::MaxKeyLengthExceed { key_length: sz });
            return false;
        }
        true
    }) {
        Err(e) => return Err(e),
        Ok(()) => {
            if let Some(e) = max_key_len_err {
                return Err(e);
            }
        }
    }

    // scan the last block which is an optional interger timestamp
    let ts: &[u8];
    (pos, ts) = scan_timestamp(buf, pos)?;

    let mut p = Point::new(&key, fields, ts);

    if ts.is_empty() {
        p.time = default_time;
        p.set_precision(precision);
    } else {
        match atoi_simd::parse::<i64>(ts) {
            Ok(t) => match safe_calc_time(t, precision) {
                Ok(dt) => p.time = dt,
                Err(_) => return TimeOutsideRangeSnafu.fail(),
            },
            Err(_) => return TimeOutsideRangeSnafu.fail(),
        }

        // determine if there are illegal non-whitespace characters after the timestamps block
        while pos < buf.len() {
            ensure!(buf[pos] == b' ', InvalidPointFormatSnafu);
            pos += 1;
        }
    }

    Ok(p)
}

// get_precision_multiplier returns a multiplication for the specified precision.
pub(crate) fn get_precision_multiplier(precision: &str) -> i64 {
    let mut d = Duration::nanoseconds(1);
    match precision {
        "us" => d = Duration::microseconds(1),
        "ms" => d = Duration::milliseconds(1),
        "s" => d = Duration::seconds(1),
        _ => {}
    }

    d.num_nanoseconds().unwrap()
}

// scan_key scans buf starting at i for the measuremnt and tag portion of the Point. It returns the ending
// position and the byte slice of key within buf. If there are tags, they will be sorted if thery are not already.
fn scan_key(buf: &[u8], i: usize) -> Result<(usize, Vec<u8>)> {
    let start = skip_white_space(buf, i);
    let mut i = start;

    // determines whether the tags are sorted, assume they are.
    let mut sorted = true;

    // indices holds the indices within buf of the start of each tag key. For example,
    // a buf of 'mem,region=beijing,host=server1 used=0.8' would have indices slice of [4, 19] which
    // indicates that the first tag key start at buf[4], second tag key starts at buf[19].
    let mut indices = Vec::<usize>::with_capacity(100);

    // tracks how many commas we've seen so we know how many values are indices. Since indices
    // is an arbitrarilylarge slice, we need to know how many values in the buffer are in use.
    let mut commas = 0;

    let state: i32;
    // first, scan the Point's measurement.
    match scan_measurement(buf, i) {
        (_s, _idx, Some(e)) => {
            return Err(e);
        }
        (s, idx, None) => {
            state = s;
            i = idx;
        }
    }

    // scan tags if needed.
    if state == TAG_KEY_STATE {
        // scan_tags function may modify indices vec.
        match scan_tags(buf, i, &mut indices) {
            Ok((idx, com)) => {
                i = idx;
                commas = com;
            }
            Err(e) => return Err(e),
        }
    }

    // iterator over tag keys ensure that we don't encounter any of the reserved tag keys
    // such '_measurement' or '_field'.
    let mut j = 0;
    while j < commas {
        // let key: &[u8];
        let (_idx, key) = scan_to(&buf[indices[j]..indices[j + 1] - 1], 0, b'=');

        for reserved in RESERVED_TAG_KEYS {
            ensure!(
                key.ne(reserved.as_bytes()),
                UseReservedTagKeySnafu {
                    reserved: String::from(*reserved),
                }
            );
        }
        j += 1;
    }

    // Now we know where the key region is within the buf, and the position of tags, we should to determine
    // if duplicated tags exist and if the tags are sorted. This iterates over the list
    // comparing each tag in the sequence with each other.
    let mut j = 0;
    while commas > 0 && j < commas - 1 {
        // get the lift and right tags
        let (_, left) = scan_to(&buf[indices[j]..indices[j + 1] - 1], 0, b'=');
        let (_, right) = scan_to(&buf[indices[j + 1]..indices[j + 2] - 1], 0, b'=');

        // If left is greater than right, the tags are not sorted, we don't have to continue
        // because the short path no longer works.
        // If the tags are equal, then there are duplicate tags, and we should abort.
        // If the tags are not sorted, this pass may not find duplicate tags and we need to do
        // a more exhaustive search later.
        match left.cmp(right) {
            Ordering::Greater => {
                sorted = false;
                break;
            }
            Ordering::Equal => {
                return DuplicateTagsSnafu {
                    duplicate: unsafe { str::from_utf8_unchecked(&buf[start..i]).to_string() },
                }
                .fail();
            }
            _ => {}
        }
        j += 1;
    }

    // If tags are not sorted, sort them first. This sort uses the tag indices we created earlier.
    // The actual buffer is not sorted, the indices are using the buffer for value comparison.
    // After the indices are sorted, the buffer is reconstructed from the sorted indices.
    if !sorted && commas > 0 {
        // Get the measurment for later
        let measurement = &buf[start..indices[0] - 1];

        // Sort the indices
        // let indices = &mut indices[0..commas];
        // insert_sort(0, commas, buf, &mut indices);
        // indices.sort_by(|a, b| b.cmp(a));  // reverse sort
        indices[0..commas].sort_by(|a, b| b.cmp(a)); // reverse sort
        let indices = &indices[0..commas];

        // Create a new key using the measurement and sorted indices
        // let mut b = Vec::<u8>::with_capacity(i - start);
        let mut b: Vec<u8> = vec![0; i - start];
        unsafe {
            b.set_len(i - start);
        }
        let _ = &b[0..measurement.len()].copy_from_slice(measurement);
        let mut pos = measurement.len();
        for i in indices {
            b[pos] = b',';
            pos += 1;
            let (_, v) = scan_to_space_or(buf, *i, b',');
            let _ = &b[pos..pos + v.len()].copy_from_slice(v);
            pos += v.len();
        }

        // Check again for duplicated tags now that the tags are sorted.
        let mut k = 0;
        while k < commas - 1 {
            // find left and right tag
            let (_, left) = scan_to(&buf[indices[k]..], 0, b'=');
            let (_, right) = scan_to(&buf[indices[k + 1]..], 0, b'=');

            ensure!(
                left.ne(right),
                DuplicateTagsSnafu {
                    duplicate: to_string!(left),
                }
            );
            k += 1;
        }

        return Ok((i, b));
    }

    Ok((i, buf[start..i].to_vec()))
}

// scan_measurement checks the measurement part of a Point, returning the next state to move to,
// and current position in the buffer.
fn scan_measurement(buf: &[u8], i: usize) -> (i32, usize, Option<Error>) {
    let mut i = i;
    // check first byte of measurement, anythind except a comma if correct.
    // it can't be a space, since whitespace is stripped prior to this function call.
    if i >= buf.len() || buf[i] == b',' {
        return (-1, i, Some(Error::MissingMeasurement));
    }

    loop {
        i += 1;
        if i >= buf.len() {
            // mem
            return (-1, i, Some(Error::MissingField));
        }

        if buf[i - 1] == b'\\' {
            // skip character (it's escaped)
            continue;
        }

        // unescaped comma, move onto scanning the tags.
        if buf[i] == b',' {
            return (TAG_KEY_STATE, i + 1, None);
        }

        // unescaped space, move onto scanning the fields.
        if buf[i] == b' ' {
            // mem value=1.0
            return (FIELD_STATE, i, None);
        }
    }
}

// scan_tags checks all the tags in a Point, keeping track of and returning the updated indices slice,
// number of commas and location in buf where to start checking the Point fields.
//
// this function may modify the indices vec.
fn scan_tags(buf: &[u8], i: usize, indices: &mut Vec<usize>) -> Result<(usize, usize)> {
    let mut i = i;
    let mut commas = 0;
    let mut state = TAG_KEY_STATE;

    loop {
        match state {
            TAG_KEY_STATE => {
                // grow indices slice if have too many tags.
                if commas >= indices.len() {
                    // double the capacity of indices
                    indices.resize(indices.capacity() * 2, 0);
                }
                indices[commas] = i;
                commas += 1;

                match scan_tags_key(buf, i) {
                    Ok(idx) => {
                        i = idx;
                        state = TAG_VALUE_STATE;
                    }
                    Err(e) => return Err(e),
                }
            }
            TAG_VALUE_STATE => match scan_tags_value(buf, i) {
                Ok((s, idx)) => {
                    state = s;
                    i = idx;
                }
                Err(e) => return Err(e),
            },
            FIELD_STATE => {
                // grow indices slice if had exactly enough tags to fill it.
                if commas >= indices.len() {
                    // the parser is in `FIELD_STATE`, so there are no more tags,
                    // we only need 1 more entry in the slice to store the final entry.
                    indices.resize(indices.capacity() + 1, 0);
                }
                indices[commas] = i + 1;
                return Ok((i, commas));
            }
            _ => {}
        }
    }
}

// scan_tags_key checks each character in a tag key.
fn scan_tags_key(buf: &[u8], i: usize) -> Result<usize> {
    let mut i = i;
    // check first character of the key.

    ensure!(
        i <= buf.len() && buf[i] != b' ' && buf[i] != b',' && buf[i] != b'=',
        MissingTagKeySnafu // mem,{'', ' ', ',', '='}
    );

    // check each chacter in the tag key until hit an unescaped equals (tag value),
    // or hit an error (i.e., unescaped space or comma).
    loop {
        i += 1;
        // either reached the end of the buffer or hit an unescaped comma or space.
        ensure!(
            !(i > buf.len() || ((buf[i] == b' ' || buf[i] == b',') && buf[i - 1] != b'\\')),
            // mem,tag{'', ' ', ','}
            MissingTagValueSnafu
        );

        if buf[i] == b'=' && buf[i - 1] != b'\\' {
            // mem,tag=
            return Ok(i + 1);
        }
    }
}

// scan_tags_value checks each character in a tag value.
fn scan_tags_value(buf: &[u8], i: usize) -> Result<(i32, usize)> {
    let mut i = i;
    // make sure tag value is not empty.
    ensure!(
        i <= buf.len() && buf[i] != b',' && buf[i] != b' ',
        // mem,tag={',',' '}
        MissingTagValueSnafu
    );

    // check each character in the tag value until hit an unescaped comma (move onto next tag key),
    // an unescaped space (move onto fields), or error out.
    loop {
        i += 1;
        ensure!(i < buf.len(), MissingTagValueSnafu); // mem,tag1=value1

        // an unescaped equals sign is an invalid tag value.
        ensure!(
            !(buf[i] == b'=' && buf[i - 1] != b'\\'),
            InvalidTagFormatSnafu
        ); // mem,tag1={'=', 'tag2=value2'}

        if buf[i] == b',' && buf[i - 1] != b'\\' {
            // mem,tag=value
            return Ok((TAG_KEY_STATE, i + 1));
        }

        // mem,tag1=key1 value1=1.0
        // mem, tag1=key1\= value1=1.0
        if buf[i] == b' ' && buf[i - 1] != b'\\' {
            return Ok((FIELD_STATE, i));
        }
    }
}

fn scan_tag_value(buf: &[u8], i: usize) -> (usize, Option<&[u8]>) {
    let (mut i, start) = (i, i);
    while i < buf.len() {
        if buf[i] == b',' && buf[i - 1] != b'\\' {
            break;
        }
        i += 1;
    }
    if i > buf.len() {
        return (i, None);
    }

    (i, Some(&buf[start..i]))
}

// scan_fields scans buf, starting at i for the fields section of a point, returns the ending position
// and the byte slice of the fields within buf.
fn scan_fields(buf: &[u8], i: usize) -> Result<(usize, &[u8])> {
    let start = skip_white_space(buf, i);
    let mut i = start;
    let mut quoted = false;

    // tracks how many '=' we have seen
    let mut equals = 0;
    // tracks how many commas we have seen
    let mut commas = 0;

    while i < buf.len() {
        // escaped characters?
        if buf[i] == b'\\' && i + 1 < buf.len() {
            i += 2;
            continue;
        }

        // If the value is quoted,  scan until we get to the end quote.
        // Only quote values in the field value since quotes are not significant in the field key.
        if buf[i] == b'"' && equals > commas {
            quoted = !quoted;
            i += 1;
            continue;
        }

        // If we seen an '=', ensure that there is at leaset on char before and after it.
        if buf[i] == b'=' && !quoted {
            equals += 1;

            // check for "... =123" but allow "a\ =123"
            ensure!(
                !(buf[i - 1] == b' ' && buf[i - 2] != b'\\'),
                MissingFieldKeySnafu
            );

            // check for "...a=123,=456" but allow "a=123,a\,=456"
            ensure!(
                !(buf[i - 1] == b',' && buf[i - 2] != b'\\'),
                MissingFieldKeySnafu
            );

            // check for "... value="
            ensure!(i + 1 < buf.len(), MissingFieldValueSnafu);

            // check for ".. value1=,value2=..."
            ensure!(
                !(buf[i + 1] == b',' || buf[i + 1] == b' '),
                MissingFieldValueSnafu
            );

            if is_numeric(buf[i + 1])
                || buf[i + 1] == b'-'
                || buf[i + 1] == b'N'
                || buf[i + 1] == b'n'
            {
                match scan_number(buf, i + 1) {
                    Ok(idx) => i = idx,
                    Err(e) => return Err(e),
                }
                continue;
            }

            // if next byte is not a double-quote, the value must be a bool
            if buf[i + 1] != b'"' {
                let _bool_buf: &[u8];
                // (i, _bool_buf) = scan_bool(buf, i+1)?;
                // return Ok((i, &buf[start..i]))
                match scan_bool(buf, i + 1) {
                    Ok((idx, _bool)) => i = idx,
                    Err(e) => return Err(e),
                }
                continue;
            }
        }

        if buf[i] == b',' && !quoted {
            commas += 1;
        }

        // if reached end of block
        if buf[i] == b' ' && !quoted {
            break;
        }
        i += 1;
    }

    ensure!(!quoted, UnbalancedQuotesSnafu);

    // check that all field sections had key and values (e.g. prevent "a=1,b")
    ensure!(
        !(equals == 0 || commas != equals - 1),
        InvalidFieldFormatSnafu
    );

    Ok((i, &buf[start..i]))
}

fn scan_field_value(buf: &[u8], i: usize) -> Result<(usize, &[u8])> {
    let (mut i, start) = (i, i);
    let mut quoted = false;
    while i < buf.len() {
        // only escape character for a field value is ad double-quote and backslash
        if buf[i] == b'\\' && i + 1 < buf.len() && (buf[i + 1] == b'"' || buf[i + 1] == b'\\') {
            i += 2;
            continue;
        }

        // check if quoted value (e.g. string)
        if buf[i] == b'"' {
            i += 1;
            quoted = !quoted;
            continue;
        }

        if buf[i] == b',' && !quoted {
            break;
        }
        i += 1;
    }

    if start > i || i > buf.len() {
        return Err(Error::InvalidFieldFormat);
    }

    Ok((i, &buf[start..i]))
}

// scan_timestamp scans buf, starting at i for the time section of a point, it returns the ending position
// and the byte slice of the timestamp within buf and error if the timestamp is not in the corrent numeric format
fn scan_timestamp(buf: &[u8], i: usize) -> Result<(usize, &[u8])> {
    let start = skip_white_space(buf, i);
    let mut i = start;

    while i < buf.len() {
        // check if reached end of block or trailing whitespace
        if buf[i] == b'\n' || buf[i] == b' ' {
            break;
        }

        // handle negative timestamps
        if i == start && buf[i] == b'-' {
            i += 1;
            continue;
        }

        // timestamps should be integers, make sure they are so we don't need to actually
        // parse the timestamp until needed.
        ensure!(buf[i] >= b'0' && buf[i] <= b'9', InvalidTimestampSnafu);
        i += 1;
    }

    Ok((i, &buf[start..i]))
}

#[allow(dead_code)]
// insert_sort function may modify the Vector.
fn insert_sort(l: usize, r: usize, buf: &[u8], indices: &mut [usize]) {
    let mut i = l + 1;
    while i < r {
        let mut j = i;
        while j > l && less(buf, indices, j, j - 1) {
            // swap(indices, j, j-1);
            (indices[j], indices[j - 1]) = (indices[j - 1], indices[j]);
            j -= 1;
        }
        i += 1;
    }
}

fn less(buf: &[u8], indices: &[usize], i: usize, j: usize) -> bool {
    let (_, a) = scan_to(buf, indices[i], b'=');
    let (_, b) = scan_to(buf, indices[j], b'=');

    a < b
}

#[allow(dead_code)]
fn swap(v: &mut [usize], i: usize, j: usize) {
    v.swap(i, j);
}

fn is_numeric(b: u8) -> bool {
    b.is_ascii_digit() || b == b'.'
}

// scan_number returns the end position within buf, start at i after scanning over buf for an integer,
// or float. It returns an error if a invalid number is scanned.
fn scan_number(buf: &[u8], i: usize) -> Result<usize> {
    let (mut i, start) = (i, i);
    let mut is_int = false;
    let mut is_unsigned = false;

    // is negative number ?
    if i < buf.len() && buf[i] == b'-' {
        i += 1;
        // there must be more characters now, as just '-' is illegal
        ensure!(i != buf.len(), InvalidNumberValueSnafu);
    }

    // tracks how many decimal points we've seen
    let mut decimal = false;

    // indicates the number is float in scientific notation
    let mut scientific = false;

    while i < buf.len() {
        if buf[i] == b',' || buf[i] == b' ' {
            break;
        }

        if buf[i] == b'i' && i > start && !(is_int || is_unsigned) {
            is_int = true;
            i += 1;
            continue;
        } else if buf[i] == b'u' && i > start && !(is_int || is_unsigned) {
            is_unsigned = true;
            i += 1;
            continue;
        }

        if buf[i] == b'.' {
            // Can't have more than 1 decimal (e.g. 1.1.1 should fail)
            ensure!(!decimal, InvalidNumberValueSnafu);
            decimal = true
        }

        // `e` is valid for floats but not as the first char
        if i > start && (buf[i] == b'e' || buf[i] == b'E') {
            scientific = true;
            i += 1;
            continue;
        }

        // + and - are only valid at this point if they follow an e (scientific notation)
        if (buf[i] == b'+' || buf[i] == b'-') && (buf[i - 1] == b'e' || buf[i - 1] == b'E') {
            i += 1;
            continue;
        }

        // NaN is an unsupported value
        ensure!(
            !(i + 2 < buf.len() && (buf[i] == b'N' || buf[i] == b'n')),
            InvalidNumberValueSnafu
        );

        ensure!(is_numeric(buf[i]), InvalidNumberValueSnafu);

        i += 1;
    }

    ensure!(
        !((is_int || is_unsigned) && (decimal || scientific)),
        InvalidNumberValueSnafu
    );

    let mut numeric_digits = i - start;
    if is_int {
        numeric_digits -= 1;
    }
    if decimal {
        numeric_digits -= 1;
    }
    if buf[start] == b'-' {
        numeric_digits -= 1;
    }

    ensure!(numeric_digits != 0, InvalidNumberValueSnafu);

    // It's more common that numbers will be within min/max range for their type but we need to prevent
    // out or range numbers from being parsed successfully.  This uses some simple heuristics to decide
    // if we should parse the number to the actual type.  It does not do it all the time because it incurs
    // extra allocations and we end up converting the type again when writing points to disk.
    if is_int {
        // make sure the last character is an 'i' for integers (e.g. 3i4 is illegal)
        ensure!(buf[i - 1] == b'i', InvalidNumberValueSnafu);

        // Parse the int to check bounds the number of digits could be larger than the max range
        // We subtract 1 from the index to remove the `i` from our tests
        if buf[start..i - 1].len() >= MAX_INT64_DIGITS
            || buf[start..i - 1].len() >= MIN_INT64_DIGITS
        {
            ensure!(
                atoi_simd::parse::<i64>(&buf[start..i - 1]).is_ok(),
                ParseIntegerFailedSnafu {
                    integer: to_string!(&buf[start..i - 1]),
                }
            );
        }
    } else if is_unsigned {
        // make sure the last character is an 'u' for unsigned
        ensure!(buf[i - 1] == b'u', InvalidNumberValueSnafu);
        // make sure the first character is not a '-' fo runsigned
        ensure!(buf[start] != b'-', InvalidNumberValueSnafu);
        // Parse the uint to check bounds the number of digits could be larger than the max range
        // We subtract 1 from the index to remove the `u` from our tests
        if buf[start..i - 1].len() >= MAX_UINT64_DIGITS {
            ensure!(
                atoi_simd::parse::<u64>(&buf[start..i - 1]).is_ok(),
                ParseUnsignedFailedSnafu {
                    unsigned: to_string!(&buf[start..i - 1]),
                }
            );
        }
    } else {
        // Parse the float to check bounds if it's scientific or the number of digits could be larger than the max range
        if scientific
            || buf[start..i].len() >= MAX_FLOAT64_DIGITS
            || buf[start..i].len() >= MIN_FLOAT_DIGITS
        {
            // Pinned InfluxDB LP float grammar after leading-byte dispatch:
            // "-"? (digits ("." digits*)? | "." digits)
            // (("e" | "E") ("+" | "-")? digits)?
            // Conversion validates digit placement; stored values must be finite.
            let parsed = fast_float::parse::<f64, _>(&buf[start..i]);
            ensure!(
                matches!(parsed, Ok(value) if value.is_finite()),
                ParseFloatFailedSnafu {
                    float: to_string!(&buf[start..i]),
                }
            );
        }
    }

    Ok(i)
}

// scan_bool returns the end position within the buf, start at i after scanning over buf for bool.
// Valid values for a bool are t, T, true, TRUE, false, FALSE. It returns an error if a invalid bool is scanned.
fn scan_bool(buf: &[u8], i: usize) -> Result<(usize, &[u8])> {
    let (mut i, start) = (i, i);
    // let start = i;

    ensure!(
        !(i < buf.len() && buf[i] != b't' && buf[i] != b'f' && buf[i] != b'T' && buf[i] != b'F'),
        InvalidBoolValueSnafu
    );

    i += 1;
    while i < buf.len() {
        if buf[i] == b',' || buf[i] == b' ' {
            break;
        }
        i += 1;
    }

    // single char bool (t, T, f, F) is fine
    if i - start == 1 {
        return Ok((i, &buf[start..i]));
    }

    // length must be 4 for true or TRUE
    ensure!(
        !((buf[start] == b't' || buf[start] == b'T') && i - start != 4),
        InvalidBoolValueSnafu
    );

    // length must be 5 for false or FALSE
    ensure!(
        !((buf[start] == b'f' || buf[start] == b'F') && i - start != 5),
        InvalidBoolValueSnafu
    );

    // otherwise
    let mut valid = false;
    match buf[start] {
        // b't' => valid = buf[start..i].cmp("true") == Ordering::Equal,
        b't' => valid = &buf[start..i] == "true".as_bytes(),
        b'f' => valid = &buf[start..i] == "false".as_bytes(),
        b'T' => valid = &buf[start..i] == "TRUE".as_bytes() || &buf[start..i] == "True".as_bytes(),
        b'F' => {
            valid = &buf[start..i] == "FALSE".as_bytes() || &buf[start..i] == "False".as_bytes()
        }
        _ => {}
    }

    ensure!(valid, InvalidBoolValueSnafu);

    Ok((i, &buf[start..i]))
}

fn series_key_size(key: &[u8], field: &[u8]) -> usize {
    // 4 is the length of FIELD_KEY_SEPARATOR (#!~#) const
    key.len() + 4 + field.len()
}

fn walk_tags<F>(buf: &[u8], mut f: F)
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    if buf.is_empty() {
        return;
    }

    let (pos, name) = scan_to(buf, 0, b',');
    // it's an empty key, so there are no tags
    if name.is_empty() {
        return;
    }

    let mut has_escape = false;
    if buf.iter().any(|&x| x == b'\\') {
        has_escape = true;
    }

    let mut i = pos + 1;
    let mut key: &[u8];
    let mut value: Option<&[u8]>;
    while i < buf.len() {
        (i, key) = scan_to(buf, i, b'=');
        // let v: Option<&[u8]>;
        (i, value) = scan_tag_value(buf, i + 1);
        if value.is_none() {
            continue;
        }

        if has_escape {
            if !f(
                unescape_tag(key).as_slice(),
                unescape_tag(value.unwrap()).as_slice(),
            ) {
                return;
            }
        } else if !f(key, value.unwrap()) {
            return;
        }

        i += 1;
    }
}

fn walk_fields<F>(buf: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> bool,
{
    let mut i;
    let mut buf = buf;
    let mut key: &[u8];
    let mut value: &[u8];
    while !buf.is_empty() {
        (i, key) = scan_to(buf, 0, b'=');
        ensure!(i <= buf.len() - 2, InvalidFieldFormatSnafu); // TODO: error corrent ? return k
        buf = &buf[i + 1..];
        (i, value) = scan_field_value(buf, 0)?;
        buf = &buf[i..];
        if !f(key, value) {
            break;
        }

        // slice off comma
        if !buf.is_empty() {
            buf = &buf[1..];
        }
    }

    Ok(())
}

// // //////////////////////////////////////////////////
// // /////// Field ////////////////////////////////////
// // //////////////////////////////////////////////////
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    IntegerValue(IntegerValue),
    UnsignedValue(UnsignedValue),
    FloatValue(FloatValue),
    BoolValue(BoolValue),
    StringValue(StringValue),
    EmptyValue(EmptyValue),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum Val {
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    String(String),
    Empty(Option<()>),
}

impl Default for Val {
    fn default() -> Self {
        Val::Empty(None)
    }
}

impl Eq for Val {}

#[derive(Clone)]
pub enum RawVal {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    ISize(isize),
    USize(usize),
    Bool(bool),
    String(String),
    Vec(Vec<u8>),
}

impl Value {
    // pub fn block_type(&self) -> BlockType {
    //     match self {
    //         Value::IntegerValue(v) => v.block_type(),
    //         Value::UnsignedValue(v) => v.block_type(),
    //         Value::FloatValue(v) => v.block_type(),
    //         Value::BoolValue(v) => v.block_type(),
    //         Value::StringValue(v) => v.block_type(),
    //     }
    // }

    // Value returns the underlying value.
    pub fn value(&self) -> Val {
        match self {
            Value::IntegerValue(v) => Val::I64(v.value),
            Value::UnsignedValue(v) => Val::U64(v.value),
            Value::FloatValue(v) => Val::F64(v.value),
            Value::BoolValue(v) => Val::Bool(v.value),
            Value::StringValue(v) => Val::String(v.value.clone()),
            Value::EmptyValue(_) => Val::Empty(None),
        }
    }

    // UnixNano returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        match self {
            Value::IntegerValue(v) => v.unix_nano,
            Value::UnsignedValue(v) => v.unix_nano,
            Value::FloatValue(v) => v.unix_nano,
            Value::BoolValue(v) => v.unix_nano,
            Value::StringValue(v) => v.unix_nano,
            Value::EmptyValue(_) => ZERO_TIME,
        }
    }

    // Size returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        match self {
            Value::IntegerValue(v) => v.size(),
            Value::UnsignedValue(v) => v.size(),
            Value::FloatValue(v) => v.size(),
            Value::BoolValue(v) => v.size(),
            Value::StringValue(v) => v.size(),
            Value::EmptyValue(v) => v.size(),
        }
    }

    pub fn raw_value(&self) -> Val {
        match self {
            Value::IntegerValue(v) => Val::I64(v.raw_value()),
            Value::UnsignedValue(v) => Val::U64(v.raw_value()),
            Value::FloatValue(v) => Val::F64(v.raw_value()),
            Value::BoolValue(v) => Val::Bool(v.raw_value()),
            Value::StringValue(v) => Val::String(v.raw_value()),
            Value::EmptyValue(v) => Val::Empty(v.raw_value()),
        }
    }

    pub fn value_type(&self) -> FieldType {
        match self {
            Value::EmptyValue(_) => EMPTY,
            Value::StringValue(v) => STRING,
            Value::IntegerValue(v) => INTEGER,
            Value::UnsignedValue(v) => UNSIGNED,
            Value::BoolValue(v) => BOOL,
            Value::FloatValue(v) => FLOAT,
        }
    }

    /// Returns the integer value of the current field.
    pub fn integer_value(&self) -> Option<i64> {
        match self {
            Value::IntegerValue(v) => Some(v.value),
            _ => None,
        }
    }

    /// Returns the unsigned value of the current field.
    pub fn unsigned_value(&self) -> Option<u64> {
        match self {
            Value::UnsignedValue(v) => Some(v.value),
            _ => None,
        }
    }

    /// Returns the float value of the current field.
    pub fn float_value(&self) -> Option<f64> {
        match self {
            Value::FloatValue(v) => Some(v.value),
            _ => None,
        }
    }

    /// Returns the string value of the current field.
    pub fn string_value(&self) -> Option<String> {
        match self {
            Value::StringValue(v) => Some(v.value.clone()),
            _ => None,
        }
    }

    /// Returns the boolean value of the current field.
    pub fn bool_value(&self) -> Option<bool> {
        match self {
            Value::BoolValue(v) => Some(v.value),
            _ => None,
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::IntegerValue(v) => write!(f, "{}", v),
            Value::UnsignedValue(v) => write!(f, "{}", v),
            Value::FloatValue(v) => write!(f, "{}", v),
            Value::BoolValue(v) => write!(f, "{}", v),
            Value::StringValue(v) => write!(f, "{}", v),
            Value::EmptyValue(v) => write!(f, "{}", v),
        }
    }
}

impl From<IntegerValue> for Value {
    fn from(value: IntegerValue) -> Self {
        Value::IntegerValue(value)
    }
}

impl From<UnsignedValue> for Value {
    fn from(value: UnsignedValue) -> Self {
        Value::UnsignedValue(value)
    }
}

impl From<FloatValue> for Value {
    fn from(value: FloatValue) -> Self {
        Value::FloatValue(value)
    }
}

impl From<BoolValue> for Value {
    fn from(value: BoolValue) -> Self {
        Value::BoolValue(value)
    }
}

impl From<StringValue> for Value {
    fn from(value: StringValue) -> Self {
        Value::StringValue(value)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Self::BoolValue(BoolValue {
            unix_nano: 0,
            value: v,
        })
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Self::IntegerValue(IntegerValue {
            unix_nano: 0,
            value: v,
        })
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Self::UnsignedValue(UnsignedValue {
            unix_nano: 0,
            value: v,
        })
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Self::FloatValue(FloatValue {
            unix_nano: 0,
            value: v,
        })
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Self::StringValue(StringValue {
            unix_nano: 0,
            value: v,
        })
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Self::StringValue(StringValue {
            unix_nano: 0,
            value: v.to_string(),
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EmptyValue;

impl EmptyValue {
    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        ZERO_TIME
    }

    /// Returns the underlying empty value.
    pub fn value(&self) -> Option<()> {
        None
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        0
    }

    pub fn raw_value(&self) -> Option<()> {
        None
    }
}

impl Display for EmptyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct IntegerValue {
    pub unix_nano: i64,
    pub value: i64,
}

impl IntegerValue {
    pub fn new(unix_nano: i64, value: i64) -> Self {
        Self { unix_nano, value }
    }

    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        self.unix_nano
    }

    /// Returns the underlying integer value.
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        16
    }

    pub fn raw_value(&self) -> i64 {
        self.value
    }
}

impl Display for IntegerValue {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Time::from_timestamp_nanos(self.unix_nano),
            self.value
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct UnsignedValue {
    pub unix_nano: i64,
    pub value: u64,
}

impl UnsignedValue {
    pub fn new(unix_nano: i64, value: u64) -> Self {
        Self { unix_nano, value }
    }

    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        self.unix_nano
    }

    /// Returns the underlying unsigned value.
    pub fn value(&self) -> u64 {
        self.value
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        16
    }

    pub fn raw_value(&self) -> u64 {
        self.value
    }
}

impl Display for UnsignedValue {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Time::from_timestamp_nanos(self.unix_nano),
            self.value
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FloatValue {
    pub unix_nano: i64,
    pub value: f64,
}

impl FloatValue {
    pub fn new(unix_nano: i64, value: f64) -> Self {
        Self { unix_nano, value }
    }

    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        self.unix_nano
    }

    /// Returns the underlying float64 value.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        16
    }

    pub fn raw_value(&self) -> f64 {
        self.value
    }
}

impl Display for FloatValue {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Time::from_timestamp_nanos(self.unix_nano),
            self.value
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BoolValue {
    pub unix_nano: i64,
    pub value: bool,
}

impl BoolValue {
    pub fn new(unix_nano: i64, value: bool) -> Self {
        Self { unix_nano, value }
    }

    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        self.unix_nano
    }

    /// Returns the underlying bool value.
    pub fn value(&self) -> bool {
        self.value
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        9
    }

    pub fn raw_value(&self) -> bool {
        self.value
    }
}

impl Display for BoolValue {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Time::from_timestamp_nanos(self.unix_nano),
            self.value
        )
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct StringValue {
    pub unix_nano: i64,
    pub value: String,
}

impl StringValue {
    pub fn new(unix_nano: i64, value: String) -> Self {
        Self { unix_nano, value }
    }

    /// Returns the timestamp of the value.
    pub fn unix_nano(&self) -> i64 {
        self.unix_nano
    }

    /// Returns the underlying string value.
    pub fn value(&self) -> String {
        self.value.clone()
    }

    /// Returns the number of bytes necessary to represent the value and its timestamp.
    pub fn size(&self) -> i64 {
        8 + self.value.len() as i64
    }

    pub fn raw_value(&self) -> String {
        self.value.clone()
    }
}

impl Display for StringValue {
    // String returns the string representation of the value and its timestamp.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            Time::from_timestamp_nanos(self.unix_nano),
            self.value
        )
    }
}

pub struct Fields(pub HashMap<String, RawVal>);

impl Fields {
    // Encodes all the fields to their proper type and returns the binary
    // representation
    // NOTE: u64 is specifically not supported due to potential overflow when we decode
    // again later to an i64
    // NOTE2: uint is accepted, and may be 64 bits, and is for some reason accepted...
    pub fn marshal_binary(&self) -> Vec<u8> {
        let mut sz = self.len() - 1; // separators
        let mut keys = vec![];
        for k in self.keys() {
            keys.push(k.clone());
            sz += k.len();
        }

        // Only sort if we have multiple fields to sort.
        // This length check removes an allocation incurred by the sort.
        if keys.len() > 1 {
            keys.sort();
        }

        let mut b = Vec::with_capacity(sz);
        for (i, k) in keys.iter().enumerate() {
            if i > 0 {
                b.push(b',');
            }
            append_field(&mut b, k, self.get(k).unwrap().clone());
        }

        b
    }
}

// TODO: optimize me
fn append_field(b: &mut Vec<u8>, k: &str, v: RawVal) {
    // FIXME: escape first
    // crate::models::escape::strings: (`,`, `\,`, `"`, `\"`, ` `, `\ `, `=`, `\=`)
    b.extend_from_slice(k.as_bytes());
    b.push(b'=');

    // check popular types first
    match v {
        RawVal::I8(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::I16(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::I32(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::I64(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v).as_bytes());
            b.push(b'i');
        }
        RawVal::U8(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::U16(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::U32(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::U64(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v).as_bytes());
            b.push(b'u');
        }
        RawVal::F32(v) => {
            let mut buffer = ryu::Buffer::new();
            b.extend_from_slice(buffer.format(v as f64).as_bytes());
        }
        RawVal::F64(v) => {
            // FIXME: b = strconv.AppendFloat(b, v, 'f', -1, 64)
            let mut buffer = ryu::Buffer::new();
            b.extend_from_slice(buffer.format(v).as_bytes());
        }
        RawVal::ISize(v) => {
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::USize(v) => {
            // TODO: 'usize' should be converted to writing as an unsigned integer,
            // but we cannot since that would break backwards compatibility.
            let mut buffer = itoa::Buffer::new();
            b.extend_from_slice(buffer.format(v as i64).as_bytes());
            b.push(b'i');
        }
        RawVal::Bool(v) => {
            b.extend(bool::to_string(&v).into_bytes());
        }
        RawVal::String(v) => {
            b.push(b'"');
            // FIXME: escape first, escapeStringFieldReplacer = strings.NewReplacer(`"`, `\"`, `\`, `\\`)
            b.extend_from_slice(v.as_bytes());
            b.push(b'"');
        }
        RawVal::Vec(v) => {
            b.extend_from_slice(&v);
        } // FIXME: there are types that we can't determine ? so conver to string.
          // _ => {
          //     default:
          //     // Can't determine the type, so convert to string
          //     b = append(b, '"')
          //     b = append(b, []byte(EscapeStringField(fmt.Sprintf("%v", v)))...)
          //     b = append(b, '"')
          // },
    }
}

// fn append_field(b: &mut Vec<u8>, k: &str, v: RawVal) {
//     b.extend_from_slice(&escape_tag(k.as_bytes()));

//     // rewrite golang：这里好像是错误的
//     // b.extend_from_slice(crate::models::escape::strings::escape_string_owned(k).as_bytes());
//     // b.append(&mut crate::models::escape::strings::escape_string_owned(k).into_bytes());
//     b.push(b'=');

//     match v {
//         RawVal::I8(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::I16(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::I32(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::I64(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::U8(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::U16(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::U32(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::U64(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v).as_bytes());
//             b.push(b'u');
//         }
//         RawVal::F32(v) => {
//             let mut buffer = ryu::Buffer::new();
//             b.extend_from_slice(buffer.format(v as f64).as_bytes());
//         }
//         RawVal::F64(v) => append_float_decimal(b, v),
//         RawVal::ISize(v) => {
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::USize(v) => {
//             // TODO: 'usize' should be converted to writing as an unsigned integer,
//             // but we cannot since that would break backwards compatibility.
//             let mut buffer = itoa::Buffer::new();
//             b.extend_from_slice(buffer.format(v as i64).as_bytes());
//             b.push(b'i');
//         }
//         RawVal::Bool(v) => {
//             b.extend(bool::to_string(&v).into_bytes());
//         }
//         RawVal::String(v) => {
//             b.push(b'"');
//             b.append(&mut escape_string_field_owned(&v).into_bytes());
//             b.push(b'"');
//         }
//         RawVal::Vec(v) => {
//             b.extend_from_slice(&v);
//         }
//     }
// }

fn append_float_decimal(b: &mut Vec<u8>, v: f64) {
    let mut buffer = ryu::Buffer::new();
    let formatted = buffer.format(v);

    if formatted
        .as_bytes()
        .iter()
        .any(|ch| matches!(*ch, b'e' | b'E'))
    {
        b.extend_from_slice(expand_exponent_float(formatted).as_bytes());
    } else {
        b.extend_from_slice(formatted.as_bytes());
    }
}

fn expand_exponent_float(input: &str) -> String {
    let (mantissa, exponent) = input.split_once(['e', 'E']).unwrap();
    let exponent: i32 = exponent.parse().unwrap();
    let (sign, mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
        ("-", rest)
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        ("+", rest)
    } else {
        ("", mantissa)
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));

    let mut digits = String::with_capacity(int_part.len() + frac_part.len());
    digits.push_str(int_part);
    digits.push_str(frac_part);

    let decimal_pos = int_part.len() as i32 + exponent;
    if decimal_pos <= 0 {
        let mut out =
            String::with_capacity(sign.len() + 2 + (-decimal_pos) as usize + digits.len());
        out.push_str(sign);
        out.push_str("0.");
        for _ in 0..(-decimal_pos) {
            out.push('0');
        }
        out.push_str(&digits);
        return out;
    }

    let decimal_pos = decimal_pos as usize;
    if decimal_pos >= digits.len() {
        let mut out = String::with_capacity(sign.len() + decimal_pos);
        out.push_str(sign);
        out.push_str(&digits);
        for _ in digits.len()..decimal_pos {
            out.push('0');
        }
        return out;
    }

    let mut out = String::with_capacity(sign.len() + digits.len() + 1);
    out.push_str(sign);
    out.push_str(&digits[..decimal_pos]);
    out.push('.');
    out.push_str(&digits[decimal_pos..]);
    out
}

// fn escape_string_field_value(value: &str) -> Vec<u8> {
//     let mut out = Vec::with_capacity(value.len());
//     for ch in value.bytes() {
//         if matches!(ch, b'\\' | b'"') {
//             out.push(b'\\');
//         }
//         out.push(ch);
//     }
//     out
// }

/// Escapes:
/// - `"`  -> `\"`
/// - `\`  -> `\\`
///
/// Fast path:
/// - returns `Cow::Borrowed` if no escaping is needed
#[inline]
pub fn escape_string_field(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();

    let mut first = 0;
    while first < bytes.len() && !needs_escape(bytes[first]) {
        first += 1;
    }

    if first == bytes.len() {
        return Cow::Borrowed(input);
    }

    let mut extra = 1; // bytes[first] needs escaping
    let mut i = first + 1;
    while i < bytes.len() {
        extra += needs_escape(bytes[i]) as usize;
        i += 1;
    }

    let mut out = Vec::with_capacity(bytes.len() + extra);
    out.extend_from_slice(&bytes[..first]);

    i = first;
    while i < bytes.len() {
        let b = bytes[i];
        if needs_escape(b) {
            out.push(b'\\');
        }
        out.push(b);
        i += 1;
    }

    // SAFETY:
    // input is valid UTF-8, and we only insert ASCII '\' bytes.
    Cow::Owned(unsafe { String::from_utf8_unchecked(out) })
}

/// Unescapes:
/// - `\\` -> `\`
/// - `\"` -> `"`
///
/// Any other `\x` sequence is preserved as-is, matching the Go behavior.
///
/// Fast path:
/// - returns `Cow::Borrowed` if no recognized escape sequence exists
#[inline]
pub fn unescape_string_field(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();

    if !bytes.contains(&b'\\') {
        return Cow::Borrowed(input);
    }

    let mut first = 0;
    while first + 1 < bytes.len() && !is_recognized_escape(bytes[first], bytes[first + 1]) {
        first += 1;
    }

    if first + 1 >= bytes.len() {
        return Cow::Borrowed(input);
    }

    let mut removed = 1; // first recognized escape removes one '\'
    let mut i = first + 2;
    while i + 1 < bytes.len() {
        if is_recognized_escape(bytes[i], bytes[i + 1]) {
            removed += 1;
            i += 2;
        } else {
            i += 1;
        }
    }

    let mut out = Vec::with_capacity(bytes.len() - removed);
    out.extend_from_slice(&bytes[..first]);

    i = first;
    while i < bytes.len() {
        if i + 1 < bytes.len() && is_recognized_escape(bytes[i], bytes[i + 1]) {
            out.push(bytes[i + 1]);
            i += 2;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }

    // SAFETY:
    // input is valid UTF-8, and we only remove ASCII '\' in recognized escapes.
    Cow::Owned(unsafe { String::from_utf8_unchecked(out) })
}

#[inline(always)]
const fn needs_escape(b: u8) -> bool {
    matches!(b, b'"' | b'\\')
}

#[inline(always)]
const fn is_recognized_escape(a: u8, b: u8) -> bool {
    a == b'\\' && matches!(b, b'\\' | b'"')
}

/// Convenience wrapper: always returns `String`.
#[inline]
pub fn escape_string_field_owned(input: &str) -> String {
    escape_string_field(input).into_owned()
}

/// Convenience wrapper: always returns `String`.
#[inline]
pub fn unescape_string_field_owned(input: &str) -> String {
    unescape_string_field(input).into_owned()
}

impl Deref for Fields {
    type Target = HashMap<String, RawVal>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Fields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub field_key: Vec<u8>,
    pub field_type: FieldType,
    pub value: Value,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.field_type {
            INTEGER => write!(
                f,
                "field_type: i64, key: {}, value: {}i",
                to_str!(&self.field_key),
                self.value.integer_value().unwrap()
            ),
            UNSIGNED => write!(
                f,
                "field_type: u64, key: {}, value: {}u",
                to_str!(&self.field_key),
                self.value.unsigned_value().unwrap()
            ),
            FLOAT => write!(
                f,
                "field_type: f64, key: {}, value: {}",
                to_str!(&self.field_key),
                self.value.float_value().unwrap()
            ),
            BOOL => write!(
                f,
                "field_type: bool, key: {}, value: {}",
                to_str!(&self.field_key),
                self.value.bool_value().unwrap()
            ),
            STRING => write!(
                f,
                "field_type: string, key: {}, value: {}",
                to_str!(&self.field_key),
                self.value.string_value().unwrap()
            ),
            _ => writeln!(f, "bad value"),
        }
    }
}

impl Field {
    // pub fn new() -> Self {
    //     Default::default()
    // }

    pub fn new(field_key: &str, field_type: FieldType, value: Value) -> Self {
        Self {
            field_key: field_key.as_bytes().to_vec(),
            field_type,
            value,
        }
    }

    // field_key returns a copy of the key of the current field.
    pub fn field_key(&self) -> &[u8] {
        &self.field_key
    }

    // type returns the FieldType of the current field.
    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    // string_value returns the string value of the current field.
    pub fn string_value(&self) -> Result<String> {
        if let Some(str) = self.value.string_value() {
            Ok(str)
        } else {
            Err(Error::ParseStringFailed {
                string: "todo-string-value".to_string(),
            })
        }
    }

    // integer_value returns the integer value of the current field.
    pub fn integer_value(&self) -> Result<i64> {
        if let Some(i) = self.value.integer_value() {
            Ok(i)
        } else {
            Err(Error::ParseIntegerFailed {
                integer: "todo-integer-value".to_string(),
            })
        }
    }

    // unsigned_value returns the unsigned value of the current field.
    pub fn unsigned_value(&self) -> Result<u64> {
        if let Some(u) = self.value.unsigned_value() {
            Ok(u)
        } else {
            Err(Error::ParseUnsignedFailed {
                unsigned: "todo-unsigned-value".to_string(),
            })
        }
    }

    // boolean_value returns the boolean value of the current field.
    pub fn bool_value(&self) -> Result<bool> {
        if let Some(b) = self.value.bool_value() {
            Ok(b)
        } else {
            Err(Error::ParseBoolFailed {
                bool: "todo-bool-value".to_string(),
            })
        }
    }

    // float_value returns the float value of the current field.
    pub fn float_value(&self) -> Result<f64> {
        if let Some(f) = self.value.float_value() {
            Ok(f)
        } else {
            Err(Error::ParseFloatFailed {
                float: "todo-float-value".to_string(),
            })
        }
    }
}

// TODO: implement zero-copy like `type fieldIterator struct` in go.
#[derive(Debug, Clone)]
pub struct FieldIterator<'a> {
    start: usize,
    end: usize,
    fields: &'a [u8],
}

impl<'a> FieldIterator<'a> {
    pub fn reset(&mut self) {
        self.start = 0;
        self.end = 0;
    }
}

// parse_points/parse_points_with_precision returns Vec<Point>, vec has implements iter() / iter_mut() /
// into_iter() methods, so we just need to implement Iterator for Point, then we can call next() method to
// iterates every field.
impl<'a> Iterator for FieldIterator<'a> {
    type Item = Field;

    fn next(&mut self) -> Option<Self::Item> {
        self.start = self.end;
        if self.start >= self.fields.len() {
            return None;
        }

        let mut field = Field::new("", EMPTY, Value::EmptyValue(EmptyValue::default()));

        // let mut key: &[u8];
        let key_buf: &[u8];
        let mut value_buf: &[u8];
        (self.end, key_buf) = scan_to(self.fields, self.start, b'=');
        field.field_key = key_buf.to_vec();
        if bytes::is_escaped(&field.field_key) {
            field.field_key = bytes::append_unescaped(&key_buf[..0], &field.field_key);
        }

        (self.end, value_buf) = scan_field_value(self.fields, self.end + 1).unwrap(); // TODO: maybe fail ?
        self.end += 1;

        if value_buf.is_empty() {
            field.field_type = EMPTY;
            return Some(field);
        }

        let c = value_buf[0];
        if c == b'"' {
            field.field_type = STRING;
            let raw = to_string!(value_buf[1..value_buf.len() - 1]);
            field.value = match unescape_string_field(&raw) {
                Cow::Borrowed(_) => raw,
                Cow::Owned(value) => value,
            }
            .into();
            return Some(field);
        }

        if "0123456789-.nNiIu".as_bytes().iter().any(|&x| x == c) {
            if value_buf[value_buf.len() - 1] == b'i' {
                field.field_type = INTEGER;
                value_buf = &value_buf[..value_buf.len() - 1];
                let val = atoi_simd::parse::<i64>(value_buf)
                    .expect("signed integer was validated while parsing the point");
                field.value = val.into();
            } else if value_buf[value_buf.len() - 1] == b'u' {
                field.field_type = UNSIGNED;
                value_buf = &value_buf[..value_buf.len() - 1];
                let mut val = 0;
                for b in value_buf {
                    if b.is_ascii_digit() {
                        val = val * 10 + (b - b'0') as u64;
                    }
                }
                field.value = val.into();
            } else {
                field.field_type = FLOAT;
                field.value = fast_float::parse::<f64, _>(value_buf).unwrap().into();
            }

            return Some(field);
        }

        // to keep the same behavior that currently exists, default to boolean
        field.field_type = BOOL;
        if c == b't' || c == b'T' {
            field.value = true.into();
        } else if c == b'f' || c == b'F' {
            field.value = false.into();
        }
        Some(field)
    }
}

pub fn escape_measurement(buf: &[u8]) -> Vec<u8> {
    let mut out = buf.to_vec();
    for e in MEASUREMENT_ESCAPE_CODES.iter() {
        if buf.iter().any(|&x| x == e.k) {
            // create a new String with replaced escape code
            out = to_str!(buf)
                .replace(e.k as char, to_str!(&e.esc))
                .into_bytes();
        }
    }

    out
}

fn unescape_measurement(buf: &[u8]) -> Vec<u8> {
    if buf.iter().any(|&x| x == b'\\') {
        return buf.to_vec();
    }

    let mut out = buf.to_vec();
    for e in MEASUREMENT_ESCAPE_CODES.iter() {
        if buf.iter().any(|&x| x == e.k) {
            // create a new String with replaced escape code
            let replacement = [e.k];
            out = to_str!(buf)
                .replace(to_str!(&e.esc), to_str!(&replacement))
                .into_bytes();
        }
    }

    out
}

pub fn escape_tag(buf: &[u8]) -> Vec<u8> {
    let mut out = to_string!(buf);
    for e in TAG_ESCAPE_CODES.iter() {
        if buf.iter().any(|&x| x == e.k) {
            out = to_string!(buf).replace(e.k as char, to_str!(&e.esc));
        }
    }

    out.into_bytes()
}

// unescape_tag reutrn a copy of input buf
fn unescape_tag(buf: &[u8]) -> Vec<u8> {
    if !buf.iter().any(|&x| x == b'\\') {
        return buf.to_vec();
    }

    let mut out = to_string!(buf);
    for (_i, e) in TAG_ESCAPE_CODES.iter().enumerate() {
        if buf.iter().any(|&x| x == e.k) {
            // create a new String with replaced escape code
            let replacement = [e.k];
            out = to_string!(buf).replace(to_str!(&e.esc), to_str!(&replacement));
        }
    }

    out.into_bytes()
}

// #[allow(dead_code)]
// // unescape_string_field returns a copy of buf with any escaped double-quotes or backslashes unescaped.
// fn unescape_string_field(buf: &[u8]) -> Vec<u8> {
//     if !buf.iter().any(|&x| x == b'\\') {
//         return buf.to_vec();
//     }

//     let mut out = Vec::<u8>::new();
//     let mut i = 0;
//     while i < buf.len() {
//         // unescape backslashes
//         if buf[i] == b'\\' && i + 1 < buf.len() && buf[i + 1] == b'\\' {
//             out.push(b'\\');
//             i += 2;
//             continue;
//         }

//         // unescape double-quotes
//         if buf[i] == b'\\' && i + 1 < buf.len() && buf[i + 1] == b'"' {
//             out.push(b'"');
//             i += 2;
//             continue;
//         }
//         out.push(buf[i]);
//         i += 1;
//     }
//     out
// }

// valid_token returns true if the provided token is a valid unicode string, and
// only contains printable, non-replacement characters.
pub fn valid_token(a: &[u8]) -> bool {
    if !utf8::valid_utf8(a) {
        return false;
    }

    // TODO: implement me
    // for _, r := range string(a) {
    // 	if !unicode.IsPrint(r) || r == unicode.ReplacementChar {
    // 		return false
    // 	}
    // }

    true
}

// valid_tag_tokens returns true if all the provided tag key and values are
// valid.
//
// valid_tag_tokens does not validate the special tag keys used to represent the
// measurement name and field key, but it does validate the associated values.
pub fn valid_tag_tokens(tags: &Tags) -> bool {
    for tag in tags.iter() {
        // Validate all external tag keys.
        if tag.key.as_slice() != MEASURE_TAG_KEY_BYTES.as_bytes()
            && tag.key.as_slice() != FIELD_KEY_TAG_KEY_BYTES.as_bytes()
            && !valid_token(&tag.key)
        {
            return false;
        }

        // Validate all tag values (this will also validate the field key, which is a tag value for the special field key tag key).
        if !valid_token(&tag.value) {
            return false;
        }
    }

    true
}

/// valid_key_tokens returns true if the measurement name and all tags are valid.
pub fn valid_key_tokens(name: &[u8], tags: &Tags) -> bool {
    if !valid_token(name) {
        return false;
    }
    valid_tag_tokens(tags)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;

    use super::*;

    #[test]
    fn omitted_timestamp_and_empty_value_time_semantics_are_unchanged() {
        let default_timestamp = 1_646_064_000_000_000_123;
        let default_time = Time::from_timestamp_nanos(default_timestamp);
        let (points, failed) =
            parse_points_with_precision(b"cpu value=1.0", default_time, "ns").unwrap();

        assert!(failed.is_none());
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].unix_nano(), default_timestamp);
        assert_eq!(EmptyValue.unix_nano(), ZERO_TIME);
        assert_eq!(Value::EmptyValue(EmptyValue).unix_nano(), ZERO_TIME);
    }

    #[test]
    fn test_parse_points() {
        // integer: 1i
        // unsigned: 1u
        // bool: [t, T, true, True, TRUE], [f, F, false, False, FALSE]
        // float: 1, 1.0, 1.e+78, 1.E+78
        // string(double quotes): "hello world"
        let str_points = "mem,region=beijing,host=server1 ival=2i,uval=4u,bval=true,fval=0.6,sval=\"hello beijing\" 1646064000000000000\nmem,region=shanghai,host=server2 ival=12i,uval=14u,bval=false,fval=1.6,sval=\"hello shanghai\" 1646064001000000000\nmem,_field=v0 value=1 1646064002000000000";

        let (parsed, failed) =
            parse_points_with_precision(str_points.as_bytes(), Time::now(), "ns").unwrap();
        // key=measurement+tags, key must always be stored sorted by tags
        assert_eq!(
            parsed[0].key(),
            "mem,host=server1,region=beijing".as_bytes()
        );
        assert_eq!(
            parsed[0].fields_str(),
            "ival=2i,uval=4u,bval=true,fval=0.6,sval=\"hello beijing\""
        );
        assert_eq!(
            parsed[1].key(),
            "mem,host=server2,region=shanghai".as_bytes()
        );
        assert_eq!(
            parsed[1].fields_str(),
            "ival=12i,uval=14u,bval=false,fval=1.6,sval=\"hello shanghai\""
        );
        assert_eq!(
            failed.unwrap(),
            "unable to parse 'mem,_field=v0 value=1 1646064002000000000': Can't use reserved tag key: '_field'"
        );
    }

    #[test]
    fn test_fields_iter() {
        // integer: 1i
        // unsigned: 1u
        // bool: [t, T, true, True, TRUE], [f, F, false, False, FALSE]
        // float: 1, 1.0, 1.e+78, 1.E+78
        // string(double quotes): "hello world"
        let str_points = "mem,region=beijing,host=server1 ival=2i,uval=4u,bval=true,fval=0.6,sval=\"hello beijing\" 1646064000000000000\nmem,region=shanghai,host=server2 ival=12i,uval=14u,bval=false,fval=1.6,sval=\"hello shanghai\" 1646064001000000000";
        let (parsed, failed) =
            parse_points_with_precision(str_points.as_bytes(), Time::now(), "ns").unwrap();

        // print the parsed failed points
        if let Some(f) = failed {
            println!("failed_points: {:?}", f);
        }

        // 1. use for loop to print point fields
        // If a type implements Iterator it implements IntoIterator automatically,
        // because all Iterators implement IntoIterator, by just returning themselves.
        // for point in &parsed {
        //     for field in point.field_iter() {
        //         println!("{}", field)
        //     }
        // }

        // 2. use iter() create a Iterator to parse field "ival=2i,uval=4u,bval=true,fval=0.6,sval=\"hello beijing\"".
        // each point in this test has four fields, so the first four calls to iter.next() returs Some(Fild)
        // and the fifth call returns None.
        let p0 = &parsed[0];
        let mut iter = p0.field_iter();
        assert_eq!(
            iter.next(),
            Some(Field::new("ival", INTEGER, (2_i64).into()))
        );
        assert_eq!(
            iter.next(),
            Some(Field::new("uval", UNSIGNED, (4_u64).into()))
        );
        assert_eq!(iter.next(), Some(Field::new("bval", BOOL, true.into())));
        assert_eq!(iter.next(), Some(Field::new("fval", FLOAT, 0.6.into())));
        assert_eq!(
            iter.next(),
            Some(Field::new("sval", STRING, "hello beijing".into()))
        );
        assert_eq!(iter.next(), None);

        // use iter() create a Iterator to parse field "ival=12i,uval=14u,bval=false,fval=1.6,sval=\"hello shanghai\"".
        let p1 = &parsed[1];
        let mut iter = p1.field_iter();
        assert_eq!(
            iter.next(),
            Some(Field::new("ival", INTEGER, (12_i64).into()))
        );
        assert_eq!(
            iter.next(),
            Some(Field::new("uval", UNSIGNED, (14_u64).into()))
        );
        assert_eq!(iter.next(), Some(Field::new("bval", BOOL, false.into())));
        assert_eq!(iter.next(), Some(Field::new("fval", FLOAT, 1.6.into())));
        assert_eq!(
            iter.next(),
            Some(Field::new("sval", STRING, "hello shanghai".into()))
        );
        assert_eq!(iter.next(), None);
    }

    fn parse_single_field_value(literal: &str) -> Field {
        let line = format!("cpu value={literal} 100");
        let (points, failed) = parse_points_string(&line).unwrap();
        assert!(failed.is_none(), "literal {literal}: {failed:?}");
        assert_eq!(points.len(), 1, "literal {literal}");
        points[0].field_iter().next().unwrap()
    }

    fn assert_line_protocol_rejected(literal: &str) {
        let line = format!("cpu value={literal} 100");
        assert_line_protocol_line_rejected(&line);
    }

    fn assert_line_protocol_line_rejected(line: &str) {
        let rejected = std::panic::catch_unwind(|| match parse_points_string(&line) {
            Err(_) => true,
            Ok((points, failed)) => {
                if failed.is_none() {
                    for point in &points {
                        let _ = point.field_iter().collect::<Vec<_>>();
                    }
                }
                points.is_empty() && failed.is_some()
            }
        })
        .unwrap_or_else(|_| panic!("line {line:?} panicked during parse or reconstruction"));
        assert!(rejected, "line {line:?} was accepted");
    }

    #[test]
    fn float_grammar_matches_pinned_influxdb() {
        for (literal, expected) in [
            ("0", 0.0),
            ("1", 1.0),
            ("-1", -1.0),
            ("0.0", 0.0),
            ("1.0", 1.0),
            ("-1.0", -1.0),
            ("1.5", 1.5),
            ("-1.5", -1.5),
            ("123.456", 123.456),
            ("-123.456", -123.456),
            ("1e3", 1_000.0),
            ("-1e3", -1_000.0),
            ("1E3", 1_000.0),
            ("-1E3", -1_000.0),
            ("1e+3", 1_000.0),
            ("1e-3", 0.001),
            ("-1e+3", -1_000.0),
            ("-1e-3", -0.001),
            ("1E+3", 1_000.0),
            ("1E-3", 0.001),
            ("-1E+3", -1_000.0),
            ("-1E-3", -0.001),
            ("1.5e3", 1_500.0),
            ("-1.5e3", -1_500.0),
            ("1.5e+3", 1_500.0),
            ("1.5e-3", 0.0015),
            ("-1.5e+3", -1_500.0),
            ("-1.5e-3", -0.0015),
            ("1.5E3", 1_500.0),
            ("-1.5E3", -1_500.0),
            (".5", 0.5),
            ("-.5", -0.5),
            (".5e3", 500.0),
            ("-.5e3", -500.0),
            ("1.", 1.0),
            ("-1.", -1.0),
            ("1.e3", 1_000.0),
            ("-1.e3", -1_000.0),
            ("00", 0.0),
            ("01", 1.0),
            ("001.5", 1.5),
            ("-001.5", -1.5),
            ("00e3", 0.0),
            ("001e3", 1_000.0),
            ("1e-308", 1e-308),
            ("-1e-308", -1e-308),
            ("1e308", 1e308),
            ("-1e308", -1e308),
            ("1.7976931348623157e308", f64::MAX),
            ("2.2250738585072014e-308", f64::MIN_POSITIVE),
            ("4.9406564584124654e-324", f64::from_bits(1)),
            (
                "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999",
                1e100,
            ),
        ] {
            let field = parse_single_field_value(literal);
            assert_eq!(field.field_type(), FLOAT, "literal {literal}");
            assert_eq!(
                field.float_value().unwrap().to_bits(),
                expected.to_bits(),
                "literal {literal}"
            );
        }
    }

    #[test]
    fn float_negative_zero_and_underflow_match_pinned_influxdb() {
        for literal in ["-0", "-0.0", "-0e0", "-0.0e0", "-1e-324", "-1e-325"] {
            let value = parse_single_field_value(literal).float_value().unwrap();
            assert_eq!(value, 0.0, "literal {literal}");
            assert!(value.is_sign_negative(), "literal {literal}");
        }

        for literal in ["0", "0.0", "0e0", "0.0e0", "1e-324", "1e-325"] {
            let value = parse_single_field_value(literal).float_value().unwrap();
            assert_eq!(value, 0.0, "literal {literal}");
            assert!(value.is_sign_positive(), "literal {literal}");
        }
    }

    #[test]
    fn invalid_float_grammar_range_and_suffixes_are_rejected_without_panic() {
        for literal in [
            "+1",
            "+1.0",
            "+1e3",
            "+1.5e3",
            "+.5",
            "+1.",
            ".",
            "-.",
            "+.",
            "1e",
            "1E",
            "1e+",
            "1e-",
            "1E+",
            "1E-",
            "e3",
            "-e3",
            "+e3",
            "1ee3",
            "1e3e4",
            "1e+-3",
            "1e--3",
            "1e++3",
            "1e309",
            "-1e309",
            "1e400",
            "-1e400",
            "1.7976931348623159e308",
            "NaN",
            "nan",
            "NAN",
            "Inf",
            "inf",
            "INF",
            "+Inf",
            "-Inf",
            "Infinity",
            "infinity",
            "INFINITY",
            "+Infinity",
            "-Infinity",
            "1e3i",
            "-1e3i",
            "1e3u",
            "-1e3u",
            "1.0i",
            "1.0u",
            "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999e999999",
        ] {
            assert_line_protocol_rejected(literal);
        }
    }

    #[test]
    fn parsed_string_fields_unescape_line_protocol_values() {
        for value in [
            "",
            "plain",
            r"path\value",
            r#"quote"value"#,
            r#"path\"quoted"#,
            "状态:正常",
            r"two\\slashes",
            r"\n",
            "a,b c",
        ] {
            let input = format!(
                "cpu,host=A value=\"{}\",following=7i 10",
                escape_string_field(value)
            );
            let (mut points, failed) = parse_points_string(&input).unwrap();
            assert!(failed.is_none(), "{failed:?}");
            let fields = points[0].field_iter().collect::<Vec<_>>();
            assert_eq!(fields[0].string_value().unwrap(), value, "{input}");
            assert_eq!(fields[1].integer_value().unwrap(), 7);
        }
        let (mut points, failed) = parse_points_string(r#"cpu value="path\q" 10"#).unwrap();
        assert!(failed.is_none(), "{failed:?}");
        assert_eq!(
            points[0]
                .field_iter()
                .next()
                .unwrap()
                .string_value()
                .unwrap(),
            r"path\q"
        );
    }

    #[test]
    fn scientific_float_reconstruction_preserves_complete_tokens_and_neighbor_types() {
        let input = concat!(
            "cpu-load,host=-1e3 a=-1e3,b=2.5,c=3i,d=4u,e=true,f=\"-1e3\" 100\n",
            "cpu-load a=1e3,b=-1e-3,c=2.5E+4 200\n",
            "cpu-load value=-3.5e-2 300"
        );
        let (mut points, failed) = parse_points_string(input).unwrap();
        assert!(failed.is_none(), "{failed:?}");
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].name(), b"cpu-load");
        assert_eq!(points[0].tags().get(b"host"), Some(b"-1e3".as_slice()));

        let fields = points[0].field_iter().collect::<Vec<_>>();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].float_value().unwrap(), -1_000.0);
        assert_eq!(fields[1].float_value().unwrap(), 2.5);
        assert_eq!(fields[2].integer_value().unwrap(), 3);
        assert_eq!(fields[3].unsigned_value().unwrap(), 4);
        assert!(fields[4].bool_value().unwrap());
        assert_eq!(fields[5].string_value().unwrap(), "-1e3");

        assert_eq!(
            points[1]
                .field_iter()
                .map(|field| field.float_value().unwrap())
                .collect::<Vec<_>>(),
            vec![1_000.0, -0.001, 25_000.0]
        );
        assert_eq!(
            points[2]
                .field_iter()
                .next()
                .unwrap()
                .float_value()
                .unwrap(),
            -0.035
        );
    }

    #[test]
    fn scientific_float_round_trip_and_precision_preserve_value_and_type() {
        for literal in ["1", "-1e3", "1.5e-3", "1E+3", "1."] {
            let line = format!("cpu value={literal} 100");
            let (points, failed) = parse_points_string(&line).unwrap();
            assert!(failed.is_none(), "literal {literal}: {failed:?}");
            let expected = points[0].field_iter().next().unwrap();
            assert_eq!(expected.field_type(), FLOAT);

            let reconstructed = format!("cpu {} {}", points[0].fields_str(), points[0].unix_nano());
            let (round_tripped, failed) = parse_points_string(&reconstructed).unwrap();
            assert!(failed.is_none(), "literal {literal}: {failed:?}");
            let actual = round_tripped[0].field_iter().next().unwrap();
            assert_eq!(actual.field_type(), FLOAT);
            assert_eq!(
                actual.float_value().unwrap().to_bits(),
                expected.float_value().unwrap().to_bits(),
                "literal {literal}"
            );
        }

        for (precision, expected_timestamp) in [
            ("ns", 1),
            ("us", 1_000),
            ("ms", 1_000_000),
            ("s", 1_000_000_000),
        ] {
            let (points, failed) =
                parse_points_with_precision(b"cpu value=-1e3 1", Time::now(), precision).unwrap();
            assert!(failed.is_none(), "precision {precision}: {failed:?}");
            assert_eq!(points[0].unix_nano(), expected_timestamp);
            let field = points[0].field_iter().next().unwrap();
            assert_eq!(field.field_type(), FLOAT);
            assert_eq!(field.float_value().unwrap(), -1_000.0);
        }

        for line in [
            "cpu value= 1.0 100",
            "cpu value=- 1.0 100",
            "cpu value=1 1e3",
        ] {
            assert_line_protocol_line_rejected(line);
        }

        let (points, failed) = parse_points_string("cpu value=1.0 ").unwrap();
        assert!(failed.is_none(), "{failed:?}");
        assert_eq!(points.len(), 1);
        assert_eq!(
            points[0]
                .field_iter()
                .next()
                .unwrap()
                .float_value()
                .unwrap(),
            1.0
        );
    }

    #[test]
    fn signed_integer_fields_preserve_sign_and_boundaries() {
        for (literal, expected) in [
            ("0i", 0),
            ("1i", 1),
            ("-1i", -1),
            ("42i", 42),
            ("-42i", -42),
            ("-0i", 0),
            ("00i", 0),
            ("9223372036854775807i", i64::MAX),
            ("-9223372036854775808i", i64::MIN),
        ] {
            let field = parse_single_field_value(literal);
            assert_eq!(field.field_type(), INTEGER, "literal {literal}");
            assert_eq!(
                field.integer_value().unwrap(),
                expected,
                "literal {literal}"
            );
        }
    }

    #[test]
    fn signed_integer_overflow_and_invalid_forms_are_rejected() {
        for literal in [
            "9223372036854775808i",
            "-9223372036854775809i",
            "+1i",
            "-1u",
            "-i",
            "+i",
            "--1i",
            "1-i",
            "1ii",
            "1.0i",
            "1e3i",
        ] {
            assert_line_protocol_rejected(literal);
        }
    }

    #[test]
    fn signed_integer_parsing_does_not_change_neighboring_fields_or_minus_characters() {
        let line = concat!(
            "cpu-load,host=server-1 ",
            "signed=-1i,unsigned=2u,float=-1.5,ok=true,name=\"node-1\",literal=\"-1i\" 100"
        );
        let (mut points, failed) = parse_points_string(line).unwrap();
        assert!(failed.is_none(), "{failed:?}");
        assert_eq!(points[0].name(), b"cpu-load");
        assert_eq!(points[0].tags().get(b"host"), Some(b"server-1".as_slice()));

        let fields = points[0].field_iter().collect::<Vec<_>>();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].integer_value().unwrap(), -1);
        assert_eq!(fields[1].unsigned_value().unwrap(), 2);
        assert_eq!(fields[2].float_value().unwrap(), -1.5);
        assert!(fields[3].bool_value().unwrap());
        assert_eq!(fields[4].string_value().unwrap(), "node-1");
        assert_eq!(fields[5].string_value().unwrap(), "-1i");
    }

    #[test]
    fn multiple_signed_integer_fields_and_points_preserve_independent_signs() {
        let input = concat!(
            "cpu a=-1i,b=-2i,c=-3i 100\n",
            "cpu value=-1i 200\n",
            "cpu value=-2i 300\n",
            "cpu value=3i 400"
        );
        let (points, failed) = parse_points_string(input).unwrap();
        assert!(failed.is_none(), "{failed:?}");
        assert_eq!(points.len(), 4);
        assert_eq!(
            points[0]
                .field_iter()
                .map(|field| field.integer_value().unwrap())
                .collect::<Vec<_>>(),
            vec![-1, -2, -3]
        );
        assert_eq!(
            points[1..]
                .iter()
                .map(|point| point.field_iter().next().unwrap().integer_value().unwrap())
                .collect::<Vec<_>>(),
            vec![-1, -2, 3]
        );
        assert_eq!(
            points[1..].iter().map(Point::unix_nano).collect::<Vec<_>>(),
            vec![200, 300, 400]
        );
    }

    #[test]
    fn signed_integer_reconstruction_and_round_trip_preserve_semantics() {
        for (literal, expected) in [
            ("-1i", -1),
            ("-9223372036854775808i", i64::MIN),
            ("0i", 0),
            ("9223372036854775807i", i64::MAX),
        ] {
            let line = format!("cpu value={literal} 100");
            let (points, failed) = parse_points_string(&line).unwrap();
            assert!(failed.is_none(), "literal {literal}: {failed:?}");
            assert_eq!(points[0].fields_str(), format!("value={literal}"));

            let reconstructed = format!("cpu {} {}", points[0].fields_str(), points[0].unix_nano());
            let (round_tripped, failed) = parse_points_string(&reconstructed).unwrap();
            assert!(failed.is_none(), "literal {literal}: {failed:?}");
            assert_eq!(
                round_tripped[0]
                    .field_iter()
                    .next()
                    .unwrap()
                    .integer_value()
                    .unwrap(),
                expected,
                "literal {literal}"
            );
        }
    }

    #[test]
    fn signed_integer_field_is_independent_of_timestamp_precision_and_sign() {
        for (precision, expected_timestamp) in [
            ("ns", -1),
            ("us", -1_000),
            ("ms", -1_000_000),
            ("s", -1_000_000_000),
        ] {
            let (points, failed) =
                parse_points_with_precision(b"cpu value=-1i -1", Time::now(), precision).unwrap();
            assert!(failed.is_none(), "precision {precision}: {failed:?}");
            assert_eq!(points[0].unix_nano(), expected_timestamp);
            assert_eq!(
                points[0]
                    .field_iter()
                    .next()
                    .unwrap()
                    .integer_value()
                    .unwrap(),
                -1
            );
        }
    }

    #[test]
    fn test_tags_hash_key() {
        let tags = Tags::from(HashMap::from([
            ("A FOO".as_bytes().to_vec(), "bar".as_bytes().to_vec()),
            ("APPLE".as_bytes().to_vec(), "orange".as_bytes().to_vec()),
            ("host".as_bytes().to_vec(), "serverA".as_bytes().to_vec()),
            ("region".as_bytes().to_vec(), "uswest".as_bytes().to_vec()),
        ]));
        let got = tags.hash_key();
        assert_eq!(
            got.as_slice(),
            ",A\\ FOO=bar,APPLE=orange,host=serverA,region=uswest".as_bytes()
        );
    }

    #[test]
    fn test_point_tags() {
        struct Example {
            point: String,
            tags: Option<Tags>,
            err: Option<String>,
        }

        let examples = vec![
        Example {
            point: "cpu value=1".to_string(),
            tags: None,
            err: None,
        },
        Example {
            point: "cpu,tag0=v0 value=1".to_string(),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), "v0".as_bytes().to_vec())
            ]))),
            err: None,
        },
        Example {
            point: "cpu,tag0=v0,tag1=v0 value=1".to_string(),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), "v0".as_bytes().to_vec()),
                ("tag1".as_bytes().to_vec(), "v1".as_bytes().to_vec()),
            ]))),
            err: None,
        },
        Example {
            point: to_string!(br"cpu,tag0=v\ 0 value=1".to_vec()),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), "v 0".as_bytes().to_vec())
            ]))),
            err: None,
        },
        Example {
            point: to_string!(br"cpu,tag0=v\ 0\ 1,tag1=v2 value=1".to_vec()),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), "v 0 1".as_bytes().to_vec()),
                ("tag1".as_bytes().to_vec(), "v2".as_bytes().to_vec()),
            ]))),
            err: None,
        },
        Example {
            point: to_string!(br"cpu,tag0=\, value=1".to_vec()),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), ",".as_bytes().to_vec())
            ]))),
            err: None,
        },
        Example {
            point: to_string!(br"cpu,ta\ g0=\, value=1".to_vec()),
            tags: Some(Tags::from(HashMap::from([
                ("ta g0".as_bytes().to_vec(), ",".as_bytes().to_vec()),
            ]))),
            err: None,
        },
        Example {
            point: to_string!(br"cpu,tag0=\,1 value=1".to_vec()),
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), ",1".as_bytes().to_vec())
            ]))),
            err: None,
        },
        Example {
            // point: "cpu,tag0=1\"\",t=k value=1".to_string(),  // err
            // point: to_string!(br#"cpu,tag0=1\"\",t=k value=1"#.to_vec()).unwrap(),  // ok
            point: r#"cpu,tag0=1\"\",t=k value=1"#.to_string(),  // ok
            tags: Some(Tags::from(HashMap::from([
                ("tag0".as_bytes().to_vec(), "1\"\"".as_bytes().to_vec()),
                ("t".as_bytes().to_vec(), "k".as_bytes().to_vec()),
            ]))),
            err: None,
        },
        Example {
            point: "cpu,_measurement=v0,tag0=v0 value=1".to_string(),
            tags: None,
            err: Some("unable to parse 'cpu,_measurement=v0,tag0=v0 value=1': Can't use reserved tag key: '_measurement'".to_string()),
        },
        Example {
            point: "cpu,tag0=v0,_measurement=v0 value=1".to_string(),
            tags: None,
            err: Some("unable to parse 'cpu,tag0=v0,_measurement=v0 value=1': Can't use reserved tag key: '_measurement'".to_string()),
        },
        Example {
            point: "cpu,tag0=v0,_field=v0 value=1".to_string(),
            tags: None,
            err: Some("unable to parse 'cpu,tag0=v0,_field=v0 value=1': Can't use reserved tag key: '_field'".to_string()),
        },
        Example {
            point: "cpu,tag0=v0,time=v0 value=1".to_string(),
            tags: None,
            err: Some("unable to parse 'cpu,tag0=v0,time=v0 value=1': Can't use reserved tag key: 'time'".to_string()),
        },
    ];

        for example in examples {
            if let Ok((mut parsed, failed)) = parse_points_string(&example.point) {
                // if parsed is not empty, check if parsed length is 1.
                assert!(matches!(parsed.len(), 0 | 1));

                if parsed.len() == 1 && !parsed[0].tags().is_empty() {
                    // parse ok and point has tags
                    assert_eq!(parsed[0].tags()[0], example.tags.clone().unwrap()[0]);
                }

                if parsed.len() == 1 && parsed[0].tags().is_empty() {
                    // parse ok and point doesn't has tags
                    assert!(matches!(example.tags, None));
                }
                // if failed is some, check if failed.unwrap is equal to example.err
                if failed.is_some() {
                    assert_eq!(failed.unwrap(), example.err.unwrap());
                }
            }
        }
    }
}
