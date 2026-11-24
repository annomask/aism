// //! Point Tags.

// use std::{
//     collections::HashMap,
//     ops::{Deref, DerefMut},
// };

// use crate::models::points::{escape_tag, TAG_ESCAPE_CODES};

// #[derive(Debug, Default, Clone, PartialEq, Eq)]
// pub struct Tag {
//     pub key: String,
//     pub value: String,
// }

// impl Tag {
//     pub fn new(key: String, value: String) -> Self {
//         Self { key, value }
//     }

//     pub fn size(&self) -> usize {
//         self.key.len() + self.value.len()
//     }

//     pub fn string(&self) -> String {
//         format!("{{{} {}}}", self.key, self.value)
//     }
// }

// #[derive(Debug, Default, Clone, PartialEq, Eq)]
// pub struct Tags(Vec<Tag>);

// // implement Deref / DerefMut, so we can use Vec methods directly.
// impl Deref for Tags {
//     type Target = Vec<Tag>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl DerefMut for Tags {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

// impl Tags {
//     /// new returns a new Tags.
//     pub fn new() -> Self {
//         Tags::default()
//     }

//     /// returns a new Tags from a map.
//     pub fn from(m: HashMap<String, String>) -> Tags {
//         if m.is_empty() {
//             return Tags::default();
//         }

//         let mut tags = Tags(Vec::<Tag>::with_capacity(m.len()));
//         for (k, v) in m {
//             tags.push(Tag { key: k, value: v });
//         }
//         // sort Tags by key
//         tags.sort_by(|x, y| x.key.cmp(&y.key));
//         tags
//     }

//     /// returns a new tags with capacity.
//     pub fn with_capacity(size: usize) -> Tags {
//         Tags(Vec::<Tag>::with_capacity(size))
//     }

//     /// tags returns the reference to Vec<Tag>
//     pub fn tags(&self) -> &Vec<Tag> {
//         self
//     }

//     // /// new_tags_key_values returns a new Tags from a list of key, value pairs,
//     // /// ensuring the returned result is correctly sorted. Duplicate keys are removed,
//     // /// however, it which duplicate that remains is undefined.
//     // /// NewTagsKeyValues will return ErrInvalidKevValuePairs if len(kvs) is not even.
//     // /// If the input is guaranteed to be even, the error can be safely ignored.
//     // /// If a has enough capacity, it will be reused.
//     // pub fn new_tags_key_values(a: &Tags, Vec<>)

//     /// keys returns the list of keys for a tag set.
//     pub fn keys(&self) -> Option<Vec<String>> {
//         if self.len() == 0 {
//             return None;
//         }

//         let mut keys = Vec::<String>::with_capacity(self.len());
//         for tag in &self.0 {
//             keys.push(tag.key.clone());
//         }
//         Some(keys)
//     }

//     /// values returns the string representationof the tags.
//     pub fn values(&self) -> Option<Vec<String>> {
//         if self.len() == 0 {
//             return None;
//         }
//         let mut values = Vec::<String>::with_capacity(self.len());
//         for tag in &self.0 {
//             values.push(tag.value.clone());
//         }
//         Some(values)
//     }

//     /// string returns the string representation of the tags.
//     pub fn string(&self) -> String {
//         let s = "[".to_string();
//         let mut out = self.iter().enumerate().fold(s, |mut acc, (i, tag)| {
//             acc += tag.string().as_str();
//             if i < self.len() - 1 {
//                 acc += " ";
//             }
//             acc
//         });
//         out += "]";

//         out
//     }

//     /// size returns the number of bytes needed to store all tags. Note, this is
//     /// the number of bytes needed to store all keys and values and does not account
//     /// for data structures or delimiters for example.
//     pub fn size(&self) -> usize {
//         let mut total = 0;
//         for tag in self.iter() {
//             total += tag.size();
//         }
//         total
//     }

//     #[allow(dead_code)]
//     /// sorted returns true if a is sorted.
//     fn sroted(&self) -> bool {
//         let mut i = self.len() - 1;
//         while i > 0 {
//             if self[i].key < self[i - 1].key {
//                 return false;
//             }
//             i -= 1;
//         }
//         true
//     }

//     /// hash_key hashes all of a tag's keys.
//     pub fn hash_key(&self) -> Option<Vec<u8>> {
//         self.append_hash_key(None)
//     }

//     fn needs_escape(&self) -> bool {
//         for tag in self.iter() {
//             for e in TAG_ESCAPE_CODES.iter() {
//                 if tag.key.as_bytes().iter().any(|&x| x == e.k)
//                     || tag.value.as_bytes().iter().any(|&x| x == e.k)
//                 {
//                     return true;
//                 }
//             }
//         }
//         false
//     }

//     /// append_hash_key appends the result of hashing all of a tag's keys and values to dst
//     /// and returns the extended buffer.
//     pub fn append_hash_key(&self, buf: Option<&[u8]>) -> Option<Vec<u8>> {
//         if self.len() == 0 {
//             if let Some(dst) = buf {
//                 return Some(dst.to_vec());
//             }
//             return None;
//         }

//         // type invariant: Tags are sorted
//         let mut sz = 0;
//         let mut escaped: Tags;
//         if self.needs_escape() {
//             let mut tmp = Tags::with_capacity(20);
//             if self.capacity() < tmp.capacity() {
//                 tmp.shrink_to(self.capacity());
//                 escaped = tmp; // derive(Clone) for Tags
//             } else {
//                 escaped = Tags::with_capacity(self.capacity());
//             }

//             for (i, tag) in self.iter().enumerate() {
//                 escaped.push(Tag {
//                     key: escape_tag(tag.key.as_bytes()),
//                     value: escape_tag(tag.value.as_bytes()),
//                 });
//                 sz += escaped[i].key.len() + escaped[i].value.len();
//             }
//         } else {
//             sz = self.size();
//             escaped = self.clone();
//         }

//         sz += escaped.len() + escaped.len() * 2; // separators

//         let mut buf_len = 0usize;
//         let mut out: Vec<u8>;
//         if let Some(bf) = buf {
//             out = vec![0; bf.len() + sz];
//             out.as_mut_slice()[..bf.len()].copy_from_slice(bf);
//             buf_len = bf.len();
//         } else {
//             out = vec![0; sz];
//         }

//         let b = if let Some(bf) = buf {
//             &mut out.as_mut_slice()[bf.len()..]
//         } else {
//             out.as_mut_slice()
//         };

//         let mut idx = 0;
//         for tag in escaped.iter() {
//             if tag.value.is_empty() {
//                 continue;
//             }

//             b[idx] = b',';
//             idx += 1;
//             b[idx..idx + tag.key.len()].copy_from_slice(tag.key.as_bytes());
//             idx += tag.key.len();
//             b[idx] = b'=';
//             idx += 1;
//             b[idx..idx + tag.value.len()].copy_from_slice(tag.value.as_bytes());
//             idx += tag.value.len();
//         }

//         Some(out.as_slice()[..buf_len + idx].to_vec())
//     }

//     /// Set the value for a key.
//     pub fn set(&mut self, key: &[u8], value: &[u8]) {
//         for tag in &mut self.0 {
//             if tag.key.as_bytes() == key {
//                 tag.value = to_string!(value.to_vec()).unwrap();
//                 return;
//             }
//         }
//         self.0.push(Tag {
//             key: to_string!(key.to_vec()).unwrap(),
//             value: to_string!(value.to_vec()).unwrap(),
//         });
//         self.sort_by(|x, y| x.key.cmp(&y.key));
//     }
// }
