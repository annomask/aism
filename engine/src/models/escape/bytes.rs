/// CODES is a map of bytes to be escaped.
use std::{collections::HashSet, str};

use once_cell::sync::Lazy;

// lazy_static! {
//     static ref CODES: <HashMap<u8, [u8;2]]> = HashMap::from([
//         (b',', [b'\\', b',']),
//         (b'"', [b'\\', b'"']),
//         (b' ', [b'\\', b' ']),
//         (b'=', [b'\\', b'=']),
//     ]);

//     // static ref CODES: HashMap<u8, String> = HashMap::from([
//     //     (b',', "\\,".to_string()),
//     //     (b'"', "\\\"".to_string()),
//     //     (b' ', "\\ ".to_string()),
//     //     (b'=', "\\ ".to_string()),
//     // ]);
// }

// TODO: !!!!!
/// bytes escapes characters on the input slice, as defined by codes.
// pub fn bytes(buf: &[u8]) -> String {
//     let out: String;
//     let codes = [b',', b'"', b' ', b'='];
//     for code in codes {
//         out = to_string!(buf.to_vec()).unwrap().replace(code, v);
//     }
//     out
// }

const ESCAPE_CHARS: [u8; 4] = [b',', b'"', b' ', b'='];

pub static ESCAPE_SET: Lazy<HashSet<u8>> = Lazy::new(|| ESCAPE_CHARS.iter().copied().collect());

// is_escaped returns whether b has any escaped characters,
// i.e. whether b seems to have been processed by bytes.
pub fn is_escaped(b: &[u8]) -> bool {
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' {
            // Check if the next byte is an escaped character
            if i + 1 < b.len() && ESCAPE_SET.contains(&b[i + 1]) {
                return true;
            }
            i += 2; // Skip the escape character and the next one.
        } else {
            i += 1; // Move to the next character.
        }
    }

    false
}

// append_unescaped appends the unescaped version of src to dst and returns the resulting slice.
// pub fn append_unescaped(dst: &[u8], src: &[u8]) -> Vec<u8> {
pub fn append_unescaped(dst: &[u8], src: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(dst.len() + src.len());
    out.extend_from_slice(dst);

    let mut i = 0;
    while i < src.len() {
        if src[i] == b'\\' {
            if i + 1 < src.len() && ESCAPE_SET.contains(&src[i + 1]) {
                out.push(src[i + 1]);
                i += 2;
            } else {
                out.push(src[i]);
                i += 1;
            }
        } else {
            out.push(src[i]);
            i += 1;
        }
    }

    out
}

// unescape returns a new slice containing the unescaped version of in.
pub fn unescape(buf: &[u8]) -> Vec<u8> {
    if buf.is_empty() {
        return vec![];
    }

    if !buf.iter().any(|&x| x == b'\\') {
        return buf.to_vec();
    }

    let mut i = 0;
    let buf_len = buf.len();
    let mut out = Vec::with_capacity(buf_len);

    while i < buf_len {
        if buf[i] == b'\\' && i + 1 < buf_len {
            match buf[i + 1] {
                b',' => {
                    out.push(b',');
                    i += 2;
                    continue;
                }
                b'"' => {
                    out.push(b'"');
                    i += 2;
                    continue;
                }
                b' ' => {
                    out.push(b' ');
                    i += 2;
                    continue;
                }
                b'=' => {
                    out.push(b'=');
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        out.push(buf[i]);
        i += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::{append_unescaped, unescape};

    #[test]
    fn test_unescape() {
        let tests: Vec<(&[u8], &[u8])> = vec![
            (&[], &[]),
            (b"", &[]),
            (b"\\,\\\"\\ \\=", b",\" ="),
            (b"\\\\", b"\\\\"),
            (b"plain and simple", b"plain and simple"),
        ];

        for (idx, (input, expected)) in tests.iter().enumerate() {
            let got = unescape(input);
            assert_eq!(
                got,
                expected.to_vec(),
                "[{idx}] unescape({:?}) = {:?}, expected {:?}",
                String::from_utf8_lossy(input),
                String::from_utf8_lossy(&got),
                String::from_utf8_lossy(expected),
            );
        }
    }

    #[test]
    fn test_append_unescaped() {
        let cases = [
            "normal",
            r#"inv\alid"#,
            r#"goo\"d"#,
            r#"sp\ ace"#,
            r#"\,\"\ \="#,
            r#"f\\\ x"#,
        ];

        for case in cases {
            let expected = unescape(case.as_bytes());
            let got = append_unescaped(&[], case.as_bytes());
            assert_eq!(
                got,
                expected,
                "append_unescaped failed for {case:?}: got {:?}, expected {:?}",
                String::from_utf8_lossy(&got),
                String::from_utf8_lossy(&expected),
            );
        }
    }
}
