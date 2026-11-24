use std::borrow::Cow;

/// Escapes ',', '"', ' ', '=' with a preceding backslash.
///
/// Fast path:
/// - returns Cow::Borrowed if no escaping is needed
///
/// Examples:
/// - `a,b`   -> `a\,b`
/// - `a"b`   -> `a\"b`
/// - `a b`   -> `a\ b`
/// - `a=b`   -> `a\=b`
#[inline]
pub fn escape_string(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();

    // Find first escapable byte.
    let mut first = 0;
    while first < bytes.len() && !needs_escape(bytes[first]) {
        first += 1;
    }

    if first == bytes.len() {
        return Cow::Borrowed(input);
    }

    // Count how many extra '\' bytes we need.
    let mut extra = 1; // bytes[first] already needs escaping
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
    // - `input` is valid UTF-8
    // - we only insert ASCII '\' before existing bytes
    // - therefore output remains valid UTF-8
    Cow::Owned(unsafe { String::from_utf8_unchecked(out) })
}

/// Unescapes the sequences:
/// - `\,` -> `,`
/// - `\"` -> `"`
/// - `\ ` -> ` `
/// - `\=` -> `=`
///
/// Any other backslash sequence is preserved as-is, matching the Go behavior
/// implied by replacing only known escape pairs.
///
/// Fast path:
/// - returns Cow::Borrowed if no recognized escape exists
#[inline]
pub fn unescape_string(input: &str) -> Cow<'_, str> {
    let bytes = input.as_bytes();

    // Find first recognized escape sequence.
    let mut first = 0;
    while first + 1 < bytes.len() && !is_recognized_escape(bytes[first], bytes[first + 1]) {
        first += 1;
    }

    if first + 1 >= bytes.len() {
        return Cow::Borrowed(input);
    }

    // Count how many backslashes will be removed.
    let mut removed = 1; // the pair at `first`
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
    // - input is valid UTF-8
    // - we only remove ASCII '\' in front of ASCII punctuation/space
    // - the remaining bytes are still valid UTF-8
    Cow::Owned(unsafe { String::from_utf8_unchecked(out) })
}

#[inline(always)]
const fn needs_escape(b: u8) -> bool {
    matches!(b, b',' | b'"' | b' ' | b'=')
}

#[inline(always)]
const fn is_recognized_escape(a: u8, b: u8) -> bool {
    a == b'\\' && matches!(b, b',' | b'"' | b' ' | b'=')
}

/// Convenience wrapper that always returns `String`.
#[inline]
pub fn escape_string_owned(input: &str) -> String {
    escape_string(input).into_owned()
}

/// Convenience wrapper that always returns `String`.
#[inline]
pub fn unescape_string_owned(input: &str) -> String {
    unescape_string(input).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_no_change() {
        let s = "abc中文xyz";
        match escape_string(s) {
            Cow::Borrowed(v) => assert_eq!(v, s),
            Cow::Owned(_) => panic!("should not allocate"),
        }
    }

    #[test]
    fn test_unescape_no_change() {
        let s = r"abc\qxyz";
        match unescape_string(s) {
            Cow::Borrowed(v) => assert_eq!(v, s),
            Cow::Owned(_) => panic!("should not allocate"),
        }
    }

    #[test]
    fn test_escape_basic() {
        assert_eq!(escape_string("a,b"), r"a\,b");
        assert_eq!(escape_string(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escape_string("a b"), r"a\ b");
        assert_eq!(escape_string("a=b"), r"a\=b");
    }

    #[test]
    fn test_unescape_basic() {
        assert_eq!(unescape_string(r"a\,b"), "a,b");
        assert_eq!(unescape_string(r#"a\"b"#), r#"a"b"#);
        assert_eq!(unescape_string(r"a\ b"), "a b");
        assert_eq!(unescape_string(r"a\=b"), "a=b");
    }

    #[test]
    fn test_unescape_unknown_escape_kept() {
        assert_eq!(unescape_string(r"a\qb"), r"a\qb");
        assert_eq!(unescape_string(r"a\\"), r"a\\");
        assert_eq!(unescape_string(r"\n"), r"\n");
    }

    #[test]
    fn test_roundtrip() {
        let s = r#"a, "b" = c 中文"#;
        assert_eq!(unescape_string(escape_string(s).as_ref()), s);
    }

    #[test]
    fn test_multiple() {
        let s = r#", "= ,"#;
        let escaped = escape_string(s);
        assert_eq!(escaped, r#"\,\ \"\=\ \,"#);
        assert_eq!(unescape_string(escaped.as_ref()), s);
    }

    #[test]
    fn test_trailing_backslash() {
        assert_eq!(unescape_string(r"abc\"), r"abc\");
    }
}
