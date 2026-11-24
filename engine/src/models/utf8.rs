/// Unicode replacement character: U+FFFD
pub const RUNE_ERROR: char = '\u{FFFD}';

/// All runes below RUNE_SELF are ASCII and can be encoded in 1 byte.
pub const RUNE_SELF: u8 = 0x80;

/// Maximum valid Unicode code point (U+10FFFF)
pub const MAX_RUNE: u32 = 0x10FFFF;

/// Maximum number of bytes in a single UTF-8 encoded character.
pub const UTF8_MAX: usize = 4;

const LO_CB: u8 = 0x80;
const HI_CB: u8 = 0xBF;
const XX: u8 = 0xF1;

#[derive(Clone, Copy, Debug)]
struct AcceptRange {
    lo: u8,
    hi: u8,
}

const ACCEPT_RANGES: [AcceptRange; 16] = [
    AcceptRange {
        lo: LO_CB,
        hi: HI_CB,
    }, // 0
    AcceptRange {
        lo: 0xA0,
        hi: HI_CB,
    }, // 1
    AcceptRange {
        lo: LO_CB,
        hi: 0x9F,
    }, // 2
    AcceptRange {
        lo: 0x90,
        hi: HI_CB,
    }, // 3
    AcceptRange {
        lo: LO_CB,
        hi: 0x8F,
    }, // 4
    AcceptRange { lo: 0, hi: 0 }, // 5
    AcceptRange { lo: 0, hi: 0 }, // 6
    AcceptRange { lo: 0, hi: 0 }, // 7
    AcceptRange { lo: 0, hi: 0 }, // 8
    AcceptRange { lo: 0, hi: 0 }, // 9
    AcceptRange { lo: 0, hi: 0 }, // 10
    AcceptRange { lo: 0, hi: 0 }, // 11
    AcceptRange { lo: 0, hi: 0 }, // 12
    AcceptRange { lo: 0, hi: 0 }, // 13
    AcceptRange { lo: 0, hi: 0 }, // 14
    AcceptRange { lo: 0, hi: 0 }, // 15
];

const FIRST: [u8; 256] = {
    let mut a = [XX; 256];
    // 1-byte, ASCII
    let mut i = 0;
    while i < 0x80 {
        a[i] = 1;
        i += 1;
    }
    // 2-byte starters
    i = 0xC2;
    while i <= 0xDF {
        a[i] = 2 | (4 << 4);
        i += 1;
    }
    // 3-byte starters
    i = 0xE0;
    while i <= 0xEF {
        a[i] = 3 | (5 << 4);
        i += 1;
    }
    // 4-byte starters
    i = 0xF0;
    while i <= 0xF4 {
        a[i] = 4 | (6 << 4);
        i += 1;
    }
    a
};

/// Returns true if the byte slice is valid UTF-8.
pub fn valid_utf8(p: &[u8]) -> bool {
    std::str::from_utf8(p).is_ok()

    //  let mut p = p; // full slice view

    // // Fast ASCII path: process 8 bytes at a time
    // while p.len() >= 8 {
    //     let first32 = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
    //     let second32 = u32::from_le_bytes([p[4], p[5], p[6], p[7]]);
    //     if (first32 | second32) & 0x80808080 != 0 {
    //         break;
    //     }
    //     p = &p[8..];
    // }

    // let n = p.len();
    // let mut i = 0;

    // while i < n {
    //     let pi = p[i];
    //     if pi < RUNE_SELF {
    //         i += 1;
    //         continue;
    //     }

    //     let x = FIRST[pi as usize];
    //     if x == XX {
    //         return false; // illegal start byte
    //     }

    //     let size = (x & 7) as usize;
    //     if i + size > n {
    //         return false; // truncated
    //     }

    //     let accept = ACCEPT_RANGES[(x >> 4) as usize];
    //     let c1 = p[i + 1];
    //     if c1 < accept.lo || accept.hi < c1 {
    //         return false;
    //     }

    //     if size == 2 {
    //         i += 2;
    //         continue;
    //     }

    //     let c2 = p[i + 2];
    //     if c2 < LO_CB || HI_CB < c2 {
    //         return false;
    //     }

    //     if size == 3 {
    //         i += 3;
    //         continue;
    //     }

    //     let c3 = p[i + 3];
    //     if c3 < LO_CB || HI_CB < c3 {
    //         return false;
    //     }

    //     i += 4;
    // }

    // true
}

mod tests {
    use super::valid_utf8;

    #[derive(Debug)]
    struct ValidTest<'a> {
        in_bytes: &'a [u8],
        expected: bool,
    }

    #[test]
    fn test_valid_utf8() {
        let tests: &[ValidTest] = &[
            ValidTest {
                in_bytes: b"",
                expected: true,
            },
            ValidTest {
                in_bytes: b"a",
                expected: true,
            },
            ValidTest {
                in_bytes: b"abc",
                expected: true,
            },
            ValidTest {
                in_bytes: "Ж".as_bytes(),
                expected: true,
            },
            ValidTest {
                in_bytes: "ЖЖ".as_bytes(),
                expected: true,
            },
            ValidTest {
                in_bytes: "брэд-ЛГТМ".as_bytes(),
                expected: true,
            },
            ValidTest {
                in_bytes: "☺☻☹".as_bytes(),
                expected: true,
            },
            ValidTest {
                in_bytes: b"aa\xe2",
                expected: false,
            },
            ValidTest {
                in_bytes: &[66, 250],
                expected: false,
            },
            ValidTest {
                in_bytes: &[66, 250, 67],
                expected: false,
            },
            ValidTest {
                in_bytes: "a\u{FFFD}b".as_bytes(),
                expected: true,
            },
            ValidTest {
                in_bytes: b"\xF4\x8F\xBF\xBF",
                expected: true,
            }, // U+10FFFF
            ValidTest {
                in_bytes: b"\xF4\x90\x80\x80",
                expected: false,
            }, // U+110000 (invalid)
            ValidTest {
                in_bytes: b"\xF7\xBF\xBF\xBF",
                expected: false,
            }, // > max rune
            ValidTest {
                in_bytes: b"\xFB\xBF\xBF\xBF\xBF",
                expected: false,
            }, // > max rune
            ValidTest {
                in_bytes: b"\xC0\x80",
                expected: false,
            }, // Overlong encoding of U+0000
            ValidTest {
                in_bytes: b"\xED\xA0\x80",
                expected: false,
            }, // High surrogate
            ValidTest {
                in_bytes: b"\xED\xBF\xBF",
                expected: false,
            }, // Low surrogate
        ];

        for (i, tt) in tests.iter().enumerate() {
            let actual = valid_utf8(tt.in_bytes);
            assert_eq!(
                actual, tt.expected,
                "Test case {} failed: valid_utf8({:?}) = {}, want {}",
                i, tt.in_bytes, actual, tt.expected
            );
        }
    }
}
