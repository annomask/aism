#[macro_export]
macro_rules! to_string {
    ($bytes:expr_2021) => {{ unsafe { String::from_utf8_unchecked($bytes.to_vec()) } }};

    ($bytes:expr_2021) => {{ unsafe { String::from_utf8_unchecked($bytes) } }};
}

#[macro_export]
macro_rules! to_str {
    ($bytes:expr_2021) => {{ unsafe { std::str::from_utf8_unchecked($bytes) } }};
}
