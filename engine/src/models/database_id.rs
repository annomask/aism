use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::Snafu;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DatabaseId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Snafu)]
#[snafu(display("invalid database ID"))]
pub struct InvalidDatabaseId;

impl DatabaseId {
    pub fn new(value: u64) -> Result<Self, InvalidDatabaseId> {
        if value == 0 {
            return Err(InvalidDatabaseId);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for DatabaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:016x}", self.0)
    }
}

impl FromStr for DatabaseId {
    type Err = InvalidDatabaseId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 16 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(InvalidDatabaseId);
        }
        Self::new(u64::from_str_radix(value, 16).map_err(|_| InvalidDatabaseId)?)
    }
}

impl Serialize for DatabaseId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DatabaseId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_id_zero_is_invalid() {
        assert_eq!(DatabaseId::new(0), Err(InvalidDatabaseId));
        assert_eq!(
            "0000000000000000".parse::<DatabaseId>(),
            Err(InvalidDatabaseId)
        );
    }

    #[test]
    fn database_id_display_is_canonical() {
        assert_eq!(
            DatabaseId::new(0x06c8_6c40_a9f3_6000).unwrap().to_string(),
            "06c86c40a9f36000"
        );
    }

    #[test]
    fn database_id_parse_round_trip() {
        let id = DatabaseId::new(0x06c8_6c40_a9f3_6000).unwrap();
        assert_eq!(id.to_string().parse(), Ok(id));
        assert_eq!("06C86C40A9F36000".parse(), Ok(id));
    }

    #[test]
    fn malformed_database_ids_are_rejected() {
        for value in [
            "",
            "1",
            "006c86c40a9f36000",
            "6c86c40a9f36000",
            "06c86c40a9f3600g",
            " 6c86c40a9f36000",
            "+6c86c40a9f36000",
            "06c86c40/a9f3600",
        ] {
            assert!(value.parse::<DatabaseId>().is_err(), "accepted {value:?}");
        }
    }
}
