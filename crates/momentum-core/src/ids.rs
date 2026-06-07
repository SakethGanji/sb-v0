use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SecurityId(String);

impl SecurityId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SecurityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DisplaySymbol(String);

impl DisplaySymbol {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DisplaySymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_not_interchangeable() {
        fn takes_security(_: &SecurityId) {}
        fn takes_display(_: &DisplaySymbol) {}

        let sid = SecurityId::new("BBG000B9XRY4");
        let disp = DisplaySymbol::new("AAPL");
        takes_security(&sid);
        takes_display(&disp);
    }

    #[test]
    fn display_roundtrip() {
        assert_eq!(SecurityId::new("X").to_string(), "X");
        assert_eq!(DisplaySymbol::new("AAPL").to_string(), "AAPL");
    }
}
