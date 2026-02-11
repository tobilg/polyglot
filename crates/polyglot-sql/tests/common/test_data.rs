//! Data structures for SQLGlot test fixtures

use serde::Deserialize;
use std::collections::HashMap;

/// Identity test fixtures from identity.json
#[derive(Debug, Deserialize)]
pub struct IdentityFixtures {
    pub tests: Vec<IdentityTest>,
}

/// A single identity test case
#[derive(Debug, Deserialize)]
pub struct IdentityTest {
    pub line: usize,
    pub sql: String,
}

/// Pretty test fixtures from pretty.json
#[derive(Debug, Deserialize)]
pub struct PrettyFixtures {
    pub tests: Vec<PrettyTest>,
}

/// A single pretty-print test case
#[derive(Debug, Deserialize)]
pub struct PrettyTest {
    pub line: usize,
    pub input: String,
    pub expected: String,
}

/// Dialect-specific test fixtures from dialects/*.json
#[derive(Debug, Deserialize)]
pub struct DialectFixture {
    pub dialect: String,
    pub identity: Vec<DialectIdentityTest>,
    #[serde(default)]
    pub transpilation: Vec<TranspilationTest>,
}

/// A dialect-specific identity test case
#[derive(Debug, Deserialize)]
pub struct DialectIdentityTest {
    pub sql: String,
    /// Expected output (None means output should match input)
    pub expected: Option<String>,
}

/// A transpilation test case
#[derive(Debug, Deserialize)]
pub struct TranspilationTest {
    /// Source SQL (parsed using the fixture's dialect)
    pub sql: String,
    /// Expected output when reading from specific dialects
    #[serde(default)]
    pub read: HashMap<String, String>,
    /// Expected output when writing to specific dialects
    #[serde(default)]
    pub write: HashMap<String, String>,
}
