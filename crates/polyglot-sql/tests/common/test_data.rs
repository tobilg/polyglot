#![allow(dead_code)]
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

// =============================================================================
// Custom dialect fixture types (for dialects not in Python sqlglot)
// =============================================================================

/// A single fixture file for a custom dialect (e.g., custom_fixtures/datafusion/select.json)
#[derive(Debug, Deserialize)]
pub struct CustomDialectFixtureFile {
    pub dialect: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub identity: Vec<CustomIdentityTest>,
    #[serde(default)]
    pub transpilation: Vec<CustomTranspileTest>,
}

/// A single custom identity test case
#[derive(Debug, Deserialize)]
pub struct CustomIdentityTest {
    pub sql: String,
    #[serde(default)]
    pub expected: Option<String>,
    #[serde(default)]
    pub description: String,
}

/// A single custom transpilation test case (sqlglot-compatible format)
#[derive(Debug, Deserialize)]
pub struct CustomTranspileTest {
    /// The SQL in the file's dialect (used as expected output for `read`, input for `write`)
    pub sql: String,
    /// Map of {target_dialect: expected_output} — forward transpilation
    #[serde(default)]
    pub write: HashMap<String, String>,
    /// Map of {source_dialect: source_sql} — reverse transpilation (source → file dialect)
    #[serde(default)]
    pub read: HashMap<String, String>,
    #[serde(default)]
    pub description: String,
}

/// All fixtures for a single custom dialect (all files in one subdirectory)
pub struct CustomDialectFixtures {
    pub dialect: String,
    pub files: Vec<CustomDialectFixtureFile>,
}

/// All custom dialect fixtures discovered from the custom_fixtures directory
pub struct AllCustomFixtures {
    pub dialects: Vec<CustomDialectFixtures>,
}

// =============================================================================
// Transpile test fixture types (from test_transpile.py)
// =============================================================================

/// Normalization test: parse generic SQL, expect normalized output
#[derive(Debug, Deserialize)]
pub struct NormalizationTest {
    pub sql: String,
    pub expected: String,
    pub line: usize,
}

/// Transpile-with-dialect test from test_transpile.py
#[derive(Debug, Deserialize)]
pub struct TranspileWriteTest {
    pub sql: String,
    pub expected: String,
    #[serde(default)]
    pub write: Option<String>,
    #[serde(default)]
    pub read: Option<String>,
    pub line: usize,
}

/// Transpile fixtures from test_transpile.py
#[derive(Debug, Deserialize)]
pub struct TranspileFixtures {
    pub normalization: Vec<NormalizationTest>,
    pub transpilation: Vec<TranspileWriteTest>,
}

// =============================================================================
// Parser test fixture types (from test_parser.py)
// =============================================================================

/// Parser round-trip test
#[derive(Debug, Deserialize)]
pub struct ParserRoundtripTest {
    pub sql: String,
    pub expected: String,
    #[serde(default)]
    pub read: Option<String>,
    #[serde(default)]
    pub write: Option<String>,
    pub line: usize,
}

/// Parser error test (SQL that should fail to parse)
#[derive(Debug, Deserialize)]
pub struct ParserErrorTest {
    pub sql: String,
    #[serde(default)]
    pub read: Option<String>,
    pub line: usize,
}

/// Parser fixtures from test_parser.py
#[derive(Debug, Deserialize)]
pub struct ParserFixtures {
    pub roundtrips: Vec<ParserRoundtripTest>,
    pub errors: Vec<ParserErrorTest>,
}
