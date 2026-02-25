use crate::dialects::DialectType;
use std::collections::HashMap;

/// Function-name casing behavior for lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FunctionNameCase {
    /// Function names are compared case-insensitively.
    #[default]
    Insensitive,
    /// Function names are compared with exact case.
    Sensitive,
}

/// Function signature metadata used by semantic validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignature {
    /// Minimum number of positional arguments.
    pub min_arity: usize,
    /// Maximum number of positional arguments.
    /// `None` means unbounded/variadic.
    pub max_arity: Option<usize>,
}

impl FunctionSignature {
    /// Build an exact-arity signature.
    pub const fn exact(arity: usize) -> Self {
        Self {
            min_arity: arity,
            max_arity: Some(arity),
        }
    }

    /// Build a bounded arity range signature.
    pub const fn range(min_arity: usize, max_arity: usize) -> Self {
        Self {
            min_arity,
            max_arity: Some(max_arity),
        }
    }

    /// Build a variadic signature with a minimum arity.
    pub const fn variadic(min_arity: usize) -> Self {
        Self {
            min_arity,
            max_arity: None,
        }
    }

    /// Whether an observed arity matches this signature.
    pub fn matches_arity(&self, arity: usize) -> bool {
        if arity < self.min_arity {
            return false;
        }
        match self.max_arity {
            Some(max) => arity <= max,
            None => true,
        }
    }

    /// Render a human-readable arity descriptor.
    pub fn describe_arity(&self) -> String {
        match self.max_arity {
            Some(max) if max == self.min_arity => self.min_arity.to_string(),
            Some(max) => format!("{}..{}", self.min_arity, max),
            None => format!("{}+", self.min_arity),
        }
    }
}

/// Catalog abstraction for dialect-specific function metadata.
///
/// Implementations can be backed by generated files, external crates, or runtime-loaded assets.
pub trait FunctionCatalog: Send + Sync {
    /// Lookup overloads for a function name in a given dialect.
    ///
    /// `raw_function_name` should preserve user query casing.
    /// `normalized_name` should be canonicalized/lowercased by the caller.
    fn lookup(
        &self,
        dialect: DialectType,
        raw_function_name: &str,
        normalized_name: &str,
    ) -> Option<&[FunctionSignature]>;
}

/// Minimal in-memory catalog implementation for runtime registration and tests.
#[derive(Debug, Clone, Default)]
pub struct HashMapFunctionCatalog {
    entries_normalized: HashMap<DialectType, HashMap<String, Vec<FunctionSignature>>>,
    entries_exact: HashMap<DialectType, HashMap<String, Vec<FunctionSignature>>>,
    dialect_name_case: HashMap<DialectType, FunctionNameCase>,
    function_name_case_overrides: HashMap<DialectType, HashMap<String, FunctionNameCase>>,
}

impl HashMapFunctionCatalog {
    /// Set default function-name casing behavior for a dialect.
    pub fn set_dialect_name_case(&mut self, dialect: DialectType, name_case: FunctionNameCase) {
        self.dialect_name_case.insert(dialect, name_case);
    }

    /// Set optional per-function casing behavior override for a dialect.
    ///
    /// The override key is normalized to lowercase.
    pub fn set_function_name_case(
        &mut self,
        dialect: DialectType,
        function_name: impl Into<String>,
        name_case: FunctionNameCase,
    ) {
        self.function_name_case_overrides
            .entry(dialect)
            .or_default()
            .insert(function_name.into().to_lowercase(), name_case);
    }

    /// Register overloads for a function in a dialect.
    pub fn register(
        &mut self,
        dialect: DialectType,
        function_name: impl Into<String>,
        signatures: Vec<FunctionSignature>,
    ) {
        let function_name = function_name.into();
        let normalized_name = function_name.to_lowercase();

        let normalized_entry = self
            .entries_normalized
            .entry(dialect)
            .or_default()
            .entry(normalized_name)
            .or_default();
        let exact_entry = self
            .entries_exact
            .entry(dialect)
            .or_default()
            .entry(function_name)
            .or_default();

        for sig in signatures {
            if !normalized_entry.contains(&sig) {
                normalized_entry.push(sig.clone());
            }
            if !exact_entry.contains(&sig) {
                exact_entry.push(sig);
            }
        }
    }

    fn effective_name_case(&self, dialect: DialectType, normalized_name: &str) -> FunctionNameCase {
        if let Some(overrides) = self.function_name_case_overrides.get(&dialect) {
            if let Some(name_case) = overrides.get(normalized_name) {
                return *name_case;
            }
        }
        self.dialect_name_case
            .get(&dialect)
            .copied()
            .unwrap_or_default()
    }
}

impl FunctionCatalog for HashMapFunctionCatalog {
    fn lookup(
        &self,
        dialect: DialectType,
        raw_function_name: &str,
        normalized_name: &str,
    ) -> Option<&[FunctionSignature]> {
        match self.effective_name_case(dialect, normalized_name) {
            FunctionNameCase::Insensitive => self
                .entries_normalized
                .get(&dialect)
                .and_then(|entries| entries.get(normalized_name))
                .map(|v| v.as_slice()),
            FunctionNameCase::Sensitive => self
                .entries_exact
                .get(&dialect)
                .and_then(|entries| entries.get(raw_function_name))
                .map(|v| v.as_slice()),
        }
    }
}
