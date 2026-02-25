#![forbid(unsafe_code)]

#[cfg(feature = "dialect-clickhouse")]
mod clickhouse;
#[cfg(feature = "dialect-duckdb")]
mod duckdb;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

/// Sink used by this crate to emit feature-enabled dialect function catalogs.
///
/// The sink abstraction keeps this crate independent of `polyglot-sql`.
pub trait CatalogSink {
    /// Set default function-name casing behavior for a dialect key.
    fn set_dialect_name_case(&mut self, dialect: &'static str, name_case: FunctionNameCase);

    /// Set optional per-function casing override for a dialect key.
    fn set_function_name_case(
        &mut self,
        dialect: &'static str,
        function_name: &str,
        name_case: FunctionNameCase,
    );

    /// Register function signatures for a dialect key.
    fn register(
        &mut self,
        dialect: &'static str,
        function_name: &str,
        signatures: Vec<FunctionSignature>,
    );
}

/// Register all catalogs enabled via crate features into a sink.
#[allow(unused_variables)]
pub fn register_enabled_catalogs<S: CatalogSink>(sink: &mut S) {
    #[cfg(feature = "dialect-clickhouse")]
    clickhouse::register(sink);
    #[cfg(feature = "dialect-duckdb")]
    duckdb::register(sink);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    struct TestSink {
        dialect_name_case: HashMap<&'static str, FunctionNameCase>,
        entries: HashMap<&'static str, HashMap<String, Vec<FunctionSignature>>>,
    }

    impl CatalogSink for TestSink {
        fn set_dialect_name_case(&mut self, dialect: &'static str, name_case: FunctionNameCase) {
            self.dialect_name_case.insert(dialect, name_case);
        }

        fn set_function_name_case(
            &mut self,
            _dialect: &'static str,
            _function_name: &str,
            _name_case: FunctionNameCase,
        ) {
        }

        fn register(
            &mut self,
            dialect: &'static str,
            function_name: &str,
            signatures: Vec<FunctionSignature>,
        ) {
            self.entries
                .entry(dialect)
                .or_default()
                .insert(function_name.to_string(), signatures);
        }
    }

    #[cfg(feature = "dialect-clickhouse")]
    #[test]
    fn clickhouse_catalog_exposes_common_functions() {
        let mut sink = TestSink::default();
        register_enabled_catalogs(&mut sink);

        assert_eq!(
            sink.dialect_name_case.get("clickhouse"),
            Some(&FunctionNameCase::Insensitive)
        );
        let if_signatures = sink
            .entries
            .get("clickhouse")
            .and_then(|entries| entries.get("if"))
            .expect("expected clickhouse function 'if'");
        assert!(if_signatures
            .iter()
            .any(|sig| sig.min_arity == 3 && sig.max_arity == Some(3)));
    }

    #[cfg(feature = "dialect-duckdb")]
    #[test]
    fn duckdb_catalog_exposes_common_functions() {
        let mut sink = TestSink::default();
        register_enabled_catalogs(&mut sink);

        assert_eq!(
            sink.dialect_name_case.get("duckdb"),
            Some(&FunctionNameCase::Insensitive)
        );
        let abs_signatures = sink
            .entries
            .get("duckdb")
            .and_then(|entries| entries.get("abs"))
            .expect("expected duckdb function 'abs'");
        assert!(abs_signatures
            .iter()
            .any(|sig| sig.min_arity == 1 && sig.max_arity == Some(1)));
    }
}
