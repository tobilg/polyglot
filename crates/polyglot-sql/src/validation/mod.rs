//! Dialect-specific function validation.
//!
//! Walks a parsed AST and checks function calls against a dialect's function
//! catalog, producing non-blocking warnings for unknown functions and arity
//! mismatches.

pub mod clickhouse;

use crate::dialects::DialectType;
use crate::error::ValidationError;
use crate::expressions::Expression;
use crate::traversal::ExpressionWalk;
use std::collections::HashMap;

/// Metadata for a single function recognized by a dialect.
#[derive(Debug, Clone)]
pub struct FunctionSignature {
    /// Canonical (display) name of the function.
    pub canonical_name: String,
    /// Minimum number of required arguments (inclusive).
    pub min_args: usize,
    /// Maximum number of arguments. `None` means variadic (no upper bound).
    pub max_args: Option<usize>,
}

impl FunctionSignature {
    pub fn new(canonical_name: &str, min_args: usize, max_args: Option<usize>) -> Self {
        Self {
            canonical_name: canonical_name.to_string(),
            min_args,
            max_args,
        }
    }

    /// Fixed-arity function (min == max).
    pub fn fixed(canonical_name: &str, arity: usize) -> Self {
        Self::new(canonical_name, arity, Some(arity))
    }

    /// Variadic function with minimum args but no maximum.
    pub fn variadic(canonical_name: &str, min_args: usize) -> Self {
        Self::new(canonical_name, min_args, None)
    }

    /// Check if the given arg count is valid.
    pub fn check_arity(&self, arg_count: usize) -> bool {
        if arg_count < self.min_args {
            return false;
        }
        if let Some(max) = self.max_args {
            if arg_count > max {
                return false;
            }
        }
        true
    }
}

/// A catalog of known functions for a specific dialect.
///
/// Keys are uppercase-normalized function names for case-insensitive lookup.
pub struct FunctionCatalog {
    functions: HashMap<String, FunctionSignature>,
    /// Optional combinator suffixes (e.g., ClickHouse `-If`, `-State`).
    /// When set, unknown functions are re-checked after stripping these suffixes.
    combinators: &'static [&'static str],
}

impl FunctionCatalog {
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
            combinators: &[],
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            functions: HashMap::with_capacity(cap),
            combinators: &[],
        }
    }

    /// Set aggregate combinator suffixes for this catalog.
    pub fn set_combinators(&mut self, combinators: &'static [&'static str]) {
        self.combinators = combinators;
    }

    /// Register a function signature. The name is uppercased for lookup.
    pub fn register(&mut self, sig: FunctionSignature) {
        self.functions
            .insert(sig.canonical_name.to_uppercase(), sig);
    }

    /// Look up a function by name (case-insensitive).
    pub fn lookup(&self, name: &str) -> Option<&FunctionSignature> {
        self.functions.get(&name.to_uppercase())
    }

    /// Look up a function, falling back to combinator suffix stripping.
    ///
    /// Returns `Some(sig)` for direct matches, `None` with `is_combinator=true`
    /// for combinator-derived functions, or `None` with `is_combinator=false`
    /// for truly unknown functions.
    pub fn lookup_or_combinator(&self, name: &str) -> LookupResult<'_> {
        if let Some(sig) = self.lookup(name) {
            return LookupResult::Found(sig);
        }

        let upper = name.to_uppercase();
        for combinator in self.combinators {
            let suffix = combinator.to_uppercase();
            if let Some(base) = upper.strip_suffix(&suffix) {
                if !base.is_empty() && self.functions.contains_key(base) {
                    return LookupResult::Combinator;
                }
            }
        }

        LookupResult::Unknown
    }

    /// Number of registered functions.
    pub fn len(&self) -> usize {
        self.functions.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.functions.is_empty()
    }
}

pub enum LookupResult<'a> {
    Found(&'a FunctionSignature),
    Combinator,
    Unknown,
}

/// Validate function calls in the given expressions against a dialect's catalog.
///
/// Returns a list of `ValidationError` warnings (severity=Warning).
pub fn validate_functions(
    expressions: &[Expression],
    catalog: &FunctionCatalog,
) -> Vec<ValidationError> {
    let mut warnings = Vec::new();
    for expr in expressions {
        validate_expr_functions(expr, catalog, &mut warnings);
    }
    warnings
}

/// Validate function calls for a specific dialect type.
///
/// Dispatches to the appropriate catalog. Returns empty vec for dialects
/// without a function catalog.
pub fn dialect_validate_functions(
    expressions: &[Expression],
    dialect: DialectType,
) -> Vec<ValidationError> {
    match dialect {
        DialectType::ClickHouse => validate_functions(expressions, &clickhouse::CATALOG),
        _ => Vec::new(),
    }
}

fn validate_expr_functions(
    expr: &Expression,
    catalog: &FunctionCatalog,
    warnings: &mut Vec<ValidationError>,
) {
    for node in expr.dfs() {
        match node {
            Expression::Function(f) => {
                check_function_call(&f.name, f.args.len(), catalog, warnings);
            }
            Expression::AggregateFunction(f) => {
                check_function_call(&f.name, f.args.len(), catalog, warnings);
            }
            Expression::CombinedParameterizedAgg(cpa) => {
                // Parametric aggregates like quantile(0.5)(a):
                // `this` holds the function name, `expressions` are the real args.
                if let Expression::Function(ref inner_f) = *cpa.this {
                    check_function_call(
                        &inner_f.name,
                        cpa.expressions.len(),
                        catalog,
                        warnings,
                    );
                }
            }
            // Typed variants (Count, Sum, Avg, Coalesce, etc.) are implicitly valid.
            _ => {}
        }
    }
}

fn check_function_call(
    name: &str,
    arg_count: usize,
    catalog: &FunctionCatalog,
    warnings: &mut Vec<ValidationError>,
) {
    match catalog.lookup_or_combinator(name) {
        LookupResult::Found(sig) => check_arity(sig, arg_count, warnings),
        LookupResult::Combinator => {} // valid combinator form, skip arity check
        LookupResult::Unknown => {
            warnings.push(ValidationError::warning(
                format!("Unknown function '{name}' for target dialect"),
                "W001",
            ));
        }
    }
}

fn check_arity(sig: &FunctionSignature, arg_count: usize, warnings: &mut Vec<ValidationError>) {
    if !sig.check_arity(arg_count) {
        let expected = match sig.max_args {
            Some(max) if max == sig.min_args => format!("exactly {}", sig.min_args),
            Some(max) => format!("{} to {}", sig.min_args, max),
            None => format!("at least {}", sig.min_args),
        };
        warnings.push(ValidationError::warning(
            format!(
                "Function '{}' expects {expected} arguments, but got {arg_count}",
                sig.canonical_name
            ),
            "W002",
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ValidationSeverity;

    fn test_catalog() -> FunctionCatalog {
        let mut cat = FunctionCatalog::new();
        cat.register(FunctionSignature::fixed("toDate", 1));
        cat.register(FunctionSignature::new("round", 1, Some(2)));
        cat.register(FunctionSignature::variadic("concat", 1));
        cat
    }

    #[test]
    fn test_lookup_case_insensitive() {
        let cat = test_catalog();
        assert!(cat.lookup("toDate").is_some());
        assert!(cat.lookup("TODATE").is_some());
        assert!(cat.lookup("todate").is_some());
        assert!(cat.lookup("nonexistent").is_none());
    }

    #[test]
    fn test_arity_fixed() {
        let sig = FunctionSignature::fixed("toDate", 1);
        assert!(sig.check_arity(1));
        assert!(!sig.check_arity(0));
        assert!(!sig.check_arity(2));
    }

    #[test]
    fn test_arity_range() {
        let sig = FunctionSignature::new("round", 1, Some(2));
        assert!(!sig.check_arity(0));
        assert!(sig.check_arity(1));
        assert!(sig.check_arity(2));
        assert!(!sig.check_arity(3));
    }

    #[test]
    fn test_arity_variadic() {
        let sig = FunctionSignature::variadic("concat", 1);
        assert!(!sig.check_arity(0));
        assert!(sig.check_arity(1));
        assert!(sig.check_arity(100));
    }

    #[test]
    fn test_check_unknown_function() {
        let cat = test_catalog();
        let mut warnings = Vec::new();
        check_function_call("nonexistent", 1, &cat, &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "W001");
        assert_eq!(warnings[0].severity, ValidationSeverity::Warning);
    }

    #[test]
    fn test_check_arity_mismatch() {
        let cat = test_catalog();
        let mut warnings = Vec::new();
        check_function_call("toDate", 3, &cat, &mut warnings);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "W002");
    }

    #[test]
    fn test_check_valid_function() {
        let cat = test_catalog();
        let mut warnings = Vec::new();
        check_function_call("toDate", 1, &cat, &mut warnings);
        assert!(warnings.is_empty());
    }
}
