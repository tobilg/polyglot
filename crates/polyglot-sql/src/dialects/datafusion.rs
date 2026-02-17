//! Apache DataFusion SQL Dialect
//!
//! DataFusion is an Arrow-based query engine with modern SQL extensions.
//! Reference: https://datafusion.apache.org/user-guide/sql/
//!
//! Key characteristics:
//! - Arrow-native type system (Int8, Int16, Int32, Int64, Float32, Float64, Utf8, etc.)
//! - Double-quote identifiers
//! - Lowercase function names by default
//! - QUALIFY clause support
//! - EXCEPT for column exclusion (SELECT * EXCEPT(col))
//! - LEFT SEMI JOIN / LEFT ANTI JOIN syntax
//! - TRY_CAST support
//! - Pipe operator (|>) for query chaining
//! - No UPDATE/DELETE support
//! - arrow_cast() and arrow_typeof() functions
//! - COPY ... TO syntax (no INTO keyword)
//! - Nested comment support

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Expression, Function};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Apache DataFusion dialect
pub struct DataFusionDialect;

impl DialectImpl for DataFusionDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::DataFusion
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // DataFusion uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // DataFusion supports nested comments
        config.nested_comments = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, LimitFetchStyle, NormalizeFunctions};
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::DataFusion),
            // DataFusion lowercases function names
            normalize_functions: NormalizeFunctions::Lower,
            // TRY_CAST is supported
            try_supported: true,
            // DataFusion uses EXCEPT for column exclusion: SELECT * EXCEPT(col)
            star_except: "EXCEPT",
            // No multi-arg DISTINCT: COUNT(DISTINCT a, b) not supported
            multi_arg_distinct: false,
            // Window EXCLUDE not supported
            supports_window_exclude: false,
            // Interval allows plural form (DAYS, HOURS, etc.)
            interval_allows_plural_form: true,
            // Normalize date parts in EXTRACT
            normalize_extract_date_parts: true,
            // LIMIT style (not FETCH)
            limit_fetch_style: LimitFetchStyle::Limit,
            // No hints
            join_hints: false,
            table_hints: false,
            query_hints: false,
            // LEFT SEMI JOIN / LEFT ANTI JOIN syntax
            semi_anti_join_with_side: true,
            // COPY does not use INTO keyword
            copy_has_into_keyword: false,
            // NVL2 is supported (via coalesce-like behavior)
            nvl2_supported: true,
            // MEDIAN is supported
            supports_median: true,
            // Can implement array_any
            can_implement_array_any: true,
            // LIKE quantifiers not supported
            supports_like_quantifiers: false,
            // Aggregate FILTER is supported
            aggregate_filter_supported: true,
            // BETWEEN flags not supported
            supports_between_flags: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // Function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DataFusionDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL → COALESCE (DataFusion uses COALESCE)
            "IFNULL" => Ok(Expression::Function(Box::new(Function::new(
                "coalesce".to_string(),
                f.args,
            )))),

            // SQUARE(x) → POWER(x, 2)
            "SQUARE" => {
                let mut args = f.args;
                args.push(Expression::Literal(crate::expressions::Literal::Number(
                    "2".to_string(),
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "power".to_string(),
                    args,
                ))))
            }

            // REGEXP_MATCHES → REGEXP_MATCH
            "REGEXP_MATCHES" => Ok(Expression::Function(Box::new(Function::new(
                "regexp_match".to_string(),
                f.args,
            )))),

            // DATE_FORMAT / TIME_TO_STR / STRFTIME → TO_CHAR
            "DATE_FORMAT" | "TIME_TO_STR" => Ok(Expression::Function(Box::new(Function::new(
                "to_char".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // GROUP_CONCAT → STRING_AGG
            "GROUP_CONCAT" => Ok(Expression::Function(Box::new(Function::new(
                "string_agg".to_string(),
                f.args,
            )))),

            // LISTAGG → STRING_AGG
            "LISTAGG" => Ok(Expression::Function(Box::new(Function::new(
                "string_agg".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}
