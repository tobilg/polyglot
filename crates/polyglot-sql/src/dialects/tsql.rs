//! T-SQL (SQL Server) Dialect
//!
//! SQL Server-specific transformations based on sqlglot patterns.
//! Key differences:
//! - TOP instead of LIMIT
//! - ISNULL instead of COALESCE (though COALESCE also works)
//! - Square brackets for identifiers
//! - + for string concatenation
//! - CONVERT vs CAST
//! - CROSS APPLY / OUTER APPLY for lateral joins
//! - Different date functions (GETDATE, DATEADD, DATEDIFF, DATENAME)

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    Alias, BinaryOp, Cast, Column, Cte, DataType, Exists, Expression, From, Function, Identifier,
    In, Join, JoinKind, LikeOp, Literal, Null, Over, Paren, QuantifiedOp, Select, Star,
    StringAggFunc, Subquery, TrimFunc, TrimPosition, Tuple, UnaryFunc, Values, Where,
    WindowFunction,
};
#[cfg(feature = "generate")]
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;
use std::collections::HashMap;

/// T-SQL (SQL Server) dialect
pub struct TSQLDialect;

impl DialectImpl for TSQLDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::TSQL
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // SQL Server uses square brackets for identifiers
        config.identifiers.insert('[', ']');
        // SQL Server also supports double quotes (when QUOTED_IDENTIFIER is ON)
        config.identifiers.insert('"', '"');
        // SQL Server uses 0x-prefixed binary/varbinary hex literals.
        config.hex_number_strings = true;
        config
    }

    #[cfg(feature = "generate")]

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            // Use square brackets by default for SQL Server
            identifier_quote: '[',
            identifier_quote_style: IdentifierQuoteStyle::BRACKET,
            dialect: Some(DialectType::TSQL),
            // T-SQL specific settings from Python sqlglot
            // SQL Server uses TOP/FETCH instead of LIMIT
            limit_fetch_style: crate::generator::LimitFetchStyle::FetchFirst,
            // NULLS FIRST/LAST not supported in SQL Server
            null_ordering_supported: false,
            // SQL Server does not support SQL:2003 aggregate FILTER clauses.
            aggregate_filter_supported: false,
            // SQL Server supports SELECT INTO
            supports_select_into: true,
            // ALTER TABLE doesn't require COLUMN keyword
            alter_table_include_column_keyword: false,
            // Computed columns don't need type declaration
            computed_column_with_type: false,
            // RECURSIVE keyword not required in CTEs
            cte_recursive_keyword_required: false,
            // Ensure boolean expressions
            ensure_bools: true,
            // CONCAT requires at least 2 args
            supports_single_arg_concat: false,
            // TABLESAMPLE REPEATABLE
            tablesample_seed_keyword: "REPEATABLE",
            // JSON path without brackets
            json_path_bracketed_key_supported: false,
            // No TO_NUMBER function
            supports_to_number: false,
            // SET operation modifiers not supported
            set_op_modifiers: false,
            // COPY params need equals sign
            copy_params_eq_required: true,
            // No ALL clause for EXCEPT/INTERSECT
            except_intersect_support_all_clause: false,
            // ALTER SET is wrapped
            alter_set_wrapped: true,
            // T-SQL supports TRY_CAST
            try_supported: true,
            // No NVL2 support
            nvl2_supported: false,
            // TSQL uses = instead of DEFAULT for parameter defaults
            parameter_default_equals: true,
            // No window EXCLUDE support
            supports_window_exclude: false,
            // No DISTINCT with multiple args
            multi_arg_distinct: false,
            // TSQL doesn't support FOR UPDATE/SHARE
            locking_reads_supported: false,
            ..Default::default()
        }
    }

    #[cfg(feature = "transpile")]

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        // Transform column data types in DDL (transform_recursive skips them by design).
        if let Expression::CreateTable(mut ct) = expr {
            for col in &mut ct.columns {
                if let Ok(Expression::DataType(new_dt)) =
                    self.transform_data_type(col.data_type.clone())
                {
                    col.data_type = new_dt;
                }
            }
            return Ok(Expression::CreateTable(ct));
        }

        match expr {
            // ===== SELECT a = 1 → SELECT 1 AS a =====
            // In T-SQL, `SELECT a = expr` is equivalent to `SELECT expr AS a`
            // BUT: `SELECT @a = expr` is a variable assignment, not an alias!
            // Python sqlglot handles this at parser level via _parse_projections()
            Expression::Select(mut select) => {
                select.expressions = select
                    .expressions
                    .into_iter()
                    .map(|e| {
                        match e {
                            Expression::Eq(op) => {
                                // Check if left side is an identifier (column name)
                                // Don't transform if it's a variable (starts with @)
                                match &op.left {
                                    Expression::Column(col)
                                        if col.table.is_none()
                                            && !col.name.name.starts_with('@') =>
                                    {
                                        Expression::Alias(Box::new(Alias {
                                            this: op.right,
                                            alias: col.name.clone(),
                                            column_aliases: Vec::new(),
                                            alias_explicit_as: false,
                                            alias_keyword: None,
                                            pre_alias_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }))
                                    }
                                    Expression::Identifier(ident)
                                        if !ident.name.starts_with('@') =>
                                    {
                                        Expression::Alias(Box::new(Alias {
                                            this: op.right,
                                            alias: ident.clone(),
                                            column_aliases: Vec::new(),
                                            alias_explicit_as: false,
                                            alias_keyword: None,
                                            pre_alias_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }))
                                    }
                                    _ => Expression::Eq(op),
                                }
                            }
                            other => other,
                        }
                    })
                    .collect();

                Self::normalize_frame_incompatible_window_functions(&mut select);

                let outer_qualifier = Self::single_select_source_qualifier(&select);

                select.expressions = select
                    .expressions
                    .into_iter()
                    .map(|expression| {
                        Self::rewrite_tuple_in_subquery_predicates(
                            expression,
                            outer_qualifier.as_ref(),
                            false,
                        )
                    })
                    .collect();

                for join in &mut select.joins {
                    if let Some(on) = join.on.take() {
                        join.on = Some(Self::rewrite_tuple_in_subquery_predicates(
                            on,
                            outer_qualifier.as_ref(),
                            false,
                        ));
                    }
                    if let Some(match_condition) = join.match_condition.take() {
                        join.match_condition = Some(Self::rewrite_tuple_in_subquery_predicates(
                            match_condition,
                            outer_qualifier.as_ref(),
                            false,
                        ));
                    }
                }

                if let Some(ref mut prewhere) = select.prewhere {
                    *prewhere = Self::rewrite_tuple_in_subquery_predicates(
                        std::mem::replace(prewhere, Expression::Null(Null)),
                        outer_qualifier.as_ref(),
                        false,
                    );
                }

                if let Some(ref mut where_clause) = select.where_clause {
                    where_clause.this = Self::rewrite_tuple_in_subquery_predicates(
                        std::mem::replace(&mut where_clause.this, Expression::Null(Null)),
                        outer_qualifier.as_ref(),
                        false,
                    );
                }

                if let Some(ref mut having) = select.having {
                    having.this = Self::rewrite_tuple_in_subquery_predicates(
                        std::mem::replace(&mut having.this, Expression::Null(Null)),
                        outer_qualifier.as_ref(),
                        false,
                    );
                }

                if let Some(ref mut qualify) = select.qualify {
                    qualify.this = Self::rewrite_tuple_in_subquery_predicates(
                        std::mem::replace(&mut qualify.this, Expression::Null(Null)),
                        outer_qualifier.as_ref(),
                        false,
                    );
                }

                // Transform CTEs in the WITH clause to add auto-aliases
                if let Some(ref mut with) = select.with {
                    with.ctes = with
                        .ctes
                        .drain(..)
                        .map(|cte| self.transform_cte_inner(cte))
                        .collect();
                }

                Self::rewrite_comma_lateral_sources_to_joins(&mut select);

                Ok(Expression::Select(select))
            }

            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ===== Boolean IS TRUE/FALSE -> T-SQL 3VL truth table =====
            // T-SQL doesn't have IS TRUE/IS FALSE syntax. Negated forms must
            // explicitly preserve UNKNOWN/NULL rows instead of using NOT (x = n).
            Expression::IsTrue(it) => Ok(Self::boolean_test_predicate(it.this, true, it.not)),
            Expression::IsFalse(it) => Ok(Self::boolean_test_predicate(it.this, false, it.not)),

            // Note: CASE WHEN boolean conditions are handled in ensure_bools preprocessing

            // NOT IN -> NOT ... IN for TSQL (TSQL prefers NOT prefix)
            Expression::In(mut in_expr) if in_expr.not => {
                in_expr.not = false;
                Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                    this: Expression::In(in_expr),
                    inferred_type: None,
                })))
            }

            // COALESCE with 2 args -> ISNULL in SQL Server (optimization)
            // Note: COALESCE works in SQL Server, ISNULL is just more idiomatic
            Expression::Coalesce(f) if f.expressions.len() == 2 => Ok(Expression::Function(
                Box::new(Function::new("ISNULL".to_string(), f.expressions)),
            )),

            // NVL -> ISNULL in SQL Server
            Expression::Nvl(f) => Ok(Expression::Function(Box::new(Function::new(
                "ISNULL".to_string(),
                vec![f.this, f.expression],
            )))),

            // GROUP_CONCAT -> STRING_AGG in SQL Server (SQL Server 2017+)
            Expression::GroupConcat(f) => Ok(Expression::StringAgg(Box::new(StringAggFunc {
                this: f.this,
                separator: f.separator,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                limit: None,
                inferred_type: None,
            }))),

            // LISTAGG -> STRING_AGG in SQL Server (SQL Server 2017+)
            Expression::ListAgg(f) => Ok(Expression::StringAgg(Box::new(StringAggFunc {
                this: f.this,
                separator: f.separator,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                limit: None,
                inferred_type: None,
            }))),

            // PostgreSQL accepts inline ORDER BY for every aggregate, even when
            // input order cannot affect the result. T-SQL only accepts ordering
            // for these functions in an analytic OVER clause.
            Expression::Sum(f) => Ok(Expression::Sum(Self::without_inert_ordering(f))),
            Expression::Avg(f) => Ok(Expression::Avg(Self::without_inert_ordering(f))),
            Expression::Min(f) => Ok(Expression::Min(Self::without_inert_ordering(f))),
            Expression::Max(f) => Ok(Expression::Max(Self::without_inert_ordering(f))),
            Expression::AnyValue(f) => Ok(Expression::Max(Self::without_inert_ordering(f))),
            Expression::ApproxCountDistinct(f) => Ok(Expression::ApproxCountDistinct(
                Self::without_inert_ordering(f),
            )),

            // T-SQL/Fabric do not have boolean aggregates. Preserve PostgreSQL NULL
            // semantics by returning NULL for unknown input predicates.
            Expression::LogicalAnd(f) => Self::transform_logical_aggregate(f.this, f.filter, "MIN"),
            Expression::LogicalOr(f) => Self::transform_logical_aggregate(f.this, f.filter, "MAX"),

            // The bottom-up transform turns a windowed boolean aggregate into
            // CAST(MIN|MAX(CASE ...) AS BIT) OVER (...). OVER belongs to the
            // aggregate in T-SQL, so keep the result cast outside the window.
            Expression::WindowFunction(f) => Ok(Self::reassociate_logical_aggregate_window(*f)),

            // TryCast -> TRY_CAST (SQL Server supports TRY_CAST starting from 2012)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILIKE -> LOWER() LIKE LOWER() in SQL Server (no ILIKE support)
            Expression::ILike(op) => {
                // SQL Server is case-insensitive by default based on collation
                // But for explicit case-insensitive matching, use LOWER
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left)));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right)));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier,
                    inferred_type: None,
                })))
            }

            // || (Concat operator) -> + in SQL Server
            // SQL Server uses + for string concatenation
            Expression::Concat(op) => {
                // Convert || to + operator (Add)
                Ok(Expression::Add(op))
            }

            // RANDOM -> RAND in SQL Server
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // UNNEST -> Not directly supported, use CROSS APPLY with STRING_SPLIT or OPENJSON
            Expression::Unnest(f) => {
                // For basic cases, we'll use a placeholder
                // Full support would require context-specific transformation
                Ok(Expression::Function(Box::new(Function::new(
                    "OPENJSON".to_string(),
                    vec![f.this],
                ))))
            }

            // EXPLODE -> Similar to UNNEST, use CROSS APPLY
            Expression::Explode(f) => Ok(Expression::Function(Box::new(Function::new(
                "OPENJSON".to_string(),
                vec![f.this],
            )))),

            // PostgreSQL LATERAL join forms -> SQL Server APPLY.
            Expression::Join(join) => Ok(Expression::Join(Box::new(
                Self::transform_lateral_join_to_apply(*join)?,
            ))),

            // LENGTH -> LEN in SQL Server
            Expression::Length(f) => Ok(Expression::Function(Box::new(Function::new(
                "LEN".to_string(),
                vec![f.this],
            )))),

            // STDDEV -> STDEV in SQL Server
            Expression::Stddev(f) => Ok(Expression::Function(Box::new(Function::new(
                "STDEV".to_string(),
                vec![f.this],
            )))),
            Expression::StddevSamp(f) => Ok(Expression::Function(Box::new(Function::new(
                "STDEV".to_string(),
                vec![f.this],
            )))),
            Expression::StddevPop(f) => Ok(Expression::Function(Box::new(Function::new(
                "STDEVP".to_string(),
                vec![f.this],
            )))),

            // Boolean literals TRUE/FALSE -> 1/0 in SQL Server
            Expression::Boolean(b) => {
                let value = if b.value { 1 } else { 0 };
                Ok(Expression::Literal(Box::new(
                    crate::expressions::Literal::Number(value.to_string()),
                )))
            }

            // LN -> LOG in SQL Server
            Expression::Ln(f) => Ok(Expression::Function(Box::new(Function::new(
                "LOG".to_string(),
                vec![f.this],
            )))),

            // ===== Date/time =====
            // CurrentDate -> CAST(GETDATE() AS DATE) in SQL Server
            Expression::CurrentDate(_) => Ok(Self::cast_getdate_to(DataType::Date)),

            // CurrentTime -> CAST(GETDATE() AS TIME) in SQL Server
            Expression::CurrentTime(_) => Ok(Self::cast_getdate_to(DataType::Time {
                precision: None,
                timezone: false,
            })),

            // CurrentTimestamp -> GETDATE() in SQL Server
            Expression::CurrentTimestamp(_) => Ok(Self::getdate()),

            // Localtimestamp -> GETDATE() in SQL Server
            Expression::Localtimestamp(_) => Ok(Self::getdate()),

            // PostgreSQL MAKE_DATE(y, m, d) -> SQL Server DATEFROMPARTS(y, m, d)
            Expression::MakeDate(f) => Ok(Self::function(
                "DATEFROMPARTS",
                vec![f.year, f.month, f.day],
            )),

            // DateDiff -> DATEDIFF
            Expression::DateDiff(f) => {
                // TSQL: DATEDIFF(unit, start, end)
                let unit_str = match f.unit {
                    Some(crate::expressions::IntervalUnit::Year) => "YEAR",
                    Some(crate::expressions::IntervalUnit::Quarter) => "QUARTER",
                    Some(crate::expressions::IntervalUnit::Month) => "MONTH",
                    Some(crate::expressions::IntervalUnit::Week) => "WEEK",
                    Some(crate::expressions::IntervalUnit::Day) => "DAY",
                    Some(crate::expressions::IntervalUnit::Hour) => "HOUR",
                    Some(crate::expressions::IntervalUnit::Minute) => "MINUTE",
                    Some(crate::expressions::IntervalUnit::Second) => "SECOND",
                    Some(crate::expressions::IntervalUnit::Millisecond) => "MILLISECOND",
                    Some(crate::expressions::IntervalUnit::Microsecond) => "MICROSECOND",
                    Some(crate::expressions::IntervalUnit::Nanosecond) => "NANOSECOND",
                    None => "DAY",
                };
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str.to_string(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                    span: None,
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, f.expression, f.this], // Note: order is different in TSQL
                ))))
            }

            // DateAdd -> DATEADD
            Expression::DateAdd(f) => {
                let unit_str = match f.unit {
                    crate::expressions::IntervalUnit::Year => "YEAR",
                    crate::expressions::IntervalUnit::Quarter => "QUARTER",
                    crate::expressions::IntervalUnit::Month => "MONTH",
                    crate::expressions::IntervalUnit::Week => "WEEK",
                    crate::expressions::IntervalUnit::Day => "DAY",
                    crate::expressions::IntervalUnit::Hour => "HOUR",
                    crate::expressions::IntervalUnit::Minute => "MINUTE",
                    crate::expressions::IntervalUnit::Second => "SECOND",
                    crate::expressions::IntervalUnit::Millisecond => "MILLISECOND",
                    crate::expressions::IntervalUnit::Microsecond => "MICROSECOND",
                    crate::expressions::IntervalUnit::Nanosecond => "NANOSECOND",
                };
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str.to_string(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                    span: None,
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, f.interval, f.this],
                ))))
            }

            // ===== UUID =====
            // Uuid -> NEWID in SQL Server
            Expression::Uuid(_) => Ok(Expression::Function(Box::new(Function::new(
                "NEWID".to_string(),
                vec![],
            )))),

            // ===== Conditional =====
            // IfFunc -> IIF in SQL Server
            Expression::IfFunc(f) => {
                let false_val = f
                    .false_value
                    .unwrap_or(Expression::Null(crate::expressions::Null));
                Ok(Expression::Function(Box::new(Function::new(
                    "IIF".to_string(),
                    vec![f.condition, f.true_value, false_val],
                ))))
            }

            // ===== String functions =====
            // StringAgg -> STRING_AGG in SQL Server 2017+ - keep as-is to preserve ORDER BY
            Expression::StringAgg(f) => Ok(Expression::StringAgg(f)),

            // LastDay -> EOMONTH (note: TSQL doesn't support date part argument)
            Expression::LastDay(f) => Ok(Expression::Function(Box::new(Function::new(
                "EOMONTH".to_string(),
                vec![f.this.clone()],
            )))),

            // Ceil -> CEILING
            Expression::Ceil(f) => Ok(Expression::Function(Box::new(Function::new(
                "CEILING".to_string(),
                vec![f.this],
            )))),

            // Repeat -> REPLICATE in SQL Server
            Expression::Repeat(f) => Ok(Expression::Function(Box::new(Function::new(
                "REPLICATE".to_string(),
                vec![f.this, f.times],
            )))),

            // Chr -> CHAR in SQL Server
            Expression::Chr(f) => Ok(Expression::Function(Box::new(Function::new(
                "CHAR".to_string(),
                vec![f.this],
            )))),

            // SQL standard OVERLAY(...) -> T-SQL STUFF(...)
            Expression::Overlay(f) => Ok(Self::overlay_to_stuff(*f)),

            // PostgreSQL starts_with(text, prefix) -> T-SQL prefix predicate.
            // Scalar SELECT positions are wrapped by the shared T-SQL boolean materializer.
            Expression::StartsWith(f) => Ok(Self::starts_with_predicate(f.this, f.expression)),

            // PostgreSQL decode(text, 'hex') -> T-SQL hexadecimal binary conversion.
            Expression::DecodeCase(mut f)
                if f.expressions.len() == 2
                    && Self::literal_string(&f.expressions[1])
                        .is_some_and(|format| format.eq_ignore_ascii_case("hex")) =>
            {
                Ok(Self::tsql_convert(
                    DataType::Custom {
                        name: "VARBINARY(MAX)".to_string(),
                    },
                    f.expressions.remove(0),
                    Some(2),
                ))
            }

            // PostgreSQL TO_NUMBER with simple literal masks can be represented as TRY_CONVERT.
            // More complex masks intentionally remain as TO_NUMBER so strict mode rejects them.
            Expression::ToNumber(f) => Ok(Self::to_number_or_fallback(*f)),

            // ===== Variance =====
            // VarPop -> VARP
            Expression::VarPop(f) => Ok(Expression::Function(Box::new(Function::new(
                "VARP".to_string(),
                vec![f.this],
            )))),

            // Variance -> VAR
            Expression::Variance(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR".to_string(),
                vec![f.this],
            )))),
            Expression::VarSamp(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR".to_string(),
                vec![f.this],
            )))),

            // ===== Hash functions =====
            // MD5Digest -> HASHBYTES('MD5', ...)
            Expression::MD5Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "HASHBYTES".to_string(),
                vec![Expression::string("MD5"), *f.this],
            )))),

            // SHA -> HASHBYTES('SHA1', ...)
            Expression::SHA(f) => Ok(Expression::Function(Box::new(Function::new(
                "HASHBYTES".to_string(),
                vec![Expression::string("SHA1"), f.this],
            )))),

            // SHA1Digest -> HASHBYTES('SHA1', ...)
            Expression::SHA1Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "HASHBYTES".to_string(),
                vec![Expression::string("SHA1"), f.this],
            )))),

            // ===== Array functions =====
            // ArrayToString -> STRING_AGG
            Expression::ArrayToString(f) => Ok(Expression::Function(Box::new(Function::new(
                "STRING_AGG".to_string(),
                vec![f.this],
            )))),

            // ===== DDL Column Constraints =====
            // AutoIncrementColumnConstraint -> IDENTITY in SQL Server
            Expression::AutoIncrementColumnConstraint(_) => Ok(Expression::Function(Box::new(
                Function::new("IDENTITY".to_string(), vec![]),
            ))),

            // ===== DDL three-part name stripping =====
            // TSQL strips database (catalog) prefix from 3-part names for CREATE VIEW/DROP VIEW
            // Python sqlglot: expression.this.set("catalog", None)
            Expression::CreateView(mut view) => {
                // Strip catalog from three-part name (a.b.c -> b.c)
                view.name.catalog = None;
                Ok(Expression::CreateView(view))
            }

            Expression::DropView(mut view) => {
                // Strip catalog from three-part name (a.b.c -> b.c)
                view.name.catalog = None;
                Ok(Expression::DropView(view))
            }

            // ParseJson: handled by generator (emits just the string literal for TSQL)

            // JSONExtract with variant_extract (Snowflake colon syntax) -> ISNULL(JSON_QUERY, JSON_VALUE)
            Expression::JSONExtract(e) if e.variant_extract.is_some() => {
                let path = match *e.expression {
                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                        let Literal::String(s) = lit.as_ref() else {
                            unreachable!()
                        };
                        let normalized = if s.starts_with('$') {
                            s.clone()
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Box::new(Literal::String(normalized)))
                    }
                    other => other,
                };
                let json_query = Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    vec![(*e.this).clone(), path.clone()],
                )));
                let json_value = Expression::Function(Box::new(Function::new(
                    "JSON_VALUE".to_string(),
                    vec![*e.this, path],
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ISNULL".to_string(),
                    vec![json_query, json_value],
                ))))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // ===== CTEs need auto-aliased outputs =====
            // In TSQL, bare expressions in CTEs need explicit aliases
            Expression::Cte(cte) => self.transform_cte(*cte),

            // ===== Subqueries need auto-aliased outputs =====
            // In TSQL, bare expressions in aliased subqueries need explicit aliases
            Expression::Subquery(subquery) => self.transform_subquery(*subquery),

            // Convert JsonQuery struct to ISNULL(JSON_QUERY(..., path), JSON_VALUE(..., path))
            Expression::JsonQuery(f) => {
                let json_query = Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    vec![f.this.clone(), f.path.clone()],
                )));
                let json_value = Expression::Function(Box::new(Function::new(
                    "JSON_VALUE".to_string(),
                    vec![f.this, f.path],
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ISNULL".to_string(),
                    vec![json_query, json_value],
                ))))
            }
            // Convert JsonValue struct to Function("JSON_VALUE", ...) for uniform handling
            Expression::JsonValue(f) => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE".to_string(),
                vec![f.this, f.path],
            )))),

            // PostgreSQL pg_get_querydef can emit scalar array comparisons for
            // literal arrays/tuples. T-SQL/Fabric require IN for this shape.
            Expression::Any(ref q) if matches!(&q.op, Some(QuantifiedOp::Eq)) => {
                match Self::scalar_array_comparison_values(&q.subquery) {
                    Some(expressions) if expressions.is_empty() => {
                        Ok(Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
                            Expression::Literal(Box::new(Literal::Number("1".to_string()))),
                            Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                        ))))
                    }
                    Some(expressions) => Ok(Expression::In(Box::new(In {
                        this: q.this.clone(),
                        expressions,
                        query: None,
                        not: false,
                        global: false,
                        unnest: None,
                        is_field: false,
                    }))),
                    None => Ok(expr.clone()),
                }
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

#[cfg(feature = "transpile")]
impl TSQLDialect {
    fn getdate() -> Expression {
        Expression::Function(Box::new(Function::new("GETDATE".to_string(), vec![])))
    }

    fn cast_getdate_to(to: DataType) -> Expression {
        Expression::Cast(Box::new(Cast {
            this: Self::getdate(),
            to,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }

    fn cast(this: Expression, to: DataType) -> Expression {
        Expression::Cast(Box::new(Cast {
            this,
            to,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }

    fn function(name: impl Into<String>, args: Vec<Expression>) -> Expression {
        Expression::Function(Box::new(Function::new(name, args)))
    }

    fn make_time(mut args: Vec<Expression>) -> Expression {
        let seconds = args.pop().expect("MAKE_TIME has three arguments");
        let minute = args.pop().expect("MAKE_TIME has three arguments");
        let hour = args.pop().expect("MAKE_TIME has three arguments");

        if let Some((whole_seconds, microseconds)) = Self::literal_time_parts(&seconds) {
            let (fractions, precision) = if microseconds == 0 {
                (Expression::number(0), Expression::number(0))
            } else {
                (Expression::number(microseconds), Expression::number(6))
            };

            return Self::function(
                "TIMEFROMPARTS",
                vec![
                    hour,
                    minute,
                    Expression::number(whole_seconds),
                    fractions,
                    precision,
                ],
            );
        }

        // TIMEFROMPARTS requires integral seconds and fractions. Round the
        // PostgreSQL double-precision seconds argument to microseconds before
        // splitting the integer value, without dropping fractional seconds.
        let rounded_microseconds = Self::cast(
            Self::function(
                "ROUND",
                vec![
                    Expression::Mul(Box::new(BinaryOp::new(
                        seconds,
                        Expression::number(1_000_000),
                    ))),
                    Expression::number(0),
                ],
            ),
            DataType::BigInt { length: None },
        );

        Self::function(
            "TIMEFROMPARTS",
            vec![
                hour,
                minute,
                Expression::Div(Box::new(BinaryOp::new(
                    rounded_microseconds.clone(),
                    Expression::number(1_000_000),
                ))),
                Expression::Mod(Box::new(BinaryOp::new(
                    rounded_microseconds,
                    Expression::number(1_000_000),
                ))),
                Expression::number(6),
            ],
        )
    }

    fn literal_time_parts(expr: &Expression) -> Option<(i64, i64)> {
        let value = match expr {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::Number(value) => value.parse::<f64>().ok()?,
                _ => return None,
            },
            Expression::Paren(paren) => return Self::literal_time_parts(&paren.this),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast)
                if Self::is_numeric_data_type(&cast.to) =>
            {
                let parts = Self::literal_time_parts(&cast.this);
                return match (&cast.to, parts) {
                    // Zero is unchanged by every numeric cast. Other literal
                    // casts are folded only when their floating-point
                    // semantics cannot truncate the seconds value.
                    (_, Some((0, 0))) => Some((0, 0)),
                    (DataType::Float { .. } | DataType::Double { .. }, parts) => parts,
                    _ => None,
                };
            }
            _ => return None,
        };

        if !value.is_finite() || value < 0.0 || value > i64::MAX as f64 / 1_000_000.0 {
            return None;
        }

        let total_microseconds = (value * 1_000_000.0).round() as i64;
        Some((
            total_microseconds / 1_000_000,
            total_microseconds % 1_000_000,
        ))
    }

    fn lower(this: Expression) -> Expression {
        Expression::Lower(Box::new(UnaryFunc::new(this)))
    }

    fn tsql_convert(to: DataType, expression: Expression, style: Option<i64>) -> Expression {
        let mut args = vec![Expression::DataType(to), expression];
        if let Some(style) = style {
            args.push(Expression::number(style));
        }
        Self::function("CONVERT", args)
    }

    fn tsql_hex_text(expression: Expression, varchar_type: DataType) -> Expression {
        Self::lower(Self::tsql_convert(varchar_type, expression, Some(2)))
    }

    fn tsql_hex_from_varbinary(expression: Expression) -> Expression {
        Self::tsql_hex_text(
            Self::cast(
                expression,
                DataType::Custom {
                    name: "VARBINARY(MAX)".to_string(),
                },
            ),
            DataType::Text,
        )
    }

    fn tsql_postgres_to_hex(expression: Expression) -> Expression {
        let hex = Self::tsql_hex_from_varbinary(expression);
        let without_leading_zeroes = Self::function("LTRIM", vec![hex, Expression::string("0")]);
        let non_empty = Self::function(
            "NULLIF",
            vec![without_leading_zeroes, Expression::string("")],
        );
        Self::function("ISNULL", vec![non_empty, Expression::string("0")])
    }

    fn tsql_md5_hex(expression: Expression) -> Expression {
        let hashbytes = Self::function("HASHBYTES", vec![Expression::string("MD5"), expression]);
        Self::tsql_hex_text(
            hashbytes,
            DataType::VarChar {
                length: Some(32),
                parenthesized_length: false,
            },
        )
    }

    fn overlay_to_stuff(f: crate::expressions::OverlayFunc) -> Expression {
        let length = f
            .length
            .unwrap_or_else(|| Self::function("LEN", vec![f.replacement.clone()]));
        Self::function("STUFF", vec![f.this, f.from, length, f.replacement])
    }

    fn starts_with_predicate(this: Expression, prefix: Expression) -> Expression {
        let prefix_len = Self::function("LEN", vec![prefix.clone()]);
        let left_prefix = Self::function("LEFT", vec![this, prefix_len]);
        Self::eq(left_prefix, prefix)
    }

    fn to_number_or_fallback(f: crate::expressions::ToNumber) -> Expression {
        let crate::expressions::ToNumber {
            this,
            format,
            nlsparam,
            precision,
            scale,
            safe,
            safe_name,
        } = f;

        if nlsparam.is_none()
            && precision.is_none()
            && scale.is_none()
            && safe.is_none()
            && safe_name.is_none()
        {
            if let Some(format) = format.as_deref() {
                if let Some(scale) = Self::simple_to_number_scale(format) {
                    return Self::function(
                        "TRY_CONVERT",
                        vec![
                            Expression::DataType(DataType::Decimal {
                                precision: Some(18),
                                scale: Some(scale),
                            }),
                            *this,
                        ],
                    );
                }
            }
        }

        Expression::ToNumber(Box::new(crate::expressions::ToNumber {
            this,
            format,
            nlsparam,
            precision,
            scale,
            safe,
            safe_name,
        }))
    }

    fn simple_to_number_scale(format: &Expression) -> Option<u32> {
        let format = Self::literal_string(format)?;
        let format = format.strip_prefix("FM").unwrap_or(format);
        let mut saw_digit = false;
        let mut saw_decimal = false;
        let mut scale = 0u32;

        for ch in format.chars() {
            match ch {
                '9' | '0' => {
                    saw_digit = true;
                    if saw_decimal {
                        scale = scale.checked_add(1)?;
                    }
                }
                '.' if !saw_decimal => saw_decimal = true,
                ',' | ' ' => {}
                _ => return None,
            }
        }

        saw_digit.then_some(scale)
    }

    fn binary(
        left: Expression,
        right: Expression,
        op: fn(Box<BinaryOp>) -> Expression,
    ) -> Expression {
        op(Box::new(BinaryOp {
            left,
            right,
            left_comments: Vec::new(),
            operator_comments: Vec::new(),
            trailing_comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn eq(left: Expression, right: Expression) -> Expression {
        Self::binary(left, right, Expression::Eq)
    }

    fn or(left: Expression, right: Expression) -> Expression {
        Self::binary(left, right, Expression::Or)
    }

    fn not(this: Expression) -> Expression {
        Expression::Not(Box::new(crate::expressions::UnaryOp {
            this,
            inferred_type: None,
        }))
    }

    fn is_null(this: Expression) -> Expression {
        Expression::IsNull(Box::new(crate::expressions::IsNull {
            this,
            not: false,
            postfix_form: false,
        }))
    }

    fn paren(this: Expression) -> Expression {
        Expression::Paren(Box::new(Paren {
            this,
            trailing_comments: Vec::new(),
        }))
    }

    fn boolean_test_case_for_predicate(
        predicate: Expression,
        test_true: bool,
        negated: bool,
    ) -> Expression {
        let condition = match (test_true, negated) {
            (true, false) => predicate,
            (false, false) => Self::not(predicate),
            (true, true) => {
                return Expression::Case(Box::new(crate::expressions::Case {
                    operand: None,
                    whens: vec![(predicate, Expression::number(0))],
                    else_: Some(Expression::number(1)),
                    comments: Vec::new(),
                    inferred_type: None,
                }))
            }
            (false, true) => {
                return Expression::Case(Box::new(crate::expressions::Case {
                    operand: None,
                    whens: vec![(Self::not(predicate), Expression::number(0))],
                    else_: Some(Expression::number(1)),
                    comments: Vec::new(),
                    inferred_type: None,
                }))
            }
        };

        Expression::Case(Box::new(crate::expressions::Case {
            operand: None,
            whens: vec![(condition, Expression::number(1))],
            else_: Some(Expression::number(0)),
            comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn boolean_test_predicate(operand: Expression, test_true: bool, negated: bool) -> Expression {
        if Self::is_boolean_predicate_operand(&operand) {
            return match (test_true, negated) {
                (true, false) => operand,
                (false, false) => Self::not(operand),
                _ => Self::eq(
                    Self::boolean_test_case_for_predicate(operand, test_true, negated),
                    Expression::number(1),
                ),
            };
        }

        match (test_true, negated) {
            (true, false) => Self::eq(operand, Expression::number(1)),
            (false, false) => Self::eq(operand, Expression::number(0)),
            (true, true) => Self::or(
                Self::eq(operand.clone(), Expression::number(0)),
                Self::is_null(operand),
            ),
            (false, true) => Self::or(
                Self::eq(operand.clone(), Expression::number(1)),
                Self::is_null(operand),
            ),
        }
    }

    fn is_boolean_predicate_operand(expr: &Expression) -> bool {
        match expr {
            Expression::Paren(paren) => Self::is_boolean_predicate_operand(&paren.this),
            Expression::Eq(_)
            | Expression::Neq(_)
            | Expression::Lt(_)
            | Expression::Lte(_)
            | Expression::Gt(_)
            | Expression::Gte(_)
            | Expression::Is(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Like(_)
            | Expression::ILike(_)
            | Expression::SimilarTo(_)
            | Expression::Glob(_)
            | Expression::RegexpLike(_)
            | Expression::In(_)
            | Expression::Between(_)
            | Expression::Exists(_)
            | Expression::And(_)
            | Expression::Or(_)
            | Expression::Not(_)
            | Expression::Any(_)
            | Expression::All(_)
            | Expression::EqualNull(_) => true,
            _ => false,
        }
    }

    fn scalar_array_comparison_values(expr: &Expression) -> Option<Vec<Expression>> {
        let (mut values, element_type) = Self::scalar_array_comparison_values_inner(expr)?;
        if let Some(to) = element_type {
            values = values
                .into_iter()
                .map(|value| Self::cast_scalar_array_comparison_value(value, to.clone()))
                .collect();
        }
        Some(values)
    }

    fn scalar_array_comparison_values_inner(
        expr: &Expression,
    ) -> Option<(Vec<Expression>, Option<DataType>)> {
        match expr {
            Expression::ArrayFunc(a) => Some((a.expressions.clone(), None)),
            Expression::Array(a) => Some((a.expressions.clone(), None)),
            Expression::Tuple(t) => Some((t.expressions.clone(), None)),
            Expression::Paren(p) => Self::scalar_array_comparison_values_inner(&p.this),
            Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
                let DataType::Array { element_type, .. } = &c.to else {
                    return None;
                };
                let (values, _) = Self::scalar_array_comparison_values_inner(&c.this)?;
                Some((values, Some((**element_type).clone())))
            }
            _ => None,
        }
    }

    fn cast_scalar_array_comparison_value(value: Expression, to: DataType) -> Expression {
        if matches!(&value, Expression::Cast(c) if c.to == to) {
            return value;
        }

        Expression::Cast(Box::new(Cast {
            this: value,
            to,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }

    fn normalize_frame_incompatible_window_functions(select: &mut Select) {
        let window_map: HashMap<String, Over> = select
            .windows
            .as_ref()
            .map(|windows| {
                windows
                    .iter()
                    .map(|window| (window.name.name.to_lowercase(), window.spec.clone()))
                    .collect()
            })
            .unwrap_or_default();

        for expr in &mut select.expressions {
            Self::normalize_frame_incompatible_window_expr(expr, &window_map);
        }

        if let Some(order_by) = &mut select.order_by {
            for ordered in &mut order_by.expressions {
                Self::normalize_frame_incompatible_window_expr(&mut ordered.this, &window_map);
            }
        }

        if let Some(qualify) = &mut select.qualify {
            Self::normalize_frame_incompatible_window_expr(&mut qualify.this, &window_map);
        }
    }

    fn normalize_frame_incompatible_window_expr(
        expr: &mut Expression,
        window_map: &HashMap<String, Over>,
    ) {
        match expr {
            Expression::WindowFunction(wf) => {
                Self::normalize_frame_incompatible_window_expr(&mut wf.this, window_map);

                if !Self::is_tsql_frame_incompatible_window_function(&wf.this) {
                    return;
                }

                wf.over.frame = None;

                let Some(window_name) = wf.over.window_name.clone() else {
                    return;
                };
                let Some(named_spec) =
                    Self::resolve_named_window_spec(&window_name.name, window_map, &mut Vec::new())
                else {
                    return;
                };

                if named_spec.frame.is_none() {
                    return;
                }

                if wf.over.partition_by.is_empty() {
                    wf.over.partition_by = named_spec.partition_by;
                }
                if wf.over.order_by.is_empty() {
                    wf.over.order_by = named_spec.order_by;
                }
                wf.over.window_name = None;
                wf.over.frame = None;
            }
            Expression::Alias(alias) => {
                Self::normalize_frame_incompatible_window_expr(&mut alias.this, window_map);
            }
            Expression::Paren(paren) => {
                Self::normalize_frame_incompatible_window_expr(&mut paren.this, window_map);
            }
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
                Self::normalize_frame_incompatible_window_expr(&mut cast.this, window_map);
            }
            Expression::Function(function) => {
                for arg in &mut function.args {
                    Self::normalize_frame_incompatible_window_expr(arg, window_map);
                }
            }
            Expression::Case(case) => {
                if let Some(operand) = &mut case.operand {
                    Self::normalize_frame_incompatible_window_expr(operand, window_map);
                }
                for (condition, result) in &mut case.whens {
                    Self::normalize_frame_incompatible_window_expr(condition, window_map);
                    Self::normalize_frame_incompatible_window_expr(result, window_map);
                }
                if let Some(else_expr) = &mut case.else_ {
                    Self::normalize_frame_incompatible_window_expr(else_expr, window_map);
                }
            }
            Expression::And(op)
            | Expression::Or(op)
            | Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op)
            | Expression::Mod(op)
            | Expression::Eq(op)
            | Expression::Neq(op)
            | Expression::Lt(op)
            | Expression::Lte(op)
            | Expression::Gt(op)
            | Expression::Gte(op)
            | Expression::Match(op)
            | Expression::BitwiseAnd(op)
            | Expression::BitwiseOr(op)
            | Expression::BitwiseXor(op)
            | Expression::Concat(op)
            | Expression::Adjacent(op)
            | Expression::TsMatch(op)
            | Expression::PropertyEQ(op)
            | Expression::ArrayContainsAll(op)
            | Expression::ArrayContainedBy(op)
            | Expression::ArrayOverlaps(op)
            | Expression::JSONBContainsAllTopKeys(op)
            | Expression::JSONBContainsAnyTopKeys(op)
            | Expression::JSONBDeleteAtPath(op)
            | Expression::ExtendsLeft(op)
            | Expression::ExtendsRight(op)
            | Expression::Is(op)
            | Expression::MemberOf(op) => {
                Self::normalize_frame_incompatible_window_expr(&mut op.left, window_map);
                Self::normalize_frame_incompatible_window_expr(&mut op.right, window_map);
            }
            Expression::Like(op) | Expression::ILike(op) => {
                Self::normalize_frame_incompatible_window_expr(&mut op.left, window_map);
                Self::normalize_frame_incompatible_window_expr(&mut op.right, window_map);
                if let Some(escape) = &mut op.escape {
                    Self::normalize_frame_incompatible_window_expr(escape, window_map);
                }
            }
            Expression::Not(op) | Expression::Neg(op) | Expression::BitwiseNot(op) => {
                Self::normalize_frame_incompatible_window_expr(&mut op.this, window_map);
            }
            Expression::In(in_expr) => {
                Self::normalize_frame_incompatible_window_expr(&mut in_expr.this, window_map);
                for value in &mut in_expr.expressions {
                    Self::normalize_frame_incompatible_window_expr(value, window_map);
                }
            }
            Expression::Between(between) => {
                Self::normalize_frame_incompatible_window_expr(&mut between.this, window_map);
                Self::normalize_frame_incompatible_window_expr(&mut between.low, window_map);
                Self::normalize_frame_incompatible_window_expr(&mut between.high, window_map);
            }
            Expression::IsNull(is_null) => {
                Self::normalize_frame_incompatible_window_expr(&mut is_null.this, window_map);
            }
            Expression::IsTrue(is_true) | Expression::IsFalse(is_true) => {
                Self::normalize_frame_incompatible_window_expr(&mut is_true.this, window_map);
            }
            _ => {}
        }
    }

    fn is_tsql_frame_incompatible_window_function(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::RowNumber(_)
                | Expression::Rank(_)
                | Expression::DenseRank(_)
                | Expression::NTile(_)
                | Expression::Ntile(_)
                | Expression::Lead(_)
                | Expression::Lag(_)
                | Expression::PercentRank(_)
                | Expression::CumeDist(_)
        )
    }

    fn resolve_named_window_spec(
        name: &str,
        window_map: &HashMap<String, Over>,
        seen: &mut Vec<String>,
    ) -> Option<Over> {
        let key = name.to_lowercase();
        if seen.iter().any(|seen_name| seen_name == &key) {
            return None;
        }

        let named_spec = window_map.get(&key)?.clone();
        seen.push(key);

        let mut resolved = if let Some(base_window) = &named_spec.window_name {
            Self::resolve_named_window_spec(&base_window.name, window_map, seen)
                .unwrap_or_else(Self::empty_over)
        } else {
            Self::empty_over()
        };

        if !named_spec.partition_by.is_empty() {
            resolved.partition_by = named_spec.partition_by;
        }
        if !named_spec.order_by.is_empty() {
            resolved.order_by = named_spec.order_by;
        }
        if named_spec.frame.is_some() {
            resolved.frame = named_spec.frame;
        }

        Some(resolved)
    }

    fn empty_over() -> Over {
        Over {
            window_name: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
            alias: None,
        }
    }

    const LATERAL_WRAPPER_SOURCE_ALIAS: &'static str = "_polyglot_lateral_source";
    const LATERAL_WRAPPER_OUTPUT_ALIAS: &'static str = "_polyglot_lateral";

    fn transform_lateral_join_to_apply(mut join: Join) -> Result<Join> {
        let Some(apply_kind) = Self::lateral_apply_kind(&join) else {
            return Ok(join);
        };

        let original_alias = Self::table_expression_alias(&join.this);
        let on = join.on.take();
        let rhs = Self::remove_lateral_marker(join.this);
        join.this = if on
            .as_ref()
            .is_some_and(|expr| !Self::is_true_condition(expr))
        {
            Self::wrap_lateral_apply_rhs(rhs, on.expect("checked as Some"), original_alias)?
        } else {
            rhs
        };
        join.using.clear();
        join.kind = apply_kind;
        join.use_inner_keyword = false;
        join.use_outer_keyword = false;
        join.deferred_condition = false;
        join.join_hint = None;
        join.match_condition = None;
        join.directed = false;
        Ok(join)
    }

    fn rewrite_comma_lateral_sources_to_joins(select: &mut Select) {
        let Some(from) = select.from.as_mut() else {
            return;
        };
        let has_comma_lateral = from
            .expressions
            .iter()
            .skip(1)
            .any(Self::is_lateral_table_expression);
        let has_apply_join = select
            .joins
            .iter()
            .any(|join| matches!(join.kind, JoinKind::CrossApply | JoinKind::OuterApply));

        if from.expressions.len() < 2 || (!has_comma_lateral && !has_apply_join) {
            return;
        }

        let mut expressions = std::mem::take(&mut from.expressions).into_iter();
        let Some(first) = expressions.next() else {
            return;
        };
        from.expressions = vec![first];

        let mut joins = expressions
            .map(|source| {
                if Self::is_lateral_table_expression(&source) {
                    Self::new_join(Self::remove_lateral_marker(source), JoinKind::CrossApply)
                } else {
                    Self::new_join(source, JoinKind::Cross)
                }
            })
            .collect::<Vec<_>>();
        joins.append(&mut select.joins);
        select.joins = joins;
    }

    fn new_join(this: Expression, kind: JoinKind) -> Join {
        Join {
            this,
            on: None,
            using: Vec::new(),
            kind,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: Vec::new(),
            comments: Vec::new(),
            nesting_group: 0,
            directed: false,
        }
    }

    fn lateral_apply_kind(join: &Join) -> Option<JoinKind> {
        if !join.using.is_empty() {
            return None;
        }

        match join.kind {
            JoinKind::Lateral => Some(JoinKind::CrossApply),
            JoinKind::LeftLateral => Some(JoinKind::OuterApply),
            JoinKind::Cross | JoinKind::Inner | JoinKind::Implicit
                if Self::is_lateral_table_expression(&join.this) =>
            {
                Some(JoinKind::CrossApply)
            }
            JoinKind::Left if Self::is_lateral_table_expression(&join.this) => {
                Some(JoinKind::OuterApply)
            }
            _ => None,
        }
    }

    fn is_true_condition(expr: &Expression) -> bool {
        match expr {
            Expression::Boolean(boolean) => boolean.value,
            Expression::Literal(lit) => {
                matches!(lit.as_ref(), Literal::Number(value) if value.trim() == "1")
            }
            Expression::Eq(op) => {
                Self::is_true_condition(&op.left) && Self::is_true_condition(&op.right)
            }
            Expression::Paren(paren) => Self::is_true_condition(&paren.this),
            _ => false,
        }
    }

    fn table_expression_alias(expr: &Expression) -> Option<(Identifier, Vec<Identifier>)> {
        match expr {
            Expression::Subquery(subquery) => subquery
                .alias
                .clone()
                .map(|alias| (alias, subquery.column_aliases.clone())),
            Expression::Alias(alias) if !alias.alias.is_empty() => {
                Some((alias.alias.clone(), alias.column_aliases.clone()))
            }
            Expression::Lateral(lateral) => lateral.alias.as_ref().map(|alias| {
                (
                    if lateral.alias_quoted {
                        Identifier::quoted(alias)
                    } else {
                        Identifier::new(alias)
                    },
                    lateral
                        .column_aliases
                        .iter()
                        .map(|column| Identifier::new(column.clone()))
                        .collect(),
                )
            }),
            _ => None,
        }
    }

    fn wrap_lateral_apply_rhs(
        rhs: Expression,
        predicate: Expression,
        original_alias: Option<(Identifier, Vec<Identifier>)>,
    ) -> Result<Expression> {
        let (outer_alias, column_aliases) = original_alias.unwrap_or_else(|| {
            (
                Identifier::new(Self::LATERAL_WRAPPER_OUTPUT_ALIAS),
                Vec::new(),
            )
        });
        let inner_alias = Identifier::new(Self::LATERAL_WRAPPER_SOURCE_ALIAS);
        let source =
            Self::with_table_expression_alias(rhs, inner_alias.clone(), column_aliases.clone());
        let predicate = Self::rewrite_column_qualifier(predicate, &outer_alias, &inner_alias)?;

        let mut select = Select::new();
        select.expressions = vec![Expression::Star(Star {
            table: None,
            except: None,
            replace: None,
            rename: None,
            trailing_comments: Vec::new(),
            span: None,
        })];
        select.from = Some(crate::expressions::From {
            expressions: vec![source],
        });
        select.where_clause = Some(Where { this: predicate });

        Ok(Expression::Subquery(Box::new(Subquery {
            this: Expression::Select(Box::new(select)),
            alias: Some(outer_alias),
            column_aliases,
            alias_explicit_as: true,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: Vec::new(),
            inferred_type: None,
        })))
    }

    fn with_table_expression_alias(
        expr: Expression,
        alias: Identifier,
        column_aliases: Vec<Identifier>,
    ) -> Expression {
        match expr {
            Expression::Subquery(mut subquery) => {
                subquery.alias = Some(alias);
                subquery.column_aliases = column_aliases;
                subquery.alias_explicit_as = true;
                subquery.alias_keyword = None;
                Expression::Subquery(subquery)
            }
            Expression::Alias(mut aliased) => {
                aliased.alias = alias;
                aliased.column_aliases = column_aliases;
                aliased.alias_explicit_as = true;
                aliased.alias_keyword = None;
                Expression::Alias(aliased)
            }
            Expression::Table(mut table) => {
                table.alias = Some(alias);
                table.alias_explicit_as = true;
                table.column_aliases = column_aliases;
                Expression::Table(table)
            }
            other => Expression::Alias(Box::new(Alias {
                this: other,
                alias,
                column_aliases,
                alias_explicit_as: true,
                alias_keyword: None,
                pre_alias_comments: Vec::new(),
                trailing_comments: Vec::new(),
                inferred_type: None,
            })),
        }
    }

    fn rewrite_column_qualifier(
        expr: Expression,
        from: &Identifier,
        to: &Identifier,
    ) -> Result<Expression> {
        super::transform_recursive(expr, &|expr| {
            Ok(match expr {
                Expression::Column(mut column)
                    if column
                        .table
                        .as_ref()
                        .is_some_and(|table| Self::same_identifier(table, from)) =>
                {
                    column.table = Some(to.clone());
                    Expression::Column(column)
                }
                other => other,
            })
        })
    }

    fn same_identifier(left: &Identifier, right: &Identifier) -> bool {
        if left.quoted || right.quoted {
            left.quoted == right.quoted && left.name == right.name
        } else {
            left.name.eq_ignore_ascii_case(&right.name)
        }
    }

    fn is_lateral_table_expression(expr: &Expression) -> bool {
        match expr {
            Expression::Subquery(subquery) => subquery.lateral,
            Expression::Lateral(_) => true,
            Expression::Alias(alias) => Self::is_lateral_table_expression(&alias.this),
            _ => false,
        }
    }

    fn remove_lateral_marker(expr: Expression) -> Expression {
        match expr {
            Expression::Subquery(mut subquery) => {
                subquery.lateral = false;
                Expression::Subquery(subquery)
            }
            Expression::Lateral(lateral) => Self::lateral_to_table_expression(*lateral),
            Expression::Alias(mut alias) => {
                alias.this = Self::remove_lateral_marker(alias.this);
                Expression::Alias(alias)
            }
            other => other,
        }
    }

    fn lateral_to_table_expression(lateral: crate::expressions::Lateral) -> Expression {
        let expr = *lateral.this;
        let Some(alias) = lateral.alias else {
            return expr;
        };

        Expression::Alias(Box::new(Alias {
            this: expr,
            alias: if lateral.alias_quoted {
                Identifier::quoted(alias)
            } else {
                Identifier::new(alias)
            },
            column_aliases: lateral
                .column_aliases
                .into_iter()
                .map(Identifier::new)
                .collect(),
            alias_explicit_as: true,
            alias_keyword: None,
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn rewrite_tuple_in_subquery_predicates(
        expr: Expression,
        outer_qualifier: Option<&Identifier>,
        under_not: bool,
    ) -> Expression {
        match expr {
            Expression::In(in_expr) if !under_not => {
                let in_expr = *in_expr;
                Self::tuple_in_subquery_to_exists(&in_expr, outer_qualifier, in_expr.not)
                    .unwrap_or_else(|| Expression::In(Box::new(in_expr)))
            }
            Expression::Eq(op) if !under_not => {
                let op = *op;
                Self::tuple_subquery_eq_to_exists(&op, outer_qualifier)
                    .unwrap_or_else(|| Expression::Eq(Box::new(op)))
            }
            Expression::And(mut op) => {
                op.left =
                    Self::rewrite_tuple_in_subquery_predicates(op.left, outer_qualifier, under_not);
                op.right = Self::rewrite_tuple_in_subquery_predicates(
                    op.right,
                    outer_qualifier,
                    under_not,
                );
                Expression::And(op)
            }
            Expression::Or(mut op) => {
                op.left =
                    Self::rewrite_tuple_in_subquery_predicates(op.left, outer_qualifier, under_not);
                op.right = Self::rewrite_tuple_in_subquery_predicates(
                    op.right,
                    outer_qualifier,
                    under_not,
                );
                Expression::Or(op)
            }
            Expression::Paren(mut paren) => {
                paren.this = Self::rewrite_tuple_in_subquery_predicates(
                    paren.this,
                    outer_qualifier,
                    under_not,
                );
                Expression::Paren(paren)
            }
            Expression::Not(mut not) => {
                if let Some(rewritten) = Self::direct_tuple_subquery_predicate_to_exists(
                    &not.this,
                    outer_qualifier,
                    true,
                ) {
                    rewritten
                } else {
                    not.this =
                        Self::rewrite_tuple_in_subquery_predicates(not.this, outer_qualifier, true);
                    Expression::Not(not)
                }
            }
            Expression::Alias(mut alias) => {
                alias.this = Self::rewrite_tuple_in_subquery_predicates(
                    alias.this,
                    outer_qualifier,
                    under_not,
                );
                Expression::Alias(alias)
            }
            Expression::Cast(mut cast) => {
                cast.this = Self::rewrite_tuple_in_subquery_predicates(
                    cast.this,
                    outer_qualifier,
                    under_not,
                );
                if let Some(format) = cast.format.take() {
                    cast.format = Some(Box::new(Self::rewrite_tuple_in_subquery_predicates(
                        *format,
                        outer_qualifier,
                        under_not,
                    )));
                }
                if let Some(default) = cast.default.take() {
                    cast.default = Some(Box::new(Self::rewrite_tuple_in_subquery_predicates(
                        *default,
                        outer_qualifier,
                        under_not,
                    )));
                }
                Expression::Cast(cast)
            }
            Expression::TryCast(mut cast) => {
                cast.this = Self::rewrite_tuple_in_subquery_predicates(
                    cast.this,
                    outer_qualifier,
                    under_not,
                );
                Expression::TryCast(cast)
            }
            Expression::SafeCast(mut cast) => {
                cast.this = Self::rewrite_tuple_in_subquery_predicates(
                    cast.this,
                    outer_qualifier,
                    under_not,
                );
                Expression::SafeCast(cast)
            }
            Expression::Case(mut case) => {
                if let Some(operand) = case.operand.take() {
                    case.operand = Some(Self::rewrite_tuple_in_subquery_predicates(
                        operand,
                        outer_qualifier,
                        under_not,
                    ));
                }
                case.whens = case
                    .whens
                    .into_iter()
                    .map(|(condition, result)| {
                        (
                            Self::rewrite_tuple_in_subquery_predicates(
                                condition,
                                outer_qualifier,
                                false,
                            ),
                            Self::rewrite_tuple_in_subquery_predicates(
                                result,
                                outer_qualifier,
                                under_not,
                            ),
                        )
                    })
                    .collect();
                if let Some(else_) = case.else_.take() {
                    case.else_ = Some(Self::rewrite_tuple_in_subquery_predicates(
                        else_,
                        outer_qualifier,
                        under_not,
                    ));
                }
                Expression::Case(case)
            }
            Expression::IfFunc(mut if_func) => {
                if_func.condition = Self::rewrite_tuple_in_subquery_predicates(
                    if_func.condition,
                    outer_qualifier,
                    false,
                );
                if_func.true_value = Self::rewrite_tuple_in_subquery_predicates(
                    if_func.true_value,
                    outer_qualifier,
                    under_not,
                );
                if let Some(false_value) = if_func.false_value.take() {
                    if_func.false_value = Some(Self::rewrite_tuple_in_subquery_predicates(
                        false_value,
                        outer_qualifier,
                        under_not,
                    ));
                }
                Expression::IfFunc(if_func)
            }
            other => other,
        }
    }

    fn direct_tuple_subquery_predicate_to_exists(
        expr: &Expression,
        outer_qualifier: Option<&Identifier>,
        negated: bool,
    ) -> Option<Expression> {
        match expr {
            Expression::In(in_expr) => {
                Self::tuple_in_subquery_to_exists(in_expr, outer_qualifier, negated ^ in_expr.not)
            }
            Expression::Paren(paren) => Self::direct_tuple_subquery_predicate_to_exists(
                &paren.this,
                outer_qualifier,
                negated,
            ),
            _ => None,
        }
    }

    fn tuple_in_subquery_to_exists(
        in_expr: &In,
        outer_qualifier: Option<&Identifier>,
        negated: bool,
    ) -> Option<Expression> {
        if in_expr.unnest.is_some() {
            return None;
        }

        let left_expressions = Self::tuple_expressions(&in_expr.this)?;
        let mut select = Self::select_from_in_rhs(in_expr)?;

        if left_expressions.len() != select.expressions.len() || left_expressions.is_empty() {
            return None;
        }

        let inner_qualifier = Self::single_select_source_qualifier(&select);
        let mut predicates = Vec::with_capacity(left_expressions.len() + 1);
        for (projection, left) in select
            .expressions
            .iter()
            .cloned()
            .zip(left_expressions.iter().cloned())
        {
            let inner = Self::tuple_in_projection_expr(projection, inner_qualifier.as_ref())?;
            let outer = Self::qualify_tuple_operand(left, outer_qualifier);
            predicates.push(if negated {
                Self::tuple_component_may_match(inner, outer)
            } else {
                Expression::Eq(Box::new(BinaryOp::new(inner, outer)))
            });
        }

        if let Some(where_clause) = select.where_clause.take() {
            predicates.push(where_clause.this);
        }

        select.expressions = vec![Expression::number(1)];
        select.where_clause = Some(Where {
            this: Self::and_all(predicates)?,
        });

        Some(Expression::Exists(Box::new(Exists {
            this: Expression::Select(Box::new(select)),
            not: negated,
        })))
    }

    fn tuple_subquery_eq_to_exists(
        op: &BinaryOp,
        outer_qualifier: Option<&Identifier>,
    ) -> Option<Expression> {
        if let Some((tuple_expr, query_expr)) = Self::tuple_and_query_operands(&op.left, &op.right)
        {
            return Self::tuple_subquery_eq_to_exists_inner(
                tuple_expr,
                query_expr,
                outer_qualifier,
            );
        }

        if let Some((tuple_expr, query_expr)) = Self::tuple_and_query_operands(&op.right, &op.left)
        {
            return Self::tuple_subquery_eq_to_exists_inner(
                tuple_expr,
                query_expr,
                outer_qualifier,
            );
        }

        None
    }

    fn tuple_subquery_eq_to_exists_inner(
        tuple_expr: &Expression,
        query_expr: &Expression,
        outer_qualifier: Option<&Identifier>,
    ) -> Option<Expression> {
        let tuple_expressions = Self::tuple_expressions(tuple_expr)?;
        let mut select = Self::select_from_query_expression(query_expr)?;

        if tuple_expressions.len() != select.expressions.len() || tuple_expressions.is_empty() {
            return None;
        }

        let inner_qualifier = Self::single_select_source_qualifier(&select);
        let mut predicates = Vec::with_capacity(tuple_expressions.len() + 1);
        for (projection, tuple_operand) in select
            .expressions
            .iter()
            .cloned()
            .zip(tuple_expressions.iter().cloned())
        {
            let inner = Self::tuple_in_projection_expr(projection, inner_qualifier.as_ref())?;
            let outer = Self::qualify_tuple_operand(tuple_operand, outer_qualifier);
            predicates.push(Expression::Eq(Box::new(BinaryOp::new(inner, outer))));
        }

        if let Some(where_clause) = select.where_clause.take() {
            predicates.push(where_clause.this);
        }

        select.expressions = vec![Expression::number(1)];
        select.where_clause = Some(Where {
            this: Self::and_all(predicates)?,
        });

        Some(Expression::Exists(Box::new(Exists {
            this: Expression::Select(Box::new(select)),
            not: false,
        })))
    }

    fn tuple_and_query_operands<'a>(
        tuple_candidate: &'a Expression,
        query_candidate: &'a Expression,
    ) -> Option<(&'a Expression, &'a Expression)> {
        if Self::tuple_expressions(tuple_candidate).is_some()
            && Self::select_from_query_expression(query_candidate).is_some()
        {
            Some((tuple_candidate, query_candidate))
        } else {
            None
        }
    }

    fn select_from_query_expression(expr: &Expression) -> Option<Select> {
        match expr {
            Expression::Select(select) => Some((**select).clone()),
            Expression::Subquery(subquery) => Self::select_from_query_expression(&subquery.this),
            Expression::Paren(paren) => Self::select_from_query_expression(&paren.this),
            _ => None,
        }
    }

    fn select_from_in_rhs(in_expr: &In) -> Option<Select> {
        if let Some(values) = Self::values_from_in_rhs(in_expr) {
            return Self::select_from_values(&values);
        }

        if let Some(query) = &in_expr.query {
            return if in_expr.expressions.is_empty() {
                Self::select_from_query_expression(query)
            } else {
                None
            };
        }

        if in_expr.expressions.len() == 1 {
            Self::select_from_query_expression(&in_expr.expressions[0])
        } else {
            None
        }
    }

    fn values_from_in_rhs(in_expr: &In) -> Option<Values> {
        if let Some(query) = &in_expr.query {
            return if in_expr.expressions.is_empty() {
                Self::values_from_expression(query)
            } else {
                None
            };
        }

        if in_expr.expressions.len() == 1 {
            if let Some(values) = Self::values_from_expression(&in_expr.expressions[0]) {
                return Some(values);
            }
        }

        // IN (VALUES ...) currently parses as VALUES(first_row), followed by tuple rows.
        let Expression::Function(first_row) = in_expr.expressions.first()? else {
            return None;
        };
        if !first_row.name.eq_ignore_ascii_case("VALUES") {
            return None;
        }

        let mut rows = Vec::with_capacity(in_expr.expressions.len());
        rows.push(Tuple {
            expressions: first_row.args.clone(),
        });
        for row in &in_expr.expressions[1..] {
            rows.push(Self::tuple_from_values_row(row)?);
        }

        Some(Values {
            expressions: rows,
            alias: None,
            column_aliases: Vec::new(),
        })
    }

    fn values_from_expression(expr: &Expression) -> Option<Values> {
        match expr {
            Expression::Values(values) => Some((**values).clone()),
            Expression::Paren(paren) => Self::values_from_expression(&paren.this),
            Expression::Subquery(subquery) => Self::values_from_expression(&subquery.this),
            _ => None,
        }
    }

    fn tuple_from_values_row(expr: &Expression) -> Option<Tuple> {
        match expr {
            Expression::Tuple(tuple) => Some((**tuple).clone()),
            Expression::Paren(paren) => match &paren.this {
                Expression::Tuple(tuple) => Some((**tuple).clone()),
                other => Some(Tuple {
                    expressions: vec![other.clone()],
                }),
            },
            _ => None,
        }
    }

    fn select_from_values(values: &Values) -> Option<Select> {
        let column_count = values.expressions.first()?.expressions.len();
        if column_count == 0
            || values
                .expressions
                .iter()
                .any(|row| row.expressions.len() != column_count)
        {
            return None;
        }

        let source_alias = Identifier::new("_polyglot_values");
        let column_aliases = (1..=column_count)
            .map(|index| Identifier::new(format!("_polyglot_value_{index}")))
            .collect::<Vec<_>>();
        let projections = column_aliases
            .iter()
            .cloned()
            .map(|column| Self::column_from_identifier(column, Some(source_alias.clone())))
            .collect();

        let mut source_values = values.clone();
        source_values.alias = None;
        source_values.column_aliases.clear();

        let source = Expression::Subquery(Box::new(Subquery {
            this: Expression::Values(Box::new(source_values)),
            alias: Some(source_alias),
            column_aliases,
            alias_explicit_as: true,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: Vec::new(),
            inferred_type: None,
        }));

        let mut select = Select::new();
        select.expressions = projections;
        select.from = Some(From {
            expressions: vec![source],
        });
        Some(select)
    }

    fn tuple_expressions(expr: &Expression) -> Option<&[Expression]> {
        match expr {
            Expression::Tuple(tuple) => Some(&tuple.expressions),
            Expression::Function(function) if function.name.eq_ignore_ascii_case("ROW") => {
                Some(&function.args)
            }
            Expression::Paren(paren) => Self::tuple_expressions(&paren.this),
            _ => None,
        }
    }

    fn tuple_in_projection_expr(
        expr: Expression,
        qualifier: Option<&Identifier>,
    ) -> Option<Expression> {
        match expr {
            Expression::Alias(alias) => Self::tuple_in_projection_expr(alias.this, qualifier),
            Expression::Column(mut column) => {
                if column.table.is_none() {
                    column.table = qualifier.cloned();
                }
                Some(Expression::Column(column))
            }
            Expression::Identifier(identifier) => {
                Some(Self::column_from_identifier(identifier, qualifier.cloned()))
            }
            Expression::Dot(_) => Some(expr),
            other => Some(Self::qualify_tuple_expression(other, qualifier)),
        }
    }

    fn qualify_tuple_operand(expr: Expression, qualifier: Option<&Identifier>) -> Expression {
        Self::qualify_tuple_expression(expr, qualifier)
    }

    fn qualify_tuple_expression(expr: Expression, qualifier: Option<&Identifier>) -> Expression {
        match expr {
            Expression::Column(mut column) => {
                if column.table.is_none() {
                    column.table = qualifier.cloned();
                }
                Expression::Column(column)
            }
            Expression::Identifier(identifier) => {
                Self::column_from_identifier(identifier, qualifier.cloned())
            }
            Expression::Alias(mut alias) => {
                alias.this = Self::qualify_tuple_expression(alias.this, qualifier);
                Expression::Alias(alias)
            }
            Expression::Paren(mut paren) => {
                paren.this = Self::qualify_tuple_expression(paren.this, qualifier);
                Expression::Paren(paren)
            }
            Expression::Cast(mut cast) => {
                cast.this = Self::qualify_tuple_expression(cast.this, qualifier);
                if let Some(format) = cast.format.take() {
                    cast.format =
                        Some(Box::new(Self::qualify_tuple_expression(*format, qualifier)));
                }
                if let Some(default) = cast.default.take() {
                    cast.default = Some(Box::new(Self::qualify_tuple_expression(
                        *default, qualifier,
                    )));
                }
                Expression::Cast(cast)
            }
            Expression::TryCast(mut cast) => {
                cast.this = Self::qualify_tuple_expression(cast.this, qualifier);
                Expression::TryCast(cast)
            }
            Expression::SafeCast(mut cast) => {
                cast.this = Self::qualify_tuple_expression(cast.this, qualifier);
                Expression::SafeCast(cast)
            }
            Expression::Function(mut function) => {
                function.args = function
                    .args
                    .into_iter()
                    .map(|arg| Self::qualify_tuple_expression(arg, qualifier))
                    .collect();
                Expression::Function(function)
            }
            Expression::Add(mut op) => {
                op.left = Self::qualify_tuple_expression(op.left, qualifier);
                op.right = Self::qualify_tuple_expression(op.right, qualifier);
                Expression::Add(op)
            }
            Expression::Sub(mut op) => {
                op.left = Self::qualify_tuple_expression(op.left, qualifier);
                op.right = Self::qualify_tuple_expression(op.right, qualifier);
                Expression::Sub(op)
            }
            Expression::Mul(mut op) => {
                op.left = Self::qualify_tuple_expression(op.left, qualifier);
                op.right = Self::qualify_tuple_expression(op.right, qualifier);
                Expression::Mul(op)
            }
            Expression::Div(mut op) => {
                op.left = Self::qualify_tuple_expression(op.left, qualifier);
                op.right = Self::qualify_tuple_expression(op.right, qualifier);
                Expression::Div(op)
            }
            Expression::Mod(mut op) => {
                op.left = Self::qualify_tuple_expression(op.left, qualifier);
                op.right = Self::qualify_tuple_expression(op.right, qualifier);
                Expression::Mod(op)
            }
            other => other,
        }
    }

    fn tuple_component_may_match(inner: Expression, outer: Expression) -> Expression {
        Self::paren(
            Self::or_all(vec![
                Expression::Eq(Box::new(BinaryOp::new(inner.clone(), outer.clone()))),
                Self::is_null(inner),
                Self::is_null(outer),
            ])
            .expect("tuple component match condition is non-empty"),
        )
    }

    fn column_from_identifier(identifier: Identifier, table: Option<Identifier>) -> Expression {
        Expression::Column(Box::new(Column {
            name: identifier,
            table,
            join_mark: false,
            trailing_comments: Vec::new(),
            span: None,
            inferred_type: None,
        }))
    }

    fn single_select_source_qualifier(select: &Select) -> Option<Identifier> {
        if !select.joins.is_empty() {
            return None;
        }

        let from = select.from.as_ref()?;
        if from.expressions.len() != 1 {
            return None;
        }

        Self::source_qualifier(&from.expressions[0])
    }

    fn source_qualifier(source: &Expression) -> Option<Identifier> {
        match source {
            Expression::Table(table) => table.alias.clone().or_else(|| Some(table.name.clone())),
            Expression::Subquery(subquery) => subquery.alias.clone(),
            _ => None,
        }
    }

    fn and_all(mut predicates: Vec<Expression>) -> Option<Expression> {
        if predicates.is_empty() {
            return None;
        }

        let first = predicates.remove(0);
        Some(predicates.into_iter().fold(first, |left, right| {
            Expression::And(Box::new(BinaryOp::new(left, right)))
        }))
    }

    fn or_all(mut predicates: Vec<Expression>) -> Option<Expression> {
        if predicates.is_empty() {
            return None;
        }

        let first = predicates.remove(0);
        Some(predicates.into_iter().fold(first, |left, right| {
            Expression::Or(Box::new(BinaryOp::new(left, right)))
        }))
    }

    /// Transform data types according to T-SQL TYPE_MAPPING
    pub(super) fn transform_data_type(
        &self,
        dt: crate::expressions::DataType,
    ) -> Result<Expression> {
        use crate::expressions::DataType;
        let transformed = match dt {
            // BOOLEAN -> BIT
            DataType::Boolean => DataType::Custom {
                name: "BIT".to_string(),
            },
            // INT stays as INT in TSQL (native type)
            DataType::Int { .. } => dt,
            // DOUBLE stays as Double internally (TSQL generator outputs FLOAT for it)
            // DECIMAL -> NUMERIC
            DataType::Decimal { precision, scale } => DataType::Custom {
                name: if let (Some(p), Some(s)) = (&precision, &scale) {
                    format!("NUMERIC({}, {})", p, s)
                } else if let Some(p) = &precision {
                    format!("NUMERIC({})", p)
                } else {
                    "NUMERIC".to_string()
                },
            },
            // TEXT -> VARCHAR(MAX)
            DataType::Text => DataType::Custom {
                name: "VARCHAR(MAX)".to_string(),
            },
            // TIMESTAMP -> DATETIME2
            DataType::Timestamp { .. } => DataType::Custom {
                name: "DATETIME2".to_string(),
            },
            // UUID -> UNIQUEIDENTIFIER
            DataType::Uuid => DataType::Custom {
                name: "UNIQUEIDENTIFIER".to_string(),
            },
            // Normalise custom type names that have PostgreSQL aliases
            DataType::Custom { ref name } => {
                let upper = name.trim().to_uppercase();
                let (base_name, precision, _scale) = Self::parse_type_precision_and_scale(&upper);
                match base_name.as_str() {
                    // PostgreSQL DOUBLE PRECISION is SQL Server FLOAT.
                    "DOUBLE PRECISION" => DataType::Custom {
                        name: "FLOAT".to_string(),
                    },
                    // BPCHAR is PostgreSQL's blank-padded CHAR alias — map to CHAR
                    "BPCHAR" => {
                        if let Some(len) = precision {
                            DataType::Char { length: Some(len) }
                        } else {
                            DataType::Char { length: None }
                        }
                    }
                    _ => dt,
                }
            }
            // Keep all other types as-is
            other => other,
        };
        Ok(Expression::DataType(transformed))
    }

    /// Parse a type name that may embed precision/scale: `"TYPENAME(n, m)"` → `("TYPENAME", Some(n), Some(m))`.
    pub(super) fn parse_type_precision_and_scale(name: &str) -> (String, Option<u32>, Option<u32>) {
        if let Some(paren_pos) = name.find('(') {
            let base = name[..paren_pos].to_string();
            let rest = &name[paren_pos + 1..];
            if let Some(close_pos) = rest.find(')') {
                let args = &rest[..close_pos];
                let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
                let precision = parts.first().and_then(|s| s.parse::<u32>().ok());
                let scale = parts.get(1).and_then(|s| s.parse::<u32>().ok());
                return (base, precision, scale);
            }
            (base, None, None)
        } else {
            (name.to_string(), None, None)
        }
    }

    fn transform_logical_aggregate(
        condition: Expression,
        filter: Option<Expression>,
        aggregate_name: &str,
    ) -> Result<Expression> {
        let false_condition = Expression::Not(Box::new(crate::expressions::UnaryOp {
            this: condition.clone(),
            inferred_type: None,
        }));
        let true_condition = Self::apply_aggregate_filter(condition, filter.clone());
        let false_condition = Self::apply_aggregate_filter(false_condition, filter);

        let case_expr = Expression::Case(Box::new(crate::expressions::Case {
            operand: None,
            whens: vec![
                (true_condition, Expression::number(1)),
                (false_condition, Expression::number(0)),
            ],
            else_: Some(Expression::null()),
            comments: Vec::new(),
            inferred_type: None,
        }));

        let case_expr = crate::transforms::ensure_bools(case_expr)?;
        let aggregate = Expression::Function(Box::new(Function::new(
            aggregate_name.to_string(),
            vec![case_expr],
        )));

        Ok(Expression::Cast(Box::new(Cast {
            this: aggregate,
            to: DataType::Custom {
                name: "BIT".to_string(),
            },
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        })))
    }

    fn reassociate_logical_aggregate_window(mut window: WindowFunction) -> Expression {
        let Expression::Cast(mut cast) = window.this else {
            return Expression::WindowFunction(Box::new(window));
        };

        if !Self::is_transformed_logical_aggregate_cast(&cast) {
            window.this = Expression::Cast(cast);
            return Expression::WindowFunction(Box::new(window));
        }

        window.this = cast.this;
        cast.this = Expression::WindowFunction(Box::new(window));
        Expression::Cast(cast)
    }

    fn is_transformed_logical_aggregate_cast(cast: &Cast) -> bool {
        if !matches!(
            &cast.to,
            DataType::Custom { name } if name.eq_ignore_ascii_case("BIT")
        ) {
            return false;
        }

        let Expression::Function(function) = &cast.this else {
            return false;
        };
        if !matches!(function.name.to_ascii_uppercase().as_str(), "MIN" | "MAX")
            || function.args.len() != 1
        {
            return false;
        }

        matches!(
            function.args.first(),
            Some(Expression::Case(case))
                if case.operand.is_none()
                    && case.whens.len() == 2
                    && matches!(case.else_.as_ref(), Some(Expression::Null(_)))
        )
    }

    fn apply_aggregate_filter(condition: Expression, filter: Option<Expression>) -> Expression {
        match filter {
            Some(filter) => Expression::And(Box::new(crate::expressions::BinaryOp::new(
                filter, condition,
            ))),
            None => condition,
        }
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // COALESCE -> ISNULL for 2 args (optimization)
            "COALESCE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "ISNULL".to_string(),
                f.args,
            )))),

            // NVL -> ISNULL (SQL Server function)
            "NVL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "ISNULL".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT -> STRING_AGG in SQL Server 2017+
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // STRING_AGG is native to SQL Server 2017+
            "STRING_AGG" => Ok(Expression::Function(Box::new(f))),

            // LISTAGG -> STRING_AGG
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "STRING_AGG".to_string(),
                f.args,
            )))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LENGTH -> LEN in SQL Server
            "LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LEN".to_string(),
                f.args,
            )))),

            // PostgreSQL btrim(text[, characters]) -> T-SQL TRIM([characters FROM] text)
            "BTRIM" if f.args.len() == 1 || f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let characters = if args.is_empty() {
                    None
                } else {
                    Some(args.remove(0))
                };
                Ok(Expression::Trim(Box::new(TrimFunc {
                    this,
                    sql_standard_syntax: characters.is_some(),
                    characters,
                    position: TrimPosition::Both,
                    position_explicit: false,
                })))
            }

            // PostgreSQL md5(text) returns lowercase hex text; HASHBYTES returns varbinary.
            "MD5" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Self::tsql_md5_hex(args.remove(0)))
            }

            "SHA256" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Self::function(
                    "HASHBYTES",
                    vec![Expression::string("SHA2_256"), args.remove(0)],
                ))
            }

            "SHA512" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Self::function(
                    "HASHBYTES",
                    vec![Expression::string("SHA2_512"), args.remove(0)],
                ))
            }

            // PostgreSQL octet_length(text/bytea) -> DATALENGTH(...)
            "OCTET_LENGTH" if f.args.len() == 1 => Ok(Self::function("DATALENGTH", f.args)),

            // PostgreSQL bit_length(text/bytea) -> DATALENGTH(...) * 8
            "BIT_LENGTH" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Expression::Mul(Box::new(BinaryOp::new(
                    Self::function("DATALENGTH", vec![args.remove(0)]),
                    Expression::number(8),
                ))))
            }

            // PostgreSQL to_hex(int) -> unpadded lowercase hex text. SQL Server's
            // binary conversion preserves the integer width, so remove only leading
            // zeroes and retain a single zero for the all-zero value.
            "TO_HEX" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Self::tsql_postgres_to_hex(args.remove(0)))
            }

            // PostgreSQL encode(bytea, 'hex') -> lowercase hex text.
            "ENCODE" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let encoding = args.remove(0);
                if Self::literal_string(&encoding)
                    .is_some_and(|encoding| encoding.eq_ignore_ascii_case("hex"))
                {
                    Ok(Self::tsql_hex_from_varbinary(this))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ENCODE".to_string(),
                        vec![this, encoding],
                    ))))
                }
            }

            // Preserve support for manually constructed/generic DECODE ASTs in addition
            // to the parser's typed DecodeCase representation.
            "DECODE"
                if f.args.len() == 2
                    && Self::literal_string(&f.args[1])
                        .is_some_and(|format| format.eq_ignore_ascii_case("hex")) =>
            {
                let mut args = f.args;
                Ok(Self::tsql_convert(
                    DataType::Custom {
                        name: "VARBINARY(MAX)".to_string(),
                    },
                    args.remove(0),
                    Some(2),
                ))
            }

            // PostgreSQL repeat(text, count) -> SQL Server REPLICATE(text, count)
            "REPEAT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "REPLICATE".to_string(),
                f.args,
            )))),

            // PostgreSQL chr(code) -> SQL Server CHAR(code)
            "CHR" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CHAR".to_string(),
                f.args,
            )))),

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // NOW -> GETDATE or CURRENT_TIMESTAMP (both work)
            "NOW" => Ok(Self::getdate()),

            // CURRENT_TIMESTAMP -> GETDATE (SQL Server prefers GETDATE)
            "CURRENT_TIMESTAMP" => Ok(Self::getdate()),

            // CURRENT_DATE -> CAST(GETDATE() AS DATE)
            "CURRENT_DATE" => Ok(Self::cast_getdate_to(DataType::Date)),

            // CURRENT_TIME -> CAST(GETDATE() AS TIME)
            "CURRENT_TIME" => Ok(Self::cast_getdate_to(DataType::Time {
                precision: None,
                timezone: false,
            })),

            // LOCALTIMESTAMP -> GETDATE()
            "LOCALTIMESTAMP" => Ok(Self::getdate()),

            // PostgreSQL clock_timestamp() -> high-precision current system timestamp.
            "CLOCK_TIMESTAMP" if f.args.is_empty() => Ok(Self::function("SYSDATETIME", vec![])),

            // PostgreSQL make_date(year, month, day) -> SQL Server DATEFROMPARTS.
            "MAKE_DATE" if f.args.len() == 3 => Ok(Self::function("DATEFROMPARTS", f.args)),

            // PostgreSQL make_time(hour, minute, double-precision seconds) ->
            // SQL Server TIMEFROMPARTS(hour, minute, seconds, fractions, precision).
            "MAKE_TIME" if f.args.len() == 3 => Ok(Self::make_time(f.args)),

            // PostgreSQL/Oracle-style TO_DATE(value, fmt) -> typed parse expression.
            // The generator will emit native CONVERT(DATE, value, style) when
            // the literal format maps cleanly to a T-SQL style code.
            "TO_DATE" if f.args.len() == 2 => {
                Self::formatted_str_to_date_or_fallback(f.args, "TO_DATE")
            }

            // One-arg TO_DATE(value) has no format string; use a native cast shape.
            "TO_DATE" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Expression::Cast(Box::new(Cast {
                    this: args.remove(0),
                    to: DataType::Date,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            }

            // PostgreSQL/Oracle-style TO_TIMESTAMP(value, fmt) -> typed parse expression.
            // This avoids the invalid CONVERT(value, fmt) argument order.
            "TO_TIMESTAMP" if f.args.len() == 2 => {
                Self::formatted_str_to_time_or_fallback(f.args, "TO_TIMESTAMP")
            }

            // PostgreSQL's one-arg TO_TIMESTAMP is epoch seconds.
            "TO_TIMESTAMP" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Expression::UnixToTime(Box::new(
                    crate::expressions::UnixToTime {
                        this: Box::new(args.remove(0)),
                        scale: Some(0),
                        zone: None,
                        hours: None,
                        minutes: None,
                        format: None,
                        target_type: None,
                    },
                )))
            }

            // PostgreSQL/Oracle-style TO_CHAR(value, fmt) -> typed format expression.
            // The generator converts the normalized strftime format to .NET FORMAT().
            "TO_CHAR" if f.args.len() == 2 => {
                Self::formatted_time_to_str_or_fallback(f.args, "TO_CHAR")
            }

            // TO_CHAR(value) without a format remains a normal T-SQL FORMAT call.
            "TO_CHAR" => Ok(Expression::Function(Box::new(Function::new(
                "FORMAT".to_string(),
                f.args,
            )))),

            // DATE_FORMAT -> FORMAT
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(Function::new(
                "FORMAT".to_string(),
                f.args,
            )))),

            // DATE_TRUNC -> DATETRUNC in SQL Server 2022+
            // For older versions, use DATEADD/DATEDIFF combo
            "DATE_TRUNC" | "DATETRUNC" => {
                let mut args = Self::uppercase_first_arg_if_identifier(f.args);
                // Cast string literal date arg to DATETIME2
                if args.len() >= 2 {
                    if let Expression::Literal(lit) = &args[1] {
                        if let Literal::String(_) = lit.as_ref() {
                            args[1] = Expression::Cast(Box::new(Cast {
                                this: args[1].clone(),
                                to: DataType::Custom {
                                    name: "DATETIME2".to_string(),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                        }
                    }
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATETRUNC".to_string(),
                    args,
                ))))
            }

            // DATEADD is native to SQL Server - uppercase the unit
            "DATEADD" => {
                let args = Self::uppercase_first_arg_if_identifier(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    args,
                ))))
            }

            // DATEDIFF is native to SQL Server - uppercase the unit
            "DATEDIFF" => {
                let args = Self::uppercase_first_arg_if_identifier(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    args,
                ))))
            }

            // EXTRACT -> DATEPART in SQL Server
            "EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "DATEPART".to_string(),
                f.args,
            )))),

            // STRPOS / POSITION -> CHARINDEX
            "STRPOS" | "POSITION" if f.args.len() >= 2 => {
                // CHARINDEX(substring, string) - same arg order as POSITION
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    f.args,
                ))))
            }

            // CHARINDEX is native
            "CHARINDEX" => Ok(Expression::Function(Box::new(f))),

            // CEILING -> CEILING (native)
            "CEILING" | "CEIL" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("CEILING".to_string(), f.args),
            ))),

            // ARRAY functions don't exist in SQL Server
            // Would need JSON or table-valued parameters

            // JSON_EXTRACT -> JSON_VALUE or JSON_QUERY
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_SCALAR -> JSON_VALUE
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE".to_string(),
                f.args,
            )))),

            // PARSE_JSON -> strip in TSQL (just keep the string argument)
            "PARSE_JSON" if f.args.len() == 1 => Ok(f.args.into_iter().next().unwrap()),

            // GET_PATH(obj, path) -> ISNULL(JSON_QUERY(obj, path), JSON_VALUE(obj, path)) in TSQL
            "GET_PATH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                let json_path = match &path {
                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                        let Literal::String(s) = lit.as_ref() else {
                            unreachable!()
                        };
                        let normalized = if s.starts_with('$') {
                            s.clone()
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Box::new(Literal::String(normalized)))
                    }
                    _ => path,
                };
                // ISNULL(JSON_QUERY(obj, path), JSON_VALUE(obj, path))
                let json_query = Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    vec![this.clone(), json_path.clone()],
                )));
                let json_value = Expression::Function(Box::new(Function::new(
                    "JSON_VALUE".to_string(),
                    vec![this, json_path],
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ISNULL".to_string(),
                    vec![json_query, json_value],
                ))))
            }

            // JSON_QUERY with 1 arg: add '$' path and wrap in ISNULL
            // JSON_QUERY with 2 args: leave as-is (already processed or inside ISNULL)
            "JSON_QUERY" if f.args.len() == 1 => {
                let this = f.args.into_iter().next().unwrap();
                let path = Expression::Literal(Box::new(Literal::String("$".to_string())));
                let json_query = Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    vec![this.clone(), path.clone()],
                )));
                let json_value = Expression::Function(Box::new(Function::new(
                    "JSON_VALUE".to_string(),
                    vec![this, path],
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ISNULL".to_string(),
                    vec![json_query, json_value],
                ))))
            }

            // SPLIT -> STRING_SPLIT (returns a table, needs CROSS APPLY)
            "SPLIT" => Ok(Expression::Function(Box::new(Function::new(
                "STRING_SPLIT".to_string(),
                f.args,
            )))),

            // REGEXP_LIKE -> Not directly supported, use LIKE or PATINDEX
            // SQL Server has limited regex support via PATINDEX and LIKE
            "REGEXP_LIKE" => {
                // Fall back to LIKE (loses regex functionality)
                Ok(Expression::Function(Box::new(Function::new(
                    "PATINDEX".to_string(),
                    f.args,
                ))))
            }

            // LN -> LOG in SQL Server
            "LN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LOG".to_string(),
                f.args,
            )))),

            // LOG with 2 args is LOG(base, value) in most DBs but LOG(value, base) in SQL Server
            // This needs careful handling

            // STDDEV -> STDEV in SQL Server
            "STDDEV" | "STDDEV_SAMP" => Ok(Expression::Function(Box::new(Function::new(
                "STDEV".to_string(),
                f.args,
            )))),

            // STDDEV_POP -> STDEVP in SQL Server
            "STDDEV_POP" => Ok(Expression::Function(Box::new(Function::new(
                "STDEVP".to_string(),
                f.args,
            )))),

            // VAR_SAMP -> VAR in SQL Server
            "VARIANCE" | "VAR_SAMP" => Ok(Expression::Function(Box::new(Function::new(
                "VAR".to_string(),
                f.args,
            )))),

            // VAR_POP -> VARP in SQL Server
            "VAR_POP" => Ok(Expression::Function(Box::new(Function::new(
                "VARP".to_string(),
                f.args,
            )))),

            // Boolean aggregates -> MIN/MAX over a null-preserving CASE, cast back to BIT.
            "BOOL_AND" | "LOGICAL_AND" | "BOOLAND_AGG" | "EVERY" if f.args.len() == 1 => {
                let mut args = f.args;
                Self::transform_logical_aggregate(args.remove(0), None, "MIN")
            }
            "BOOL_OR" | "LOGICAL_OR" | "BOOLOR_AGG" if f.args.len() == 1 => {
                let mut args = f.args;
                Self::transform_logical_aggregate(args.remove(0), None, "MAX")
            }

            // DATE_ADD(date, interval) -> DATEADD(DAY, interval, date)
            "DATE_ADD" => {
                if f.args.len() == 2 {
                    let mut args = f.args;
                    let date = args.remove(0);
                    let interval = args.remove(0);
                    let unit = Expression::Identifier(crate::expressions::Identifier {
                        name: "DAY".to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                        span: None,
                    });
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEADD".to_string(),
                        vec![unit, interval, date],
                    ))))
                } else {
                    let args = Self::uppercase_first_arg_if_identifier(f.args);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEADD".to_string(),
                        args,
                    ))))
                }
            }

            // INSERT → STUFF (Snowflake/MySQL string INSERT → T-SQL STUFF)
            "INSERT" => Ok(Expression::Function(Box::new(Function::new(
                "STUFF".to_string(),
                f.args,
            )))),

            // SUSER_NAME(), SUSER_SNAME(), SYSTEM_USER() -> CURRENT_USER
            "SUSER_NAME" | "SUSER_SNAME" | "SYSTEM_USER" => Ok(Expression::CurrentUser(Box::new(
                crate::expressions::CurrentUser { this: None },
            ))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    fn literal_string(expr: &Expression) -> Option<&str> {
        match expr {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::String(s) => Some(s),
                _ => None,
            },
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast)
                if Self::is_text_data_type(&cast.to) =>
            {
                Self::literal_string(&cast.this)
            }
            _ => None,
        }
    }

    fn is_text_data_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::String { .. }
            | DataType::Text
            | DataType::TextWithLength { .. } => true,
            DataType::Custom { name } => {
                let base = name
                    .split_once('(')
                    .map_or(name.as_str(), |(base, _)| base)
                    .trim();
                matches!(
                    base.to_ascii_uppercase().as_str(),
                    "CHAR"
                        | "NCHAR"
                        | "VARCHAR"
                        | "NVARCHAR"
                        | "TEXT"
                        | "NTEXT"
                        | "STRING"
                        | "CHARACTER VARYING"
                )
            }
            _ => false,
        }
    }

    fn is_numeric_data_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::TinyInt { .. }
            | DataType::SmallInt { .. }
            | DataType::Int { .. }
            | DataType::BigInt { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::Decimal { .. } => true,
            DataType::Custom { name } => {
                let base = name
                    .split_once('(')
                    .map_or(name.as_str(), |(base, _)| base)
                    .trim();
                matches!(
                    base.to_ascii_uppercase().as_str(),
                    "TINYINT"
                        | "SMALLINT"
                        | "INT"
                        | "INTEGER"
                        | "BIGINT"
                        | "DECIMAL"
                        | "NUMERIC"
                        | "REAL"
                        | "FLOAT"
                        | "MONEY"
                        | "SMALLMONEY"
                )
            }
            _ => false,
        }
    }

    fn is_explicitly_numeric_expression(expr: &Expression) -> bool {
        if expr.inferred_type().is_some_and(Self::is_numeric_data_type) {
            return true;
        }

        match expr {
            Expression::Literal(literal) => matches!(literal.as_ref(), Literal::Number(_)),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
                Self::is_numeric_data_type(&cast.to)
            }
            Expression::Alias(alias) => Self::is_explicitly_numeric_expression(&alias.this),
            Expression::Paren(paren) => Self::is_explicitly_numeric_expression(&paren.this),
            Expression::Neg(unary) => Self::is_explicitly_numeric_expression(&unary.this),
            _ => false,
        }
    }

    fn is_postgres_numeric_to_char_format(format: &str) -> bool {
        let mut unquoted = String::with_capacity(format.len());
        let mut quoted = false;
        let mut chars = format.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '"' {
                if quoted && chars.peek() == Some(&'"') {
                    chars.next();
                } else {
                    quoted = !quoted;
                }
            } else if !quoted {
                unquoted.extend(ch.to_uppercase());
            }
        }

        unquoted.contains(['9', '0'])
            || ["PR", "SG", "PL", "RN", "EEEE"]
                .iter()
                .any(|token| unquoted.contains(token))
    }

    fn postgres_format_to_strftime(format: &str) -> String {
        const POSTGRES_FORMAT_TO_STRFTIME: &[(&str, &str)] = &[
            ("FMHH24", "%-H"),
            ("FMHH12", "%-I"),
            ("FMDDD", "%-j"),
            ("TMMonth", "%B"),
            ("TMMon", "%b"),
            ("TMDay", "%A"),
            ("TMDy", "%a"),
            ("YYYY", "%Y"),
            ("yyyy", "%Y"),
            ("HH24", "%H"),
            ("HH12", "%I"),
            ("FMDD", "%-d"),
            ("FMMM", "%-m"),
            ("FMMI", "%-M"),
            ("FMSS", "%-S"),
            ("DDD", "%j"),
            ("ddd", "%j"),
            ("YY", "%y"),
            ("yy", "%y"),
            ("MM", "%m"),
            ("mm", "%m"),
            ("DD", "%d"),
            ("dd", "%d"),
            ("MI", "%M"),
            ("mi", "%M"),
            ("SS", "%S"),
            ("ss", "%S"),
            ("US", "%f"),
            ("OF", "%z"),
            ("TZ", "%Z"),
            ("WW", "%U"),
            ("ww", "%U"),
            ("D", "%u"),
            ("d", "%u"),
        ];
        crate::format_tokens::convert_format_tokens(format, POSTGRES_FORMAT_TO_STRFTIME)
            .unwrap_or_else(|| format.to_string())
    }

    fn formatted_str_to_time_or_fallback(
        mut args: Vec<Expression>,
        original_name: &str,
    ) -> Result<Expression> {
        let this = args.remove(0);
        let format = args.remove(0);
        if let Some(format) = Self::literal_string(&format) {
            Ok(Expression::StrToTime(Box::new(
                crate::expressions::StrToTime {
                    this: Box::new(this),
                    format: Self::postgres_format_to_strftime(format),
                    zone: None,
                    safe: None,
                    target_type: Some(Box::new(Expression::DataType(DataType::Custom {
                        name: "DATETIME2".to_string(),
                    }))),
                },
            )))
        } else {
            Ok(Expression::Function(Box::new(Function::new(
                original_name.to_string(),
                vec![this, format],
            ))))
        }
    }

    fn formatted_str_to_date_or_fallback(
        mut args: Vec<Expression>,
        original_name: &str,
    ) -> Result<Expression> {
        let this = args.remove(0);
        let format = args.remove(0);
        if let Some(format) = Self::literal_string(&format) {
            Ok(Expression::StrToDate(Box::new(
                crate::expressions::StrToDate {
                    this: Box::new(this),
                    format: Some(Self::postgres_format_to_strftime(format)),
                    safe: None,
                },
            )))
        } else {
            Ok(Expression::Function(Box::new(Function::new(
                original_name.to_string(),
                vec![this, format],
            ))))
        }
    }

    fn formatted_time_to_str_or_fallback(
        mut args: Vec<Expression>,
        original_name: &str,
    ) -> Result<Expression> {
        let this = args.remove(0);
        let format = args.remove(0);
        if let Some(format_string) = Self::literal_string(&format).map(str::to_owned) {
            if Self::is_explicitly_numeric_expression(&this)
                || Self::is_postgres_numeric_to_char_format(&format_string)
            {
                return Ok(Expression::Function(Box::new(Function::new(
                    original_name.to_string(),
                    vec![this, format],
                ))));
            }

            Ok(Expression::TimeToStr(Box::new(
                crate::expressions::TimeToStr {
                    this: Box::new(this),
                    format: Self::postgres_format_to_strftime(&format_string),
                    culture: None,
                    zone: None,
                },
            )))
        } else {
            Ok(Expression::Function(Box::new(Function::new(
                original_name.to_string(),
                vec![this, format],
            ))))
        }
    }

    fn transform_aggregate_function(
        &self,
        mut f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        if matches!(
            name_upper.as_str(),
            "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "COUNT"
                | "COUNT_BIG"
                | "ANY_VALUE"
                | "APPROX_COUNT_DISTINCT"
                | "STDEV"
                | "STDEVP"
                | "VAR"
                | "VARP"
                | "BOOL_AND"
                | "BOOL_OR"
                | "LOGICAL_AND"
                | "LOGICAL_OR"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
        ) {
            f.order_by.clear();
        }

        match name_upper.as_str() {
            // GROUP_CONCAT -> STRING_AGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // LISTAGG -> STRING_AGG
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "STRING_AGG".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> Not directly supported in SQL Server
            // Would need to use FOR XML PATH or STRING_AGG
            "ARRAY_AGG" if !f.args.is_empty() => {
                // Fall back to STRING_AGG (loses array semantics)
                Ok(Expression::Function(Box::new(Function::new(
                    "STRING_AGG".to_string(),
                    f.args,
                ))))
            }

            // Boolean aggregates -> MIN/MAX over a null-preserving CASE, cast back to BIT.
            "BOOL_AND" | "LOGICAL_AND" | "BOOLAND_AGG" | "EVERY" if f.args.len() == 1 => {
                let mut args = f.args;
                Self::transform_logical_aggregate(args.remove(0), f.filter, "MIN")
            }
            "BOOL_OR" | "LOGICAL_OR" | "BOOLOR_AGG" if f.args.len() == 1 => {
                let mut args = f.args;
                Self::transform_logical_aggregate(args.remove(0), f.filter, "MAX")
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn without_inert_ordering(
        mut aggregate: Box<crate::expressions::AggFunc>,
    ) -> Box<crate::expressions::AggFunc> {
        aggregate.order_by.clear();
        aggregate
    }

    /// Transform CTEs to add auto-aliases to bare expressions in SELECT
    /// In TSQL, when a CTE doesn't have explicit column aliases, bare expressions
    /// in the SELECT need to be aliased
    fn transform_cte(&self, cte: Cte) -> Result<Expression> {
        Ok(Expression::Cte(Box::new(self.transform_cte_inner(cte))))
    }

    /// Inner method to transform a CTE, returning the modified Cte struct
    fn transform_cte_inner(&self, mut cte: Cte) -> Cte {
        // Only transform if the CTE doesn't have explicit column aliases
        // If it has column aliases like `WITH t(a, b) AS (...)`, we don't need to auto-alias
        if cte.columns.is_empty() {
            cte.this = self.qualify_derived_table_outputs(cte.this);
        }
        cte
    }

    /// Transform Subqueries to add auto-aliases to bare expressions in SELECT
    /// In TSQL, when a subquery has a table alias but no column aliases,
    /// bare expressions need to be aliased
    fn transform_subquery(&self, mut subquery: Subquery) -> Result<Expression> {
        // Only transform if the subquery has a table alias but no column aliases
        // e.g., `(SELECT 1) AS subq` needs aliasing, but `(SELECT 1) AS subq(a)` doesn't
        if subquery.alias.is_some() && subquery.column_aliases.is_empty() {
            subquery.this = self.qualify_derived_table_outputs(subquery.this);
        }
        Ok(Expression::Subquery(Box::new(subquery)))
    }

    /// Add aliases to bare (unaliased) expressions in a SELECT statement
    /// This transforms expressions like `SELECT 1` into `SELECT 1 AS [1]`
    /// BUT only when the SELECT has no FROM clause (i.e., it's a value expression)
    fn qualify_derived_table_outputs(&self, expr: Expression) -> Expression {
        match expr {
            Expression::Select(mut select) => {
                // Only auto-alias if the SELECT has NO from clause
                // If there's a FROM clause, column references already have names from the source tables
                let has_from = select.from.is_some();
                if !has_from {
                    select.expressions = select
                        .expressions
                        .into_iter()
                        .map(|e| self.maybe_alias_expression(e))
                        .collect();
                }
                Expression::Select(select)
            }
            // For UNION/INTERSECT/EXCEPT, transform the first SELECT
            Expression::Union(mut u) => {
                let left = std::mem::replace(&mut u.left, Expression::Null(Null));
                u.left = self.qualify_derived_table_outputs(left);
                Expression::Union(u)
            }
            Expression::Intersect(mut i) => {
                let left = std::mem::replace(&mut i.left, Expression::Null(Null));
                i.left = self.qualify_derived_table_outputs(left);
                Expression::Intersect(i)
            }
            Expression::Except(mut e) => {
                let left = std::mem::replace(&mut e.left, Expression::Null(Null));
                e.left = self.qualify_derived_table_outputs(left);
                Expression::Except(e)
            }
            // Already wrapped in a Subquery (nested), transform the inner
            Expression::Subquery(mut s) => {
                s.this = self.qualify_derived_table_outputs(s.this);
                Expression::Subquery(s)
            }
            // Pass through anything else
            other => other,
        }
    }

    /// Add an alias to a bare expression if needed
    /// Returns the expression unchanged if it already has an alias or is a star
    /// NOTE: This is only called for SELECTs without a FROM clause, so all bare
    /// expressions (including identifiers and columns) need to be aliased.
    fn maybe_alias_expression(&self, expr: Expression) -> Expression {
        match &expr {
            // Already has an alias, leave it alone
            Expression::Alias(_) => expr,
            // Multiple aliases, leave it alone
            Expression::Aliases(_) => expr,
            // Star (including qualified star like t.*) doesn't need an alias
            Expression::Star(_) => expr,
            // When there's no FROM clause (which is the only case when this method is called),
            // we need to alias columns and identifiers too since they're standalone values
            // that need explicit names for the derived table output.
            // Everything else (literals, functions, columns, identifiers, etc.) needs an alias
            _ => {
                if let Some(output_name) = self.get_output_name(&expr) {
                    Expression::Alias(Box::new(Alias {
                        this: expr,
                        alias: Identifier {
                            name: output_name,
                            quoted: true, // Force quoting for TSQL bracket syntax
                            trailing_comments: Vec::new(),
                            span: None,
                        },
                        column_aliases: Vec::new(),
                        alias_explicit_as: false,
                        alias_keyword: None,
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                        inferred_type: None,
                    }))
                } else {
                    // No output name, leave as-is (shouldn't happen for valid expressions)
                    expr
                }
            }
        }
    }

    /// Get the "output name" of an expression for auto-aliasing
    /// For literals, this is the literal value
    /// For columns, this is the column name
    fn get_output_name(&self, expr: &Expression) -> Option<String> {
        match expr {
            // Literals - use the literal value as the name
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::Number(n) => Some(n.clone()),
                Literal::String(s) => Some(s.clone()),
                Literal::HexString(h) => Some(format!("0x{}", h)),
                Literal::HexNumber(h) => Some(format!("0x{}", h)),
                Literal::BitString(b) => Some(format!("b{}", b)),
                Literal::ByteString(b) => Some(format!("b'{}'", b)),
                Literal::NationalString(s) => Some(format!("N'{}'", s)),
                Literal::Date(d) => Some(d.clone()),
                Literal::Time(t) => Some(t.clone()),
                Literal::Timestamp(ts) => Some(ts.clone()),
                Literal::Datetime(dt) => Some(dt.clone()),
                Literal::TripleQuotedString(s, _) => Some(s.clone()),
                Literal::EscapeString(s) => Some(s.clone()),
                Literal::DollarString(s) => Some(s.clone()),
                Literal::RawString(s) => Some(s.clone()),
            },
            // Columns - use the column name
            Expression::Column(col) => Some(col.name.name.clone()),
            // Identifiers - use the identifier name
            Expression::Identifier(ident) => Some(ident.name.clone()),
            // Boolean literals
            Expression::Boolean(b) => Some(if b.value { "1" } else { "0" }.to_string()),
            // NULL
            Expression::Null(_) => Some("NULL".to_string()),
            // For functions, use the function name as a fallback
            Expression::Function(f) => Some(f.name.clone()),
            // For aggregates, use the function name
            Expression::AggregateFunction(f) => Some(f.name.clone()),
            // For other expressions, generate a generic name
            _ => Some(format!("_col_{}", 0)),
        }
    }

    /// Helper to uppercase the first argument if it's an identifier or column (for DATEDIFF, DATEADD units)
    fn uppercase_first_arg_if_identifier(mut args: Vec<Expression>) -> Vec<Expression> {
        use crate::expressions::Identifier;
        if !args.is_empty() {
            match &args[0] {
                Expression::Identifier(id) => {
                    args[0] = Expression::Identifier(Identifier {
                        name: id.name.to_uppercase(),
                        quoted: id.quoted,
                        trailing_comments: id.trailing_comments.clone(),
                        span: None,
                    });
                }
                Expression::Var(v) => {
                    args[0] = Expression::Identifier(Identifier {
                        name: v.this.to_uppercase(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                        span: None,
                    });
                }
                Expression::Column(col) if col.table.is_none() => {
                    args[0] = Expression::Identifier(Identifier {
                        name: col.name.name.to_uppercase(),
                        quoted: col.name.quoted,
                        trailing_comments: col.name.trailing_comments.clone(),
                        span: None,
                    });
                }
                _ => {}
            }
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_tsql(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile(sql, DialectType::TSQL)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_nvl_to_isnull() {
        let result = transpile_to_tsql("SELECT NVL(a, b)");
        assert!(
            result.contains("ISNULL"),
            "Expected ISNULL, got: {}",
            result
        );
    }

    #[test]
    fn test_coalesce_to_isnull() {
        let result = transpile_to_tsql("SELECT COALESCE(a, b)");
        assert!(
            result.contains("ISNULL"),
            "Expected ISNULL, got: {}",
            result
        );
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_tsql("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_length_to_len() {
        let result = transpile_to_tsql("SELECT LENGTH(name)");
        assert!(result.contains("LEN"), "Expected LEN, got: {}", result);
    }

    #[test]
    fn test_now_to_getdate() {
        let result = transpile_to_tsql("SELECT NOW()");
        assert!(
            result.contains("GETDATE"),
            "Expected GETDATE, got: {}",
            result
        );
    }

    #[test]
    fn test_group_concat_to_string_agg() {
        let result = transpile_to_tsql("SELECT GROUP_CONCAT(name)");
        assert!(
            result.contains("STRING_AGG"),
            "Expected STRING_AGG, got: {}",
            result
        );
    }

    #[test]
    fn test_listagg_to_string_agg() {
        let result = transpile_to_tsql("SELECT LISTAGG(name)");
        assert!(
            result.contains("STRING_AGG"),
            "Expected STRING_AGG, got: {}",
            result
        );
    }

    #[test]
    fn test_ln_to_log() {
        let result = transpile_to_tsql("SELECT LN(x)");
        assert!(result.contains("LOG"), "Expected LOG, got: {}", result);
    }

    #[test]
    fn test_stddev_to_stdev() {
        let result = transpile_to_tsql("SELECT STDDEV(x)");
        assert!(result.contains("STDEV"), "Expected STDEV, got: {}", result);
    }

    #[test]
    fn test_bracket_identifiers() {
        // SQL Server uses square brackets for identifiers
        let dialect = Dialect::get(DialectType::TSQL);
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '[');
    }

    #[test]
    fn test_json_query_isnull_wrapper_simple() {
        // JSON_QUERY with two args needs ISNULL wrapper when transpiling to TSQL
        let dialect = Dialect::get(DialectType::TSQL);
        let result = dialect
            .transpile(r#"JSON_QUERY(x, '$')"#, DialectType::TSQL)
            .expect("transpile failed");
        assert!(
            result[0].contains("ISNULL"),
            "JSON_QUERY should be wrapped with ISNULL: {}",
            result[0]
        );
    }

    #[test]
    fn test_json_query_isnull_wrapper_nested() {
        let dialect = Dialect::get(DialectType::TSQL);
        let result = dialect
            .transpile(
                r#"JSON_QUERY(REPLACE(REPLACE(x, '''', '"'), '""', '"'))"#,
                DialectType::TSQL,
            )
            .expect("transpile failed");
        let expected = r#"ISNULL(JSON_QUERY(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'), JSON_VALUE(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'))"#;
        assert_eq!(
            result[0], expected,
            "JSON_QUERY should be wrapped with ISNULL"
        );
    }
}
