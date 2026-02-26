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
    Alias, Cast, Cte, DataType, Expression, Function, Identifier, Join, JoinKind, LikeOp, Literal,
    StringAggFunc, Subquery, UnaryFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

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
        config
    }

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

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
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
                                            pre_alias_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                        }))
                                    }
                                    Expression::Identifier(ident)
                                        if !ident.name.starts_with('@') =>
                                    {
                                        Expression::Alias(Box::new(Alias {
                                            this: op.right,
                                            alias: ident.clone(),
                                            column_aliases: Vec::new(),
                                            pre_alias_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                        }))
                                    }
                                    _ => Expression::Eq(op),
                                }
                            }
                            other => other,
                        }
                    })
                    .collect();

                // Transform CTEs in the WITH clause to add auto-aliases
                if let Some(ref mut with) = select.with {
                    with.ctes = with
                        .ctes
                        .drain(..)
                        .map(|cte| self.transform_cte_inner(cte))
                        .collect();
                }

                Ok(Expression::Select(select))
            }

            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ===== Boolean IS TRUE/FALSE -> = 1/0 for TSQL =====
            // TSQL doesn't have IS TRUE/IS FALSE syntax
            Expression::IsTrue(it) => {
                let one = Expression::Literal(crate::expressions::Literal::Number("1".to_string()));
                if it.not {
                    // a IS NOT TRUE -> NOT a = 1
                    Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                        this: Expression::Eq(Box::new(crate::expressions::BinaryOp {
                            left: it.this,
                            right: one,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                        })),
                    })))
                } else {
                    // a IS TRUE -> a = 1
                    Ok(Expression::Eq(Box::new(crate::expressions::BinaryOp {
                        left: it.this,
                        right: one,
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                    })))
                }
            }
            Expression::IsFalse(it) => {
                let zero =
                    Expression::Literal(crate::expressions::Literal::Number("0".to_string()));
                if it.not {
                    // a IS NOT FALSE -> NOT a = 0
                    Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                        this: Expression::Eq(Box::new(crate::expressions::BinaryOp {
                            left: it.this,
                            right: zero,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                        })),
                    })))
                } else {
                    // a IS FALSE -> a = 0
                    Ok(Expression::Eq(Box::new(crate::expressions::BinaryOp {
                        left: it.this,
                        right: zero,
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                    })))
                }
            }

            // Note: CASE WHEN boolean conditions are handled in ensure_bools preprocessing

            // NOT IN -> NOT ... IN for TSQL (TSQL prefers NOT prefix)
            Expression::In(mut in_expr) if in_expr.not => {
                in_expr.not = false;
                Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                    this: Expression::In(in_expr),
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
            }))),

            // LISTAGG -> STRING_AGG in SQL Server (SQL Server 2017+)
            Expression::ListAgg(f) => Ok(Expression::StringAgg(Box::new(StringAggFunc {
                this: f.this,
                separator: f.separator,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                limit: None,
            }))),

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

            // LATERAL JOIN -> CROSS APPLY in SQL Server
            Expression::Join(join) if join.kind == JoinKind::Lateral => {
                Ok(Expression::Join(Box::new(Join {
                    this: join.this,
                    on: None,
                    using: join.using,
                    kind: JoinKind::CrossApply,
                    use_inner_keyword: false,
                    use_outer_keyword: false,
                    deferred_condition: false,
                    join_hint: None,
                    match_condition: None,
                    pivots: join.pivots,
                    comments: join.comments,
                    nesting_group: 0,
                    directed: false,
                })))
            }

            // LEFT LATERAL JOIN -> OUTER APPLY in SQL Server
            Expression::Join(join) if join.kind == JoinKind::LeftLateral => {
                Ok(Expression::Join(Box::new(Join {
                    this: join.this,
                    on: None, // APPLY doesn't use ON clause
                    using: join.using,
                    kind: JoinKind::OuterApply,
                    use_inner_keyword: false,
                    use_outer_keyword: false,
                    deferred_condition: false,
                    join_hint: None,
                    match_condition: None,
                    pivots: join.pivots,
                    comments: join.comments,
                    nesting_group: 0,
                    directed: false,
                })))
            }

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

            // Boolean literals TRUE/FALSE -> 1/0 in SQL Server
            Expression::Boolean(b) => {
                let value = if b.value { 1 } else { 0 };
                Ok(Expression::Literal(crate::expressions::Literal::Number(
                    value.to_string(),
                )))
            }

            // LN -> LOG in SQL Server
            Expression::Ln(f) => Ok(Expression::Function(Box::new(Function::new(
                "LOG".to_string(),
                vec![f.this],
            )))),

            // ===== Date/time =====
            // CurrentDate -> CAST(GETDATE() AS DATE) in SQL Server
            Expression::CurrentDate(_) => {
                let getdate =
                    Expression::Function(Box::new(Function::new("GETDATE".to_string(), vec![])));
                Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                    this: getdate,
                    to: crate::expressions::DataType::Date,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // CurrentTimestamp -> GETDATE() in SQL Server
            Expression::CurrentTimestamp(_) => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

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
                    Expression::Literal(Literal::String(s)) => {
                        let normalized = if s.starts_with('$') {
                            s
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Literal::String(normalized))
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

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl TSQLDialect {
    /// Transform data types according to T-SQL TYPE_MAPPING
    fn transform_data_type(&self, dt: crate::expressions::DataType) -> Result<Expression> {
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
            // Keep all other types as-is
            other => other,
        };
        Ok(Expression::DataType(transformed))
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

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // NOW -> GETDATE or CURRENT_TIMESTAMP (both work)
            "NOW" => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

            // CURRENT_TIMESTAMP -> GETDATE (SQL Server prefers GETDATE)
            "CURRENT_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

            // CURRENT_DATE -> CAST(GETDATE() AS DATE)
            "CURRENT_DATE" => {
                // In SQL Server, use CAST(GETDATE() AS DATE)
                Ok(Expression::Function(Box::new(Function::new(
                    "CAST".to_string(),
                    vec![
                        Expression::Function(Box::new(Function::new(
                            "GETDATE".to_string(),
                            vec![],
                        ))),
                        Expression::Identifier(crate::expressions::Identifier::new("DATE")),
                    ],
                ))))
            }

            // TO_DATE -> CONVERT or CAST
            "TO_DATE" => Ok(Expression::Function(Box::new(Function::new(
                "CONVERT".to_string(),
                f.args,
            )))),

            // TO_TIMESTAMP -> CONVERT
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "CONVERT".to_string(),
                f.args,
            )))),

            // TO_CHAR -> FORMAT in SQL Server 2012+
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
                    if let Expression::Literal(Literal::String(_)) = &args[1] {
                        args[1] = Expression::Cast(Box::new(Cast {
                            this: args[1].clone(),
                            to: DataType::Custom {
                                name: "DATETIME2".to_string(),
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }));
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
                    Expression::Literal(Literal::String(s)) => {
                        let normalized = if s.starts_with('$') {
                            s.clone()
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Literal::String(normalized))
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
                let path = Expression::Literal(Literal::String("$".to_string()));
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

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
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

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
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
                u.left = self.qualify_derived_table_outputs(u.left);
                Expression::Union(u)
            }
            Expression::Intersect(mut i) => {
                i.left = self.qualify_derived_table_outputs(i.left);
                Expression::Intersect(i)
            }
            Expression::Except(mut e) => {
                e.left = self.qualify_derived_table_outputs(e.left);
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
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
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
            Expression::Literal(lit) => match lit {
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
            .transpile_to(sql, DialectType::TSQL)
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
            .transpile_to(r#"JSON_QUERY(x, '$')"#, DialectType::TSQL)
            .expect("transpile failed");
        assert!(
            result[0].contains("ISNULL"),
            "JSON_QUERY should be wrapped with ISNULL: {}",
            result[0]
        );
    }

    #[test]
    fn test_json_query_isnull_wrapper_nested() {
        // Run in a thread with larger stack to avoid stack overflow
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let dialect = Dialect::get(DialectType::TSQL);
                let result = dialect.transpile_to(
                    r#"JSON_QUERY(REPLACE(REPLACE(x, '''', '"'), '""', '"'))"#,
                    DialectType::TSQL,
                ).expect("transpile failed");
                let expected = r#"ISNULL(JSON_QUERY(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'), JSON_VALUE(REPLACE(REPLACE(x, '''', '"'), '""', '"'), '$'))"#;
                assert_eq!(result[0], expected, "JSON_QUERY should be wrapped with ISNULL");
            })
            .expect("Failed to spawn test thread")
            .join()
            .expect("Test thread panicked");
    }
}
