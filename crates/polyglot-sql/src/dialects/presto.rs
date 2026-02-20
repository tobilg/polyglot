//! Presto Dialect
//!
//! Presto-specific transformations based on sqlglot patterns.
//! Presto is the base for Trino dialect.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, AggregateFunction, BinaryOp, Case, Cast, Column, DataType, Expression, Function,
    JsonExtractFunc, LikeOp, Literal, UnaryFunc, VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Presto dialect
pub struct PrestoDialect;

impl DialectImpl for PrestoDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Presto
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Presto uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Presto does NOT support nested comments
        config.nested_comments = false;
        // Presto does NOT support QUALIFY - it's a valid identifier
        // (unlike Snowflake, BigQuery, DuckDB which have QUALIFY clause)
        config.keywords.remove("QUALIFY");
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Presto),
            limit_only_literals: true,
            tz_to_with_time_zone: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Presto
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in Presto
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // TryCast stays as TryCast (Presto supports TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in Presto
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILike -> LOWER() LIKE LOWER() (Presto doesn't support ILIKE)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left.clone())));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right.clone())));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier.clone(),
                })))
            }

            // CountIf is native in Presto (keep as-is)
            Expression::CountIf(f) => Ok(Expression::CountIf(f)),

            // EXPLODE -> UNNEST in Presto
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ExplodeOuter -> UNNEST in Presto
            Expression::ExplodeOuter(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // StringAgg -> ARRAY_JOIN(ARRAY_AGG()) in Presto
            Expression::StringAgg(f) => {
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![f.this.clone()],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = f.separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // GroupConcat -> ARRAY_JOIN(ARRAY_AGG()) in Presto
            Expression::GroupConcat(f) => {
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![f.this.clone()],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = f.separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // ListAgg -> ARRAY_JOIN(ARRAY_AGG()) in Presto
            Expression::ListAgg(f) => {
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![f.this.clone()],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = f.separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // ParseJson: handled by generator (outputs JSON_PARSE for Presto)

            // JSONExtract (variant_extract/colon accessor) -> JSON_EXTRACT in Presto
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
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this: *e.this,
                    path,
                    returning: None,
                    arrow_syntax: false,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // Div: Presto has TYPED_DIVISION - wrap left operand in CAST(AS DOUBLE)
            // to ensure float division (only when left isn't already a float cast)
            Expression::Div(mut op) => {
                if !Self::is_float_cast(&op.left) {
                    op.left = Expression::Cast(Box::new(crate::expressions::Cast {
                        this: op.left,
                        to: DataType::Double {
                            precision: None,
                            scale: None,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }));
                }
                Ok(Expression::Div(op))
            }

            // IntDiv -> CAST(CAST(x AS DOUBLE) / y AS INTEGER) in Presto
            Expression::IntDiv(f) => {
                let cast_x = Expression::Cast(Box::new(Cast {
                    this: f.this,
                    to: crate::expressions::DataType::Double {
                        precision: None,
                        scale: None,
                    },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                let div_expr = Expression::Div(Box::new(BinaryOp::new(cast_x, f.expression)));
                Ok(Expression::Cast(Box::new(Cast {
                    this: div_expr,
                    to: crate::expressions::DataType::Int {
                        length: None,
                        integer_spelling: true,
                    },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // DELETE: Strip table alias and unqualify columns (Presto doesn't support DELETE aliases)
            Expression::Delete(mut d) => {
                if d.alias.is_some() {
                    d.alias = None;
                    d.alias_explicit_as = false;
                    // Unqualify all columns in the WHERE clause
                    if let Some(ref mut where_clause) = d.where_clause {
                        where_clause.this = Self::unqualify_columns(where_clause.this.clone());
                    }
                }
                Ok(Expression::Delete(d))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl PrestoDialect {
    /// Recursively unqualify columns - remove table qualifiers from Column references
    fn unqualify_columns(expr: Expression) -> Expression {
        match expr {
            Expression::Column(c) => {
                if c.table.is_some() {
                    Expression::Column(Column {
                        name: c.name,
                        table: None,
                        join_mark: c.join_mark,
                        trailing_comments: c.trailing_comments,
                    })
                } else {
                    Expression::Column(c)
                }
            }
            // DotAccess: db.t2.c -> c (strip all qualifiers, keep only the final field name)
            Expression::Dot(d) => Expression::Column(Column {
                name: d.field,
                table: None,
                join_mark: false,
                trailing_comments: Vec::new(),
            }),
            // Recursively walk common binary expression types
            Expression::And(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::And(op)
            }
            Expression::Or(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Or(op)
            }
            Expression::Eq(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Eq(op)
            }
            Expression::Neq(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Neq(op)
            }
            Expression::Gt(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Gt(op)
            }
            Expression::Lt(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Lt(op)
            }
            Expression::Gte(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Gte(op)
            }
            Expression::Lte(mut op) => {
                op.left = Self::unqualify_columns(op.left);
                op.right = Self::unqualify_columns(op.right);
                Expression::Lte(op)
            }
            // Unary operators
            Expression::Not(mut e) => {
                e.this = Self::unqualify_columns(e.this);
                Expression::Not(e)
            }
            // Predicates
            Expression::In(mut i) => {
                i.this = Self::unqualify_columns(i.this);
                i.expressions = i
                    .expressions
                    .into_iter()
                    .map(Self::unqualify_columns)
                    .collect();
                // Also recurse into subquery if present
                if let Some(q) = i.query {
                    i.query = Some(Self::unqualify_columns(q));
                }
                Expression::In(i)
            }
            Expression::IsNull(mut f) => {
                f.this = Self::unqualify_columns(f.this);
                Expression::IsNull(f)
            }
            Expression::Paren(mut p) => {
                p.this = Self::unqualify_columns(p.this);
                Expression::Paren(p)
            }
            Expression::Function(mut f) => {
                f.args = f.args.into_iter().map(Self::unqualify_columns).collect();
                Expression::Function(f)
            }
            // For subqueries (SELECT statements inside IN, etc), also unqualify
            Expression::Select(mut s) => {
                s.expressions = s
                    .expressions
                    .into_iter()
                    .map(Self::unqualify_columns)
                    .collect();
                if let Some(ref mut w) = s.where_clause {
                    w.this = Self::unqualify_columns(w.this.clone());
                }
                Expression::Select(s)
            }
            Expression::Subquery(mut sq) => {
                sq.this = Self::unqualify_columns(sq.this);
                Expression::Subquery(sq)
            }
            Expression::Alias(mut a) => {
                a.this = Self::unqualify_columns(a.this);
                Expression::Alias(a)
            }
            // Pass through other expressions unchanged
            other => other,
        }
    }

    /// Check if an expression is already a CAST to a float type
    fn is_float_cast(expr: &Expression) -> bool {
        if let Expression::Cast(cast) = expr {
            matches!(&cast.to, DataType::Double { .. } | DataType::Float { .. })
        } else {
            false
        }
    }

    /// Convert Oracle/PostgreSQL-style date format to Presto's C-style format
    /// Oracle: dd, hh, hh24, mi, mm, ss, yyyy, yy
    /// Presto: %d, %H, %H, %i, %m, %s, %Y, %y
    pub fn oracle_to_presto_format(fmt: &str) -> String {
        // Process character by character to avoid double-replacement issues
        let chars: Vec<char> = fmt.chars().collect();
        let mut result = String::new();
        let mut i = 0;
        while i < chars.len() {
            let remaining = &fmt[i..];
            if remaining.starts_with("yyyy") {
                result.push_str("%Y");
                i += 4;
            } else if remaining.starts_with("yy") {
                result.push_str("%y");
                i += 2;
            } else if remaining.starts_with("hh24") {
                result.push_str("%H");
                i += 4;
            } else if remaining.starts_with("hh") {
                result.push_str("%H");
                i += 2;
            } else if remaining.starts_with("mi") {
                result.push_str("%i");
                i += 2;
            } else if remaining.starts_with("mm") {
                result.push_str("%m");
                i += 2;
            } else if remaining.starts_with("dd") {
                result.push_str("%d");
                i += 2;
            } else if remaining.starts_with("ss") {
                result.push_str("%s");
                i += 2;
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }

    /// Convert Presto's C-style date format to Java-style format (for Hive/Spark)
    /// Presto: %Y, %m, %d, %H, %i, %S, %s, %y, %T, %F
    /// Java:   yyyy, MM, dd, HH, mm, ss, ss, yy, HH:mm:ss, yyyy-MM-dd
    pub fn presto_to_java_format(fmt: &str) -> String {
        fmt.replace("%Y", "yyyy")
            .replace("%m", "MM")
            .replace("%d", "dd")
            .replace("%H", "HH")
            .replace("%i", "mm")
            .replace("%S", "ss")
            .replace("%s", "ss")
            .replace("%y", "yy")
            .replace("%T", "HH:mm:ss")
            .replace("%F", "yyyy-MM-dd")
            .replace("%M", "MMMM")
    }

    /// Normalize Presto format strings (e.g., %H:%i:%S -> %T, %Y-%m-%d -> %F)
    pub fn normalize_presto_format(fmt: &str) -> String {
        fmt.replace("%H:%i:%S", "%T").replace("%H:%i:%s", "%T")
    }

    /// Convert Presto's C-style format to DuckDB C-style (only difference: %i -> %M for minutes)
    pub fn presto_to_duckdb_format(fmt: &str) -> String {
        fmt.replace("%i", "%M")
            .replace("%s", "%S")
            .replace("%T", "%H:%M:%S")
    }

    /// Convert Presto's C-style format to BigQuery format
    pub fn presto_to_bigquery_format(fmt: &str) -> String {
        // BigQuery uses %F for %Y-%m-%d, %T for %H:%M:%S
        // BigQuery uses %M for minutes (like DuckDB), not %i
        let result = fmt
            .replace("%Y-%m-%d", "%F")
            .replace("%H:%i:%S", "%T")
            .replace("%H:%i:%s", "%T")
            .replace("%i", "%M")
            .replace("%s", "%S");
        result
    }

    /// Check if a Presto format string matches the default timestamp format
    pub fn is_default_timestamp_format(fmt: &str) -> bool {
        let normalized = Self::normalize_presto_format(fmt);
        normalized == "%Y-%m-%d %T"
            || normalized == "%Y-%m-%d %H:%i:%S"
            || fmt == "%Y-%m-%d %H:%i:%S"
            || fmt == "%Y-%m-%d %T"
    }

    /// Check if a Presto format string matches the default date format
    pub fn is_default_date_format(fmt: &str) -> bool {
        fmt == "%Y-%m-%d" || fmt == "%F"
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // RAND -> RANDOM in Presto (but it's actually RANDOM())
            "RAND" => Ok(Expression::Function(Box::new(Function::new(
                "RANDOM".to_string(),
                vec![],
            )))),

            // GROUP_CONCAT -> ARRAY_JOIN(ARRAY_AGG())
            "GROUP_CONCAT" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // STRING_AGG -> ARRAY_JOIN(ARRAY_AGG())
            "STRING_AGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // LISTAGG -> ARRAY_JOIN(ARRAY_AGG())
            "LISTAGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // SUBSTR is native in Presto (keep as-is, don't convert to SUBSTRING)
            "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CHARINDEX -> STRPOS in Presto (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // STRPOS(string, substring) - note: argument order is reversed
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // INSTR -> STRPOS (with same argument order)
            "INSTR" if f.args.len() >= 2 => {
                let args = f.args;
                // INSTR(string, substring) -> STRPOS(string, substring)
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    args,
                ))))
            }

            // LOCATE -> STRPOS in Presto (with swapped args)
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // LOCATE(substring, string) -> STRPOS(string, substring)
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // ARRAY_LENGTH -> CARDINALITY in Presto
            "ARRAY_LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("CARDINALITY".to_string(), f.args),
            ))),

            // SIZE -> CARDINALITY in Presto
            "SIZE" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CARDINALITY".to_string(),
                f.args,
            )))),

            // ARRAY_CONTAINS -> CONTAINS in Presto
            "ARRAY_CONTAINS" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("CONTAINS".to_string(), f.args),
            ))),

            // TO_DATE -> DATE_PARSE in Presto (or CAST to DATE)
            "TO_DATE" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    // Simple case: just cast to DATE
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Date,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    // With format: use DATE_PARSE
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // TO_TIMESTAMP -> DATE_PARSE / CAST
            "TO_TIMESTAMP" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Timestamp {
                            precision: None,
                            timezone: false,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // DATE_FORMAT -> DATE_FORMAT (native in Presto)
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT in Presto
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                // strftime(format, date) -> DATE_FORMAT(date, format)
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR -> DATE_FORMAT in Presto (convert Oracle-style format to Presto C-style)
            "TO_CHAR" if f.args.len() >= 2 => {
                let mut args = f.args;
                // Convert Oracle-style format string to Presto C-style
                if let Expression::Literal(Literal::String(ref s)) = args[1] {
                    let converted = Self::oracle_to_presto_format(s);
                    args[1] = Expression::Literal(Literal::String(converted));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    args,
                ))))
            }

            // LEVENSHTEIN -> LEVENSHTEIN_DISTANCE in Presto
            "LEVENSHTEIN" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LEVENSHTEIN_DISTANCE".to_string(), f.args),
            ))),

            // FLATTEN -> FLATTEN is supported in Presto for nested arrays
            "FLATTEN" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT -> JSON_EXTRACT (native in Presto)
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT_SCALAR -> JSON_EXTRACT_SCALAR (native in Presto)
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(f))),

            // GET_JSON_OBJECT -> JSON_EXTRACT_SCALAR in Presto
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_SCALAR".to_string(), f.args),
            ))),

            // COLLECT_LIST -> ARRAY_AGG
            "COLLECT_LIST" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("ARRAY_AGG".to_string(), f.args),
            ))),

            // COLLECT_SET -> ARRAY_DISTINCT(ARRAY_AGG())
            "COLLECT_SET" if !f.args.is_empty() => {
                let array_agg =
                    Expression::Function(Box::new(Function::new("ARRAY_AGG".to_string(), f.args)));
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_DISTINCT".to_string(),
                    vec![array_agg],
                ))))
            }

            // RLIKE -> REGEXP_LIKE in Presto
            "RLIKE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // REGEXP -> REGEXP_LIKE in Presto
            "REGEXP" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // PARSE_JSON -> JSON_PARSE in Presto
            "PARSE_JSON" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_PARSE".to_string(),
                f.args,
            )))),

            // GET_PATH(obj, path) -> JSON_EXTRACT(obj, json_path) in Presto
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
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this,
                    path: json_path,
                    returning: None,
                    arrow_syntax: false,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // REGEXP_SUBSTR(subject, pattern, ...) -> REGEXP_EXTRACT(subject, pattern[, group])
            "REGEXP_SUBSTR" if f.args.len() >= 2 => {
                let mut args = f.args;
                let subject = args.remove(0);
                let pattern = args.remove(0);
                // If 6-arg form: (subject, pattern, pos, occ, params, group) -> keep group
                if args.len() >= 4 {
                    let _pos = args.remove(0);
                    let _occ = args.remove(0);
                    let _params = args.remove(0);
                    let group = args.remove(0);
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_EXTRACT".to_string(),
                        vec![subject, pattern, group],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_EXTRACT".to_string(),
                        vec![subject, pattern],
                    ))))
                }
            }

            // DATE_PART(epoch_second, x) -> TO_UNIXTIME(CAST(x AS TIMESTAMP))
            // DATE_PART(epoch_millisecond[s], x) -> TO_UNIXTIME(CAST(x AS TIMESTAMP)) * 1000
            "DATE_PART" if f.args.len() == 2 => {
                let part_name = match &f.args[0] {
                    Expression::Identifier(id) => Some(id.name.to_uppercase()),
                    Expression::Column(c) => Some(c.name.name.to_uppercase()),
                    _ => None,
                };
                match part_name.as_deref() {
                    Some("EPOCH_SECOND" | "EPOCH_SECONDS") => {
                        let mut args = f.args;
                        let value = args.remove(1);
                        let cast_expr = Expression::Cast(Box::new(Cast {
                            this: value,
                            to: DataType::Timestamp {
                                precision: None,
                                timezone: false,
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }));
                        Ok(Expression::Function(Box::new(Function::new(
                            "TO_UNIXTIME".to_string(),
                            vec![cast_expr],
                        ))))
                    }
                    Some("EPOCH_MILLISECOND" | "EPOCH_MILLISECONDS") => {
                        let mut args = f.args;
                        let value = args.remove(1);
                        let cast_expr = Expression::Cast(Box::new(Cast {
                            this: value,
                            to: DataType::Timestamp {
                                precision: None,
                                timezone: false,
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }));
                        let unixtime = Expression::Function(Box::new(Function::new(
                            "TO_UNIXTIME".to_string(),
                            vec![cast_expr],
                        )));
                        Ok(Expression::Mul(Box::new(BinaryOp {
                            left: unixtime,
                            right: Expression::Literal(Literal::Number("1000".to_string())),
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        })))
                    }
                    _ => Ok(Expression::Function(Box::new(f))),
                }
            }

            // REPLACE(x, y) with 2 args -> REPLACE(x, y, '') - Presto requires explicit empty string
            "REPLACE" if f.args.len() == 2 => {
                let mut args = f.args;
                args.push(Expression::string(""));
                Ok(Expression::Function(Box::new(Function::new(
                    "REPLACE".to_string(),
                    args,
                ))))
            }

            // REGEXP_REPLACE(x, y) with 2 args -> REGEXP_REPLACE(x, y, '')
            "REGEXP_REPLACE" if f.args.len() == 2 => {
                let mut args = f.args;
                args.push(Expression::string(""));
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    args,
                ))))
            }

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
            // COUNT_IF -> SUM(CASE WHEN...)
            "COUNT_IF" if !f.args.is_empty() => {
                let condition = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // ANY_VALUE -> ARBITRARY in Presto
            "ANY_VALUE" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "ARBITRARY".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT -> ARRAY_JOIN(ARRAY_AGG())
            "GROUP_CONCAT" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // STRING_AGG -> ARRAY_JOIN(ARRAY_AGG())
            "STRING_AGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // LISTAGG -> ARRAY_JOIN(ARRAY_AGG())
            "LISTAGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    vec![first],
                )));
                let mut join_args = vec![array_agg];
                if let Some(sep) = separator {
                    join_args.push(sep);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_JOIN".to_string(),
                    join_args,
                ))))
            }

            // VAR -> VAR_POP in Presto
            "VAR" if !f.args.is_empty() => {
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: "VAR_POP".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                })))
            }

            // VARIANCE -> VAR_SAMP in Presto (for sample variance)
            "VARIANCE" if !f.args.is_empty() => {
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: "VAR_SAMP".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                })))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Presto type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
