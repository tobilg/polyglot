//! MySQL Dialect
//!
//! MySQL-specific transformations based on sqlglot patterns.
//! Key differences from standard SQL:
//! - || is OR operator, not string concatenation (use CONCAT)
//! - Uses backticks for identifiers
//! - No TRY_CAST, no ILIKE
//! - Different date/time function names

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{BinaryFunc, BinaryOp, Cast, DataType, Expression, Function, JsonExtractFunc, LikeOp, Literal, Paren, UnaryFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Helper to wrap JSON arrow expressions in parentheses when they appear
/// in contexts that require it (Binary, In, Not expressions)
/// This matches Python sqlglot's WRAPPED_JSON_EXTRACT_EXPRESSIONS behavior
fn wrap_if_json_arrow(expr: Expression) -> Expression {
    match &expr {
        Expression::JsonExtract(f) if f.arrow_syntax => {
            Expression::Paren(Box::new(Paren {
                this: expr,
                trailing_comments: Vec::new(),
            }))
        }
        Expression::JsonExtractScalar(f) if f.arrow_syntax => {
            Expression::Paren(Box::new(Paren {
                this: expr,
                trailing_comments: Vec::new(),
            }))
        }
        _ => expr,
    }
}

/// Convert JSON arrow expression (-> or ->>) to JSON_EXTRACT function form
/// This is needed for contexts like MEMBER OF where arrow syntax must become function form
fn json_arrow_to_function(expr: Expression) -> Expression {
    match expr {
        Expression::JsonExtract(f) if f.arrow_syntax => {
            Expression::Function(Box::new(Function::new(
                "JSON_EXTRACT".to_string(),
                vec![f.this, f.path],
            )))
        }
        Expression::JsonExtractScalar(f) if f.arrow_syntax => {
            // ->> becomes JSON_UNQUOTE(JSON_EXTRACT(...)) but can be simplified to JSON_EXTRACT_SCALAR
            // For MySQL, use JSON_UNQUOTE(JSON_EXTRACT(...))
            let json_extract = Expression::Function(Box::new(Function::new(
                "JSON_EXTRACT".to_string(),
                vec![f.this, f.path],
            )));
            Expression::Function(Box::new(Function::new(
                "JSON_UNQUOTE".to_string(),
                vec![json_extract],
            )))
        }
        other => other,
    }
}

/// MySQL dialect
pub struct MySQLDialect;

impl DialectImpl for MySQLDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::MySQL
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        use crate::tokens::TokenType;
        let mut config = TokenizerConfig::default();
        // MySQL uses backticks for identifiers
        config.identifiers.insert('`', '`');
        // Remove double quotes from identifiers - in MySQL they are string delimiters
        // (unless ANSI_QUOTES mode is set, but default mode uses them as strings)
        config.identifiers.remove(&'"');
        // MySQL supports double quotes as string literals by default
        config.quotes.insert("\"".to_string(), "\"".to_string());
        // MySQL supports backslash escapes in strings
        config.string_escapes.push('\\');
        // MySQL has XOR as a logical operator keyword
        config.keywords.insert("XOR".to_string(), TokenType::Xor);
        // MySQL: backslash followed by chars NOT in this list -> discard backslash
        // See: https://dev.mysql.com/doc/refman/8.4/en/string-literals.html
        config.escape_follow_chars = vec!['0', 'b', 'n', 'r', 't', 'Z', '%', '_'];
        // MySQL allows identifiers to start with digits (e.g., 1a, 1_a)
        config.identifiers_can_start_with_digit = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::MySQL),
            // MySQL doesn't support null ordering in most contexts
            null_ordering_supported: false,
            // MySQL LIMIT only
            limit_only_literals: true,
            // MySQL doesn't support semi/anti join
            semi_anti_join_with_side: false,
            // MySQL doesn't support table alias columns in some contexts
            supports_table_alias_columns: false,
            // MySQL VALUES not used as table
            values_as_table: false,
            // MySQL doesn't support TABLESAMPLE
            tablesample_requires_parens: false,
            tablesample_with_method: false,
            // MySQL doesn't support aggregate FILTER
            aggregate_filter_supported: false,
            // MySQL doesn't support TRY
            try_supported: false,
            // MySQL doesn't support CONVERT_TIMEZONE
            supports_convert_timezone: false,
            // MySQL doesn't support UESCAPE
            supports_uescape: false,
            // MySQL doesn't support BETWEEN flags
            supports_between_flags: false,
            // MySQL supports EXPLAIN but not query hints in standard way
            query_hints: false,
            // MySQL parameter token
            parameter_token: "?",
            // MySQL doesn't support window EXCLUDE
            supports_window_exclude: false,
            // MySQL doesn't support exploding projections
            supports_exploding_projections: false,
            identifiers_can_start_with_digit: true,
            // MySQL supports FOR UPDATE/SHARE
            locking_reads_supported: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // NVL -> IFNULL in MySQL
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // Note: COALESCE is valid in MySQL and should be preserved.
            // Unlike some other dialects, we do NOT convert COALESCE to IFNULL
            // as this would break identity tests.

            // TryCast -> CAST or TIMESTAMP() (MySQL doesn't support TRY_CAST)
            Expression::TryCast(c) => self.transform_cast(*c),

            // SafeCast -> CAST or TIMESTAMP() (MySQL doesn't support safe casts)
            Expression::SafeCast(c) => self.transform_cast(*c),

            // Cast -> Transform cast type according to MySQL restrictions
            // CAST AS TIMESTAMP -> TIMESTAMP() function in MySQL
            Expression::Cast(c) => self.transform_cast(*c),

            // ILIKE -> LOWER() LIKE LOWER() in MySQL
            Expression::ILike(op) => {
                // Transform ILIKE to: LOWER(left) LIKE LOWER(right)
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left)));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right)));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier,
                })))
            }

            // || (Concat operator) -> OR in MySQL
            // MySQL uses || as OR by default (unless PIPES_AS_CONCAT mode is set)
            // For identity preservation, we transform to OR
            Expression::Concat(op) => {
                Ok(Expression::Or(op))
            }

            // RANDOM -> RAND in MySQL
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None, lower: None, upper: None,
            }))),

            // ArrayAgg -> GROUP_CONCAT in MySQL
            Expression::ArrayAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                vec![f.this],
            )))),

            // StringAgg -> GROUP_CONCAT in MySQL
            Expression::StringAgg(f) => {
                let mut args = vec![f.this.clone()];
                if let Some(separator) = &f.separator {
                    args.push(separator.clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "GROUP_CONCAT".to_string(),
                    args,
                ))))
            }

            // UNNEST -> Not directly supported in MySQL, use JSON_TABLE or inline
            // For basic cases, pass through (may need manual handling)
            Expression::Unnest(f) => {
                // MySQL 8.0+ has JSON_TABLE which can be used for unnesting
                // For now, pass through with a function call
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_TABLE".to_string(),
                    vec![f.this],
                ))))
            }

            // Substring: Use comma syntax (not FROM/FOR) in MySQL
            Expression::Substring(mut f) => {
                f.from_for_syntax = false;
                Ok(Expression::Substring(f))
            }

            // ===== Bitwise operations =====
            // BitwiseAndAgg -> BIT_AND
            Expression::BitwiseAndAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_AND".to_string(),
                vec![f.this],
            )))),

            // BitwiseOrAgg -> BIT_OR
            Expression::BitwiseOrAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_OR".to_string(),
                vec![f.this],
            )))),

            // BitwiseXorAgg -> BIT_XOR
            Expression::BitwiseXorAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_XOR".to_string(),
                vec![f.this],
            )))),

            // BitwiseCount -> BIT_COUNT
            Expression::BitwiseCount(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_COUNT".to_string(),
                vec![f.this],
            )))),

            // TimeFromParts -> MAKETIME
            Expression::TimeFromParts(f) => {
                let mut args = Vec::new();
                if let Some(h) = f.hour {
                    args.push(*h);
                }
                if let Some(m) = f.min {
                    args.push(*m);
                }
                if let Some(s) = f.sec {
                    args.push(*s);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKETIME".to_string(),
                    args,
                ))))
            }

            // ===== Boolean aggregates =====
            // In MySQL, there's no BOOL_AND/BOOL_OR, use MIN/MAX on boolean values
            // LogicalAnd -> MIN (0 is false, non-0 is true)
            Expression::LogicalAnd(f) => Ok(Expression::Function(Box::new(Function::new(
                "MIN".to_string(),
                vec![f.this],
            )))),

            // LogicalOr -> MAX
            Expression::LogicalOr(f) => Ok(Expression::Function(Box::new(Function::new(
                "MAX".to_string(),
                vec![f.this],
            )))),

            // ===== Date/time functions =====
            // DayOfMonth -> DAYOFMONTH
            Expression::DayOfMonth(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFMONTH".to_string(),
                vec![f.this],
            )))),

            // DayOfWeek -> DAYOFWEEK
            Expression::DayOfWeek(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEK".to_string(),
                vec![f.this],
            )))),

            // DayOfYear -> DAYOFYEAR
            Expression::DayOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFYEAR".to_string(),
                vec![f.this],
            )))),

            // WeekOfYear -> WEEKOFYEAR
            Expression::WeekOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "WEEKOFYEAR".to_string(),
                vec![f.this],
            )))),

            // DateDiff -> DATEDIFF
            Expression::DateDiff(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                vec![f.this, f.expression],
            )))),

            // TimeStrToUnix -> UNIX_TIMESTAMP
            Expression::TimeStrToUnix(f) => Ok(Expression::Function(Box::new(Function::new(
                "UNIX_TIMESTAMP".to_string(),
                vec![f.this],
            )))),

            // TimestampDiff -> TIMESTAMPDIFF
            Expression::TimestampDiff(f) => Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMPDIFF".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== String functions =====
            // StrPosition -> LOCATE in MySQL
            // STRPOS(str, substr) -> LOCATE(substr, str) (args are swapped)
            Expression::StrPosition(f) => {
                let mut args = vec![];
                if let Some(substr) = f.substr {
                    args.push(*substr);
                }
                args.push(*f.this);
                if let Some(pos) = f.position {
                    args.push(*pos);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    args,
                ))))
            }

            // Stuff -> INSERT in MySQL
            Expression::Stuff(f) => {
                let mut args = vec![*f.this];
                if let Some(start) = f.start {
                    args.push(*start);
                }
                if let Some(length) = f.length {
                    args.push(Expression::number(length));
                }
                args.push(*f.expression);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSERT".to_string(),
                    args,
                ))))
            }

            // ===== Session/User functions =====
            // SessionUser -> SESSION_USER()
            Expression::SessionUser(_) => Ok(Expression::Function(Box::new(Function::new(
                "SESSION_USER".to_string(),
                vec![],
            )))),

            // CurrentDate -> CURRENT_DATE (no parentheses in MySQL) - keep as CurrentDate
            Expression::CurrentDate(_) => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // ===== Null-safe comparison =====
            // NullSafeNeq -> NOT (a <=> b) in MySQL
            Expression::NullSafeNeq(op) => {
                // Create: NOT (left <=> right)
                let null_safe_eq = Expression::NullSafeEq(Box::new(crate::expressions::BinaryOp {
                    left: op.left,
                    right: op.right,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                    this: null_safe_eq,
                })))
            }

            // ParseJson: handled by generator (emits just the string literal for MySQL)

            // JSONExtract with variant_extract (Snowflake colon syntax) -> JSON_EXTRACT
            Expression::JSONExtract(e) if e.variant_extract.is_some() => {
                let path = match *e.expression {
                    Expression::Literal(Literal::String(s)) => {
                        // Convert bracket notation ["key"] to quoted dot notation ."key"
                        let s = Self::convert_bracket_to_quoted_path(&s);
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
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_EXTRACT".to_string(),
                    vec![*e.this, path],
                ))))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // ===== Context-aware JSON arrow wrapping =====
            // When JSON arrow expressions appear in Binary/In/Not contexts,
            // they need to be wrapped in parentheses for correct precedence.
            // This matches Python sqlglot's WRAPPED_JSON_EXTRACT_EXPRESSIONS behavior.

            // Binary operators that need JSON wrapping
            Expression::Eq(op) => Ok(Expression::Eq(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Neq(op) => Ok(Expression::Neq(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Lt(op) => Ok(Expression::Lt(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Lte(op) => Ok(Expression::Lte(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Gt(op) => Ok(Expression::Gt(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Gte(op) => Ok(Expression::Gte(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),

            // In expression - wrap the this part if it's JSON arrow
            Expression::In(mut i) => {
                i.this = wrap_if_json_arrow(i.this);
                Ok(Expression::In(i))
            }

            // Not expression - wrap the this part if it's JSON arrow
            Expression::Not(mut n) => {
                n.this = wrap_if_json_arrow(n.this);
                Ok(Expression::Not(n))
            }

            // && in MySQL is logical AND, not array overlaps
            // Transform ArrayOverlaps -> And for MySQL identity
            Expression::ArrayOverlaps(op) => Ok(Expression::And(op)),

            // MOD(x, y) -> x % y in MySQL
            Expression::ModFunc(f) => Ok(Expression::Mod(Box::new(BinaryOp {
                left: f.this,
                right: f.expression,
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))),

            // SHOW SLAVE STATUS -> SHOW REPLICA STATUS
            Expression::Show(mut s) => {
                if s.this == "SLAVE STATUS" {
                    s.this = "REPLICA STATUS".to_string();
                }
                if matches!(s.this.as_str(), "INDEX" | "COLUMNS") && s.db.is_none() {
                    if let Some(Expression::Table(mut t)) = s.target.take() {
                        if let Some(db_ident) = t.schema.take().or(t.catalog.take()) {
                            s.db = Some(Expression::Identifier(db_ident));
                            s.target = Some(Expression::Identifier(t.name));
                        } else {
                            s.target = Some(Expression::Table(t));
                        }
                    }
                }
                Ok(Expression::Show(s))
            }

            // AT TIME ZONE -> strip timezone (MySQL doesn't support AT TIME ZONE)
            // But keep it for CURRENT_DATE/CURRENT_TIMESTAMP with timezone (transpiled from BigQuery)
            Expression::AtTimeZone(atz) => {
                let is_current = match &atz.this {
                    Expression::CurrentDate(_) | Expression::CurrentTimestamp(_) => true,
                    Expression::Function(f) => {
                        let n = f.name.to_uppercase();
                        (n == "CURRENT_DATE" || n == "CURRENT_TIMESTAMP") && f.no_parens
                    }
                    _ => false,
                };
                if is_current {
                    Ok(Expression::AtTimeZone(atz)) // Keep AT TIME ZONE for CURRENT_DATE/CURRENT_TIMESTAMP
                } else {
                    Ok(atz.this) // Strip timezone for other expressions
                }
            }

            // MEMBER OF with JSON arrow -> convert arrow to JSON_EXTRACT function
            // MySQL's MEMBER OF requires JSON_EXTRACT function form, not arrow syntax
            Expression::MemberOf(mut op) => {
                op.right = json_arrow_to_function(op.right);
                Ok(Expression::MemberOf(op))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl MySQLDialect {
    fn normalize_mysql_date_format(fmt: &str) -> String {
        fmt
            .replace("%H:%i:%s", "%T")
            .replace("%H:%i:%S", "%T")
    }

    /// Convert bracket notation ["key with spaces"] to quoted dot notation ."key with spaces"
    /// in JSON path strings.
    fn convert_bracket_to_quoted_path(path: &str) -> String {
        let mut result = String::new();
        let mut chars = path.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '[' && chars.peek() == Some(&'"') {
                chars.next(); // consume "
                let mut key = String::new();
                while let Some(kc) = chars.next() {
                    if kc == '"' && chars.peek() == Some(&']') {
                        chars.next(); // consume ]
                        break;
                    }
                    key.push(kc);
                }
                if !result.is_empty() && !result.ends_with('.') {
                    result.push('.');
                }
                result.push('"');
                result.push_str(&key);
                result.push('"');
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Transform data types according to MySQL TYPE_MAPPING
    /// Note: MySQL's TIMESTAMP is kept as TIMESTAMP (not converted to DATETIME)
    /// because MySQL's TIMESTAMP has timezone awareness built-in
    fn transform_data_type(&self, dt: crate::expressions::DataType) -> Result<Expression> {
        use crate::expressions::DataType;
        let transformed = match dt {
            // All TIMESTAMP variants (with or without timezone) -> TIMESTAMP in MySQL
            DataType::Timestamp { precision, timezone: _ } => DataType::Timestamp {
                precision,
                timezone: false,
            },
            // TIMESTAMPTZ / TIMESTAMPLTZ parsed as Custom -> normalize to TIMESTAMP
            DataType::Custom { name } if name.to_uppercase() == "TIMESTAMPTZ"
                || name.to_uppercase() == "TIMESTAMPLTZ" => {
                DataType::Timestamp {
                    precision: None,
                    timezone: false,
                }
            },
            // Keep native MySQL types as-is
            // MySQL supports TEXT, MEDIUMTEXT, LONGTEXT, BLOB, etc. natively
            other => other,
        };
        Ok(Expression::DataType(transformed))
    }

    /// Transform CAST expression
    /// MySQL uses TIMESTAMP() function instead of CAST(x AS TIMESTAMP)
    /// For Generic->MySQL, TIMESTAMP (no tz) is pre-converted to DATETIME in cross_dialect_normalize
    fn transform_cast(&self, cast: Cast) -> Result<Expression> {
        // CAST AS TIMESTAMP/TIMESTAMPTZ/TIMESTAMPLTZ -> TIMESTAMP() function
        match &cast.to {
            DataType::Timestamp { .. } => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    vec![cast.this],
                ))))
            }
            DataType::Custom { name } if name.to_uppercase() == "TIMESTAMPTZ"
                || name.to_uppercase() == "TIMESTAMPLTZ" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    vec![cast.this],
                ))))
            }
            // All other casts go through normal type transformation
            _ => Ok(Expression::Cast(Box::new(self.transform_cast_type(cast)))),
        }
    }

    /// Transform CAST type according to MySQL restrictions
    /// MySQL doesn't support many types in CAST - they get mapped to CHAR or SIGNED
    /// Based on Python sqlglot's CHAR_CAST_MAPPING and SIGNED_CAST_MAPPING
    fn transform_cast_type(&self, cast: Cast) -> Cast {
        let new_type = match &cast.to {
            // CHAR_CAST_MAPPING: These types become CHAR in MySQL CAST, preserving length
            DataType::VarChar { length, .. } => DataType::Char { length: *length },
            DataType::Text => DataType::Char { length: None },

            // SIGNED_CAST_MAPPING: These integer types become SIGNED in MySQL CAST
            DataType::BigInt { .. } => DataType::Custom { name: "SIGNED".to_string() },
            DataType::Int { .. } => DataType::Custom { name: "SIGNED".to_string() },
            DataType::SmallInt { .. } => DataType::Custom { name: "SIGNED".to_string() },
            DataType::TinyInt { .. } => DataType::Custom { name: "SIGNED".to_string() },
            DataType::Boolean => DataType::Custom { name: "SIGNED".to_string() },

            // Custom types that need mapping
            DataType::Custom { name } => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    // Text/Blob types -> keep as Custom for cross-dialect mapping
                    // MySQL generator will output CHAR for these in CAST context
                    "LONGTEXT" | "MEDIUMTEXT" | "TINYTEXT" | "LONGBLOB" | "MEDIUMBLOB" | "TINYBLOB" => {
                        DataType::Custom { name: upper }
                    }
                    // MEDIUMINT -> SIGNED in MySQL CAST
                    "MEDIUMINT" => DataType::Custom { name: "SIGNED".to_string() },
                    // Unsigned integer types -> UNSIGNED
                    "UBIGINT" | "UINT" | "USMALLINT" | "UTINYINT" | "UMEDIUMINT" => {
                        DataType::Custom { name: "UNSIGNED".to_string() }
                    }
                    // Keep other custom types
                    _ => cast.to.clone(),
                }
            }

            // Types that are valid in MySQL CAST - pass through
            DataType::Binary { .. } => cast.to.clone(),
            DataType::VarBinary { .. } => cast.to.clone(),
            DataType::Date => cast.to.clone(),
            DataType::Time { .. } => cast.to.clone(),
            DataType::Decimal { .. } => cast.to.clone(),
            DataType::Json => cast.to.clone(),
            DataType::Float { .. } => cast.to.clone(),
            DataType::Double { .. } => cast.to.clone(),
            DataType::Char { .. } => cast.to.clone(),
            DataType::CharacterSet { .. } => cast.to.clone(),
            DataType::Enum { .. } => cast.to.clone(),
            DataType::Set { .. } => cast.to.clone(),
            DataType::Timestamp { .. } => cast.to.clone(),

            // All other unsupported types -> CHAR
            _ => DataType::Char { length: None },
        };

        Cast {
            this: cast.this,
            to: new_type,
            trailing_comments: cast.trailing_comments,
            double_colon_syntax: cast.double_colon_syntax,
            format: cast.format,
            default: cast.default,
        }
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // Normalize DATE_FORMAT short-hands to canonical MySQL forms.
            "DATE_FORMAT" if f.args.len() >= 2 => {
                let mut f = f;
                if let Some(Expression::Literal(Literal::String(fmt))) = f.args.get(1) {
                    let normalized = Self::normalize_mysql_date_format(fmt);
                    if normalized != *fmt {
                        f.args[1] = Expression::Literal(Literal::String(normalized));
                    }
                }
                Ok(Expression::Function(Box::new(f)))
            }

            // NVL -> IFNULL
            "NVL" if f.args.len() == 2 => {
                let mut args = f.args;
                let second = args.pop().unwrap();
                let first = args.pop().unwrap();
                Ok(Expression::IfNull(Box::new(BinaryFunc { original_name: None,
                    this: first,
                    expression: second,
                })))
            }

            // Note: COALESCE is native to MySQL. We do NOT convert it to IFNULL
            // because this would break identity tests (Python SQLGlot preserves COALESCE).

            // ARRAY_AGG -> GROUP_CONCAT
            "ARRAY_AGG" if f.args.len() == 1 => {
                let mut args = f.args;
                Ok(Expression::Function(Box::new(Function::new(
                    "GROUP_CONCAT".to_string(),
                    vec![args.pop().unwrap()],
                ))))
            }

            // STRING_AGG -> GROUP_CONCAT
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None, lower: None, upper: None,
            }))),

            // CURRENT_TIMESTAMP -> NOW() or CURRENT_TIMESTAMP (both work)
            // Preserve precision if specified: CURRENT_TIMESTAMP(6)
            "CURRENT_TIMESTAMP" => {
                let precision = if let Some(Expression::Literal(crate::expressions::Literal::Number(n))) = f.args.first() {
                    n.parse::<u32>().ok()
                } else {
                    None
                };
                Ok(Expression::CurrentTimestamp(
                    crate::expressions::CurrentTimestamp { precision, sysdate: false },
                ))
            }

            // POSITION -> LOCATE in MySQL (argument order is different)
            // POSITION(substr IN str) -> LOCATE(substr, str)
            "POSITION" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "LOCATE".to_string(),
                f.args,
            )))),

            // LENGTH is native to MySQL (returns bytes, not characters)
            // CHAR_LENGTH for character count
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // CEIL -> CEILING in MySQL (both work)
            "CEIL" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CEILING".to_string(),
                f.args,
            )))),

            // STDDEV -> STD or STDDEV_POP in MySQL
            "STDDEV" => Ok(Expression::Function(Box::new(Function::new(
                "STD".to_string(),
                f.args,
            )))),

            // STDDEV_SAMP -> STDDEV in MySQL
            "STDDEV_SAMP" => Ok(Expression::Function(Box::new(Function::new(
                "STDDEV".to_string(),
                f.args,
            )))),

            // TO_DATE -> STR_TO_DATE in MySQL
            "TO_DATE" => Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                f.args,
            )))),

            // TO_TIMESTAMP -> STR_TO_DATE in MySQL
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                f.args,
            )))),

            // DATE_TRUNC -> Complex transformation
            // Typically uses DATE() or DATE_FORMAT() depending on unit
            "DATE_TRUNC" if f.args.len() >= 2 => {
                // Simplified: DATE_TRUNC('day', x) -> DATE(x)
                // Full implementation would handle different units
                let mut args = f.args;
                let _unit = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE".to_string(),
                    vec![date],
                ))))
            }

            // EXTRACT is native but syntax varies

            // COALESCE is native to MySQL (keep as-is for more than 2 args)
            "COALESCE" if f.args.len() > 2 => Ok(Expression::Function(Box::new(f))),

            // DAYOFMONTH -> DAY (both work)
            "DAY" => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFMONTH".to_string(),
                f.args,
            )))),

            // DAYOFWEEK is native to MySQL
            "DAYOFWEEK" => Ok(Expression::Function(Box::new(f))),

            // DAYOFYEAR is native to MySQL
            "DAYOFYEAR" => Ok(Expression::Function(Box::new(f))),

            // WEEKOFYEAR is native to MySQL
            "WEEKOFYEAR" => Ok(Expression::Function(Box::new(f))),

            // LAST_DAY is native to MySQL
            "LAST_DAY" => Ok(Expression::Function(Box::new(f))),

            // TIMESTAMPADD -> DATE_ADD
            "TIMESTAMPADD" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_ADD".to_string(),
                f.args,
            )))),

            // TIMESTAMPDIFF is native to MySQL
            "TIMESTAMPDIFF" => Ok(Expression::Function(Box::new(f))),

            // CONVERT_TIMEZONE(from_tz, to_tz, timestamp) -> CONVERT_TZ(timestamp, from_tz, to_tz) in MySQL
            "CONVERT_TIMEZONE" if f.args.len() == 3 => {
                let mut args = f.args;
                let from_tz = args.remove(0);
                let to_tz = args.remove(0);
                let timestamp = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "CONVERT_TZ".to_string(),
                    vec![timestamp, from_tz, to_tz],
                ))))
            }

            // UTC_TIMESTAMP is native to MySQL
            "UTC_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // UTC_TIME is native to MySQL
            "UTC_TIME" => Ok(Expression::Function(Box::new(f))),

            // MAKETIME is native to MySQL (TimeFromParts)
            "MAKETIME" => Ok(Expression::Function(Box::new(f))),

            // TIME_FROM_PARTS -> MAKETIME
            "TIME_FROM_PARTS" if f.args.len() == 3 => Ok(Expression::Function(Box::new(Function::new(
                "MAKETIME".to_string(),
                f.args,
            )))),

            // STUFF -> INSERT in MySQL
            "STUFF" if f.args.len() == 4 => Ok(Expression::Function(Box::new(Function::new(
                "INSERT".to_string(),
                f.args,
            )))),

            // LOCATE is native to MySQL (reverse of POSITION args)
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // FIND_IN_SET is native to MySQL
            "FIND_IN_SET" => Ok(Expression::Function(Box::new(f))),

            // FORMAT is native to MySQL (NumberToStr)
            "FORMAT" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT is native to MySQL
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // JSON_UNQUOTE is native to MySQL
            "JSON_UNQUOTE" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT_PATH_TEXT -> JSON_UNQUOTE(JSON_EXTRACT(...))
            "JSON_EXTRACT_PATH_TEXT" if f.args.len() >= 2 => {
                let extract = Expression::Function(Box::new(Function::new(
                    "JSON_EXTRACT".to_string(),
                    f.args,
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_UNQUOTE".to_string(),
                    vec![extract],
                ))))
            }

            // GEN_RANDOM_UUID / UUID -> UUID()
            "GEN_RANDOM_UUID" | "GENERATE_UUID" => Ok(Expression::Function(Box::new(Function::new(
                "UUID".to_string(),
                vec![],
            )))),

            // DATABASE() -> SCHEMA() in MySQL (both return current database name)
            "DATABASE" => Ok(Expression::Function(Box::new(Function::new(
                "SCHEMA".to_string(),
                f.args,
            )))),

            // INSTR -> LOCATE in MySQL (with swapped arguments)
            // INSTR(str, substr) -> LOCATE(substr, str)
            "INSTR" if f.args.len() == 2 => {
                let mut args = f.args;
                let str_arg = args.remove(0);
                let substr_arg = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    vec![substr_arg, str_arg],
                ))))
            }

            // TIME_STR_TO_UNIX -> UNIX_TIMESTAMP in MySQL
            "TIME_STR_TO_UNIX" => Ok(Expression::Function(Box::new(Function::new(
                "UNIX_TIMESTAMP".to_string(),
                f.args,
            )))),

            // TIME_STR_TO_TIME -> CAST AS DATETIME(N) or TIMESTAMP() in MySQL
            "TIME_STR_TO_TIME" if f.args.len() >= 1 => {
                let mut args = f.args.into_iter();
                let arg = args.next().unwrap();

                // If there's a timezone arg, use TIMESTAMP() function instead
                if args.next().is_some() {
                    return Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP".to_string(),
                        vec![arg],
                    ))));
                }

                // Extract sub-second precision from the string literal
                let precision = if let Expression::Literal(crate::expressions::Literal::String(ref s)) = arg {
                    // Find fractional seconds: look for .NNN pattern after HH:MM:SS
                    if let Some(dot_pos) = s.rfind('.') {
                        let after_dot = &s[dot_pos + 1..];
                        // Count digits until non-digit
                        let frac_digits = after_dot.chars().take_while(|c| c.is_ascii_digit()).count();
                        if frac_digits > 0 {
                            // Round up: 1-3 digits → 3, 4-6 digits → 6
                            if frac_digits <= 3 { Some(3) } else { Some(6) }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let type_name = match precision {
                    Some(p) => format!("DATETIME({})", p),
                    None => "DATETIME".to_string(),
                };

                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Custom { name: type_name },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // UCASE -> UPPER in MySQL
            "UCASE" => Ok(Expression::Function(Box::new(Function::new(
                "UPPER".to_string(),
                f.args,
            )))),

            // LCASE -> LOWER in MySQL
            "LCASE" => Ok(Expression::Function(Box::new(Function::new(
                "LOWER".to_string(),
                f.args,
            )))),

            // DAY_OF_MONTH -> DAYOFMONTH in MySQL
            "DAY_OF_MONTH" => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFMONTH".to_string(),
                f.args,
            )))),

            // DAY_OF_WEEK -> DAYOFWEEK in MySQL
            "DAY_OF_WEEK" => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEK".to_string(),
                f.args,
            )))),

            // DAY_OF_YEAR -> DAYOFYEAR in MySQL
            "DAY_OF_YEAR" => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFYEAR".to_string(),
                f.args,
            )))),

            // WEEK_OF_YEAR -> WEEKOFYEAR in MySQL
            "WEEK_OF_YEAR" => Ok(Expression::Function(Box::new(Function::new(
                "WEEKOFYEAR".to_string(),
                f.args,
            )))),

            // MOD(x, y) -> x % y in MySQL
            "MOD" if f.args.len() == 2 => {
                let mut args = f.args;
                let left = args.remove(0);
                let right = args.remove(0);
                Ok(Expression::Mod(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // PARSE_JSON -> strip in MySQL (just keep the string argument)
            "PARSE_JSON" if f.args.len() == 1 => Ok(f.args.into_iter().next().unwrap()),

            // GET_PATH(obj, path) -> JSON_EXTRACT(obj, json_path) in MySQL
            "GET_PATH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                let json_path = match &path {
                    Expression::Literal(Literal::String(s)) => {
                        // Convert bracket notation ["key"] to quoted dot notation ."key"
                        let s = Self::convert_bracket_to_quoted_path(s);
                        let normalized = if s.starts_with('$') {
                            s
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

            // REGEXP -> REGEXP_LIKE (MySQL standard form)
            "REGEXP" if f.args.len() >= 2 => {
                Ok(Expression::Function(Box::new(Function::new("REGEXP_LIKE".to_string(), f.args))))
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
            // STRING_AGG -> GROUP_CONCAT
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> GROUP_CONCAT
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_mysql(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile_to(sql, DialectType::MySQL)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_nvl_to_ifnull() {
        let result = transpile_to_mysql("SELECT NVL(a, b)");
        assert!(
            result.contains("IFNULL"),
            "Expected IFNULL, got: {}",
            result
        );
    }

    #[test]
    fn test_coalesce_preserved() {
        // COALESCE should be preserved in MySQL (it's a native function)
        let result = transpile_to_mysql("SELECT COALESCE(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE to be preserved, got: {}",
            result
        );
    }

    #[test]
    fn test_random_to_rand() {
        let result = transpile_to_mysql("SELECT RANDOM()");
        assert!(result.contains("RAND"), "Expected RAND, got: {}", result);
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_mysql("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_string_agg_to_group_concat() {
        let result = transpile_to_mysql("SELECT STRING_AGG(name)");
        assert!(
            result.contains("GROUP_CONCAT"),
            "Expected GROUP_CONCAT, got: {}",
            result
        );
    }

    #[test]
    fn test_array_agg_to_group_concat() {
        let result = transpile_to_mysql("SELECT ARRAY_AGG(name)");
        assert!(
            result.contains("GROUP_CONCAT"),
            "Expected GROUP_CONCAT, got: {}",
            result
        );
    }

    #[test]
    fn test_to_date_to_str_to_date() {
        let result = transpile_to_mysql("SELECT TO_DATE('2023-01-01')");
        assert!(
            result.contains("STR_TO_DATE"),
            "Expected STR_TO_DATE, got: {}",
            result
        );
    }

    #[test]
    fn test_backtick_identifiers() {
        // MySQL uses backticks for identifiers
        let dialect = MySQLDialect;
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '`');
    }

    fn mysql_identity(sql: &str, expected: &str) {
        let dialect = Dialect::get(DialectType::MySQL);
        let ast = dialect.parse(sql).expect("Parse failed");
        let transformed = dialect.transform(ast[0].clone()).expect("Transform failed");
        let result = dialect.generate(&transformed).expect("Generate failed");
        assert_eq!(result, expected, "SQL: {}", sql);
    }

    #[test]
    fn test_ucase_to_upper() {
        mysql_identity("SELECT UCASE('foo')", "SELECT UPPER('foo')");
    }

    #[test]
    fn test_lcase_to_lower() {
        mysql_identity("SELECT LCASE('foo')", "SELECT LOWER('foo')");
    }

    #[test]
    fn test_day_of_month() {
        mysql_identity("SELECT DAY_OF_MONTH('2023-01-01')", "SELECT DAYOFMONTH('2023-01-01')");
    }

    #[test]
    fn test_day_of_week() {
        mysql_identity("SELECT DAY_OF_WEEK('2023-01-01')", "SELECT DAYOFWEEK('2023-01-01')");
    }

    #[test]
    fn test_day_of_year() {
        mysql_identity("SELECT DAY_OF_YEAR('2023-01-01')", "SELECT DAYOFYEAR('2023-01-01')");
    }

    #[test]
    fn test_week_of_year() {
        mysql_identity("SELECT WEEK_OF_YEAR('2023-01-01')", "SELECT WEEKOFYEAR('2023-01-01')");
    }

    #[test]
    fn test_mod_func_to_percent() {
        // MOD(x, y) function is transformed to x % y in MySQL
        mysql_identity("MOD(x, y)", "x % y");
    }

    #[test]
    fn test_database_to_schema() {
        mysql_identity("DATABASE()", "SCHEMA()");
    }

    #[test]
    fn test_and_operator() {
        mysql_identity("SELECT 1 && 0", "SELECT 1 AND 0");
    }

    #[test]
    fn test_or_operator() {
        mysql_identity("SELECT a || b", "SELECT a OR b");
    }

}
