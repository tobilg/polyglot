//! SQLite Dialect
//!
//! SQLite-specific transformations based on sqlglot patterns.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, BinaryFunc, BinaryOp, Case, Cast, CeilFunc, DataType, DateTimeField, DateTruncFunc,
    Expression, ExtractFunc, Function, LikeOp, Literal, TrimFunc, TrimPosition, UnaryFunc,
    VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// SQLite dialect
pub struct SQLiteDialect;

impl DialectImpl for SQLiteDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::SQLite
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // SQLite supports multiple identifier quote styles
        config.identifiers.insert('"', '"');
        config.identifiers.insert('[', ']');
        config.identifiers.insert('`', '`');
        // SQLite does NOT support nested comments
        config.nested_comments = false;
        // SQLite supports 0x/0X hex number literals (e.g., 0XCC -> x'CC')
        config.hex_number_strings = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::SQLite),
            // SQLite uses comma syntax for JSON_OBJECT: JSON_OBJECT('key', value)
            json_key_value_pair_sep: ",",
            // SQLite doesn't support table alias columns: t AS t(c1, c2)
            supports_table_alias_columns: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL is native to SQLite, but we also support COALESCE
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // TryCast -> CAST (SQLite doesn't support TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST (SQLite doesn't support safe casts)
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // RAND -> RANDOM in SQLite
            Expression::Rand(r) => {
                // SQLite's RANDOM() doesn't take a seed argument
                let _ = r.seed; // Ignore seed
                Ok(Expression::Function(Box::new(Function::new(
                    "RANDOM".to_string(),
                    vec![],
                ))))
            }

            // RANDOM expression -> RANDOM() function
            Expression::Random(_) => Ok(Expression::Function(Box::new(Function::new(
                "RANDOM".to_string(),
                vec![],
            )))),

            // ILike -> LOWER() LIKE LOWER() (SQLite doesn't support ILIKE)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left.clone())));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right.clone())));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier.clone(),
                    inferred_type: None,
                })))
            }

            // CountIf -> SUM(IIF(condition, 1, 0))
            Expression::CountIf(f) => {
                let iif_expr = Expression::Function(Box::new(Function::new(
                    "IIF".to_string(),
                    vec![f.this.clone(), Expression::number(1), Expression::number(0)],
                )));
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: iif_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                    inferred_type: None,
                })))
            }

            // UNNEST -> not supported in SQLite, pass through
            Expression::Unnest(_) => Ok(expr),

            // EXPLODE -> not supported in SQLite, pass through
            Expression::Explode(_) => Ok(expr),

            // Concat expressions -> use || operator (handled in generator)
            Expression::Concat(c) => {
                // SQLite uses || for concatenation
                // We'll keep the Concat expression and let the generator handle it
                Ok(Expression::Concat(c))
            }

            // IfFunc -> IIF in SQLite
            Expression::IfFunc(f) => {
                let mut args = vec![f.condition, f.true_value];
                if let Some(false_val) = f.false_value {
                    args.push(false_val);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "IIF".to_string(),
                    args,
                ))))
            }

            // Normalize single-argument PRAGMAs to assignment syntax.
            Expression::Pragma(mut p) => {
                if p.value.is_some() || p.args.len() == 1 {
                    p.use_assignment_syntax = true;
                }
                Ok(Expression::Pragma(p))
            }

            // PostgreSQL DATE '...' literals are not SQLite syntax.
            Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Date(_)) => {
                let Literal::Date(date) = lit.as_ref() else {
                    unreachable!()
                };
                Ok(Self::string_literal(date))
            }

            // SQLite scalar MIN/MAX are multi-argument equivalents of LEAST/GREATEST.
            Expression::Least(f) => Ok(Self::function("MIN", f.expressions)),
            Expression::Greatest(f) => Ok(Self::function("MAX", f.expressions)),

            // PostgreSQL EXTRACT(...) lowers to SQLite strftime() for common units.
            Expression::Extract(f) => Self::transform_extract(*f),

            // PostgreSQL DATE_TRUNC(...) lowers to date/strftime() for common units.
            Expression::DateTrunc(f) => Self::transform_date_trunc(*f),

            // SQLite uses comma-call SUBSTRING/SUBSTR syntax, not SQL-standard FROM/FOR.
            Expression::Substring(mut f) => {
                f.from_for_syntax = false;
                Ok(Expression::Substring(f))
            }

            // SQLite supports LTRIM/RTRIM/TRIM(str, chars), not TRIM(LEADING ... FROM ...).
            Expression::Trim(f) => Ok(Self::transform_trim(*f)),

            // Strip PostgreSQL's default public schema in SQLite DDL.
            Expression::CreateTable(mut ct)
                if ct
                    .name
                    .schema
                    .as_ref()
                    .is_some_and(|schema| schema.name.eq_ignore_ascii_case("public"))
                    && ct.name.catalog.is_none() =>
            {
                ct.name.schema = None;
                Ok(Expression::CreateTable(ct))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations for type mapping
            Expression::Cast(c) => self.transform_cast(*c),

            // Div: SQLite has TYPED_DIVISION - wrap left operand in CAST(AS REAL)
            Expression::Div(mut op) => {
                // Don't add CAST AS REAL if either operand is already a float literal
                let right_is_float = matches!(&op.right, Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::Number(n) if n.contains('.')));
                let right_is_float_cast = Self::is_float_cast(&op.right);
                if !Self::is_float_cast(&op.left) && !right_is_float && !right_is_float_cast {
                    op.left = Expression::Cast(Box::new(crate::expressions::Cast {
                        this: op.left,
                        to: crate::expressions::DataType::Float {
                            precision: None,
                            scale: None,
                            real_spelling: true,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                }
                Ok(Expression::Div(op))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl SQLiteDialect {
    fn function(name: &str, args: Vec<Expression>) -> Expression {
        Expression::Function(Box::new(Function::new(name.to_string(), args)))
    }

    fn string_literal(value: &str) -> Expression {
        Expression::Literal(Box::new(Literal::String(value.to_string())))
    }

    fn strftime(format: &str, expr: Expression) -> Expression {
        Expression::Function(Box::new(Function::new(
            "STRFTIME".to_string(),
            vec![Self::string_literal(format), expr],
        )))
    }

    fn cast(expr: Expression, to: DataType) -> Expression {
        Expression::Cast(Box::new(Cast {
            this: expr,
            to,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }

    fn sqlite_int_type() -> DataType {
        DataType::Int {
            length: None,
            integer_spelling: true,
        }
    }

    fn sqlite_real_type() -> DataType {
        DataType::Float {
            precision: None,
            scale: None,
            real_spelling: true,
        }
    }

    fn transform_extract(f: ExtractFunc) -> Result<Expression> {
        let strftime_format = match &f.field {
            DateTimeField::Year => "%Y",
            DateTimeField::Month => "%m",
            DateTimeField::Day => "%d",
            DateTimeField::Hour => "%H",
            DateTimeField::Minute => "%M",
            DateTimeField::Second => "%f",
            DateTimeField::DayOfWeek => "%w",
            DateTimeField::DayOfYear => "%j",
            DateTimeField::Epoch => "%s",
            _ => return Ok(Expression::Extract(Box::new(f))),
        };
        let target_type = if matches!(f.field, DateTimeField::Epoch | DateTimeField::Second) {
            Self::sqlite_real_type()
        } else {
            Self::sqlite_int_type()
        };
        Ok(Self::cast(
            Self::strftime(strftime_format, f.this),
            target_type,
        ))
    }

    fn transform_date_trunc(f: DateTruncFunc) -> Result<Expression> {
        match &f.unit {
            DateTimeField::Day => Ok(Self::function("DATE", vec![f.this])),
            DateTimeField::Hour => Ok(Self::strftime("%Y-%m-%d %H:00:00", f.this)),
            DateTimeField::Minute => Ok(Self::strftime("%Y-%m-%d %H:%M:00", f.this)),
            DateTimeField::Second => Ok(Self::strftime("%Y-%m-%d %H:%M:%S", f.this)),
            DateTimeField::Month => Ok(Self::strftime("%Y-%m-01", f.this)),
            DateTimeField::Year => Ok(Self::strftime("%Y-01-01", f.this)),
            _ => Ok(Expression::DateTrunc(Box::new(f))),
        }
    }

    fn datetime_field_from_expr(expr: &Expression) -> Option<DateTimeField> {
        let unit = match expr {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::String(s) => s.as_str(),
                _ => return None,
            },
            Expression::Identifier(id) => id.name.as_str(),
            Expression::Var(v) => v.this.as_str(),
            Expression::Column(col) if col.table.is_none() => col.name.name.as_str(),
            _ => return None,
        };
        match unit.to_ascii_lowercase().as_str() {
            "year" | "yyyy" | "yy" => Some(DateTimeField::Year),
            "month" | "mon" | "mm" => Some(DateTimeField::Month),
            "day" | "dd" => Some(DateTimeField::Day),
            "hour" | "hours" | "h" | "hh" | "hr" | "hrs" => Some(DateTimeField::Hour),
            "minute" | "minutes" | "mi" | "min" | "mins" => Some(DateTimeField::Minute),
            "second" | "seconds" | "s" | "sec" | "secs" | "ss" => Some(DateTimeField::Second),
            "dow" | "dayofweek" | "dw" => Some(DateTimeField::DayOfWeek),
            "doy" | "dayofyear" | "dy" => Some(DateTimeField::DayOfYear),
            "epoch" => Some(DateTimeField::Epoch),
            _ => None,
        }
    }

    fn transform_trim(f: TrimFunc) -> Expression {
        let function_name = match f.position {
            TrimPosition::Leading => "LTRIM",
            TrimPosition::Trailing => "RTRIM",
            TrimPosition::Both => "TRIM",
        };
        let mut args = vec![f.this];
        if let Some(characters) = f.characters {
            args.push(characters);
        }
        Expression::Function(Box::new(Function::new(function_name.to_string(), args)))
    }

    /// Check if an expression is already a CAST to a float type
    fn is_float_cast(expr: &Expression) -> bool {
        if let Expression::Cast(cast) = expr {
            match &cast.to {
                crate::expressions::DataType::Double { .. }
                | crate::expressions::DataType::Float { .. } => true,
                crate::expressions::DataType::Custom { name } => {
                    name.eq_ignore_ascii_case("REAL") || name.eq_ignore_ascii_case("DOUBLE")
                }
                _ => false,
            }
        } else {
            false
        }
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // LIKE(pattern, string) -> string LIKE pattern (SQLite function form)
            "LIKE" if f.args.len() == 2 => {
                let mut args = f.args;
                let pattern = args.remove(0);
                let string = args.remove(0);
                // Swap: string LIKE pattern
                Ok(Expression::Like(Box::new(LikeOp::new(string, pattern))))
            }
            // LIKE(pattern, string, escape) -> string LIKE pattern ESCAPE escape
            "LIKE" if f.args.len() == 3 => {
                let mut args = f.args;
                let pattern = args.remove(0);
                let string = args.remove(0);
                let escape = args.remove(0);
                Ok(Expression::Like(Box::new(LikeOp {
                    left: string,
                    right: pattern,
                    escape: Some(escape),
                    quantifier: None,
                    inferred_type: None,
                })))
            }
            // GLOB(pattern, string) -> string GLOB pattern (SQLite function form)
            "GLOB" if f.args.len() == 2 => {
                let mut args = f.args;
                let pattern = args.remove(0);
                let string = args.remove(0);
                // Swap: string GLOB pattern
                Ok(Expression::Glob(Box::new(BinaryOp::new(string, pattern))))
            }
            // NVL -> IFNULL
            "NVL" if f.args.len() == 2 => {
                let mut args = f.args;
                let expr1 = args.remove(0);
                let expr2 = args.remove(0);
                Ok(Expression::IfNull(Box::new(BinaryFunc {
                    original_name: None,
                    this: expr1,
                    expression: expr2,
                    inferred_type: None,
                })))
            }

            // COALESCE stays as COALESCE (native to SQLite)
            "COALESCE" => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
                inferred_type: None,
            }))),

            // RAND -> RANDOM in SQLite
            "RAND" => Ok(Expression::Function(Box::new(Function::new(
                "RANDOM".to_string(),
                vec![],
            )))),

            // CHR -> CHAR in SQLite
            "CHR" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CHAR".to_string(),
                f.args,
            )))),

            // POSITION -> INSTR in SQLite (with swapped arguments)
            "POSITION" if f.args.len() == 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // INSTR(string, substring) - note: argument order is reversed from POSITION
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // STRPOS -> INSTR in SQLite (with swapped arguments)
            "STRPOS" if f.args.len() == 2 => {
                let mut args = f.args;
                let string = args.remove(0);
                let substring = args.remove(0);
                // INSTR(string, substring)
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // CHARINDEX -> INSTR in SQLite
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // INSTR(string, substring)
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // LEVENSHTEIN -> EDITDIST3 in SQLite
            "LEVENSHTEIN" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("EDITDIST3".to_string(), f.args),
            ))),

            // PostgreSQL-compatible scalar/JSON rewrites.
            "LEAST" if !f.args.is_empty() => Ok(Self::function("MIN", f.args)),
            "GREATEST" if !f.args.is_empty() => Ok(Self::function("MAX", f.args)),
            "JSON_BUILD_ARRAY" => Ok(Self::function("JSON_ARRAY", f.args)),
            "JSON_BUILD_OBJECT" => Ok(Self::function("JSON_OBJECT", f.args)),
            "JSON_AGG" | "JSONB_AGG" if f.args.len() == 1 => {
                Ok(Self::function("JSON_GROUP_ARRAY", f.args))
            }
            "JSON_OBJECT_AGG" if f.args.len() == 2 => {
                Ok(Self::function("JSON_GROUP_OBJECT", f.args))
            }

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

            // CEILING -> CEIL (not supported in SQLite, but we try)
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // LEN -> LENGTH in SQLite
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // SUBSTRING is native to SQLite (keep as-is)
            "SUBSTRING" => Ok(Self::function("SUBSTRING", f.args)),

            // STRING_AGG -> GROUP_CONCAT in SQLite
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("GROUP_CONCAT".to_string(), f.args),
            ))),

            // LISTAGG -> GROUP_CONCAT in SQLite
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            "DATE_PART" if f.args.len() == 2 => {
                let mut args = f.args;
                let unit = args.remove(0);
                let expr = args.remove(0);
                if let Some(field) = Self::datetime_field_from_expr(&unit) {
                    Self::transform_extract(ExtractFunc { this: expr, field })
                } else {
                    Ok(Self::function("DATE_PART", vec![unit, expr]))
                }
            }

            "DATE_TRUNC" if f.args.len() == 2 => {
                let mut args = f.args;
                let unit = args.remove(0);
                let expr = args.remove(0);
                if let Some(unit) = Self::datetime_field_from_expr(&unit) {
                    Self::transform_date_trunc(DateTruncFunc { this: expr, unit })
                } else {
                    Ok(Self::function("DATE_TRUNC", vec![unit, expr]))
                }
            }

            // DATEDIFF(a, b, unit_string) -> JULIANDAY arithmetic for SQLite
            "DATEDIFF" | "DATE_DIFF" if f.args.len() == 3 => {
                let mut args = f.args;
                let first = args.remove(0); // date1
                let second = args.remove(0); // date2
                let unit_expr = args.remove(0); // unit string like 'day'

                // Extract unit string
                let unit_str = match &unit_expr {
                    Expression::Literal(lit)
                        if matches!(lit.as_ref(), crate::expressions::Literal::String(_)) =>
                    {
                        let crate::expressions::Literal::String(s) = lit.as_ref() else {
                            unreachable!()
                        };
                        s.to_lowercase()
                    }
                    Expression::Identifier(id) => id.name.to_lowercase(),
                    Expression::Var(v) => v.this.to_lowercase(),
                    Expression::Column(col) if col.table.is_none() => col.name.name.to_lowercase(),
                    _ => "day".to_string(),
                };

                // JULIANDAY(first) - JULIANDAY(second)
                let jd_first = Expression::Function(Box::new(Function::new(
                    "JULIANDAY".to_string(),
                    vec![first],
                )));
                let jd_second = Expression::Function(Box::new(Function::new(
                    "JULIANDAY".to_string(),
                    vec![second],
                )));
                let diff = Expression::Sub(Box::new(BinaryOp::new(jd_first, jd_second)));
                let paren_diff = Expression::Paren(Box::new(crate::expressions::Paren {
                    this: diff,
                    trailing_comments: Vec::new(),
                }));

                // Apply multiplier based on unit
                let adjusted = match unit_str.as_str() {
                    "hour" => Expression::Mul(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            "24.0".to_string(),
                        ))),
                    ))),
                    "minute" => Expression::Mul(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            "1440.0".to_string(),
                        ))),
                    ))),
                    "second" => Expression::Mul(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            "86400.0".to_string(),
                        ))),
                    ))),
                    "month" => Expression::Div(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            "30.0".to_string(),
                        ))),
                    ))),
                    "year" => Expression::Div(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            "365.0".to_string(),
                        ))),
                    ))),
                    _ => paren_diff, // day is the default
                };

                // CAST(... AS INTEGER)
                Ok(Expression::Cast(Box::new(Cast {
                    this: adjusted,
                    to: crate::expressions::DataType::Int {
                        length: None,
                        integer_spelling: true,
                    },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            }

            // STRFTIME with single arg -> add CURRENT_TIMESTAMP as second arg
            "STRFTIME" if f.args.len() == 1 => {
                let mut args = f.args;
                args.push(Expression::CurrentTimestamp(
                    crate::expressions::CurrentTimestamp {
                        precision: None,
                        sysdate: false,
                    },
                ));
                Ok(Expression::Function(Box::new(Function::new(
                    "STRFTIME".to_string(),
                    args,
                ))))
            }

            // CONCAT(a, b, ...) -> a || b || ... for SQLite
            "CONCAT" if f.args.len() >= 2 => {
                let mut args = f.args;
                let mut result = args.remove(0);
                for arg in args {
                    result = Expression::DPipe(Box::new(crate::expressions::DPipe {
                        this: Box::new(result),
                        expression: Box::new(arg),
                        safe: None,
                    }));
                }
                Ok(result)
            }

            // TRUNC: SQLite doesn't support decimals arg, strip second arg
            "TRUNC" if f.args.len() > 1 => Ok(Expression::Function(Box::new(Function::new(
                "TRUNC".to_string(),
                vec![f.args[0].clone()],
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
            // COUNT_IF -> SUM(CASE WHEN...)
            "COUNT_IF" if !f.args.is_empty() => {
                let condition = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                    inferred_type: None,
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
                    inferred_type: None,
                })))
            }

            // ANY_VALUE -> MAX in SQLite
            "ANY_VALUE" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Max(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: arg,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                    inferred_type: None,
                })))
            }

            // STRING_AGG -> GROUP_CONCAT
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("GROUP_CONCAT".to_string(), f.args),
            ))),

            // LISTAGG -> GROUP_CONCAT
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> GROUP_CONCAT (SQLite doesn't have arrays)
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            "JSON_AGG" | "JSONB_AGG" if f.args.len() == 1 => {
                Ok(Self::function("JSON_GROUP_ARRAY", f.args))
            }

            "JSON_OBJECT_AGG" if f.args.len() == 2 => {
                Ok(Self::function("JSON_GROUP_OBJECT", f.args))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // SQLite has limited type support, map types appropriately
        // The type mapping is handled in the generator via type_mapping
        // For now, just pass through
        Ok(Expression::Cast(Box::new(c)))
    }
}
