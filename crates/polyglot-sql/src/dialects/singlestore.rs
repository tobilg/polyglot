//! SingleStore Dialect
//!
//! SingleStore (formerly MemSQL) specific transformations based on sqlglot patterns.
//! SingleStore is MySQL-compatible with distributed database extensions.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, BinaryOp, Case, Cast, CollationExpr, DataType, Expression, Function, Paren, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// SingleStore dialect (MySQL-compatible distributed database)
pub struct SingleStoreDialect;

impl DialectImpl for SingleStoreDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::SingleStore
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // SingleStore uses backticks for identifiers (MySQL-style)
        config.identifiers.insert('`', '`');
        config.nested_comments = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::SingleStore),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // SHOW INDEXES/KEYS -> SHOW INDEX in SingleStore
            Expression::Show(mut s) => {
                // Normalize INDEXES and KEYS to INDEX
                if s.this == "INDEXES" || s.this == "KEYS" {
                    s.this = "INDEX".to_string();
                }
                Ok(Expression::Show(s))
            }

            // SingleStore: Cast followed by COLLATE needs double cast
            // e.g., name :> LONGTEXT COLLATE 'utf8mb4_bin' -> name :> LONGTEXT :> LONGTEXT COLLATE 'utf8mb4_bin'
            Expression::Collation(c) => {
                if let Expression::Cast(inner_cast) = &c.this {
                    // Wrap the cast in another cast with the same type
                    let double_cast = Expression::Cast(Box::new(Cast {
                        this: c.this.clone(),
                        to: inner_cast.to.clone(),
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }));
                    Ok(Expression::Collation(Box::new(CollationExpr {
                        this: double_cast,
                        collation: c.collation.clone(),
                        quoted: c.quoted,
                        double_quoted: c.double_quoted,
                    })))
                } else {
                    Ok(Expression::Collation(c))
                }
            }

            // IFNULL is native in SingleStore (MySQL-style)
            Expression::IfNull(f) => Ok(Expression::IfNull(f)),

            // NVL -> IFNULL in SingleStore
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // TryCast -> not directly supported, use :> operator
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TryCast in SingleStore
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                name: None,
                limit: None,
                })))
            }

            // RAND is native in SingleStore
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // Second -> DATE_FORMAT(..., '%s') :> INT (SingleStore doesn't have native SECOND)
            Expression::Second(f) => {
                let date = f.this;
                // Cast to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%s")],
                )));
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // Hour -> DATE_FORMAT(..., '%k') :> INT (SingleStore uses DATE_FORMAT for HOUR)
            Expression::Hour(f) => {
                let date = f.this;
                // Cast to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%k")],
                )));
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // Minute -> DATE_FORMAT(..., '%i') :> INT (SingleStore doesn't have native MINUTE)
            Expression::Minute(f) => {
                let date = f.this;
                // Cast to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%i")],
                )));
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl SingleStoreDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // NVL -> IFNULL
            "NVL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // COALESCE is native in SingleStore
            "COALESCE" => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NOW is native in SingleStore - preserve as function
            "NOW" => Ok(Expression::Function(Box::new(f))),

            // GETDATE -> NOW
            "GETDATE" => Ok(Expression::Function(Box::new(Function::new(
                "NOW".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT is native in SingleStore
            "GROUP_CONCAT" => Ok(Expression::Function(Box::new(f))),

            // STRING_AGG -> GROUP_CONCAT
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // LISTAGG -> GROUP_CONCAT
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // SUBSTR is native in SingleStore
            "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // SUBSTRING is native in SingleStore
            "SUBSTRING" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native in SingleStore
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LENGTH".to_string(),
                f.args,
            )))),

            // CHARINDEX -> INSTR (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // STRPOS -> INSTR
            "STRPOS" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "INSTR".to_string(),
                f.args,
            )))),

            // LOCATE is native in SingleStore
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // INSTR is native in SingleStore
            "INSTR" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT is native in SingleStore
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR is native in SingleStore - preserve as function
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // TO_DATE is native in SingleStore
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP is native in SingleStore
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT_JSON is native in SingleStore
            "JSON_EXTRACT_JSON" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT -> JSON_EXTRACT_JSON
            "JSON_EXTRACT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_JSON".to_string(), f.args),
            ))),

            // GET_JSON_OBJECT -> JSON_EXTRACT_STRING
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_STRING".to_string(), f.args),
            ))),

            // REGEXP_LIKE -> RLIKE
            "REGEXP_LIKE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "RLIKE".to_string(),
                f.args,
            )))),

            // RLIKE is native in SingleStore
            "RLIKE" => Ok(Expression::Function(Box::new(f))),

            // TIME_BUCKET is native in SingleStore
            "TIME_BUCKET" => Ok(Expression::Function(Box::new(f))),

            // DATE_BIN -> TIME_BUCKET
            "DATE_BIN" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "TIME_BUCKET".to_string(),
                f.args,
            )))),

            // TIME_FORMAT -> DATE_FORMAT with cast to TIME(6)
            // TIME_FORMAT(date, fmt) -> DATE_FORMAT(date :> TIME(6), fmt)
            "TIME_FORMAT" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let format = args.remove(0);
                // Cast date to TIME(6)
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, format],
                ))))
            }

            // DAYNAME -> DATE_FORMAT with '%W'
            "DAYNAME" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, Expression::string("%W")],
                ))))
            }

            // MONTHNAME -> DATE_FORMAT with '%M'
            "MONTHNAME" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, Expression::string("%M")],
                ))))
            }

            // HOUR -> DATE_FORMAT with '%k' :> INT
            "HOUR" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                // Cast date to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                // DATE_FORMAT(... :> TIME(6), '%k')
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%k")],
                )));
                // Cast result to INT
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // MINUTE -> DATE_FORMAT with '%i' :> INT
            "MINUTE" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                // Cast date to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                // DATE_FORMAT(... :> TIME(6), '%i')
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%i")],
                )));
                // Cast result to INT
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // SECOND -> DATE_FORMAT with '%s' :> INT
            "SECOND" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                // Cast date to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                // DATE_FORMAT(... :> TIME(6), '%s')
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%s")],
                )));
                // Cast result to INT
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // MICROSECOND -> DATE_FORMAT with '%f' :> INT
            "MICROSECOND" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                // Cast date to TIME(6) first
                let cast_to_time = Expression::Cast(Box::new(Cast {
                    this: date,
                    to: DataType::Time { precision: Some(6), timezone: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                // DATE_FORMAT(... :> TIME(6), '%f')
                let date_format = Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![cast_to_time, Expression::string("%f")],
                )));
                // Cast result to INT
                Ok(Expression::Cast(Box::new(Cast {
                    this: date_format,
                    to: DataType::Int { length: None, integer_spelling: false },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // WEEKDAY -> (DAYOFWEEK(...) + 5) % 7
            "WEEKDAY" if f.args.len() == 1 => {
                let date = f.args.into_iter().next().unwrap();
                // DAYOFWEEK(date)
                let dayofweek = Expression::Function(Box::new(Function::new(
                    "DAYOFWEEK".to_string(),
                    vec![date],
                )));
                // (DAYOFWEEK(date) + 5) - wrap in explicit parentheses
                let add_five = Expression::Add(Box::new(BinaryOp::new(
                    dayofweek,
                    Expression::number(5),
                )));
                let add_five_paren = Expression::Paren(Box::new(Paren {
                    this: add_five,
                    trailing_comments: Vec::new(),
                }));
                // (DAYOFWEEK(date) + 5) % 7
                Ok(Expression::Mod(Box::new(BinaryOp::new(
                    add_five_paren,
                    Expression::number(7),
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
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                name: None,
                limit: None,
                })))
            }

            // APPROX_COUNT_DISTINCT is native in SingleStore
            "APPROX_COUNT_DISTINCT" => Ok(Expression::AggregateFunction(f)),

            // HLL -> APPROX_COUNT_DISTINCT
            "HLL" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                f.args,
            )))),

            // VARIANCE -> VAR_SAMP
            "VARIANCE" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "VAR_SAMP".to_string(),
                f.args,
            )))),

            // VAR_POP is native in SingleStore
            "VAR_POP" => Ok(Expression::AggregateFunction(f)),

            // VAR_SAMP is native in SingleStore
            "VAR_SAMP" => Ok(Expression::AggregateFunction(f)),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // SingleStore type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
