//! Teradata Dialect
//!
//! Teradata-specific transformations based on sqlglot patterns.
//! Teradata has unique syntax including ** for exponentiation,
//! TOP instead of LIMIT, and TRYCAST for safe casting.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, Case, Cast, Expression, Function, UnaryFunc, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Teradata dialect
pub struct TeradataDialect;

impl DialectImpl for TeradataDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Teradata
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Teradata uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Teradata does NOT support nested comments
        config.nested_comments = false;
        // Teradata-specific keywords and operators
        config.keywords.insert("SEL".to_string(), crate::tokens::TokenType::Select);
        config.keywords.insert("UPD".to_string(), crate::tokens::TokenType::Update);
        config.keywords.insert("DEL".to_string(), crate::tokens::TokenType::Delete);
        config.keywords.insert("INS".to_string(), crate::tokens::TokenType::Insert);
        config.keywords.insert("SAMPLE".to_string(), crate::tokens::TokenType::Sample);
        config.keywords.insert("LOCKING".to_string(), crate::tokens::TokenType::Lock);
        config.keywords.insert("HELP".to_string(), crate::tokens::TokenType::Command);
        config.keywords.insert("COLLECT".to_string(), crate::tokens::TokenType::Command);
        config.keywords.insert("EQ".to_string(), crate::tokens::TokenType::Eq);
        config.keywords.insert("NE".to_string(), crate::tokens::TokenType::Neq);
        config.keywords.insert("GE".to_string(), crate::tokens::TokenType::Gte);
        config.keywords.insert("GT".to_string(), crate::tokens::TokenType::Gt);
        config.keywords.insert("LE".to_string(), crate::tokens::TokenType::Lte);
        config.keywords.insert("LT".to_string(), crate::tokens::TokenType::Lt);
        config.keywords.insert("MOD".to_string(), crate::tokens::TokenType::Mod);
        config.keywords.insert("BYTEINT".to_string(), crate::tokens::TokenType::SmallInt);
        config.keywords.insert("ST_GEOMETRY".to_string(), crate::tokens::TokenType::Geometry);
        // Teradata does not support % as modulo operator
        config.single_tokens.remove(&'%');
        // Teradata treats 0x prefix as hex string literals
        config.hex_number_strings = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Teradata),
            tablesample_keywords: "SAMPLE",
            tablesample_requires_parens: false,
            tz_to_with_time_zone: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Teradata
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in Teradata
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // TryCast -> TRYCAST in Teradata (native)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRYCAST in Teradata
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
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

            // RAND -> RANDOM in Teradata (but preserve lower/upper for RANDOM(l, u))
            Expression::Rand(r) => {
                if r.lower.is_some() || r.upper.is_some() {
                    // Keep as Rand with lower/upper for Teradata RANDOM(l, u)
                    Ok(Expression::Rand(r))
                } else {
                    Ok(Expression::Random(crate::expressions::Random))
                }
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

impl TeradataDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // RAND -> RANDOM in Teradata
            "RAND" => Ok(Expression::Random(crate::expressions::Random)),

            // LEN -> CHARACTER_LENGTH in Teradata
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // LENGTH -> CHARACTER_LENGTH in Teradata
            "LENGTH" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CHARINDEX -> INSTR in Teradata (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // STRPOS -> INSTR in Teradata
            "STRPOS" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "INSTR".to_string(),
                f.args,
            )))),

            // LOCATE -> INSTR in Teradata (with swapped args)
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![string, substring],
                ))))
            }

            // ARRAY_LENGTH -> CARDINALITY in Teradata
            "ARRAY_LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("CARDINALITY".to_string(), f.args),
            ))),

            // SIZE -> CARDINALITY in Teradata
            "SIZE" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CARDINALITY".to_string(),
                f.args,
            )))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // DATE_FORMAT -> TO_CHAR in Teradata
            "DATE_FORMAT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("TO_CHAR".to_string(), f.args),
            ))),

            // strftime -> TO_CHAR in Teradata
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_CHAR".to_string(),
                    vec![date, format],
                ))))
            }

            // GREATEST is native in Teradata
            "GREATEST" => Ok(Expression::Function(Box::new(f))),

            // LEAST is native in Teradata
            "LEAST" => Ok(Expression::Function(Box::new(f))),

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
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                name: None,
                limit: None,
                })))
            }

            // MAX_BY is native in Teradata
            "MAX_BY" => Ok(Expression::AggregateFunction(f)),

            // MIN_BY is native in Teradata
            "MIN_BY" => Ok(Expression::AggregateFunction(f)),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Teradata CAST(x AS DATE FORMAT 'fmt') -> StrToDate(x, converted_fmt)
        // Teradata CAST(x AS TIMESTAMP FORMAT 'fmt') -> StrToTime(x, converted_fmt)
        if let Some(format_expr) = &c.format {
            let is_date = matches!(c.to, crate::expressions::DataType::Date);
            let is_timestamp = matches!(c.to, crate::expressions::DataType::Timestamp { .. });

            if is_date || is_timestamp {
                // Extract the format string from the expression
                let fmt_str = match format_expr.as_ref() {
                    Expression::Literal(crate::expressions::Literal::String(s)) => Some(s.clone()),
                    _ => None,
                };

                if let Some(teradata_fmt) = fmt_str {
                    // Convert Teradata format to strftime format
                    let strftime_fmt = Self::teradata_to_strftime(&teradata_fmt);

                    if is_date {
                        return Ok(Expression::StrToDate(Box::new(crate::expressions::StrToDate {
                            this: Box::new(c.this),
                            format: Some(strftime_fmt),
                            safe: None,
                        })));
                    } else {
                        return Ok(Expression::StrToTime(Box::new(crate::expressions::StrToTime {
                            this: Box::new(c.this),
                            format: strftime_fmt,
                            zone: None,
                            safe: None,
                            target_type: None,
                        })));
                    }
                }
            }
        }
        // Teradata type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }

    /// Convert Teradata date/time format string to strftime format
    fn teradata_to_strftime(fmt: &str) -> String {
        // Teradata TIME_MAPPING: longest tokens first to avoid partial matches
        let mut result = fmt.to_string();
        // Order matters: replace longer tokens first
        result = result.replace("YYYY", "%Y");
        result = result.replace("Y4", "%Y");
        result = result.replace("YY", "%y");
        result = result.replace("MMMM", "%B");
        result = result.replace("MMM", "%b");
        result = result.replace("MM", "%m");
        result = result.replace("M4", "%B");
        result = result.replace("M3", "%b");
        result = result.replace("EEEE", "%A");
        result = result.replace("EEE", "%a");
        result = result.replace("EE", "%a");
        result = result.replace("E4", "%A");
        result = result.replace("E3", "%a");
        result = result.replace("DDD", "%j");
        result = result.replace("DD", "%d");
        result = result.replace("D3", "%j");
        result = result.replace("HH24", "%H");
        result = result.replace("HH", "%H");
        result = result.replace("SSSSSS", "%f");
        result = result.replace("SS", "%S");
        result = result.replace("MI", "%M");
        result
    }
}
