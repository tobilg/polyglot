//! SAP HANA Cloud SQL Dialect
//!
//! SAP HANA is an in-memory, column-oriented, relational database management system.
//! Reference: SAP HANA Cloud SQL Reference Guide.
//!
//! Key characteristics:
//! - Double-quote identifiers (case preserved, like Oracle)
//! - Standard SQL string quoting with single-quote escaping (`''`)
//! - No nested comment support
//! - Uppercase keyword generation
//! - Phase 2: ~40 HANA-specific function transforms for cross-dialect transpilation

use super::{DialectImpl, DialectType};
use crate::error::Result;
#[cfg(feature = "transpile")]
use crate::expressions::{
    Case, Cast, CurrentDate, CurrentTime, CurrentTimestamp, DataType, Expression, Function, LikeOp,
    UnaryFunc, VarArgFunc,
};
#[cfg(feature = "generate")]
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// SAP HANA Cloud dialect
pub struct HanaDialect;

impl DialectImpl for HanaDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::HANA
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // HANA uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // HANA does not support nested comments
        config.nested_comments = false;
        config
    }

    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, NormalizeFunctions};
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::HANA),
            // HANA uses the COLUMN keyword in ALTER TABLE ADD
            alter_table_include_column_keyword: true,
            // Preserve function name casing for identity round-trip
            normalize_functions: NormalizeFunctions::None,
            ..Default::default()
        }
    }

    #[cfg(feature = "transpile")]
    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // Typed date arithmetic: ADD_MONTHS → DATE_ADD('month', val, date)
            Expression::AddMonths(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATE_ADD".to_string(),
                vec![Expression::string("month"), f.expression, f.this],
            )))),

            // Typed date diff: MONTHS_BETWEEN → DATE_DIFF('month', d1, d2)
            Expression::MonthsBetween(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATE_DIFF".to_string(),
                vec![Expression::string("month"), f.this, f.expression],
            )))),

            // NVL → COALESCE
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
                inferred_type: None,
            }))),

            // IFNULL → COALESCE
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
                inferred_type: None,
            }))),

            // IF(cond, a, b) → CASE WHEN cond THEN a ELSE b END
            Expression::IfFunc(f) => Ok(Expression::Case(Box::new(Case {
                operand: None,
                whens: vec![(f.condition, f.true_value)],
                else_: f.false_value,
                comments: Vec::new(),
                inferred_type: None,
            }))),

            // ILIKE → LOWER() LIKE LOWER() (HANA doesn't support ILIKE natively)
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

            // HANA FLOAT (64-bit) → DOUBLE in CAST expressions
            // HANA's FLOAT is equivalent to DOUBLE, not single-precision REAL
            Expression::Cast(mut c) => {
                if let DataType::Float {
                    real_spelling: false,
                    ..
                } = c.to
                {
                    c.to = DataType::Double {
                        precision: None,
                        scale: None,
                    };
                }
                Ok(Expression::Cast(c))
            }

            // IFNULL is parsed as Coalesce with original_name "IFNULL" — clear it
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // TO_DECIMAL/TO_NUMBER is parsed as Expression::ToNumber → CAST AS DECIMAL
            // (intercept before cross_dialect_normalize converts it to CAST AS DOUBLE)
            Expression::ToNumber(f) => Ok(Expression::Cast(Box::new(Cast {
                this: *f.this,
                to: DataType::Decimal {
                    precision: None,
                    scale: None,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // CURRENT_UTCTIMESTAMP/CURRENT_UTCDATE/CURRENT_UTCTIME without parens
            // are parsed as Column references — convert to proper datetime expressions
            Expression::Column(c) => {
                let name_upper = c.name.name.to_ascii_uppercase();
                match name_upper.as_str() {
                    "CURRENT_UTCTIMESTAMP" => Ok(Expression::CurrentTimestamp(CurrentTimestamp {
                        precision: None,
                        sysdate: false,
                    })),
                    "CURRENT_UTCDATE" => Ok(Expression::CurrentDate(CurrentDate)),
                    "CURRENT_UTCTIME" => {
                        Ok(Expression::CurrentTime(CurrentTime { precision: None }))
                    }
                    _ => Ok(Expression::Column(c)),
                }
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

#[cfg(feature = "transpile")]
impl HanaDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // Date arithmetic: ADD_DAYS(d, n) → DATE_ADD('day', n, d)
            "ADD_DAYS" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let val = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![Expression::string("day"), val, date],
                ))))
            }

            // ADD_SECONDS(ts, n) → DATE_ADD('second', n, ts)
            "ADD_SECONDS" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let val = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![Expression::string("second"), val, date],
                ))))
            }

            // ADD_YEARS(d, n) → DATE_ADD('year', n, d)
            "ADD_YEARS" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let val = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![Expression::string("year"), val, date],
                ))))
            }

            // Date diff: DAYS_BETWEEN(d1, d2) → DATE_DIFF('day', d1, d2)
            "DAYS_BETWEEN" if f.args.len() == 2 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::string("day"),
                        f.args[0].clone(),
                        f.args[1].clone(),
                    ],
                ))))
            }

            "SECONDS_BETWEEN" if f.args.len() == 2 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::string("second"),
                        f.args[0].clone(),
                        f.args[1].clone(),
                    ],
                ))))
            }

            "YEARS_BETWEEN" if f.args.len() == 2 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::string("year"),
                        f.args[0].clone(),
                        f.args[1].clone(),
                    ],
                ))))
            }

            // Conversion: TO_VARCHAR(x) without format → CAST(x AS VARCHAR)
            "TO_VARCHAR" if f.args.len() == 1 => Ok(Expression::Cast(Box::new(Cast {
                this: f.args.into_iter().next().unwrap(),
                to: DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // TO_VARCHAR(d, fmt) with format → DATE_FORMAT(d, converted_fmt)
            "TO_VARCHAR" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let fmt = args.remove(0);
                let converted_fmt = convert_format_arg(&fmt);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, converted_fmt],
                ))))
            }

            // TO_INTEGER(x) → CAST(x AS INTEGER)
            "TO_INTEGER" if f.args.len() == 1 => Ok(Expression::Cast(Box::new(Cast {
                this: f.args.into_iter().next().unwrap(),
                to: DataType::Int {
                    length: None,
                    integer_spelling: true,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // TO_NUMBER/TO_DECIMAL(x) → CAST(x AS DECIMAL)
            // (Parser canonicalizes TO_DECIMAL to TO_NUMBER; intercept before
            // cross_dialect_normalize converts it to CAST AS DOUBLE)
            "TO_NUMBER" if f.args.len() == 1 => Ok(Expression::Cast(Box::new(Cast {
                this: f.args.into_iter().next().unwrap(),
                to: DataType::Decimal {
                    precision: None,
                    scale: None,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // TO_REAL(x) → CAST(x AS REAL)
            "TO_REAL" if f.args.len() == 1 => Ok(Expression::Cast(Box::new(Cast {
                this: f.args.into_iter().next().unwrap(),
                to: DataType::Float {
                    precision: None,
                    scale: None,
                    real_spelling: true,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // TO_DOUBLE(x) → CAST(x AS DOUBLE)
            "TO_DOUBLE" if f.args.len() == 1 => Ok(Expression::Cast(Box::new(Cast {
                this: f.args.into_iter().next().unwrap(),
                to: DataType::Double {
                    precision: None,
                    scale: None,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))),

            // TO_DATE(s, fmt) with format → keep as TO_DATE but convert format
            // (Trino's transform_function handles TO_DATE → DATE_PARSE)
            "TO_DATE" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let fmt = args.remove(0);
                let converted_fmt = convert_format_arg(&fmt);
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_DATE".to_string(),
                    vec![date, converted_fmt],
                ))))
            }

            // TO_TIMESTAMP(s, fmt) with format → keep as TO_TIMESTAMP but convert format
            // (Trino's transform_function handles TO_TIMESTAMP → DATE_PARSE)
            "TO_TIMESTAMP" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let fmt = args.remove(0);
                let converted_fmt = convert_format_arg(&fmt);
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIMESTAMP".to_string(),
                    vec![date, converted_fmt],
                ))))
            }

            // Datetime constants: CURRENT_UTCTIMESTAMP → CURRENT_TIMESTAMP
            "CURRENT_UTCTIMESTAMP" => Ok(Expression::CurrentTimestamp(CurrentTimestamp {
                precision: None,
                sysdate: false,
            })),

            // CURRENT_UTCDATE → CURRENT_DATE
            "CURRENT_UTCDATE" => Ok(Expression::CurrentDate(CurrentDate)),

            // CURRENT_UTCTIME → CURRENT_TIME
            "CURRENT_UTCTIME" => Ok(Expression::CurrentTime(CurrentTime { precision: None })),

            // NOW() → CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(CurrentTimestamp {
                precision: None,
                sysdate: false,
            })),

            // SYSDATE → CURRENT_TIMESTAMP
            "SYSDATE" => Ok(Expression::CurrentTimestamp(CurrentTimestamp {
                precision: None,
                sysdate: false,
            })),

            // TRUNC(x, n) → TRUNCATE(x, n)
            "TRUNC" => Ok(Expression::Function(Box::new(Function::new(
                "TRUNCATE".to_string(),
                f.args,
            )))),

            // Bitwise: BITAND → BITWISE_AND
            "BITAND" => Ok(Expression::Function(Box::new(Function::new(
                "BITWISE_AND".to_string(),
                f.args,
            )))),

            // BITOR → BITWISE_OR
            "BITOR" => Ok(Expression::Function(Box::new(Function::new(
                "BITWISE_OR".to_string(),
                f.args,
            )))),

            // BITNOT → BITWISE_NOT
            "BITNOT" => Ok(Expression::Function(Box::new(Function::new(
                "BITWISE_NOT".to_string(),
                f.args,
            )))),

            // HEX_TO_VARCHAR → FROM_HEX
            "HEX_TO_VARCHAR" => Ok(Expression::Function(Box::new(Function::new(
                "FROM_HEX".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }
}

/// Convert a HANA Oracle-style date format string argument to Java SimpleDateFormat.
/// If the argument is a string literal, converts the format tokens in-place.
/// Otherwise, returns the expression unchanged.
#[cfg(feature = "transpile")]
fn convert_format_arg(fmt: &Expression) -> Expression {
    if let Expression::Literal(ref lit) = fmt {
        if let crate::expressions::Literal::String(ref s) = lit.as_ref() {
            return Expression::string(convert_hana_to_java_format(s));
        }
    }
    fmt.clone()
}

/// Convert HANA Oracle-style date format tokens to Java SimpleDateFormat tokens.
///
/// Token mapping (case-insensitive match on HANA tokens):
/// - YYYY→yyyy, YY→yy, MM→MM, DD→dd
/// - HH24→HH, HH12→hh, MI→mm, SS→ss
/// - FF3→SSS, FF6→SSSSSS
/// - DAY→EEEE, DY→EEE, MONTH→MMMM, MON→MMM
/// - AM→a
///
/// Unknown tokens and literal text pass through unchanged.
#[cfg(feature = "transpile")]
fn convert_hana_to_java_format(hana_fmt: &str) -> String {
    // Token mapping sorted by length (longest first) to ensure correct matching.
    // (hana_token_uppercase, java_equivalent)
    const TOKENS: &[(&str, &str)] = &[
        ("MONTH", "MMMM"),
        ("YYYY", "yyyy"),
        ("HH24", "HH"),
        ("HH12", "hh"),
        ("FF3", "SSS"),
        ("FF6", "SSSSSS"),
        ("DAY", "EEEE"),
        ("MON", "MMM"),
        ("YY", "yy"),
        ("MM", "MM"),
        ("DD", "dd"),
        ("MI", "mm"),
        ("SS", "ss"),
        ("DY", "EEE"),
        ("AM", "a"),
        ("PM", "a"),
    ];

    let chars: Vec<char> = hana_fmt.chars().collect();
    let mut result = String::new();
    let mut i = 0;

    while i < chars.len() {
        let remaining = &chars[i..];

        // Try to match the longest token at the current position (case-insensitive)
        let mut matched = false;
        for (hana_token, java_eq) in TOKENS {
            let token_len = hana_token.chars().count();
            if remaining.len() >= token_len {
                let candidate: String = remaining[..token_len].iter().collect();
                if candidate.eq_ignore_ascii_case(hana_token) {
                    result.push_str(java_eq);
                    i += token_len;
                    matched = true;
                    break;
                }
            }
        }

        // No token matched — copy the character as-is (literal text)
        if !matched {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}
