//! SQLite Dialect
//!
//! SQLite-specific transformations based on sqlglot patterns.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, BinaryFunc, BinaryOp, Case, Cast, CeilFunc, Expression, Function, LikeOp, UnaryFunc,
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

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations for type mapping
            Expression::Cast(c) => self.transform_cast(*c),

            // Div: SQLite has TYPED_DIVISION - wrap left operand in CAST(AS REAL)
            Expression::Div(mut op) => {
                // Don't add CAST AS REAL if either operand is already a float literal
                let right_is_float = matches!(&op.right, Expression::Literal(crate::expressions::Literal::Number(n)) if n.contains('.'));
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
                })))
            }

            // COALESCE stays as COALESCE (native to SQLite)
            "COALESCE" => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
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
            "SUBSTRING" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // STRING_AGG -> GROUP_CONCAT in SQLite
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("GROUP_CONCAT".to_string(), f.args),
            ))),

            // LISTAGG -> GROUP_CONCAT in SQLite
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "GROUP_CONCAT".to_string(),
                f.args,
            )))),

            // DATEDIFF(a, b, unit_string) -> JULIANDAY arithmetic for SQLite
            "DATEDIFF" | "DATE_DIFF" if f.args.len() == 3 => {
                let mut args = f.args;
                let first = args.remove(0); // date1
                let second = args.remove(0); // date2
                let unit_expr = args.remove(0); // unit string like 'day'

                // Extract unit string
                let unit_str = match &unit_expr {
                    Expression::Literal(crate::expressions::Literal::String(s)) => s.to_lowercase(),
                    Expression::Identifier(id) => id.name.to_lowercase(),
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
                        Expression::Literal(crate::expressions::Literal::Number(
                            "24.0".to_string(),
                        )),
                    ))),
                    "minute" => Expression::Mul(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(crate::expressions::Literal::Number(
                            "1440.0".to_string(),
                        )),
                    ))),
                    "second" => Expression::Mul(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(crate::expressions::Literal::Number(
                            "86400.0".to_string(),
                        )),
                    ))),
                    "month" => Expression::Div(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(crate::expressions::Literal::Number(
                            "30.0".to_string(),
                        )),
                    ))),
                    "year" => Expression::Div(Box::new(BinaryOp::new(
                        paren_diff,
                        Expression::Literal(crate::expressions::Literal::Number(
                            "365.0".to_string(),
                        )),
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
