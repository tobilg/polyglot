//! ClickHouse Dialect
//!
//! ClickHouse-specific transformations based on sqlglot patterns.
//! ClickHouse is case-sensitive and has unique function naming conventions.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggregateFunction, BinaryOp, Case, Cast, Expression, Function, In, IsNull, LikeOp, MapConstructor, Paren, UnaryOp};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// ClickHouse dialect
pub struct ClickHouseDialect;

impl DialectImpl for ClickHouseDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::ClickHouse
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // ClickHouse uses double quotes and backticks for identifiers
        config.identifiers.insert('"', '"');
        config.identifiers.insert('`', '`');
        // ClickHouse does NOT support nested comments
        config.nested_comments = false;
        // ClickHouse allows identifiers to start with digits
        config.identifiers_can_start_with_digit = true;
        // ClickHouse uses backslash escaping in strings
        config.string_escapes.push('\\');
        // ClickHouse supports # as single-line comment
        config.hash_comments = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, NormalizeFunctions};
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::ClickHouse),
            // ClickHouse uses uppercase keywords (matching Python SQLGlot behavior)
            uppercase_keywords: true,
            // ClickHouse function names are case-sensitive and typically camelCase
            normalize_functions: NormalizeFunctions::None,
            // ClickHouse identifiers are case-sensitive
            case_sensitive_identifiers: true,
            tablesample_keywords: "SAMPLE",
            tablesample_requires_parens: false,
            identifiers_can_start_with_digit: true,
            // ClickHouse uses bracket-only notation for arrays: [1, 2, 3]
            array_bracket_only: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        let wrap_predicate_left = |expr: Expression| -> Expression {
            let needs_parens = matches!(
                expr,
                Expression::Add(_)
                    | Expression::Sub(_)
                    | Expression::Mul(_)
                    | Expression::Div(_)
                    | Expression::Mod(_)
                    | Expression::Concat(_)
                    | Expression::And(_)
                    | Expression::Or(_)
                    | Expression::Not(_)
                    | Expression::Case(_)
            );

            if needs_parens {
                Expression::Paren(Box::new(Paren {
                    this: expr,
                    trailing_comments: Vec::new(),
                }))
            } else {
                expr
            }
        };

        let wrap_not_target = |expr: Expression| -> Expression {
            match expr {
                Expression::Paren(_) => expr,
                Expression::In(_)
                | Expression::Between(_)
                | Expression::Is(_)
                | Expression::IsNull(_)
                | Expression::IsTrue(_)
                | Expression::IsFalse(_)
                | Expression::IsJson(_)
                | Expression::Like(_)
                | Expression::ILike(_)
                | Expression::SimilarTo(_)
                | Expression::Glob(_)
                | Expression::RegexpLike(_)
                | Expression::RegexpILike(_)
                | Expression::MemberOf(_) => Expression::Paren(Box::new(Paren {
                    this: expr,
                    trailing_comments: Vec::new(),
                })),
                _ => expr,
            }
        };

        let unwrap_in_array = |mut expressions: Vec<Expression>,
                               query: &Option<Expression>,
                               unnest: &Option<Box<Expression>>| -> Vec<Expression> {
            if query.is_none() && unnest.is_none() && expressions.len() == 1 {
                if matches!(expressions[0], Expression::ArrayFunc(_)) {
                    if let Expression::ArrayFunc(arr) = expressions.remove(0) {
                        return arr.expressions;
                    }
                }
            }
            expressions
        };

        match expr {
            // TryCast stays as TryCast (ClickHouse doesn't have TRY_CAST by default)
            // But we can emulate with toXOrNull functions
            Expression::TryCast(c) => {
                // For simplicity, just use regular CAST
                // ClickHouse has toXOrNull/toXOrZero functions for safe casts
                Ok(Expression::Cast(c))
            }

            // SafeCast -> CAST in ClickHouse
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // CountIf is native in ClickHouse (lowercase)
            Expression::CountIf(f) => Ok(Expression::Function(Box::new(Function::new(
                "countIf".to_string(),
                vec![f.this],
            )))),

            // UNNEST -> arrayJoin in ClickHouse
            Expression::Unnest(f) => Ok(Expression::Function(Box::new(Function::new(
                "arrayJoin".to_string(),
                vec![f.this],
            )))),

            // EXPLODE -> arrayJoin in ClickHouse
            Expression::Explode(f) => Ok(Expression::Function(Box::new(Function::new(
                "arrayJoin".to_string(),
                vec![f.this],
            )))),

            // ExplodeOuter -> arrayJoin in ClickHouse
            Expression::ExplodeOuter(f) => Ok(Expression::Function(Box::new(Function::new(
                "arrayJoin".to_string(),
                vec![f.this],
            )))),

            // RAND -> randCanonical() in ClickHouse
            Expression::Rand(_) => Ok(Expression::Function(Box::new(Function::new(
                "randCanonical".to_string(),
                vec![],
            )))),

            // Random -> randCanonical() in ClickHouse
            Expression::Random(_) => Ok(Expression::Function(Box::new(Function::new(
                "randCanonical".to_string(),
                vec![],
            )))),

            // startsWith -> startsWith
            Expression::StartsWith(f) => Ok(Expression::Function(Box::new(Function::new(
                "startsWith".to_string(),
                vec![f.this, f.expression],
            )))),

            // endsWith -> endsWith
            Expression::EndsWith(f) => Ok(Expression::Function(Box::new(Function::new(
                "endsWith".to_string(),
                vec![f.this, f.expression],
            )))),

            // ClickHouse prefers NOT (x IN (...)) over x NOT IN (...)
            Expression::In(in_expr) if in_expr.not => {
                if in_expr.global {
                    return Ok(Expression::In(in_expr));
                }
                let In { this, expressions, query, unnest, global, .. } = *in_expr;
                let expressions = unwrap_in_array(expressions, &query, &unnest);
                let base = Expression::In(Box::new(In {
                    this: wrap_predicate_left(this),
                    expressions,
                    query,
                    not: false,
                    global,
                    unnest,
                }));
                Ok(Expression::Not(Box::new(UnaryOp {
                    this: wrap_not_target(base),
                })))
            }

            // ClickHouse prefers NOT (x IS NULL) over x IS NOT NULL
            Expression::IsNull(is_null) if is_null.not => {
                let IsNull { this, .. } = *is_null;
                let base = Expression::IsNull(Box::new(IsNull {
                    this: wrap_predicate_left(this),
                    not: false,
                    postfix_form: false,
                }));
                Ok(Expression::Not(Box::new(UnaryOp {
                    this: wrap_not_target(base),
                })))
            }

            Expression::In(mut in_expr) => {
                in_expr.expressions = unwrap_in_array(in_expr.expressions, &in_expr.query, &in_expr.unnest);
                in_expr.this = wrap_predicate_left(in_expr.this);
                Ok(Expression::In(in_expr))
            }

            Expression::IsNull(mut is_null) => {
                is_null.this = wrap_predicate_left(is_null.this);
                Ok(Expression::IsNull(is_null))
            }

            // IF(cond, true, false) -> CASE WHEN cond THEN true ELSE false END
            Expression::IfFunc(f) => {
                let f = *f;
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.condition, f.true_value)],
                    else_: f.false_value,
                })))
            }

            Expression::Is(mut is_expr) => {
                is_expr.left = wrap_predicate_left(is_expr.left);
                Ok(Expression::Is(is_expr))
            }

            Expression::Or(op) => {
                let BinaryOp {
                    left,
                    right,
                    left_comments,
                    operator_comments,
                    trailing_comments,
                } = *op;
                let left = if matches!(left, Expression::And(_)) {
                    Expression::Paren(Box::new(Paren {
                        this: left,
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    left
                };
                let right = if matches!(right, Expression::And(_)) {
                    Expression::Paren(Box::new(Paren {
                        this: right,
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    right
                };
                Ok(Expression::Or(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments,
                    operator_comments,
                    trailing_comments,
                })))
            }

            Expression::Not(op) => {
                let inner = wrap_not_target(op.this);
                Ok(Expression::Not(Box::new(UnaryOp { this: inner })))
            }

            Expression::MapFunc(map) if map.curly_brace_syntax => {
                let MapConstructor { keys, values, .. } = *map;
                let mut args = Vec::with_capacity(keys.len() * 2);
                for (key, value) in keys.into_iter().zip(values.into_iter()) {
                    args.push(key);
                    args.push(value);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "map".to_string(),
                    args,
                ))))
            }

            Expression::Insert(mut insert) => {
                for row in insert.values.iter_mut() {
                    for value in row.iter_mut() {
                        if !matches!(value, Expression::Paren(_)) {
                            let wrapped = Expression::Paren(Box::new(Paren {
                                this: value.clone(),
                                trailing_comments: Vec::new(),
                            }));
                            *value = wrapped;
                        }
                    }
                }
                Ok(Expression::Insert(insert))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // TYPEOF -> toTypeName in ClickHouse
            Expression::Typeof(f) => Ok(Expression::Function(Box::new(Function::new(
                "toTypeName".to_string(),
                vec![f.this],
            )))),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl ClickHouseDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            "CURRENTDATABASE" | "CURRENT_DATABASE" => Ok(Expression::Function(Box::new(Function::new(
                "CURRENT_DATABASE".to_string(),
                f.args,
            )))),
            "CURRENTSCHEMAS" | "CURRENT_SCHEMAS" => Ok(Expression::Function(Box::new(Function::new(
                "CURRENT_SCHEMAS".to_string(),
                f.args,
            )))),
            "LEVENSHTEIN" | "LEVENSHTEINDISTANCE" | "EDITDISTANCE" => Ok(Expression::Function(Box::new(
                Function::new("editDistance".to_string(), f.args),
            ))),
            "CHAR" | "CHR" => Ok(Expression::Function(Box::new(Function::new(
                "CHAR".to_string(),
                f.args,
            )))),
            "STR_TO_DATE" => Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                f.args,
            )))),
            "JSONEXTRACTSTRING" => Ok(Expression::Function(Box::new(Function::new(
                "JSONExtractString".to_string(),
                f.args,
            )))),
            "MATCH" => Ok(Expression::Function(Box::new(Function::new(
                "match".to_string(),
                f.args,
            )))),
            "LIKE" if f.args.len() == 2 => {
                let left = f.args[0].clone();
                let right = f.args[1].clone();
                Ok(Expression::Like(Box::new(LikeOp::new(left, right))))
            }
            "NOTLIKE" if f.args.len() == 2 => {
                let left = f.args[0].clone();
                let right = f.args[1].clone();
                let like = Expression::Like(Box::new(LikeOp::new(left, right)));
                Ok(Expression::Not(Box::new(UnaryOp { this: like })))
            }
            "ILIKE" if f.args.len() == 2 => {
                let left = f.args[0].clone();
                let right = f.args[1].clone();
                Ok(Expression::ILike(Box::new(LikeOp::new(left, right))))
            }
            "AND" if f.args.len() >= 2 => {
                let mut iter = f.args.into_iter();
                let mut expr = iter.next().unwrap();
                for arg in iter {
                    expr = Expression::And(Box::new(BinaryOp::new(expr, arg)));
                }
                Ok(expr)
            }
            "OR" if f.args.len() >= 2 => {
                let mut iter = f.args.into_iter();
                let mut expr = iter.next().unwrap();
                for arg in iter {
                    expr = Expression::Or(Box::new(BinaryOp::new(expr, arg)));
                }
                self.transform_expr(expr)
            }
            // TYPEOF -> toTypeName in ClickHouse
            "TYPEOF" => Ok(Expression::Function(Box::new(Function::new(
                "toTypeName".to_string(),
                f.args,
            )))),

            // DATE_TRUNC: ClickHouse uses dateTrunc (camelCase)
            "DATE_TRUNC" | "DATETRUNC" => Ok(Expression::Function(Box::new(Function::new(
                "dateTrunc".to_string(),
                f.args,
            )))),
            "TOSTARTOFDAY" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("dateTrunc".to_string(), vec![Expression::string("DAY"), f.args[0].clone()]),
            ))),

            // SUBSTRING_INDEX: preserve original case (substringIndex in ClickHouse)
            "SUBSTRING_INDEX" => Ok(Expression::Function(Box::new(Function::new(
                f.name.clone(),
                f.args,
            )))),

            // IS_NAN / ISNAN -> isNaN (ClickHouse camelCase)
            "IS_NAN" | "ISNAN" => Ok(Expression::Function(Box::new(Function::new(
                "isNaN".to_string(),
                f.args,
            )))),

            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // COUNT_IF -> countIf
            "COUNT_IF" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "countIf".to_string(),
                f.args,
            )))),

            // SUM_IF -> sumIf
            "SUM_IF" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "sumIf".to_string(),
                f.args,
            )))),

            // AVG_IF -> avgIf
            "AVG_IF" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "avgIf".to_string(),
                f.args,
            )))),

            // ANY_VALUE -> any in ClickHouse
            "ANY_VALUE" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("any".to_string(), f.args),
            ))),

            // GROUP_CONCAT -> groupArray + arrayStringConcat
            "GROUP_CONCAT" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let group_array = Expression::Function(Box::new(Function::new(
                    "groupArray".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array],
                    ))))
                }
            }

            // STRING_AGG -> groupArray + arrayStringConcat
            "STRING_AGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let group_array = Expression::Function(Box::new(Function::new(
                    "groupArray".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array],
                    ))))
                }
            }

            // LISTAGG -> groupArray + arrayStringConcat
            "LISTAGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let group_array = Expression::Function(Box::new(Function::new(
                    "groupArray".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayStringConcat".to_string(),
                        vec![group_array],
                    ))))
                }
            }

            // ARRAY_AGG -> groupArray
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("groupArray".to_string(), f.args),
            ))),

            // STDDEV -> stddevSamp in ClickHouse (sample stddev)
            "STDDEV" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "stddevSamp".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // STDDEV_POP -> stddevPop
            "STDDEV_POP" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "stddevPop".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // VARIANCE -> varSamp in ClickHouse
            "VARIANCE" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "varSamp".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // VAR_POP -> varPop
            "VAR_POP" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "varPop".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // MEDIAN -> median
            "MEDIAN" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "median".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // APPROX_COUNT_DISTINCT -> uniq in ClickHouse
            "APPROX_COUNT_DISTINCT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("uniq".to_string(), f.args),
            ))),

            // APPROX_DISTINCT -> uniq
            "APPROX_DISTINCT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("uniq".to_string(), f.args),
            ))),

            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // ClickHouse type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
