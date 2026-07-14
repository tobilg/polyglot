use super::{temporal, NormalizationContext, RewriteOutcome};
use crate::dialects::DialectType;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    ArrayGenerateRange,
    ArrayLengthConvert,
    ArraySyntaxConvert,
    ElementAtConvert,
    BigQueryArraySelectAsStructToSnowflake,
    ArrayIndexConvert,
    ArrayConcatBracketConvert,
    StructToRow,
    SparkStructConvert,
    ArrayContainsConvert,
    MapFromArraysConvert,
    GenerateSeriesConvert,
    ArraySumConvert,
    ArraySizeConvert,
    ArrayAnyConvert,
    ArrayRemoveConvert,
    ArrayReverseConvert,
    ArraySizeDrill,
    ArrayExceptConvert,
    ArrayPositionSnowflakeSwap,
    ArrayDistinctConvert,
    ArrayDistinctClickHouse,
    ArrayContainsDuckDBConvert,
    SnowflakeArrayPositionToDuckDB,
}

pub(super) fn rewrite(
    action: Action,
    expression: Expression,
    context: &NormalizationContext,
) -> Result<RewriteOutcome> {
    let source = context.source;
    let target = context.target;
    let e = expression;
    let expression = (|| -> Result<Expression> {
        match action {
            Action::ArrayGenerateRange => {
                let f = if let Expression::Function(f) = e {
                    *f
                } else {
                    unreachable!("action only triggered for Function expressions")
                };
                let start = f.args[0].clone();
                let end = f.args[1].clone();
                let step = f.args.get(2).cloned();

                // Helper: compute end - 1 for converting exclusive→inclusive end.
                // When end is a literal number, simplify to a computed literal.
                fn exclusive_to_inclusive_end(end: &Expression) -> Expression {
                    // Try to simplify literal numbers
                    match end {
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
                            let Literal::Number(n) = lit.as_ref() else {
                                unreachable!()
                            };
                            if let Ok(val) = n.parse::<i64>() {
                                return Expression::number(val - 1);
                            }
                        }
                        Expression::Neg(u) => {
                            if let Expression::Literal(lit) = &u.this {
                                if let Literal::Number(n) = lit.as_ref() {
                                    if let Ok(val) = n.parse::<i64>() {
                                        return Expression::number(-val - 1);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    // Non-literal: produce end - 1 expression
                    Expression::Sub(Box::new(BinaryOp::new(end.clone(), Expression::number(1))))
                }

                match target {
                    // Snowflake ARRAY_GENERATE_RANGE and DuckDB RANGE both use exclusive end,
                    // so no adjustment needed — just rename the function.
                    DialectType::Snowflake => {
                        let mut args = vec![start, end];
                        if let Some(s) = step {
                            args.push(s);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_GENERATE_RANGE".to_string(),
                            args,
                        ))))
                    }
                    DialectType::DuckDB => {
                        let mut args = vec![start, end];
                        if let Some(s) = step {
                            args.push(s);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            "RANGE".to_string(),
                            args,
                        ))))
                    }
                    // These dialects use inclusive end, so convert exclusive→inclusive.
                    // Presto/Trino: simplify literal numbers (3 → 2).
                    DialectType::Presto | DialectType::Trino => {
                        let end_inclusive = exclusive_to_inclusive_end(&end);
                        let mut args = vec![start, end_inclusive];
                        if let Some(s) = step {
                            args.push(s);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            "SEQUENCE".to_string(),
                            args,
                        ))))
                    }
                    // PostgreSQL, Redshift, BigQuery: keep as end - 1 expression form.
                    DialectType::PostgreSQL | DialectType::Redshift => {
                        let end_minus_1 = Expression::Sub(Box::new(BinaryOp::new(
                            end.clone(),
                            Expression::number(1),
                        )));
                        let mut args = vec![start, end_minus_1];
                        if let Some(s) = step {
                            args.push(s);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            "GENERATE_SERIES".to_string(),
                            args,
                        ))))
                    }
                    DialectType::BigQuery => {
                        let end_minus_1 = Expression::Sub(Box::new(BinaryOp::new(
                            end.clone(),
                            Expression::number(1),
                        )));
                        let mut args = vec![start, end_minus_1];
                        if let Some(s) = step {
                            args.push(s);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            "GENERATE_ARRAY".to_string(),
                            args,
                        ))))
                    }
                    _ => Ok(Expression::Function(Box::new(Function::new(
                        f.name, f.args,
                    )))),
                }
            }

            Action::ArrayLengthConvert => {
                // Extract the argument from the expression
                let arg = match e {
                    Expression::Cardinality(ref f) => f.this.clone(),
                    Expression::ArrayLength(ref f) => f.this.clone(),
                    Expression::ArraySize(ref f) => f.this.clone(),
                    _ => return Ok(e),
                };
                match target {
                    DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                        Ok(Expression::Function(Box::new(Function::new(
                            "SIZE".to_string(),
                            vec![arg],
                        ))))
                    }
                    DialectType::Presto | DialectType::Trino | DialectType::Athena => Ok(
                        Expression::Cardinality(Box::new(crate::expressions::UnaryFunc::new(arg))),
                    ),
                    DialectType::BigQuery => Ok(Expression::ArrayLength(Box::new(
                        crate::expressions::UnaryFunc::new(arg),
                    ))),
                    DialectType::DuckDB => Ok(Expression::ArrayLength(Box::new(
                        crate::expressions::UnaryFunc::new(arg),
                    ))),
                    DialectType::PostgreSQL | DialectType::Redshift => {
                        // PostgreSQL ARRAY_LENGTH requires dimension arg
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_LENGTH".to_string(),
                            vec![arg, Expression::number(1)],
                        ))))
                    }
                    DialectType::Snowflake => Ok(Expression::ArraySize(Box::new(
                        crate::expressions::UnaryFunc::new(arg),
                    ))),
                    _ => Ok(e), // Keep original
                }
            }

            Action::ArraySyntaxConvert => {
                match e {
                    // ARRAY[1, 2] (ArrayFunc bracket_notation=false) -> set bracket_notation=true
                    // so the generator uses dialect-specific output (ARRAY() for Spark, [] for BigQuery)
                    Expression::ArrayFunc(arr) if !arr.bracket_notation => Ok(
                        Expression::ArrayFunc(Box::new(crate::expressions::ArrayConstructor {
                            expressions: arr.expressions,
                            bracket_notation: true,
                            use_list_keyword: false,
                        })),
                    ),
                    // ARRAY(y) function style -> ArrayFunc for target dialect
                    // bracket_notation=true for BigQuery/DuckDB/ClickHouse/StarRocks (output []), false for Presto (output ARRAY[])
                    Expression::Function(f) if f.name.eq_ignore_ascii_case("ARRAY") => {
                        let bracket = matches!(
                            target,
                            DialectType::BigQuery
                                | DialectType::DuckDB
                                | DialectType::Snowflake
                                | DialectType::ClickHouse
                                | DialectType::StarRocks
                        );
                        Ok(Expression::ArrayFunc(Box::new(
                            crate::expressions::ArrayConstructor {
                                expressions: f.args,
                                bracket_notation: bracket,
                                use_list_keyword: false,
                            },
                        )))
                    }
                    _ => Ok(e),
                }
            }

            Action::ElementAtConvert => {
                // ELEMENT_AT(arr, idx) -> arr[idx] for PostgreSQL, arr[SAFE_ORDINAL(idx)] for BigQuery
                let (arr, idx) = if let Expression::ElementAt(bf) = e {
                    (bf.this, bf.expression)
                } else if let Expression::Function(ref f) = e {
                    if f.args.len() >= 2 {
                        if let Expression::Function(f) = e {
                            let mut args = f.args;
                            let arr = args.remove(0);
                            let idx = args.remove(0);
                            (arr, idx)
                        } else {
                            unreachable!("outer condition already matched Expression::Function")
                        }
                    } else {
                        return Ok(e);
                    }
                } else {
                    return Ok(e);
                };
                match target {
                    DialectType::PostgreSQL => {
                        // Wrap array in parens for PostgreSQL: (ARRAY[1,2,3])[4]
                        let arr_expr = Expression::Paren(Box::new(Paren {
                            this: arr,
                            trailing_comments: vec![],
                        }));
                        Ok(Expression::Subscript(Box::new(
                            crate::expressions::Subscript {
                                this: arr_expr,
                                index: idx,
                            },
                        )))
                    }
                    DialectType::BigQuery => {
                        // BigQuery: convert ARRAY[...] to bare [...] for subscript
                        let arr_expr = match arr {
                            Expression::ArrayFunc(af) => Expression::ArrayFunc(Box::new(
                                crate::expressions::ArrayConstructor {
                                    expressions: af.expressions,
                                    bracket_notation: true,
                                    use_list_keyword: false,
                                },
                            )),
                            other => other,
                        };
                        let safe_ordinal = Expression::Function(Box::new(Function::new(
                            "SAFE_ORDINAL".to_string(),
                            vec![idx],
                        )));
                        Ok(Expression::Subscript(Box::new(
                            crate::expressions::Subscript {
                                this: arr_expr,
                                index: safe_ordinal,
                            },
                        )))
                    }
                    _ => Ok(Expression::Function(Box::new(Function::new(
                        "ELEMENT_AT".to_string(),
                        vec![arr, idx],
                    )))),
                }
            }

            Action::BigQueryArraySelectAsStructToSnowflake => {
                // ARRAY(SELECT AS STRUCT x1 AS x1, x2 AS x2 FROM t)
                // -> (SELECT ARRAY_AGG(OBJECT_CONSTRUCT('x1', x1, 'x2', x2)) FROM t)
                if let Expression::Function(mut f) = e {
                    let is_match = f.args.len() == 1
                        && matches!(&f.args[0], Expression::Select(s) if s.kind.as_deref() == Some("STRUCT"));
                    if is_match {
                        let inner_select = match f.args.remove(0) {
                            Expression::Select(s) => *s,
                            _ => {
                                unreachable!("argument already verified to be a Select expression")
                            }
                        };
                        // Build OBJECT_CONSTRUCT args from SELECT expressions
                        let mut oc_args = Vec::new();
                        for expr in &inner_select.expressions {
                            match expr {
                                Expression::Alias(a) => {
                                    let key = Expression::Literal(Box::new(Literal::String(
                                        a.alias.name.clone(),
                                    )));
                                    let value = a.this.clone();
                                    oc_args.push(key);
                                    oc_args.push(value);
                                }
                                Expression::Column(c) => {
                                    let key = Expression::Literal(Box::new(Literal::String(
                                        c.name.name.clone(),
                                    )));
                                    oc_args.push(key);
                                    oc_args.push(expr.clone());
                                }
                                _ => {
                                    oc_args.push(expr.clone());
                                }
                            }
                        }
                        let object_construct = Expression::Function(Box::new(Function::new(
                            "OBJECT_CONSTRUCT".to_string(),
                            oc_args,
                        )));
                        let array_agg = Expression::Function(Box::new(Function::new(
                            "ARRAY_AGG".to_string(),
                            vec![object_construct],
                        )));
                        let mut new_select = crate::expressions::Select::new();
                        new_select.expressions = vec![array_agg];
                        new_select.from = inner_select.from.clone();
                        new_select.where_clause = inner_select.where_clause.clone();
                        new_select.group_by = inner_select.group_by.clone();
                        new_select.having = inner_select.having.clone();
                        new_select.joins = inner_select.joins.clone();
                        Ok(Expression::Subquery(Box::new(
                            crate::expressions::Subquery {
                                this: Expression::Select(Box::new(new_select)),
                                alias: None,
                                column_aliases: Vec::new(),
                                alias_explicit_as: false,
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
                            },
                        )))
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArrayIndexConvert => {
                // Subscript index: 1-based to 0-based for BigQuery
                if let Expression::Subscript(mut sub) = e {
                    if let Expression::Literal(ref lit) = sub.index {
                        if let Literal::Number(ref n) = lit.as_ref() {
                            if let Ok(val) = n.parse::<i64>() {
                                sub.index = Expression::Literal(Box::new(Literal::Number(
                                    (val - 1).to_string(),
                                )));
                            }
                        }
                    }
                    Ok(Expression::Subscript(sub))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayConcatBracketConvert => {
                // Expression::Array/ArrayFunc -> target-specific
                // For PostgreSQL: Array -> ArrayFunc (bracket_notation: false)
                // For Redshift: Array/ArrayFunc -> Function("ARRAY", args) to produce ARRAY(1, 2) with parens
                match e {
                    Expression::Array(arr) => {
                        if matches!(target, DialectType::Redshift) {
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY".to_string(),
                                arr.expressions,
                            ))))
                        } else {
                            Ok(Expression::ArrayFunc(Box::new(
                                crate::expressions::ArrayConstructor {
                                    expressions: arr.expressions,
                                    bracket_notation: false,
                                    use_list_keyword: false,
                                },
                            )))
                        }
                    }
                    Expression::ArrayFunc(arr) => {
                        // Only for Redshift: convert bracket-notation ArrayFunc to Function("ARRAY")
                        if matches!(target, DialectType::Redshift) {
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY".to_string(),
                                arr.expressions,
                            ))))
                        } else {
                            Ok(Expression::ArrayFunc(arr))
                        }
                    }
                    _ => Ok(e),
                }
            }

            Action::StructToRow => {
                // DuckDB struct/dict -> BigQuery STRUCT(value AS key, ...) / Presto ROW
                // Handles both Expression::Struct and Expression::MapFunc(curly_brace_syntax=true)

                // Extract key-value pairs from either Struct or MapFunc
                let kv_pairs: Option<Vec<(String, Expression)>> = match &e {
                    Expression::Struct(s) => Some(
                        s.fields
                            .iter()
                            .map(|(opt_name, field_expr)| {
                                if let Some(name) = opt_name {
                                    (name.clone(), field_expr.clone())
                                } else if let Expression::NamedArgument(na) = field_expr {
                                    (na.name.name.clone(), na.value.clone())
                                } else {
                                    (String::new(), field_expr.clone())
                                }
                            })
                            .collect(),
                    ),
                    Expression::MapFunc(m) if m.curly_brace_syntax => Some(
                        m.keys
                            .iter()
                            .zip(m.values.iter())
                            .map(|(key, value)| {
                                let key_name = match key {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        let Literal::String(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        s.clone()
                                    }
                                    Expression::Identifier(id) => id.name.clone(),
                                    _ => String::new(),
                                };
                                (key_name, value.clone())
                            })
                            .collect(),
                    ),
                    _ => None,
                };

                if let Some(pairs) = kv_pairs {
                    let mut named_args = Vec::new();
                    for (key_name, value) in pairs {
                        if matches!(target, DialectType::BigQuery) && !key_name.is_empty() {
                            named_args.push(Expression::Alias(Box::new(
                                crate::expressions::Alias::new(value, Identifier::new(key_name)),
                            )));
                        } else if matches!(target, DialectType::Presto | DialectType::Trino) {
                            named_args.push(value);
                        } else {
                            named_args.push(value);
                        }
                    }

                    if matches!(target, DialectType::BigQuery) {
                        Ok(Expression::Function(Box::new(Function::new(
                            "STRUCT".to_string(),
                            named_args,
                        ))))
                    } else if matches!(target, DialectType::Presto | DialectType::Trino) {
                        // For Presto/Trino, infer types and wrap in CAST(ROW(...) AS ROW(name TYPE, ...))
                        let row_func = Expression::Function(Box::new(Function::new(
                            "ROW".to_string(),
                            named_args,
                        )));

                        // Try to infer types for each pair
                        let kv_pairs_again: Option<Vec<(String, Expression)>> = match &e {
                            Expression::Struct(s) => Some(
                                s.fields
                                    .iter()
                                    .map(|(opt_name, field_expr)| {
                                        if let Some(name) = opt_name {
                                            (name.clone(), field_expr.clone())
                                        } else if let Expression::NamedArgument(na) = field_expr {
                                            (na.name.name.clone(), na.value.clone())
                                        } else {
                                            (String::new(), field_expr.clone())
                                        }
                                    })
                                    .collect(),
                            ),
                            Expression::MapFunc(m) if m.curly_brace_syntax => Some(
                                m.keys
                                    .iter()
                                    .zip(m.values.iter())
                                    .map(|(key, value)| {
                                        let key_name = match key {
                                            Expression::Literal(lit)
                                                if matches!(lit.as_ref(), Literal::String(_)) =>
                                            {
                                                let Literal::String(s) = lit.as_ref() else {
                                                    unreachable!()
                                                };
                                                s.clone()
                                            }
                                            Expression::Identifier(id) => id.name.clone(),
                                            _ => String::new(),
                                        };
                                        (key_name, value.clone())
                                    })
                                    .collect(),
                            ),
                            _ => None,
                        };

                        if let Some(pairs) = kv_pairs_again {
                            // Infer types for all values
                            let mut all_inferred = true;
                            let mut fields = Vec::new();
                            for (name, value) in &pairs {
                                let inferred_type = match value {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::Number(_)) =>
                                    {
                                        let Literal::Number(n) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        if n.contains('.') {
                                            Some(DataType::Double {
                                                precision: None,
                                                scale: None,
                                            })
                                        } else {
                                            Some(DataType::Int {
                                                length: None,
                                                integer_spelling: true,
                                            })
                                        }
                                    }
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        Some(DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        })
                                    }
                                    Expression::Boolean(_) => Some(DataType::Boolean),
                                    _ => None,
                                };
                                if let Some(dt) = inferred_type {
                                    fields.push(crate::expressions::StructField::new(
                                        name.clone(),
                                        dt,
                                    ));
                                } else {
                                    all_inferred = false;
                                    break;
                                }
                            }

                            if all_inferred && !fields.is_empty() {
                                let row_type = DataType::Struct {
                                    fields,
                                    nested: true,
                                };
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: row_func,
                                    to: row_type,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                Ok(row_func)
                            }
                        } else {
                            Ok(row_func)
                        }
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "ROW".to_string(),
                            named_args,
                        ))))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::SparkStructConvert => {
                // Spark STRUCT(val AS name, ...) -> Presto CAST(ROW(...) AS ROW(name TYPE, ...))
                // or DuckDB {'name': val, ...}
                if let Expression::Function(f) = e {
                    // Extract name-value pairs from aliased args
                    let mut pairs: Vec<(String, Expression)> = Vec::new();
                    for arg in &f.args {
                        match arg {
                            Expression::Alias(a) => {
                                pairs.push((a.alias.name.clone(), a.this.clone()));
                            }
                            _ => {
                                pairs.push((String::new(), arg.clone()));
                            }
                        }
                    }

                    match target {
                        DialectType::DuckDB => {
                            // Convert to DuckDB struct literal {'name': value, ...}
                            let mut keys = Vec::new();
                            let mut values = Vec::new();
                            for (name, value) in &pairs {
                                keys.push(Expression::Literal(Box::new(Literal::String(
                                    name.clone(),
                                ))));
                                values.push(value.clone());
                            }
                            Ok(Expression::MapFunc(Box::new(
                                crate::expressions::MapConstructor {
                                    keys,
                                    values,
                                    curly_brace_syntax: true,
                                    with_map_keyword: false,
                                },
                            )))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // Convert to CAST(ROW(val1, val2) AS ROW(name1 TYPE1, name2 TYPE2))
                            let row_args: Vec<Expression> =
                                pairs.iter().map(|(_, v)| v.clone()).collect();
                            let row_func = Expression::Function(Box::new(Function::new(
                                "ROW".to_string(),
                                row_args,
                            )));

                            // Infer types
                            let mut all_inferred = true;
                            let mut fields = Vec::new();
                            for (name, value) in &pairs {
                                let inferred_type = match value {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::Number(_)) =>
                                    {
                                        let Literal::Number(n) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        if n.contains('.') {
                                            Some(DataType::Double {
                                                precision: None,
                                                scale: None,
                                            })
                                        } else {
                                            Some(DataType::Int {
                                                length: None,
                                                integer_spelling: true,
                                            })
                                        }
                                    }
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        Some(DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        })
                                    }
                                    Expression::Boolean(_) => Some(DataType::Boolean),
                                    _ => None,
                                };
                                if let Some(dt) = inferred_type {
                                    fields.push(crate::expressions::StructField::new(
                                        name.clone(),
                                        dt,
                                    ));
                                } else {
                                    all_inferred = false;
                                    break;
                                }
                            }

                            if all_inferred && !fields.is_empty() {
                                let row_type = DataType::Struct {
                                    fields,
                                    nested: true,
                                };
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: row_func,
                                    to: row_type,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                Ok(row_func)
                            }
                        }
                        _ => Ok(Expression::Function(f)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArrayContainsConvert => {
                if let Expression::ArrayContains(f) = e {
                    match target {
                        DialectType::Presto | DialectType::Trino => {
                            // ARRAY_CONTAINS(arr, val) -> CONTAINS(arr, val)
                            Ok(Expression::Function(Box::new(Function::new(
                                "CONTAINS".to_string(),
                                vec![f.this, f.expression],
                            ))))
                        }
                        DialectType::Snowflake => {
                            // ARRAY_CONTAINS(arr, val) -> ARRAY_CONTAINS(CAST(val AS VARIANT), arr)
                            let cast_val = Expression::Cast(Box::new(crate::expressions::Cast {
                                this: f.expression,
                                to: crate::expressions::DataType::Custom {
                                    name: "VARIANT".to_string(),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_CONTAINS".to_string(),
                                vec![cast_val, f.this],
                            ))))
                        }
                        _ => Ok(Expression::ArrayContains(f)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::MapFromArraysConvert => {
                // Expression::MapFromArrays -> target-specific
                if let Expression::MapFromArrays(mfa) = e {
                    let keys = mfa.this;
                    let values = mfa.expression;
                    match target {
                        DialectType::Snowflake => Ok(Expression::Function(Box::new(
                            Function::new("OBJECT_CONSTRUCT".to_string(), vec![keys, values]),
                        ))),
                        _ => {
                            // Hive, Presto, DuckDB, etc.: MAP(keys, values)
                            Ok(Expression::Function(Box::new(Function::new(
                                "MAP".to_string(),
                                vec![keys, values],
                            ))))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::GenerateSeriesConvert => {
                // GENERATE_SERIES(start, end[, step]) -> SEQUENCE for Spark/Databricks/Hive, wrapped in UNNEST/EXPLODE
                // For DuckDB target: wrap in UNNEST(GENERATE_SERIES(...))
                // For PG/Redshift target: keep as GENERATE_SERIES but normalize interval string step
                if let Expression::Function(f) = e {
                    if f.name.eq_ignore_ascii_case("GENERATE_SERIES") && f.args.len() >= 2 {
                        let start = f.args[0].clone();
                        let end = f.args[1].clone();
                        let step = f.args.get(2).cloned();

                        // Normalize step: convert string interval like '1day' or '  2   days  ' to INTERVAL expression
                        let step = step.map(|s| temporal::normalize_interval_string(s, target));

                        // Helper: wrap CURRENT_TIMESTAMP in CAST(... AS TIMESTAMP) for Presto/Trino/Spark
                        let maybe_cast_timestamp = |arg: Expression| -> Expression {
                            if matches!(
                                target,
                                DialectType::Presto
                                    | DialectType::Trino
                                    | DialectType::Athena
                                    | DialectType::Spark
                                    | DialectType::Databricks
                                    | DialectType::Hive
                            ) {
                                match &arg {
                                    Expression::CurrentTimestamp(_) => {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg,
                                            to: DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    }
                                    _ => arg,
                                }
                            } else {
                                arg
                            }
                        };

                        let start = maybe_cast_timestamp(start);
                        let end = maybe_cast_timestamp(end);

                        // For PostgreSQL/Redshift target, keep as GENERATE_SERIES
                        if matches!(target, DialectType::PostgreSQL | DialectType::Redshift) {
                            let mut gs_args = vec![start, end];
                            if let Some(step) = step {
                                gs_args.push(step);
                            }
                            return Ok(Expression::Function(Box::new(Function::new(
                                "GENERATE_SERIES".to_string(),
                                gs_args,
                            ))));
                        }

                        // For DuckDB target: wrap in UNNEST(GENERATE_SERIES(...))
                        if matches!(target, DialectType::DuckDB) {
                            let mut gs_args = vec![start, end];
                            if let Some(step) = step {
                                gs_args.push(step);
                            }
                            let gs = Expression::Function(Box::new(Function::new(
                                "GENERATE_SERIES".to_string(),
                                gs_args,
                            )));
                            return Ok(Expression::Function(Box::new(Function::new(
                                "UNNEST".to_string(),
                                vec![gs],
                            ))));
                        }

                        let mut seq_args = vec![start, end];
                        if let Some(step) = step {
                            seq_args.push(step);
                        }

                        let seq = Expression::Function(Box::new(Function::new(
                            "SEQUENCE".to_string(),
                            seq_args,
                        )));

                        match target {
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                // Wrap in UNNEST
                                Ok(Expression::Function(Box::new(Function::new(
                                    "UNNEST".to_string(),
                                    vec![seq],
                                ))))
                            }
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                // Wrap in EXPLODE
                                Ok(Expression::Function(Box::new(Function::new(
                                    "EXPLODE".to_string(),
                                    vec![seq],
                                ))))
                            }
                            _ => {
                                // Just SEQUENCE for others
                                Ok(seq)
                            }
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArraySumConvert => {
                // ARRAY_SUM(arr) -> dialect-specific
                if let Expression::Function(f) = e {
                    let args = f.args;
                    match target {
                        DialectType::DuckDB => Ok(Expression::Function(Box::new(Function::new(
                            "LIST_SUM".to_string(),
                            args,
                        )))),
                        DialectType::Spark | DialectType::Databricks => {
                            // AGGREGATE(arr, 0, (acc, x) -> acc + x, acc -> acc)
                            let arr = args.into_iter().next().unwrap();
                            let zero =
                                Expression::Literal(Box::new(Literal::Number("0".to_string())));
                            let acc_id = Identifier::new("acc");
                            let x_id = Identifier::new("x");
                            let acc = Expression::Identifier(acc_id.clone());
                            let x = Expression::Identifier(x_id.clone());
                            let add = Expression::Add(Box::new(BinaryOp {
                                left: acc.clone(),
                                right: x,
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            let lambda1 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![acc_id.clone(), x_id],
                                    body: add,
                                    colon: false,
                                    parameter_types: Vec::new(),
                                }));
                            let lambda2 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![acc_id],
                                    body: acc,
                                    colon: false,
                                    parameter_types: Vec::new(),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "AGGREGATE".to_string(),
                                vec![arr, zero, lambda1, lambda2],
                            ))))
                        }
                        DialectType::Presto | DialectType::Athena => {
                            // Presto/Athena keep ARRAY_SUM natively
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_SUM".to_string(),
                                args,
                            ))))
                        }
                        DialectType::Trino => {
                            // REDUCE(arr, 0, (acc, x) -> acc + x, acc -> acc)
                            if args.len() == 1 {
                                let arr = args.into_iter().next().unwrap();
                                let zero =
                                    Expression::Literal(Box::new(Literal::Number("0".to_string())));
                                let acc_id = Identifier::new("acc");
                                let x_id = Identifier::new("x");
                                let acc = Expression::Identifier(acc_id.clone());
                                let x = Expression::Identifier(x_id.clone());
                                let add = Expression::Add(Box::new(BinaryOp {
                                    left: acc.clone(),
                                    right: x,
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                }));
                                let lambda1 =
                                    Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                        parameters: vec![acc_id.clone(), x_id],
                                        body: add,
                                        colon: false,
                                        parameter_types: Vec::new(),
                                    }));
                                let lambda2 =
                                    Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                        parameters: vec![acc_id],
                                        body: acc,
                                        colon: false,
                                        parameter_types: Vec::new(),
                                    }));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REDUCE".to_string(),
                                    vec![arr, zero, lambda1, lambda2],
                                ))))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_SUM".to_string(),
                                    args,
                                ))))
                            }
                        }
                        DialectType::ClickHouse => {
                            // arraySum(lambda, arr) or arraySum(arr)
                            Ok(Expression::Function(Box::new(Function::new(
                                "arraySum".to_string(),
                                args,
                            ))))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_SUM".to_string(),
                            args,
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArraySizeConvert => {
                if let Expression::Function(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "REPEATED_COUNT".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayAnyConvert => {
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    if args.len() == 2 {
                        let arr = args.remove(0);
                        let lambda = args.remove(0);

                        // Extract lambda parameter name and body
                        let (param_name, pred_body) = if let Expression::Lambda(ref lam) = lambda {
                            let name = if let Some(p) = lam.parameters.first() {
                                p.name.clone()
                            } else {
                                "x".to_string()
                            };
                            (name, lam.body.clone())
                        } else {
                            ("x".to_string(), lambda.clone())
                        };

                        // Helper: build a function call Expression
                        let make_func = |name: &str, args: Vec<Expression>| -> Expression {
                            Expression::Function(Box::new(Function::new(name.to_string(), args)))
                        };

                        // Helper: build (len_func(arr) = 0 OR len_func(filter_expr) <> 0) wrapped in Paren
                        let build_filter_pattern = |len_func: &str,
                                                    len_args_extra: Vec<Expression>,
                                                    filter_expr: Expression|
                         -> Expression {
                            // len_func(arr, ...extra) = 0
                            let mut len_arr_args = vec![arr.clone()];
                            len_arr_args.extend(len_args_extra.clone());
                            let len_arr = make_func(len_func, len_arr_args);
                            let eq_zero = Expression::Eq(Box::new(BinaryOp::new(
                                len_arr,
                                Expression::number(0),
                            )));

                            // len_func(filter_expr, ...extra) <> 0
                            let mut len_filter_args = vec![filter_expr];
                            len_filter_args.extend(len_args_extra);
                            let len_filter = make_func(len_func, len_filter_args);
                            let neq_zero = Expression::Neq(Box::new(BinaryOp::new(
                                len_filter,
                                Expression::number(0),
                            )));

                            // (eq_zero OR neq_zero)
                            let or_expr =
                                Expression::Or(Box::new(BinaryOp::new(eq_zero, neq_zero)));
                            Expression::Paren(Box::new(Paren {
                                this: or_expr,
                                trailing_comments: Vec::new(),
                            }))
                        };

                        match target {
                            DialectType::Trino | DialectType::Presto | DialectType::Athena => {
                                Ok(make_func("ANY_MATCH", vec![arr, lambda]))
                            }
                            DialectType::ClickHouse => {
                                // (LENGTH(arr) = 0 OR LENGTH(arrayFilter(x -> pred, arr)) <> 0)
                                // ClickHouse arrayFilter takes lambda first, then array
                                let filter_expr =
                                    make_func("arrayFilter", vec![lambda, arr.clone()]);
                                Ok(build_filter_pattern("LENGTH", vec![], filter_expr))
                            }
                            DialectType::Databricks | DialectType::Spark => {
                                // (SIZE(arr) = 0 OR SIZE(FILTER(arr, x -> pred)) <> 0)
                                let filter_expr = make_func("FILTER", vec![arr.clone(), lambda]);
                                Ok(build_filter_pattern("SIZE", vec![], filter_expr))
                            }
                            DialectType::DuckDB => {
                                // (ARRAY_LENGTH(arr) = 0 OR ARRAY_LENGTH(LIST_FILTER(arr, x -> pred)) <> 0)
                                let filter_expr =
                                    make_func("LIST_FILTER", vec![arr.clone(), lambda]);
                                Ok(build_filter_pattern("ARRAY_LENGTH", vec![], filter_expr))
                            }
                            DialectType::Teradata => {
                                // (CARDINALITY(arr) = 0 OR CARDINALITY(FILTER(arr, x -> pred)) <> 0)
                                let filter_expr = make_func("FILTER", vec![arr.clone(), lambda]);
                                Ok(build_filter_pattern("CARDINALITY", vec![], filter_expr))
                            }
                            DialectType::BigQuery => {
                                // (ARRAY_LENGTH(arr) = 0 OR ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(arr) AS x WHERE pred)) <> 0)
                                // Build: SELECT x FROM UNNEST(arr) AS x WHERE pred
                                let param_col = Expression::column(&param_name);
                                let unnest_expr =
                                    Expression::Unnest(Box::new(crate::expressions::UnnestFunc {
                                        this: arr.clone(),
                                        expressions: vec![],
                                        with_ordinality: false,
                                        alias: Some(Identifier::new(&param_name)),
                                        offset_alias: None,
                                    }));
                                let mut sel = crate::expressions::Select::default();
                                sel.expressions = vec![param_col];
                                sel.from = Some(crate::expressions::From {
                                    expressions: vec![unnest_expr],
                                });
                                sel.where_clause =
                                    Some(crate::expressions::Where { this: pred_body });
                                let array_subquery =
                                    make_func("ARRAY", vec![Expression::Select(Box::new(sel))]);
                                Ok(build_filter_pattern("ARRAY_LENGTH", vec![], array_subquery))
                            }
                            DialectType::PostgreSQL => {
                                // (ARRAY_LENGTH(arr, 1) = 0 OR ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(arr) AS _t0(x) WHERE pred), 1) <> 0)
                                // Build: SELECT x FROM UNNEST(arr) AS _t0(x) WHERE pred
                                let param_col = Expression::column(&param_name);
                                // For PostgreSQL, UNNEST uses AS _t0(x) syntax - use TableAlias
                                let unnest_with_alias =
                                    Expression::Alias(Box::new(crate::expressions::Alias {
                                        this: Expression::Unnest(Box::new(
                                            crate::expressions::UnnestFunc {
                                                this: arr.clone(),
                                                expressions: vec![],
                                                with_ordinality: false,
                                                alias: None,
                                                offset_alias: None,
                                            },
                                        )),
                                        alias: Identifier::new("_t0"),
                                        column_aliases: vec![Identifier::new(&param_name)],
                                        alias_explicit_as: false,
                                        alias_keyword: None,
                                        pre_alias_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                let mut sel = crate::expressions::Select::default();
                                sel.expressions = vec![param_col];
                                sel.from = Some(crate::expressions::From {
                                    expressions: vec![unnest_with_alias],
                                });
                                sel.where_clause =
                                    Some(crate::expressions::Where { this: pred_body });
                                let array_subquery =
                                    make_func("ARRAY", vec![Expression::Select(Box::new(sel))]);
                                Ok(build_filter_pattern(
                                    "ARRAY_LENGTH",
                                    vec![Expression::number(1)],
                                    array_subquery,
                                ))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_ANY".to_string(),
                                vec![arr, lambda],
                            )))),
                        }
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_ANY".to_string(),
                            args,
                        ))))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArrayRemoveConvert => {
                // ARRAY_REMOVE(arr, target) -> LIST_FILTER/arrayFilter
                if let Expression::ArrayRemove(bf) = e {
                    let arr = bf.this;
                    let target_val = bf.expression;
                    match target {
                        DialectType::DuckDB => {
                            let u_id = crate::expressions::Identifier::new("_u");
                            let lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![u_id.clone()],
                                    body: Expression::Neq(Box::new(BinaryOp {
                                        left: Expression::Identifier(u_id),
                                        right: target_val,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    })),
                                    colon: false,
                                    parameter_types: Vec::new(),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![arr, lambda],
                            ))))
                        }
                        DialectType::ClickHouse => {
                            let u_id = crate::expressions::Identifier::new("_u");
                            let lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![u_id.clone()],
                                    body: Expression::Neq(Box::new(BinaryOp {
                                        left: Expression::Identifier(u_id),
                                        right: target_val,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    })),
                                    colon: false,
                                    parameter_types: Vec::new(),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "arrayFilter".to_string(),
                                vec![lambda, arr],
                            ))))
                        }
                        DialectType::BigQuery => {
                            // ARRAY(SELECT _u FROM UNNEST(the_array) AS _u WHERE _u <> target)
                            let u_id = crate::expressions::Identifier::new("_u");
                            let u_col = Expression::Column(Box::new(crate::expressions::Column {
                                name: u_id.clone(),
                                table: None,
                                join_mark: false,
                                trailing_comments: Vec::new(),
                                span: None,
                                inferred_type: None,
                            }));
                            let unnest_expr =
                                Expression::Unnest(Box::new(crate::expressions::UnnestFunc {
                                    this: arr,
                                    expressions: Vec::new(),
                                    with_ordinality: false,
                                    alias: None,
                                    offset_alias: None,
                                }));
                            let aliased_unnest =
                                Expression::Alias(Box::new(crate::expressions::Alias {
                                    this: unnest_expr,
                                    alias: u_id.clone(),
                                    column_aliases: Vec::new(),
                                    alias_explicit_as: false,
                                    alias_keyword: None,
                                    pre_alias_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                }));
                            let where_cond = Expression::Neq(Box::new(BinaryOp {
                                left: u_col.clone(),
                                right: target_val,
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            let subquery = Expression::Select(Box::new(
                                crate::expressions::Select::new()
                                    .column(u_col)
                                    .from(aliased_unnest)
                                    .where_(where_cond),
                            ));
                            Ok(Expression::ArrayFunc(Box::new(
                                crate::expressions::ArrayConstructor {
                                    expressions: vec![subquery],
                                    bracket_notation: false,
                                    use_list_keyword: false,
                                },
                            )))
                        }
                        _ => Ok(Expression::ArrayRemove(Box::new(
                            crate::expressions::BinaryFunc {
                                original_name: None,
                                this: arr,
                                expression: target_val,
                                inferred_type: None,
                            },
                        ))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArrayReverseConvert => {
                // ARRAY_REVERSE(x) -> arrayReverse(x) for ClickHouse
                if let Expression::ArrayReverse(af) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayReverse".to_string(),
                        vec![af.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ArraySizeDrill => {
                // ARRAY_SIZE(x) -> REPEATED_COUNT(x) for Drill
                if let Expression::ArraySize(uf) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "REPEATED_COUNT".to_string(),
                        vec![uf.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayExceptConvert => {
                if let Expression::ArrayExcept(f) = e {
                    let source_arr = f.this;
                    let exclude_arr = f.expression;
                    match target {
                        DialectType::DuckDB if matches!(source, DialectType::Snowflake) => {
                            // Snowflake ARRAY_EXCEPT -> DuckDB bag semantics:
                            // CASE WHEN source IS NULL OR exclude IS NULL THEN NULL
                            // ELSE LIST_TRANSFORM(LIST_FILTER(
                            //   LIST_ZIP(source, GENERATE_SERIES(1, LENGTH(source))),
                            //   pair -> (LENGTH(LIST_FILTER(source[1:pair[2]], e -> e IS NOT DISTINCT FROM pair[1]))
                            //            > LENGTH(LIST_FILTER(exclude, e -> e IS NOT DISTINCT FROM pair[1])))),
                            //   pair -> pair[1])
                            // END

                            // Build null check
                            let source_is_null =
                                Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: source_arr.clone(),
                                    not: false,
                                    postfix_form: false,
                                }));
                            let exclude_is_null =
                                Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: exclude_arr.clone(),
                                    not: false,
                                    postfix_form: false,
                                }));
                            let null_check =
                                Expression::Or(Box::new(crate::expressions::BinaryOp {
                                    left: source_is_null,
                                    right: exclude_is_null,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));

                            // GENERATE_SERIES(1, LENGTH(source))
                            let gen_series = Expression::Function(Box::new(Function::new(
                                "GENERATE_SERIES".to_string(),
                                vec![
                                    Expression::number(1),
                                    Expression::Function(Box::new(Function::new(
                                        "LENGTH".to_string(),
                                        vec![source_arr.clone()],
                                    ))),
                                ],
                            )));

                            // LIST_ZIP(source, GENERATE_SERIES(1, LENGTH(source)))
                            let list_zip = Expression::Function(Box::new(Function::new(
                                "LIST_ZIP".to_string(),
                                vec![source_arr.clone(), gen_series],
                            )));

                            // pair[1] and pair[2]
                            let pair_col = Expression::column("pair");
                            let pair_1 =
                                Expression::Subscript(Box::new(crate::expressions::Subscript {
                                    this: pair_col.clone(),
                                    index: Expression::number(1),
                                }));
                            let pair_2 =
                                Expression::Subscript(Box::new(crate::expressions::Subscript {
                                    this: pair_col.clone(),
                                    index: Expression::number(2),
                                }));

                            // source[1:pair[2]]
                            let source_slice =
                                Expression::ArraySlice(Box::new(crate::expressions::ArraySlice {
                                    this: source_arr.clone(),
                                    start: Some(Expression::number(1)),
                                    end: Some(pair_2),
                                }));

                            let e_col = Expression::column("e");

                            // e -> e IS NOT DISTINCT FROM pair[1]
                            let inner_lambda1 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("e")],
                                    body: Expression::NullSafeEq(Box::new(
                                        crate::expressions::BinaryOp {
                                            left: e_col.clone(),
                                            right: pair_1.clone(),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        },
                                    )),
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(source[1:pair[2]], e -> e IS NOT DISTINCT FROM pair[1])
                            let inner_filter1 = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![source_slice, inner_lambda1],
                            )));

                            // LENGTH(LIST_FILTER(source[1:pair[2]], ...))
                            let len1 = Expression::Function(Box::new(Function::new(
                                "LENGTH".to_string(),
                                vec![inner_filter1],
                            )));

                            // e -> e IS NOT DISTINCT FROM pair[1]
                            let inner_lambda2 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("e")],
                                    body: Expression::NullSafeEq(Box::new(
                                        crate::expressions::BinaryOp {
                                            left: e_col,
                                            right: pair_1.clone(),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        },
                                    )),
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(exclude, e -> e IS NOT DISTINCT FROM pair[1])
                            let inner_filter2 = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![exclude_arr.clone(), inner_lambda2],
                            )));

                            // LENGTH(LIST_FILTER(exclude, ...))
                            let len2 = Expression::Function(Box::new(Function::new(
                                "LENGTH".to_string(),
                                vec![inner_filter2],
                            )));

                            // (LENGTH(...) > LENGTH(...))
                            let cond = Expression::Paren(Box::new(Paren {
                                this: Expression::Gt(Box::new(crate::expressions::BinaryOp {
                                    left: len1,
                                    right: len2,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                })),
                                trailing_comments: vec![],
                            }));

                            // pair -> (condition)
                            let filter_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("pair")],
                                    body: cond,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(LIST_ZIP(...), pair -> ...)
                            let outer_filter = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![list_zip, filter_lambda],
                            )));

                            // pair -> pair[1]
                            let transform_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("pair")],
                                    body: pair_1,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_TRANSFORM(LIST_FILTER(...), pair -> pair[1])
                            let list_transform = Expression::Function(Box::new(Function::new(
                                "LIST_TRANSFORM".to_string(),
                                vec![outer_filter, transform_lambda],
                            )));

                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(null_check, Expression::Null(Null))],
                                else_: Some(list_transform),
                                comments: Vec::new(),
                                inferred_type: None,
                            })))
                        }
                        DialectType::DuckDB => {
                            // ARRAY_EXCEPT(source, exclude) -> set semantics for DuckDB:
                            // CASE WHEN source IS NULL OR exclude IS NULL THEN NULL
                            // ELSE LIST_FILTER(LIST_DISTINCT(source),
                            //   e -> LENGTH(LIST_FILTER(exclude, x -> x IS NOT DISTINCT FROM e)) = 0)
                            // END

                            // Build: source IS NULL
                            let source_is_null =
                                Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: source_arr.clone(),
                                    not: false,
                                    postfix_form: false,
                                }));
                            // Build: exclude IS NULL
                            let exclude_is_null =
                                Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: exclude_arr.clone(),
                                    not: false,
                                    postfix_form: false,
                                }));
                            // source IS NULL OR exclude IS NULL
                            let null_check =
                                Expression::Or(Box::new(crate::expressions::BinaryOp {
                                    left: source_is_null,
                                    right: exclude_is_null,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));

                            // LIST_DISTINCT(source)
                            let list_distinct = Expression::Function(Box::new(Function::new(
                                "LIST_DISTINCT".to_string(),
                                vec![source_arr.clone()],
                            )));

                            // x IS NOT DISTINCT FROM e
                            let x_col = Expression::column("x");
                            let e_col = Expression::column("e");
                            let is_not_distinct =
                                Expression::NullSafeEq(Box::new(crate::expressions::BinaryOp {
                                    left: x_col,
                                    right: e_col.clone(),
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));

                            // x -> x IS NOT DISTINCT FROM e
                            let inner_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("x")],
                                    body: is_not_distinct,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(exclude, x -> x IS NOT DISTINCT FROM e)
                            let inner_list_filter = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![exclude_arr.clone(), inner_lambda],
                            )));

                            // LENGTH(LIST_FILTER(exclude, x -> x IS NOT DISTINCT FROM e))
                            let len_inner = Expression::Function(Box::new(Function::new(
                                "LENGTH".to_string(),
                                vec![inner_list_filter],
                            )));

                            // LENGTH(...) = 0
                            let eq_zero = Expression::Eq(Box::new(crate::expressions::BinaryOp {
                                left: len_inner,
                                right: Expression::number(0),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            }));

                            // e -> LENGTH(LIST_FILTER(...)) = 0
                            let outer_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("e")],
                                    body: eq_zero,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(LIST_DISTINCT(source), e -> ...)
                            let outer_list_filter = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![list_distinct, outer_lambda],
                            )));

                            // CASE WHEN ... IS NULL ... THEN NULL ELSE LIST_FILTER(...) END
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(null_check, Expression::Null(Null))],
                                else_: Some(outer_list_filter),
                                comments: Vec::new(),
                                inferred_type: None,
                            })))
                        }
                        DialectType::Snowflake => {
                            // Snowflake: ARRAY_EXCEPT(source, exclude) - keep as-is
                            Ok(Expression::ArrayExcept(Box::new(
                                crate::expressions::BinaryFunc {
                                    this: source_arr,
                                    expression: exclude_arr,
                                    original_name: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // Presto/Trino: ARRAY_EXCEPT(source, exclude) - keep function name, array syntax already converted
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_EXCEPT".to_string(),
                                vec![source_arr, exclude_arr],
                            ))))
                        }
                        _ => Ok(Expression::ArrayExcept(Box::new(
                            crate::expressions::BinaryFunc {
                                this: source_arr,
                                expression: exclude_arr,
                                original_name: None,
                                inferred_type: None,
                            },
                        ))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ArrayPositionSnowflakeSwap => {
                // ARRAY_POSITION(arr, elem) -> ARRAY_POSITION(elem, arr) for Snowflake
                if let Expression::ArrayPosition(f) = e {
                    Ok(Expression::ArrayPosition(Box::new(
                        crate::expressions::BinaryFunc {
                            this: f.expression,
                            expression: f.this,
                            original_name: f.original_name,
                            inferred_type: f.inferred_type,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayDistinctConvert => {
                // ARRAY_DISTINCT(arr) -> DuckDB NULL-aware CASE:
                // CASE WHEN ARRAY_LENGTH(arr) <> LIST_COUNT(arr)
                //   THEN LIST_APPEND(LIST_DISTINCT(LIST_FILTER(arr, _u -> NOT _u IS NULL)), NULL)
                //   ELSE LIST_DISTINCT(arr)
                // END
                if let Expression::ArrayDistinct(f) = e {
                    let arr = f.this;

                    // ARRAY_LENGTH(arr)
                    let array_length = Expression::Function(Box::new(Function::new(
                        "ARRAY_LENGTH".to_string(),
                        vec![arr.clone()],
                    )));
                    // LIST_COUNT(arr)
                    let list_count = Expression::Function(Box::new(Function::new(
                        "LIST_COUNT".to_string(),
                        vec![arr.clone()],
                    )));
                    // ARRAY_LENGTH(arr) <> LIST_COUNT(arr)
                    let neq = Expression::Neq(Box::new(crate::expressions::BinaryOp {
                        left: array_length,
                        right: list_count,
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                        inferred_type: None,
                    }));

                    // _u column
                    let u_col = Expression::column("_u");
                    // NOT _u IS NULL
                    let u_is_null = Expression::IsNull(Box::new(crate::expressions::IsNull {
                        this: u_col.clone(),
                        not: false,
                        postfix_form: false,
                    }));
                    let not_u_is_null = Expression::Not(Box::new(crate::expressions::UnaryOp {
                        this: u_is_null,
                        inferred_type: None,
                    }));
                    // _u -> NOT _u IS NULL
                    let filter_lambda =
                        Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                            parameters: vec![crate::expressions::Identifier::new("_u")],
                            body: not_u_is_null,
                            colon: false,
                            parameter_types: vec![],
                        }));
                    // LIST_FILTER(arr, _u -> NOT _u IS NULL)
                    let list_filter = Expression::Function(Box::new(Function::new(
                        "LIST_FILTER".to_string(),
                        vec![arr.clone(), filter_lambda],
                    )));
                    // LIST_DISTINCT(LIST_FILTER(arr, ...))
                    let list_distinct_filtered = Expression::Function(Box::new(Function::new(
                        "LIST_DISTINCT".to_string(),
                        vec![list_filter],
                    )));
                    // LIST_APPEND(LIST_DISTINCT(LIST_FILTER(...)), NULL)
                    let list_append = Expression::Function(Box::new(Function::new(
                        "LIST_APPEND".to_string(),
                        vec![list_distinct_filtered, Expression::Null(Null)],
                    )));

                    // LIST_DISTINCT(arr)
                    let list_distinct = Expression::Function(Box::new(Function::new(
                        "LIST_DISTINCT".to_string(),
                        vec![arr],
                    )));

                    // CASE WHEN neq THEN list_append ELSE list_distinct END
                    Ok(Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![(neq, list_append)],
                        else_: Some(list_distinct),
                        comments: Vec::new(),
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayDistinctClickHouse => {
                // ARRAY_DISTINCT(arr) -> arrayDistinct(arr) for ClickHouse
                if let Expression::ArrayDistinct(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "arrayDistinct".to_string(),
                        vec![f.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ArrayContainsDuckDBConvert => {
                // Snowflake ARRAY_CONTAINS(value, array) -> DuckDB NULL-aware:
                // CASE WHEN value IS NULL
                //   THEN NULLIF(ARRAY_LENGTH(array) <> LIST_COUNT(array), FALSE)
                //   ELSE ARRAY_CONTAINS(array, value)
                // END
                // Note: In Rust AST from Snowflake parse, this=value (first arg), expression=array (second arg)
                if let Expression::ArrayContains(f) = e {
                    let value = f.this;
                    let array = f.expression;

                    // value IS NULL
                    let value_is_null = Expression::IsNull(Box::new(crate::expressions::IsNull {
                        this: value.clone(),
                        not: false,
                        postfix_form: false,
                    }));

                    // ARRAY_LENGTH(array)
                    let array_length = Expression::Function(Box::new(Function::new(
                        "ARRAY_LENGTH".to_string(),
                        vec![array.clone()],
                    )));
                    // LIST_COUNT(array)
                    let list_count = Expression::Function(Box::new(Function::new(
                        "LIST_COUNT".to_string(),
                        vec![array.clone()],
                    )));
                    // ARRAY_LENGTH(array) <> LIST_COUNT(array)
                    let neq = Expression::Neq(Box::new(crate::expressions::BinaryOp {
                        left: array_length,
                        right: list_count,
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                        inferred_type: None,
                    }));
                    // NULLIF(ARRAY_LENGTH(array) <> LIST_COUNT(array), FALSE)
                    let nullif = Expression::Nullif(Box::new(crate::expressions::Nullif {
                        this: Box::new(neq),
                        expression: Box::new(Expression::Boolean(
                            crate::expressions::BooleanLiteral { value: false },
                        )),
                    }));

                    // ARRAY_CONTAINS(array, value) - DuckDB syntax: array first, value second
                    let array_contains = Expression::Function(Box::new(Function::new(
                        "ARRAY_CONTAINS".to_string(),
                        vec![array, value],
                    )));

                    // CASE WHEN value IS NULL THEN NULLIF(...) ELSE ARRAY_CONTAINS(array, value) END
                    Ok(Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![(value_is_null, nullif)],
                        else_: Some(array_contains),
                        comments: Vec::new(),
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::SnowflakeArrayPositionToDuckDB => {
                // Snowflake ARRAY_POSITION(value, array) -> DuckDB ARRAY_POSITION(array, value) - 1
                // Snowflake uses 0-based indexing, DuckDB uses 1-based
                // The parser has this=value, expression=array (Snowflake order)
                if let Expression::ArrayPosition(f) = e {
                    // Create ARRAY_POSITION(array, value) in standard order
                    let standard_pos =
                        Expression::ArrayPosition(Box::new(crate::expressions::BinaryFunc {
                            this: f.expression, // array
                            expression: f.this, // value
                            original_name: f.original_name,
                            inferred_type: f.inferred_type,
                        }));
                    // Subtract 1 for zero-based indexing
                    Ok(Expression::Sub(Box::new(BinaryOp {
                        left: standard_pos,
                        right: Expression::number(1),
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

pub(super) fn rewrite_unnest_expansion(
    select: &crate::expressions::Select,
    target: DialectType,
) -> Option<crate::expressions::Select> {
    use crate::expressions::{
        Alias, BinaryOp, Column, From, Function, Identifier, Join, JoinKind, Literal, UnnestFunc,
    };

    let index_offset: i64 = match target {
        DialectType::Presto | DialectType::Trino => 1,
        _ => 0, // BigQuery, Snowflake
    };

    let if_func_name = match target {
        DialectType::Snowflake => "IFF",
        _ => "IF",
    };

    let array_length_func = match target {
        DialectType::BigQuery => "ARRAY_LENGTH",
        DialectType::Presto | DialectType::Trino => "CARDINALITY",
        DialectType::Snowflake => "ARRAY_SIZE",
        _ => "ARRAY_LENGTH",
    };

    let use_table_aliases = matches!(
        target,
        DialectType::Presto | DialectType::Trino | DialectType::Snowflake
    );
    let null_third_arg = matches!(target, DialectType::BigQuery | DialectType::Snowflake);

    fn make_col(name: &str, table: Option<&str>) -> Expression {
        if let Some(tbl) = table {
            Expression::boxed_column(Column {
                name: Identifier::new(name.to_string()),
                table: Some(Identifier::new(tbl.to_string())),
                join_mark: false,
                trailing_comments: Vec::new(),
                span: None,
                inferred_type: None,
            })
        } else {
            Expression::Identifier(Identifier::new(name.to_string()))
        }
    }

    fn make_join(this: Expression) -> Join {
        Join {
            this,
            on: None,
            using: Vec::new(),
            kind: JoinKind::Cross,
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

    // Collect UNNEST info from SELECT expressions
    struct UnnestInfo {
        arr_expr: Expression,
        col_alias: String,
        pos_alias: String,
        source_alias: String,
        original_expr: Expression,
        has_outer_alias: Option<String>,
    }

    let mut unnest_infos: Vec<UnnestInfo> = Vec::new();
    let mut col_counter = 0usize;
    let mut pos_counter = 1usize;
    let mut source_counter = 1usize;

    fn extract_unnest_arg(expr: &Expression) -> Option<Expression> {
        match expr {
            Expression::Unnest(u) => Some(u.this.clone()),
            Expression::Function(f)
                if f.name.eq_ignore_ascii_case("UNNEST") && !f.args.is_empty() =>
            {
                Some(f.args[0].clone())
            }
            Expression::Alias(a) => extract_unnest_arg(&a.this),
            Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op) => {
                extract_unnest_arg(&op.left).or_else(|| extract_unnest_arg(&op.right))
            }
            _ => None,
        }
    }

    fn get_alias_name(expr: &Expression) -> Option<String> {
        if let Expression::Alias(a) = expr {
            Some(a.alias.name.clone())
        } else {
            None
        }
    }

    for sel_expr in &select.expressions {
        if let Some(arr) = extract_unnest_arg(sel_expr) {
            col_counter += 1;
            pos_counter += 1;
            source_counter += 1;

            let col_alias = if col_counter == 1 {
                "col".to_string()
            } else {
                format!("col_{}", col_counter)
            };
            let pos_alias = format!("pos_{}", pos_counter);
            let source_alias = format!("_u_{}", source_counter);
            let has_outer_alias = get_alias_name(sel_expr);

            unnest_infos.push(UnnestInfo {
                arr_expr: arr,
                col_alias,
                pos_alias,
                source_alias,
                original_expr: sel_expr.clone(),
                has_outer_alias,
            });
        }
    }

    if unnest_infos.is_empty() {
        return None;
    }

    let series_alias = "pos".to_string();
    let series_source_alias = "_u".to_string();
    let tbl_ref = if use_table_aliases {
        Some(series_source_alias.as_str())
    } else {
        None
    };

    // Build new SELECT expressions
    let mut new_select_exprs = Vec::new();
    for info in &unnest_infos {
        let actual_col_name = info.has_outer_alias.as_ref().unwrap_or(&info.col_alias);
        let src_ref = if use_table_aliases {
            Some(info.source_alias.as_str())
        } else {
            None
        };

        let pos_col = make_col(&series_alias, tbl_ref);
        let unnest_pos_col = make_col(&info.pos_alias, src_ref);
        let col_ref = make_col(actual_col_name, src_ref);

        let eq_cond = Expression::Eq(Box::new(BinaryOp::new(
            pos_col.clone(),
            unnest_pos_col.clone(),
        )));
        let mut if_args = vec![eq_cond, col_ref];
        if null_third_arg {
            if_args.push(Expression::Null(crate::expressions::Null));
        }

        let if_expr =
            Expression::Function(Box::new(Function::new(if_func_name.to_string(), if_args)));
        let final_expr = replace_unnest_with_if(&info.original_expr, &if_expr);

        new_select_exprs.push(Expression::Alias(Box::new(Alias::new(
            final_expr,
            Identifier::new(actual_col_name.clone()),
        ))));
    }

    // Build array size expressions for GREATEST
    let size_exprs: Vec<Expression> = unnest_infos
        .iter()
        .map(|info| {
            Expression::Function(Box::new(Function::new(
                array_length_func.to_string(),
                vec![info.arr_expr.clone()],
            )))
        })
        .collect();

    let greatest =
        Expression::Function(Box::new(Function::new("GREATEST".to_string(), size_exprs)));

    let series_end = if index_offset == 0 {
        Expression::Sub(Box::new(BinaryOp::new(
            greatest,
            Expression::Literal(Box::new(Literal::Number("1".to_string()))),
        )))
    } else {
        greatest
    };

    // Build the position array source
    let series_unnest_expr = match target {
        DialectType::BigQuery => {
            let gen_array = Expression::Function(Box::new(Function::new(
                "GENERATE_ARRAY".to_string(),
                vec![
                    Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                    series_end,
                ],
            )));
            Expression::Unnest(Box::new(UnnestFunc {
                this: gen_array,
                expressions: Vec::new(),
                with_ordinality: false,
                alias: None,
                offset_alias: None,
            }))
        }
        DialectType::Presto | DialectType::Trino => {
            let sequence = Expression::Function(Box::new(Function::new(
                "SEQUENCE".to_string(),
                vec![
                    Expression::Literal(Box::new(Literal::Number("1".to_string()))),
                    series_end,
                ],
            )));
            Expression::Unnest(Box::new(UnnestFunc {
                this: sequence,
                expressions: Vec::new(),
                with_ordinality: false,
                alias: None,
                offset_alias: None,
            }))
        }
        DialectType::Snowflake => {
            let range_end = Expression::Add(Box::new(BinaryOp::new(
                Expression::Paren(Box::new(crate::expressions::Paren {
                    this: series_end,
                    trailing_comments: Vec::new(),
                })),
                Expression::Literal(Box::new(Literal::Number("1".to_string()))),
            )));
            let gen_range = Expression::Function(Box::new(Function::new(
                "ARRAY_GENERATE_RANGE".to_string(),
                vec![
                    Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                    range_end,
                ],
            )));
            let flatten_arg =
                Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                    name: Identifier::new("INPUT".to_string()),
                    value: gen_range,
                    separator: crate::expressions::NamedArgSeparator::DArrow,
                }));
            let flatten = Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                vec![flatten_arg],
            )));
            Expression::Function(Box::new(Function::new("TABLE".to_string(), vec![flatten])))
        }
        _ => return None,
    };

    // Build series alias expression
    let series_alias_expr = if use_table_aliases {
        let col_aliases = if matches!(target, DialectType::Snowflake) {
            vec![
                Identifier::new("seq".to_string()),
                Identifier::new("key".to_string()),
                Identifier::new("path".to_string()),
                Identifier::new("index".to_string()),
                Identifier::new(series_alias.clone()),
                Identifier::new("this".to_string()),
            ]
        } else {
            vec![Identifier::new(series_alias.clone())]
        };
        Expression::Alias(Box::new(Alias {
            this: series_unnest_expr,
            alias: Identifier::new(series_source_alias.clone()),
            column_aliases: col_aliases,
            alias_explicit_as: false,
            alias_keyword: None,
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
            inferred_type: None,
        }))
    } else {
        Expression::Alias(Box::new(Alias::new(
            series_unnest_expr,
            Identifier::new(series_alias.clone()),
        )))
    };

    // Build CROSS JOINs for each UNNEST
    let mut joins = Vec::new();
    for info in &unnest_infos {
        let actual_col_name = info.has_outer_alias.as_ref().unwrap_or(&info.col_alias);

        let unnest_join_expr = match target {
            DialectType::BigQuery => {
                // UNNEST([1,2,3]) AS col WITH OFFSET AS pos_2
                let unnest = UnnestFunc {
                    this: info.arr_expr.clone(),
                    expressions: Vec::new(),
                    with_ordinality: true,
                    alias: Some(Identifier::new(actual_col_name.clone())),
                    offset_alias: Some(Identifier::new(info.pos_alias.clone())),
                };
                Expression::Unnest(Box::new(unnest))
            }
            DialectType::Presto | DialectType::Trino => {
                let unnest = UnnestFunc {
                    this: info.arr_expr.clone(),
                    expressions: Vec::new(),
                    with_ordinality: true,
                    alias: None,
                    offset_alias: None,
                };
                Expression::Alias(Box::new(Alias {
                    this: Expression::Unnest(Box::new(unnest)),
                    alias: Identifier::new(info.source_alias.clone()),
                    column_aliases: vec![
                        Identifier::new(actual_col_name.clone()),
                        Identifier::new(info.pos_alias.clone()),
                    ],
                    alias_explicit_as: false,
                    alias_keyword: None,
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                    inferred_type: None,
                }))
            }
            DialectType::Snowflake => {
                let flatten_arg =
                    Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                        name: Identifier::new("INPUT".to_string()),
                        value: info.arr_expr.clone(),
                        separator: crate::expressions::NamedArgSeparator::DArrow,
                    }));
                let flatten = Expression::Function(Box::new(Function::new(
                    "FLATTEN".to_string(),
                    vec![flatten_arg],
                )));
                let table_fn = Expression::Function(Box::new(Function::new(
                    "TABLE".to_string(),
                    vec![flatten],
                )));
                Expression::Alias(Box::new(Alias {
                    this: table_fn,
                    alias: Identifier::new(info.source_alias.clone()),
                    column_aliases: vec![
                        Identifier::new("seq".to_string()),
                        Identifier::new("key".to_string()),
                        Identifier::new("path".to_string()),
                        Identifier::new(info.pos_alias.clone()),
                        Identifier::new(actual_col_name.clone()),
                        Identifier::new("this".to_string()),
                    ],
                    alias_explicit_as: false,
                    alias_keyword: None,
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                    inferred_type: None,
                }))
            }
            _ => return None,
        };

        joins.push(make_join(unnest_join_expr));
    }

    // Build WHERE clause
    let mut where_conditions: Vec<Expression> = Vec::new();
    for info in &unnest_infos {
        let src_ref = if use_table_aliases {
            Some(info.source_alias.as_str())
        } else {
            None
        };
        let pos_col = make_col(&series_alias, tbl_ref);
        let unnest_pos_col = make_col(&info.pos_alias, src_ref);

        let arr_size = Expression::Function(Box::new(Function::new(
            array_length_func.to_string(),
            vec![info.arr_expr.clone()],
        )));

        let size_ref = if index_offset == 0 {
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: Expression::Sub(Box::new(BinaryOp::new(
                    arr_size,
                    Expression::Literal(Box::new(Literal::Number("1".to_string()))),
                ))),
                trailing_comments: Vec::new(),
            }))
        } else {
            arr_size
        };

        let eq = Expression::Eq(Box::new(BinaryOp::new(
            pos_col.clone(),
            unnest_pos_col.clone(),
        )));
        let gt = Expression::Gt(Box::new(BinaryOp::new(pos_col, size_ref.clone())));
        let pos_eq_size = Expression::Eq(Box::new(BinaryOp::new(unnest_pos_col, size_ref)));
        let and_cond = Expression::And(Box::new(BinaryOp::new(gt, pos_eq_size)));
        let paren_and = Expression::Paren(Box::new(crate::expressions::Paren {
            this: and_cond,
            trailing_comments: Vec::new(),
        }));
        let or_cond = Expression::Or(Box::new(BinaryOp::new(eq, paren_and)));

        where_conditions.push(or_cond);
    }

    let where_expr = if where_conditions.len() == 1 {
        // Single condition: no parens needed
        where_conditions.into_iter().next().unwrap()
    } else {
        // Multiple conditions: wrap each OR in parens, then combine with AND
        let wrap = |e: Expression| {
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: e,
                trailing_comments: Vec::new(),
            }))
        };
        let mut iter = where_conditions.into_iter();
        let first = wrap(iter.next().unwrap());
        let second = wrap(iter.next().unwrap());
        let mut combined = Expression::Paren(Box::new(crate::expressions::Paren {
            this: Expression::And(Box::new(BinaryOp::new(first, second))),
            trailing_comments: Vec::new(),
        }));
        for cond in iter {
            combined = Expression::And(Box::new(BinaryOp::new(combined, wrap(cond))));
        }
        combined
    };

    // Build the new SELECT
    let mut new_select = select.clone();
    new_select.expressions = new_select_exprs;

    if new_select.from.is_some() {
        let mut all_joins = vec![make_join(series_alias_expr)];
        all_joins.extend(joins);
        new_select.joins.extend(all_joins);
    } else {
        new_select.from = Some(From {
            expressions: vec![series_alias_expr],
        });
        new_select.joins.extend(joins);
    }

    if let Some(ref existing_where) = new_select.where_clause {
        let combined = Expression::And(Box::new(BinaryOp::new(
            existing_where.this.clone(),
            where_expr,
        )));
        new_select.where_clause = Some(crate::expressions::Where { this: combined });
    } else {
        new_select.where_clause = Some(crate::expressions::Where { this: where_expr });
    }

    Some(new_select)
}

pub(super) fn replace_unnest_with_if(
    original: &Expression,
    replacement: &Expression,
) -> Expression {
    match original {
        Expression::Unnest(_) => replacement.clone(),
        Expression::Function(f) if f.name.eq_ignore_ascii_case("UNNEST") => replacement.clone(),
        Expression::Alias(a) => replace_unnest_with_if(&a.this, replacement),
        Expression::Add(op) => {
            let left = replace_unnest_with_if(&op.left, replacement);
            let right = replace_unnest_with_if(&op.right, replacement);
            Expression::Add(Box::new(crate::expressions::BinaryOp::new(left, right)))
        }
        Expression::Sub(op) => {
            let left = replace_unnest_with_if(&op.left, replacement);
            let right = replace_unnest_with_if(&op.right, replacement);
            Expression::Sub(Box::new(crate::expressions::BinaryOp::new(left, right)))
        }
        Expression::Mul(op) => {
            let left = replace_unnest_with_if(&op.left, replacement);
            let right = replace_unnest_with_if(&op.right, replacement);
            Expression::Mul(Box::new(crate::expressions::BinaryOp::new(left, right)))
        }
        Expression::Div(op) => {
            let left = replace_unnest_with_if(&op.left, replacement);
            let right = replace_unnest_with_if(&op.right, replacement);
            Expression::Div(Box::new(crate::expressions::BinaryOp::new(left, right)))
        }
        _ => original.clone(),
    }
}
