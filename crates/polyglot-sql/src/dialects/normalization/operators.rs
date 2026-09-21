use super::{types, NormalizationContext, RewriteOutcome};
use crate::dialects::DialectType;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    Div0TypedDivision,
    SourceDivisionToClickHouse,
    RegexpReplaceSnowflakeToDuckDB,
    RegexpExtractPrestoToDuckDB,
    RegexpReplacePrestoToDuckDB,
    BigQuerySafeDivide,
    RegexpLikeToDuckDB,
    RegexpLikeToTsqlPatindex,
    SimilarToToTsqlLike,
    MySQLSafeDivide,
    NullsOrdering,
    XorExpand,
    DollarParamConvert,
    AnyToExists,
    RespectNullsConvert,
    MysqlNullsOrdering,
    BigQueryNullsOrdering,
    PipeConcatToConcat,
    PostgresPipeConcatToTsql,
    DivFuncConvert,
    RegexpSubstrSnowflakeToDuckDB,
    RegexpSubstrSnowflakeIdentity,
    RegexpSubstrAllSnowflakeToDuckDB,
    RegexpCountSnowflakeToDuckDB,
    RegexpInstrSnowflakeToDuckDB,
    RegexpReplacePositionSnowflakeToDuckDB,
    RlikeSnowflakeToDuckDB,
    RegexpExtractAllToSnowflake,
    RegexpLikeExasolAnchor,
    SnowflakeRegexpLikeToClickHouse,
    SnowflakeWindowFrameStrip,
    SnowflakeWindowFrameAdd,
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
            Action::RegexpReplacePrestoToDuckDB => {
                if let Expression::Function(mut f) = e {
                    if f.args.len() == 2 {
                        f.args.push(Expression::string(""));
                    } else if let Expression::Literal(lit) = &f.args[2] {
                        if let Literal::String(replacement) = lit.as_ref() {
                            f.args[2] = Expression::string(presto_regexp_replacement_to_duckdb(
                                replacement,
                            )?);
                        }
                    }
                    f.args.push(Expression::string("g"));
                    Ok(Expression::Function(f))
                } else {
                    unreachable!("action only triggered for REGEXP_REPLACE functions")
                }
            }
            Action::RegexpExtractPrestoToDuckDB => {
                if let Expression::Function(mut f) = e {
                    // A missing first match (or capture group) is NULL, while
                    // a successful empty capture must remain an empty string.
                    f.name = "REGEXP_EXTRACT_ALL".to_string();
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_EXTRACT",
                        vec![Expression::Function(f), Expression::number(1)],
                    ))))
                } else {
                    unreachable!("action only triggered for REGEXP_EXTRACT functions")
                }
            }
            Action::SourceDivisionToClickHouse => {
                if let Expression::Div(div) = e {
                    normalize_source_division_to_clickhouse(*div, context)
                } else {
                    unreachable!("action only triggered for Div expressions")
                }
            }
            Action::SnowflakeRegexpLikeToClickHouse => {
                if let Expression::RegexpLike(f) = e {
                    snowflake_regexp_like_to_clickhouse(*f, context)
                } else {
                    unreachable!("action only triggered for RegexpLike expressions")
                }
            }
            Action::Div0TypedDivision => {
                let if_func = if let Expression::IfFunc(f) = e {
                    *f
                } else {
                    unreachable!("action only triggered for IfFunc expressions")
                };
                if let Some(Expression::Div(div)) = if_func.false_value {
                    let cast_type = if matches!(target, DialectType::SQLite) {
                        DataType::Float {
                            precision: None,
                            scale: None,
                            real_spelling: true,
                        }
                    } else {
                        DataType::Double {
                            precision: None,
                            scale: None,
                        }
                    };
                    let casted_left = Expression::Cast(Box::new(Cast {
                        this: div.left,
                        to: cast_type,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                        condition: if_func.condition,
                        true_value: if_func.true_value,
                        false_value: Some(Expression::Div(Box::new(BinaryOp::new(
                            casted_left,
                            div.right,
                        )))),
                        original_name: if_func.original_name,
                        inferred_type: None,
                    })))
                } else {
                    // Not actually a Div, reconstruct
                    Ok(Expression::IfFunc(Box::new(if_func)))
                }
            }

            Action::RegexpReplaceSnowflakeToDuckDB => {
                // Snowflake REGEXP_REPLACE(s, p, r, position) -> REGEXP_REPLACE(s, p, r, 'g')
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let subject = args.remove(0);
                    let pattern = args.remove(0);
                    let replacement = args.remove(0);
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_REPLACE".to_string(),
                        vec![
                            subject,
                            pattern,
                            replacement,
                            Expression::Literal(Box::new(crate::expressions::Literal::String(
                                "g".to_string(),
                            ))),
                        ],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::BigQuerySafeDivide => {
                // Convert SafeDivide expression to IF/CASE form for most targets
                if let Expression::SafeDivide(sd) = e {
                    let x = *sd.this;
                    let y = *sd.expression;
                    // Wrap x and y in parens if they're complex expressions
                    let y_ref = match &y {
                        Expression::Column(_)
                        | Expression::Literal(_)
                        | Expression::Identifier(_) => y.clone(),
                        _ => Expression::Paren(Box::new(Paren {
                            this: y.clone(),
                            trailing_comments: vec![],
                        })),
                    };
                    let x_ref = match &x {
                        Expression::Column(_)
                        | Expression::Literal(_)
                        | Expression::Identifier(_) => x.clone(),
                        _ => Expression::Paren(Box::new(Paren {
                            this: x.clone(),
                            trailing_comments: vec![],
                        })),
                    };
                    let condition = Expression::Neq(Box::new(BinaryOp::new(
                        y_ref.clone(),
                        Expression::number(0),
                    )));
                    let div_expr = Expression::Div(Box::new(BinaryOp::new(x_ref, y_ref)));

                    if matches!(target, DialectType::Spark | DialectType::Databricks) {
                        Ok(Expression::Function(Box::new(Function::new(
                            "TRY_DIVIDE".to_string(),
                            vec![x, y],
                        ))))
                    } else if matches!(target, DialectType::Presto | DialectType::Trino) {
                        // Presto/Trino: IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)
                        let cast_x = Expression::Cast(Box::new(Cast {
                            this: match &x {
                                Expression::Column(_)
                                | Expression::Literal(_)
                                | Expression::Identifier(_) => x,
                                _ => Expression::Paren(Box::new(Paren {
                                    this: x,
                                    trailing_comments: vec![],
                                })),
                            },
                            to: DataType::Double {
                                precision: None,
                                scale: None,
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }));
                        let cast_div = Expression::Div(Box::new(BinaryOp::new(
                            cast_x,
                            match &y {
                                Expression::Column(_)
                                | Expression::Literal(_)
                                | Expression::Identifier(_) => y,
                                _ => Expression::Paren(Box::new(Paren {
                                    this: y,
                                    trailing_comments: vec![],
                                })),
                            },
                        )));
                        Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                            condition,
                            true_value: cast_div,
                            false_value: Some(Expression::Null(Null)),
                            original_name: None,
                            inferred_type: None,
                        })))
                    } else if matches!(target, DialectType::PostgreSQL) {
                        // PostgreSQL: CASE WHEN y <> 0 THEN CAST(x AS DOUBLE PRECISION) / y ELSE NULL END
                        let cast_x = Expression::Cast(Box::new(Cast {
                            this: match &x {
                                Expression::Column(_)
                                | Expression::Literal(_)
                                | Expression::Identifier(_) => x,
                                _ => Expression::Paren(Box::new(Paren {
                                    this: x,
                                    trailing_comments: vec![],
                                })),
                            },
                            to: DataType::Custom {
                                name: "DOUBLE PRECISION".to_string(),
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }));
                        let y_paren = match &y {
                            Expression::Column(_)
                            | Expression::Literal(_)
                            | Expression::Identifier(_) => y,
                            _ => Expression::Paren(Box::new(Paren {
                                this: y,
                                trailing_comments: vec![],
                            })),
                        };
                        let cast_div = Expression::Div(Box::new(BinaryOp::new(cast_x, y_paren)));
                        Ok(Expression::Case(Box::new(Case {
                            operand: None,
                            whens: vec![(condition, cast_div)],
                            else_: Some(Expression::Null(Null)),
                            comments: Vec::new(),
                            inferred_type: None,
                        })))
                    } else if matches!(target, DialectType::DuckDB) {
                        // DuckDB: CASE WHEN y <> 0 THEN x / y ELSE NULL END
                        Ok(Expression::Case(Box::new(Case {
                            operand: None,
                            whens: vec![(condition, div_expr)],
                            else_: Some(Expression::Null(Null)),
                            comments: Vec::new(),
                            inferred_type: None,
                        })))
                    } else if matches!(target, DialectType::Snowflake) {
                        // Snowflake: IFF(y <> 0, x / y, NULL)
                        Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                            condition,
                            true_value: div_expr,
                            false_value: Some(Expression::Null(Null)),
                            original_name: Some("IFF".to_string()),
                            inferred_type: None,
                        })))
                    } else {
                        // All others: IF(y <> 0, x / y, NULL)
                        Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                            condition,
                            true_value: div_expr,
                            false_value: Some(Expression::Null(Null)),
                            original_name: None,
                            inferred_type: None,
                        })))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RegexpLikeToDuckDB => {
                if let Expression::RegexpLike(f) = e {
                    let mut args = vec![f.this, f.pattern];
                    if let Some(flags) = f.flags {
                        args.push(flags);
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_MATCHES".to_string(),
                        args,
                    ))))
                } else {
                    Ok(e)
                }
            }
            Action::RegexpLikeToTsqlPatindex => match e {
                Expression::RegexpLike(f) => Ok(build_tsql_regex_patindex_predicate(
                    f.this, f.pattern, false,
                )),
                Expression::RegexpILike(f) => Ok(build_tsql_regex_patindex_predicate(
                    *f.this,
                    *f.expression,
                    true,
                )),
                _ => Ok(e),
            },
            Action::SimilarToToTsqlLike => match e {
                Expression::SimilarTo(f) => {
                    let like = Expression::Like(Box::new(LikeOp {
                        left: f.this,
                        right: f.pattern,
                        escape: f.escape,
                        quantifier: None,
                        inferred_type: None,
                    }));
                    if f.not {
                        Ok(Expression::Not(Box::new(crate::expressions::UnaryOp::new(
                            like,
                        ))))
                    } else {
                        Ok(like)
                    }
                }
                _ => Ok(e),
            },
            Action::MySQLSafeDivide => {
                use crate::expressions::{BinaryOp, Cast};
                if let Expression::Div(op) = e {
                    let left = op.left;
                    let right = op.right;
                    // For SQLite: CAST left as REAL but NO NULLIF wrapping
                    if matches!(target, DialectType::SQLite) {
                        let new_left = Expression::Cast(Box::new(Cast {
                            this: left,
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
                        }));
                        return Ok(Expression::Div(Box::new(BinaryOp::new(new_left, right))));
                    }
                    // Wrap right in NULLIF(right, 0)
                    let nullif_right = Expression::Function(Box::new(Function::new(
                        "NULLIF".to_string(),
                        vec![right, Expression::number(0)],
                    )));
                    // For some dialects, also CAST the left side
                    let new_left = match target {
                        DialectType::PostgreSQL
                        | DialectType::Redshift
                        | DialectType::Teradata
                        | DialectType::Materialize
                        | DialectType::RisingWave => Expression::Cast(Box::new(Cast {
                            this: left,
                            to: DataType::Custom {
                                name: "DOUBLE PRECISION".to_string(),
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })),
                        DialectType::Drill
                        | DialectType::Trino
                        | DialectType::Presto
                        | DialectType::Athena => Expression::Cast(Box::new(Cast {
                            this: left,
                            to: DataType::Double {
                                precision: None,
                                scale: None,
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })),
                        DialectType::TSQL => Expression::Cast(Box::new(Cast {
                            this: left,
                            to: DataType::Float {
                                precision: None,
                                scale: None,
                                real_spelling: false,
                            },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })),
                        _ => left,
                    };
                    Ok(Expression::Div(Box::new(BinaryOp::new(
                        new_left,
                        nullif_right,
                    ))))
                } else {
                    Ok(e)
                }
            }
            Action::NullsOrdering => {
                // Fill in the source dialect's implied null ordering default.
                // This makes implicit null ordering explicit so the target generator
                // can correctly strip or keep it.
                //
                // Dialect null ordering categories:
                // nulls_are_large (Oracle, PostgreSQL, Redshift, Snowflake):
                //   ASC -> NULLS LAST, DESC -> NULLS FIRST
                // nulls_are_small (Spark, Hive, BigQuery, MySQL, Databricks, ClickHouse, etc.):
                //   ASC -> NULLS FIRST, DESC -> NULLS LAST
                // nulls_are_last (DuckDB, Presto, Trino, Dremio, Athena):
                //   NULLS LAST always (both ASC and DESC)
                if let Expression::Ordered(mut o) = e {
                    let is_asc = !o.desc;

                    let is_source_nulls_large = matches!(
                        source,
                        DialectType::Oracle
                            | DialectType::PostgreSQL
                            | DialectType::Redshift
                            | DialectType::Snowflake
                    );
                    let is_source_nulls_last = matches!(
                        source,
                        DialectType::DuckDB
                            | DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Dremio
                            | DialectType::Athena
                            | DialectType::ClickHouse
                            | DialectType::Drill
                            | DialectType::Exasol
                            | DialectType::DataFusion
                    );

                    // Determine target category to check if default matches
                    let is_target_nulls_large = matches!(
                        target,
                        DialectType::Oracle
                            | DialectType::PostgreSQL
                            | DialectType::Redshift
                            | DialectType::Snowflake
                    );
                    let is_target_nulls_last = matches!(
                        target,
                        DialectType::DuckDB
                            | DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Dremio
                            | DialectType::Athena
                            | DialectType::ClickHouse
                            | DialectType::Drill
                            | DialectType::Exasol
                            | DialectType::DataFusion
                    );

                    // Compute the implied nulls_first for source
                    let source_nulls_first = if is_source_nulls_large {
                        !is_asc // ASC -> NULLS LAST (false), DESC -> NULLS FIRST (true)
                    } else if is_source_nulls_last {
                        false // NULLS LAST always
                    } else {
                        is_asc // nulls_are_small: ASC -> NULLS FIRST (true), DESC -> NULLS LAST (false)
                    };

                    // Compute the target's default
                    let target_nulls_first = if is_target_nulls_large {
                        !is_asc
                    } else if is_target_nulls_last {
                        false
                    } else {
                        is_asc
                    };

                    // Only add explicit nulls ordering if source and target defaults differ.
                    // Vertica's default is data-type dependent, so always make it explicit.
                    if source_nulls_first != target_nulls_first
                        || matches!(target, DialectType::Vertica)
                    {
                        o.nulls_first = Some(source_nulls_first);
                    }
                    // If they match, leave nulls_first as None so the generator won't output it

                    Ok(Expression::Ordered(o))
                } else {
                    Ok(e)
                }
            }
            Action::XorExpand => {
                // Expand XOR to (a AND NOT b) OR (NOT a AND b) for dialects without XOR keyword
                // Snowflake: use BOOLXOR(a, b) instead
                if let Expression::Xor(xor) = e {
                    // Collect all XOR operands
                    let mut operands = Vec::new();
                    if let Some(this) = xor.this {
                        operands.push(*this);
                    }
                    if let Some(expr) = xor.expression {
                        operands.push(*expr);
                    }
                    operands.extend(xor.expressions);

                    // Snowflake: use BOOLXOR(a, b)
                    if matches!(target, DialectType::Snowflake) && operands.len() == 2 {
                        let a = operands.remove(0);
                        let b = operands.remove(0);
                        return Ok(Expression::Function(Box::new(Function::new(
                            "BOOLXOR".to_string(),
                            vec![a, b],
                        ))));
                    }

                    // Helper to build (a AND NOT b) OR (NOT a AND b)
                    let make_xor = |a: Expression, b: Expression| -> Expression {
                        let not_b =
                            Expression::Not(Box::new(crate::expressions::UnaryOp::new(b.clone())));
                        let not_a =
                            Expression::Not(Box::new(crate::expressions::UnaryOp::new(a.clone())));
                        let left_and = Expression::And(Box::new(BinaryOp {
                            left: a,
                            right: Expression::Paren(Box::new(Paren {
                                this: not_b,
                                trailing_comments: Vec::new(),
                            })),
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));
                        let right_and = Expression::And(Box::new(BinaryOp {
                            left: Expression::Paren(Box::new(Paren {
                                this: not_a,
                                trailing_comments: Vec::new(),
                            })),
                            right: b,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));
                        Expression::Or(Box::new(BinaryOp {
                            left: Expression::Paren(Box::new(Paren {
                                this: left_and,
                                trailing_comments: Vec::new(),
                            })),
                            right: Expression::Paren(Box::new(Paren {
                                this: right_and,
                                trailing_comments: Vec::new(),
                            })),
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }))
                    };

                    if operands.len() >= 2 {
                        let mut result = make_xor(operands.remove(0), operands.remove(0));
                        for operand in operands {
                            result = make_xor(result, operand);
                        }
                        Ok(result)
                    } else if operands.len() == 1 {
                        Ok(operands.remove(0))
                    } else {
                        // No operands - return FALSE (shouldn't happen)
                        Ok(Expression::Boolean(crate::expressions::BooleanLiteral {
                            value: false,
                        }))
                    }
                } else {
                    Ok(e)
                }
            }
            Action::DollarParamConvert => {
                if let Expression::Parameter(p) = e {
                    Ok(Expression::Parameter(Box::new(
                        crate::expressions::Parameter {
                            name: p.name,
                            index: p.index,
                            style: crate::expressions::ParameterStyle::At,
                            quoted: p.quoted,
                            string_quoted: p.string_quoted,
                            expression: p.expression,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::AnyToExists => {
                if let Expression::Any(q) = e {
                    if let Some(op) = q.op.clone() {
                        let lambda_param = crate::expressions::Identifier::new("x");
                        let rhs = Expression::Identifier(lambda_param.clone());
                        let body = match op {
                            crate::expressions::QuantifiedOp::Eq => {
                                Expression::Eq(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                            crate::expressions::QuantifiedOp::Neq => {
                                Expression::Neq(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                            crate::expressions::QuantifiedOp::Lt => {
                                Expression::Lt(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                            crate::expressions::QuantifiedOp::Lte => {
                                Expression::Lte(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                            crate::expressions::QuantifiedOp::Gt => {
                                Expression::Gt(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                            crate::expressions::QuantifiedOp::Gte => {
                                Expression::Gte(Box::new(BinaryOp::new(q.this, rhs)))
                            }
                        };
                        let lambda = Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                            parameters: vec![lambda_param],
                            body,
                            colon: false,
                            parameter_types: Vec::new(),
                        }));
                        Ok(Expression::Function(Box::new(Function::new(
                            "EXISTS".to_string(),
                            vec![q.subquery, lambda],
                        ))))
                    } else {
                        Ok(Expression::Any(q))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RespectNullsConvert => {
                // RESPECT NULLS -> strip for SQLite (FIRST_VALUE(c) OVER (...))
                if let Expression::WindowFunction(mut wf) = e {
                    match &mut wf.this {
                        Expression::FirstValue(ref mut vf) => {
                            if vf.ignore_nulls == Some(false) {
                                vf.ignore_nulls = None;
                                // For SQLite, we'd need to add NULLS LAST to ORDER BY in the OVER clause
                                // but that's handled by the generator's NULLS ordering
                            }
                        }
                        Expression::LastValue(ref mut vf) => {
                            if vf.ignore_nulls == Some(false) {
                                vf.ignore_nulls = None;
                            }
                        }
                        _ => {}
                    }
                    Ok(Expression::WindowFunction(wf))
                } else {
                    Ok(e)
                }
            }

            Action::MysqlNullsOrdering => {
                // MySQL doesn't support NULLS FIRST/LAST - strip or rewrite
                if let Expression::Ordered(mut o) = e {
                    let nulls_last = o.nulls_first == Some(false);
                    let desc = o.desc;
                    // MySQL default: ASC -> NULLS LAST, DESC -> NULLS FIRST
                    // If requested ordering matches default, just strip NULLS clause
                    let matches_default = if desc {
                        // DESC default is NULLS FIRST, so nulls_first=true matches
                        o.nulls_first == Some(true)
                    } else {
                        // ASC default is NULLS LAST, so nulls_first=false matches
                        nulls_last
                    };
                    if matches_default {
                        o.nulls_first = None;
                        Ok(Expression::Ordered(o))
                    } else {
                        // Need CASE WHEN x IS NULL THEN 0/1 ELSE 0/1 END, x
                        // For ASC NULLS FIRST: ORDER BY CASE WHEN x IS NULL THEN 0 ELSE 1 END, x ASC
                        // For DESC NULLS LAST: ORDER BY CASE WHEN x IS NULL THEN 1 ELSE 0 END, x DESC
                        let null_val = if desc { 1 } else { 0 };
                        let non_null_val = if desc { 0 } else { 1 };
                        let _case_expr = Expression::Case(Box::new(Case {
                            operand: None,
                            whens: vec![(
                                Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: o.this.clone(),
                                    not: false,
                                    postfix_form: false,
                                })),
                                Expression::number(null_val),
                            )],
                            else_: Some(Expression::number(non_null_val)),
                            comments: Vec::new(),
                            inferred_type: None,
                        }));
                        o.nulls_first = None;
                        // Return a tuple of [case_expr, ordered_expr]
                        // We need to return both as part of the ORDER BY
                        // But since transform_recursive processes individual expressions,
                        // we can't easily add extra ORDER BY items here.
                        // Instead, strip the nulls_first
                        o.nulls_first = None;
                        Ok(Expression::Ordered(o))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::BigQueryNullsOrdering => {
                // BigQuery doesn't support NULLS FIRST/LAST in window function ORDER BY
                if let Expression::WindowFunction(mut wf) = e {
                    for o in &mut wf.over.order_by {
                        o.nulls_first = None;
                    }
                    Ok(Expression::WindowFunction(wf))
                } else if let Expression::Ordered(mut o) = e {
                    o.nulls_first = None;
                    Ok(Expression::Ordered(o))
                } else {
                    Ok(e)
                }
            }

            Action::PipeConcatToConcat => {
                // a || b (Concat operator) -> CONCAT(CAST(a AS VARCHAR), CAST(b AS VARCHAR)) for Presto/Trino
                if let Expression::Concat(op) = e {
                    let cast_left = Expression::Cast(Box::new(Cast {
                        this: op.left,
                        to: DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let cast_right = Expression::Cast(Box::new(Cast {
                        this: op.right,
                        to: DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "CONCAT".to_string(),
                        vec![cast_left, cast_right],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::PostgresPipeConcatToTsql => {
                fn text_operand(expression: Expression) -> Expression {
                    let is_text = match &expression {
                        Expression::Literal(literal) => literal.is_string(),
                        Expression::Cast(cast)
                        | Expression::TryCast(cast)
                        | Expression::SafeCast(cast) => types::unit_cast_target_is_string(&cast.to),
                        // Normalization is bottom-up, so nested concatenations already
                        // have text-safe leaves when their parent is visited.
                        Expression::Concat(_) => true,
                        _ => false,
                    };

                    if is_text {
                        expression
                    } else {
                        Expression::Cast(Box::new(Cast {
                            this: expression,
                            to: DataType::Text,
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))
                    }
                }

                if let Expression::Concat(mut op) = e {
                    op.left = text_operand(op.left);
                    op.right = text_operand(op.right);
                    Ok(Expression::Concat(op))
                } else {
                    Ok(e)
                }
            }

            Action::DivFuncConvert => {
                // DIV(a, b) -> target-specific integer division
                if let Expression::Function(f) = e {
                    if f.name.eq_ignore_ascii_case("DIV") && f.args.len() == 2 {
                        let a = f.args[0].clone();
                        let b = f.args[1].clone();
                        match target {
                            DialectType::DuckDB => {
                                // DIV(a, b) -> CAST(a // b AS DECIMAL)
                                let int_div =
                                    Expression::IntDiv(Box::new(crate::expressions::BinaryFunc {
                                        this: a,
                                        expression: b,
                                        original_name: None,
                                        inferred_type: None,
                                    }));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: int_div,
                                    to: DataType::Decimal {
                                        precision: None,
                                        scale: None,
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                            DialectType::BigQuery => {
                                // DIV(a, b) -> CAST(DIV(a, b) AS NUMERIC)
                                let div_func = Expression::Function(Box::new(Function::new(
                                    "DIV".to_string(),
                                    vec![a, b],
                                )));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: div_func,
                                    to: DataType::Custom {
                                        name: "NUMERIC".to_string(),
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                            DialectType::SQLite => {
                                // DIV(a, b) -> CAST(CAST(CAST(a AS REAL) / b AS INTEGER) AS REAL)
                                let cast_a = Expression::Cast(Box::new(Cast {
                                    this: a,
                                    to: DataType::Custom {
                                        name: "REAL".to_string(),
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let div = Expression::Div(Box::new(BinaryOp::new(cast_a, b)));
                                let cast_int = Expression::Cast(Box::new(Cast {
                                    this: div,
                                    to: DataType::Int {
                                        length: None,
                                        integer_spelling: true,
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: cast_int,
                                    to: DataType::Custom {
                                        name: "REAL".to_string(),
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                            DialectType::TSQL | DialectType::Fabric => {
                                Ok(build_tsql_div_func(a, b, target))
                            }
                            _ => Ok(Expression::Function(f)),
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RegexpSubstrSnowflakeToDuckDB => {
                // Snowflake REGEXP_SUBSTR -> DuckDB REGEXP_EXTRACT variants
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let arg_count = args.len();
                    match arg_count {
                        // REGEXP_SUBSTR(s, p) -> REGEXP_EXTRACT(s, p)
                        0..=2 => Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT".to_string(),
                            args,
                        )))),
                        // REGEXP_SUBSTR(s, p, pos) -> REGEXP_EXTRACT(NULLIF(SUBSTRING(s, pos), ''), p)
                        3 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let position = args.remove(0);
                            let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                            if is_pos_1 {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    vec![subject, pattern],
                                ))))
                            } else {
                                let substring_expr = Expression::Function(Box::new(Function::new(
                                    "SUBSTRING".to_string(),
                                    vec![subject, position],
                                )));
                                let nullif_expr = Expression::Function(Box::new(Function::new(
                                    "NULLIF".to_string(),
                                    vec![
                                        substring_expr,
                                        Expression::Literal(Box::new(Literal::String(
                                            String::new(),
                                        ))),
                                    ],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    vec![nullif_expr, pattern],
                                ))))
                            }
                        }
                        // REGEXP_SUBSTR(s, p, pos, occ) -> depends on pos and occ
                        4 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let position = args.remove(0);
                            let occurrence = args.remove(0);
                            let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                            let is_occ_1 = matches!(&occurrence, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));

                            let effective_subject = if is_pos_1 {
                                subject
                            } else {
                                let substring_expr = Expression::Function(Box::new(Function::new(
                                    "SUBSTRING".to_string(),
                                    vec![subject, position],
                                )));
                                Expression::Function(Box::new(Function::new(
                                    "NULLIF".to_string(),
                                    vec![
                                        substring_expr,
                                        Expression::Literal(Box::new(Literal::String(
                                            String::new(),
                                        ))),
                                    ],
                                )))
                            };

                            if is_occ_1 {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    vec![effective_subject, pattern],
                                ))))
                            } else {
                                // ARRAY_EXTRACT(REGEXP_EXTRACT_ALL(s, p), occ)
                                let extract_all = Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![effective_subject, pattern],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_EXTRACT".to_string(),
                                    vec![extract_all, occurrence],
                                ))))
                            }
                        }
                        // REGEXP_SUBSTR(s, p, 1, 1, 'e') -> REGEXP_EXTRACT(s, p)
                        5 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let _position = args.remove(0);
                            let _occurrence = args.remove(0);
                            let _flags = args.remove(0);
                            // Strip 'e' flag, convert to REGEXP_EXTRACT
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT".to_string(),
                                vec![subject, pattern],
                            ))))
                        }
                        // REGEXP_SUBSTR(s, p, 1, 1, 'e', group) -> REGEXP_EXTRACT(s, p[, group])
                        _ => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let _position = args.remove(0);
                            let _occurrence = args.remove(0);
                            let _flags = args.remove(0);
                            let group = args.remove(0);
                            let is_group_0 = matches!(&group, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "0"));
                            if is_group_0 {
                                // Strip group=0 (default)
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    vec![subject, pattern],
                                ))))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    vec![subject, pattern, group],
                                ))))
                            }
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RegexpSubstrSnowflakeIdentity => {
                // Snowflake→Snowflake: REGEXP_SUBSTR/REGEXP_SUBSTR_ALL with 6 args
                // Strip trailing group=0
                if let Expression::Function(f) = e {
                    let func_name = f.name.clone();
                    let mut args = f.args;
                    if args.len() == 6 {
                        let is_group_0 = matches!(&args[5], Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "0"));
                        if is_group_0 {
                            args.truncate(5);
                        }
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        func_name, args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::RegexpSubstrAllSnowflakeToDuckDB => {
                // Snowflake REGEXP_SUBSTR_ALL -> DuckDB REGEXP_EXTRACT_ALL variants
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let arg_count = args.len();
                    match arg_count {
                        // REGEXP_SUBSTR_ALL(s, p) -> REGEXP_EXTRACT_ALL(s, p)
                        0..=2 => Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            args,
                        )))),
                        // REGEXP_SUBSTR_ALL(s, p, pos) -> REGEXP_EXTRACT_ALL(SUBSTRING(s, pos), p)
                        3 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let position = args.remove(0);
                            let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                            if is_pos_1 {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![subject, pattern],
                                ))))
                            } else {
                                let substring_expr = Expression::Function(Box::new(Function::new(
                                    "SUBSTRING".to_string(),
                                    vec![subject, position],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![substring_expr, pattern],
                                ))))
                            }
                        }
                        // REGEXP_SUBSTR_ALL(s, p, 1, occ) -> REGEXP_EXTRACT_ALL(s, p)[occ:]
                        4 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let position = args.remove(0);
                            let occurrence = args.remove(0);
                            let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                            let is_occ_1 = matches!(&occurrence, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));

                            let effective_subject = if is_pos_1 {
                                subject
                            } else {
                                Expression::Function(Box::new(Function::new(
                                    "SUBSTRING".to_string(),
                                    vec![subject, position],
                                )))
                            };

                            if is_occ_1 {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![effective_subject, pattern],
                                ))))
                            } else {
                                // REGEXP_EXTRACT_ALL(s, p)[occ:]
                                let extract_all = Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![effective_subject, pattern],
                                )));
                                Ok(Expression::ArraySlice(Box::new(
                                    crate::expressions::ArraySlice {
                                        this: extract_all,
                                        start: Some(occurrence),
                                        end: None,
                                    },
                                )))
                            }
                        }
                        // REGEXP_SUBSTR_ALL(s, p, 1, 1, 'e') -> REGEXP_EXTRACT_ALL(s, p)
                        5 => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let _position = args.remove(0);
                            let _occurrence = args.remove(0);
                            let _flags = args.remove(0);
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT_ALL".to_string(),
                                vec![subject, pattern],
                            ))))
                        }
                        // REGEXP_SUBSTR_ALL(s, p, 1, 1, 'e', 0) -> REGEXP_EXTRACT_ALL(s, p)
                        _ => {
                            let subject = args.remove(0);
                            let pattern = args.remove(0);
                            let _position = args.remove(0);
                            let _occurrence = args.remove(0);
                            let _flags = args.remove(0);
                            let group = args.remove(0);
                            let is_group_0 = matches!(&group, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "0"));
                            if is_group_0 {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![subject, pattern],
                                ))))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT_ALL".to_string(),
                                    vec![subject, pattern, group],
                                ))))
                            }
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RegexpCountSnowflakeToDuckDB => {
                // Snowflake REGEXP_COUNT(s, p[, pos[, flags]]) ->
                // DuckDB: CASE WHEN p = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(s, p)) END
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let arg_count = args.len();
                    let subject = args.remove(0);
                    let pattern = args.remove(0);

                    // Handle position arg
                    let effective_subject = if arg_count >= 3 {
                        let position = args.remove(0);
                        Expression::Function(Box::new(Function::new(
                            "SUBSTRING".to_string(),
                            vec![subject, position],
                        )))
                    } else {
                        subject
                    };

                    // Handle flags arg -> embed as (?flags) prefix in pattern
                    let effective_pattern = if arg_count >= 4 {
                        let flags = args.remove(0);
                        match &flags {
                            Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(f_str) if !f_str.is_empty()) =>
                            {
                                let Literal::String(f_str) = lit.as_ref() else {
                                    unreachable!()
                                };
                                // Always use concatenation: '(?flags)' || pattern
                                let prefix = Expression::Literal(Box::new(Literal::String(
                                    format!("(?{})", f_str),
                                )));
                                Expression::DPipe(Box::new(crate::expressions::DPipe {
                                    this: Box::new(prefix),
                                    expression: Box::new(pattern.clone()),
                                    safe: None,
                                }))
                            }
                            _ => pattern.clone(),
                        }
                    } else {
                        pattern.clone()
                    };

                    // Build: CASE WHEN p = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(s, p)) END
                    let extract_all = Expression::Function(Box::new(Function::new(
                        "REGEXP_EXTRACT_ALL".to_string(),
                        vec![effective_subject, effective_pattern.clone()],
                    )));
                    let length_expr = Expression::Length(Box::new(crate::expressions::UnaryFunc {
                        this: extract_all,
                        original_name: None,
                        inferred_type: None,
                    }));
                    let condition = Expression::Eq(Box::new(BinaryOp::new(
                        effective_pattern,
                        Expression::Literal(Box::new(Literal::String(String::new()))),
                    )));
                    Ok(Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![(condition, Expression::number(0))],
                        else_: Some(length_expr),
                        comments: vec![],
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::RegexpInstrSnowflakeToDuckDB => {
                // Snowflake REGEXP_INSTR(s, p[, pos[, occ[, option[, flags[, group]]]]]) ->
                // DuckDB: CASE WHEN s IS NULL OR p IS NULL [OR ...] THEN NULL
                //              WHEN p = '' THEN 0
                //              WHEN LENGTH(REGEXP_EXTRACT_ALL(eff_s, eff_p)) < occ THEN 0
                //              ELSE 1 + COALESCE(LIST_SUM(LIST_TRANSFORM(STRING_SPLIT_REGEX(eff_s, eff_p)[1:occ], x -> LENGTH(x))), 0)
                //                     + COALESCE(LIST_SUM(LIST_TRANSFORM(REGEXP_EXTRACT_ALL(eff_s, eff_p)[1:occ - 1], x -> LENGTH(x))), 0)
                //                     + pos_offset
                //         END
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let subject = args.remove(0);
                    let pattern = if !args.is_empty() {
                        args.remove(0)
                    } else {
                        Expression::Literal(Box::new(Literal::String(String::new())))
                    };

                    // Collect all original args for NULL checks
                    let position = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };
                    let occurrence = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };
                    let option = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };
                    let flags = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };
                    let _group = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };

                    let is_pos_1 = position.as_ref().map_or(true, |p| matches!(p, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1")));
                    let occurrence_expr = occurrence.clone().unwrap_or(Expression::number(1));

                    // Build NULL check: subject IS NULL OR pattern IS NULL [OR pos IS NULL ...]
                    let mut null_checks: Vec<Expression> = vec![
                        Expression::Is(Box::new(BinaryOp::new(
                            subject.clone(),
                            Expression::Null(Null),
                        ))),
                        Expression::Is(Box::new(BinaryOp::new(
                            pattern.clone(),
                            Expression::Null(Null),
                        ))),
                    ];
                    // Add NULL checks for all provided optional args
                    for opt_arg in [&position, &occurrence, &option, &flags].iter() {
                        if let Some(arg) = opt_arg {
                            null_checks.push(Expression::Is(Box::new(BinaryOp::new(
                                (*arg).clone(),
                                Expression::Null(Null),
                            ))));
                        }
                    }
                    // Chain with OR
                    let null_condition = null_checks
                        .into_iter()
                        .reduce(|a, b| Expression::Or(Box::new(BinaryOp::new(a, b))))
                        .unwrap();

                    // Effective subject (apply position offset)
                    let effective_subject = if is_pos_1 {
                        subject.clone()
                    } else {
                        let pos = position.clone().unwrap_or(Expression::number(1));
                        Expression::Function(Box::new(Function::new(
                            "SUBSTRING".to_string(),
                            vec![subject.clone(), pos],
                        )))
                    };

                    // Effective pattern (apply flags if present)
                    let effective_pattern = if let Some(ref fl) = flags {
                        if let Expression::Literal(lit) = fl {
                            if let Literal::String(f_str) = lit.as_ref() {
                                if !f_str.is_empty() {
                                    let prefix = Expression::Literal(Box::new(Literal::String(
                                        format!("(?{})", f_str),
                                    )));
                                    Expression::DPipe(Box::new(crate::expressions::DPipe {
                                        this: Box::new(prefix),
                                        expression: Box::new(pattern.clone()),
                                        safe: None,
                                    }))
                                } else {
                                    pattern.clone()
                                }
                            } else {
                                fl.clone()
                            }
                        } else {
                            pattern.clone()
                        }
                    } else {
                        pattern.clone()
                    };

                    // WHEN pattern = '' THEN 0
                    let empty_pattern_check = Expression::Eq(Box::new(BinaryOp::new(
                        effective_pattern.clone(),
                        Expression::Literal(Box::new(Literal::String(String::new()))),
                    )));

                    // WHEN LENGTH(REGEXP_EXTRACT_ALL(eff_s, eff_p)) < occ THEN 0
                    let match_count_check = Expression::Lt(Box::new(BinaryOp::new(
                        Expression::Length(Box::new(crate::expressions::UnaryFunc {
                            this: Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT_ALL".to_string(),
                                vec![effective_subject.clone(), effective_pattern.clone()],
                            ))),
                            original_name: None,
                            inferred_type: None,
                        })),
                        occurrence_expr.clone(),
                    )));

                    // Helper: build LENGTH lambda for LIST_TRANSFORM
                    let make_len_lambda = || {
                        Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                            parameters: vec![crate::expressions::Identifier::new("x")],
                            body: Expression::Length(Box::new(crate::expressions::UnaryFunc {
                                this: Expression::Identifier(crate::expressions::Identifier::new(
                                    "x",
                                )),
                                original_name: None,
                                inferred_type: None,
                            })),
                            colon: false,
                            parameter_types: vec![],
                        }))
                    };

                    // COALESCE(LIST_SUM(LIST_TRANSFORM(STRING_SPLIT_REGEX(s, p)[1:occ], x -> LENGTH(x))), 0)
                    let split_sliced =
                        Expression::ArraySlice(Box::new(crate::expressions::ArraySlice {
                            this: Expression::Function(Box::new(Function::new(
                                "STRING_SPLIT_REGEX".to_string(),
                                vec![effective_subject.clone(), effective_pattern.clone()],
                            ))),
                            start: Some(Expression::number(1)),
                            end: Some(occurrence_expr.clone()),
                        }));
                    let split_sum = Expression::Function(Box::new(Function::new(
                        "COALESCE".to_string(),
                        vec![
                            Expression::Function(Box::new(Function::new(
                                "LIST_SUM".to_string(),
                                vec![Expression::Function(Box::new(Function::new(
                                    "LIST_TRANSFORM".to_string(),
                                    vec![split_sliced, make_len_lambda()],
                                )))],
                            ))),
                            Expression::number(0),
                        ],
                    )));

                    // COALESCE(LIST_SUM(LIST_TRANSFORM(REGEXP_EXTRACT_ALL(s, p)[1:occ - 1], x -> LENGTH(x))), 0)
                    let extract_sliced =
                        Expression::ArraySlice(Box::new(crate::expressions::ArraySlice {
                            this: Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT_ALL".to_string(),
                                vec![effective_subject.clone(), effective_pattern.clone()],
                            ))),
                            start: Some(Expression::number(1)),
                            end: Some(Expression::Sub(Box::new(BinaryOp::new(
                                occurrence_expr.clone(),
                                Expression::number(1),
                            )))),
                        }));
                    let extract_sum = Expression::Function(Box::new(Function::new(
                        "COALESCE".to_string(),
                        vec![
                            Expression::Function(Box::new(Function::new(
                                "LIST_SUM".to_string(),
                                vec![Expression::Function(Box::new(Function::new(
                                    "LIST_TRANSFORM".to_string(),
                                    vec![extract_sliced, make_len_lambda()],
                                )))],
                            ))),
                            Expression::number(0),
                        ],
                    )));

                    // ELSE base: 1 + split_sum + extract_sum
                    let else_base = Expression::Add(Box::new(BinaryOp::new(
                        Expression::Add(Box::new(BinaryOp::new(Expression::number(1), split_sum))),
                        extract_sum,
                    )));

                    // Append the position offset as a left-associated `+ pos - 1` chain.
                    let else_expr = if !is_pos_1 {
                        let pos = position.clone().unwrap_or(Expression::number(1));
                        Expression::Sub(Box::new(BinaryOp::new(
                            Expression::Add(Box::new(BinaryOp::new(else_base, pos))),
                            Expression::number(1),
                        )))
                    } else {
                        Expression::Add(Box::new(BinaryOp::new(else_base, Expression::number(0))))
                    };

                    Ok(Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![
                            (null_condition, Expression::Null(Null)),
                            (empty_pattern_check, Expression::number(0)),
                            (match_count_check, Expression::number(0)),
                        ],
                        else_: Some(else_expr),
                        comments: vec![],
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::RegexpReplacePositionSnowflakeToDuckDB => {
                // Snowflake REGEXP_REPLACE(s, p, r, pos, occ) -> DuckDB form
                // pos=1, occ=1 -> REGEXP_REPLACE(s, p, r) (single replace, no 'g')
                // pos>1, occ=0 -> SUBSTRING(s, 1, pos-1) || REGEXP_REPLACE(SUBSTRING(s, pos), p, r, 'g')
                // pos>1, occ=1 -> SUBSTRING(s, 1, pos-1) || REGEXP_REPLACE(SUBSTRING(s, pos), p, r)
                // pos=1, occ=0 -> REGEXP_REPLACE(s, p, r, 'g') (replace all)
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let subject = args.remove(0);
                    let pattern = args.remove(0);
                    let replacement = args.remove(0);
                    let position = args.remove(0);
                    let occurrence = args.remove(0);

                    let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                    let is_occ_0 = matches!(&occurrence, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "0"));
                    let is_occ_1 = matches!(&occurrence, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));

                    if is_pos_1 && is_occ_1 {
                        // REGEXP_REPLACE(s, p, r) - single replace, no flags
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_REPLACE".to_string(),
                            vec![subject, pattern, replacement],
                        ))))
                    } else if is_pos_1 && is_occ_0 {
                        // REGEXP_REPLACE(s, p, r, 'g') - global replace
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_REPLACE".to_string(),
                            vec![
                                subject,
                                pattern,
                                replacement,
                                Expression::Literal(Box::new(Literal::String("g".to_string()))),
                            ],
                        ))))
                    } else {
                        // pos>1: SUBSTRING(s, 1, pos-1) || REGEXP_REPLACE(SUBSTRING(s, pos), p, r[, 'g'])
                        // Pre-compute pos-1 when position is a numeric literal
                        let pos_minus_1 = if let Expression::Literal(ref lit) = position {
                            if let Literal::Number(ref n) = lit.as_ref() {
                                if let Ok(val) = n.parse::<i64>() {
                                    Expression::number(val - 1)
                                } else {
                                    Expression::Sub(Box::new(BinaryOp::new(
                                        position.clone(),
                                        Expression::number(1),
                                    )))
                                }
                            } else {
                                position.clone()
                            }
                        } else {
                            Expression::Sub(Box::new(BinaryOp::new(
                                position.clone(),
                                Expression::number(1),
                            )))
                        };
                        let prefix = Expression::Function(Box::new(Function::new(
                            "SUBSTRING".to_string(),
                            vec![subject.clone(), Expression::number(1), pos_minus_1],
                        )));
                        let suffix_subject = Expression::Function(Box::new(Function::new(
                            "SUBSTRING".to_string(),
                            vec![subject, position],
                        )));
                        let mut replace_args = vec![suffix_subject, pattern, replacement];
                        if is_occ_0 {
                            replace_args.push(Expression::Literal(Box::new(Literal::String(
                                "g".to_string(),
                            ))));
                        }
                        let replace_expr = Expression::Function(Box::new(Function::new(
                            "REGEXP_REPLACE".to_string(),
                            replace_args,
                        )));
                        Ok(Expression::DPipe(Box::new(crate::expressions::DPipe {
                            this: Box::new(prefix),
                            expression: Box::new(replace_expr),
                            safe: None,
                        })))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RlikeSnowflakeToDuckDB => {
                // Snowflake RLIKE(a, b[, flags]) -> DuckDB REGEXP_FULL_MATCH(a, b[, flags])
                // Both do full-string matching, so no anchoring needed
                let (subject, pattern, flags) = match e {
                    Expression::RegexpLike(ref rl) => {
                        (rl.this.clone(), rl.pattern.clone(), rl.flags.clone())
                    }
                    Expression::Function(ref f) if f.args.len() >= 2 => {
                        let s = f.args[0].clone();
                        let p = f.args[1].clone();
                        let fl = f.args.get(2).cloned();
                        (s, p, fl)
                    }
                    _ => return Ok(e),
                };

                let mut result_args = vec![subject, pattern];
                if let Some(fl) = flags {
                    result_args.push(fl);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_FULL_MATCH".to_string(),
                    result_args,
                ))))
            }

            Action::RegexpExtractAllToSnowflake => {
                // BigQuery REGEXP_EXTRACT_ALL(s, p) -> Snowflake REGEXP_SUBSTR_ALL(s, p)
                // With capture group: REGEXP_SUBSTR_ALL(s, p, 1, 1, 'c', 1)
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    if args.len() >= 2 {
                        let str_expr = args.remove(0);
                        let pattern = args.remove(0);

                        let has_groups = match &pattern {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                s.contains('(') && s.contains(')')
                            }
                            _ => false,
                        };

                        if has_groups {
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_SUBSTR_ALL".to_string(),
                                vec![
                                    str_expr,
                                    pattern,
                                    Expression::number(1),
                                    Expression::number(1),
                                    Expression::Literal(Box::new(Literal::String("c".to_string()))),
                                    Expression::number(1),
                                ],
                            ))))
                        } else {
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_SUBSTR_ALL".to_string(),
                                vec![str_expr, pattern],
                            ))))
                        }
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_SUBSTR_ALL".to_string(),
                            args,
                        ))))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::RegexpLikeExasolAnchor => {
                // RegexpLike -> Exasol: wrap pattern with .*...*
                // Exasol REGEXP_LIKE does full-string match, but RLIKE/REGEXP from other
                // dialects does partial match, so we need to anchor with .* on both sides
                if let Expression::RegexpLike(mut f) = e {
                    match &f.pattern {
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                            let Literal::String(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            // String literal: wrap with .*...*
                            f.pattern = Expression::Literal(Box::new(Literal::String(format!(
                                ".*{}.*",
                                s
                            ))));
                        }
                        _ => {
                            // Non-literal: wrap with CONCAT('.*', pattern, '.*')
                            f.pattern = Expression::Paren(Box::new(crate::expressions::Paren {
                                this: Expression::Concat(Box::new(crate::expressions::BinaryOp {
                                    left: Expression::Concat(Box::new(
                                        crate::expressions::BinaryOp {
                                            left: Expression::Literal(Box::new(Literal::String(
                                                ".*".to_string(),
                                            ))),
                                            right: f.pattern,
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        },
                                    )),
                                    right: Expression::Literal(Box::new(Literal::String(
                                        ".*".to_string(),
                                    ))),
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                })),
                                trailing_comments: vec![],
                            }));
                        }
                    }
                    Ok(Expression::RegexpLike(f))
                } else {
                    Ok(e)
                }
            }

            Action::SnowflakeWindowFrameStrip => {
                // Strip the default ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                // for FIRST_VALUE/LAST_VALUE/NTH_VALUE when targeting Snowflake
                if let Expression::WindowFunction(mut wf) = e {
                    wf.over.frame = None;
                    Ok(Expression::WindowFunction(wf))
                } else {
                    Ok(e)
                }
            }

            Action::SnowflakeWindowFrameAdd => {
                // Add default ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                // for FIRST_VALUE/LAST_VALUE/NTH_VALUE when transpiling from Snowflake to non-Snowflake
                if let Expression::WindowFunction(mut wf) = e {
                    wf.over.frame = Some(crate::expressions::WindowFrame {
                        kind: crate::expressions::WindowFrameKind::Rows,
                        start: crate::expressions::WindowFrameBound::UnboundedPreceding,
                        end: Some(crate::expressions::WindowFrameBound::UnboundedFollowing),
                        exclude: None,
                        kind_text: None,
                        start_side_text: None,
                        end_side_text: None,
                    });
                    Ok(Expression::WindowFunction(wf))
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

pub(super) fn snowflake_regexp_like_to_clickhouse(
    mut regexp: RegexpFunc,
    context: &NormalizationContext,
) -> Result<Expression> {
    let flags = match snowflake_regex_inline_flags(regexp.flags.as_ref()) {
        Ok(flags) => flags,
        Err(reason) if context.strict => {
            return Err(crate::error::Error::unsupported(
                format!("Snowflake REGEXP_LIKE {reason} cannot be preserved"),
                context.target.to_string(),
            ));
        }
        Err(_) => return Ok(Expression::RegexpLike(Box::new(regexp))),
    };

    let prefix = format!("{flags}^(");
    regexp.pattern = match regexp.pattern {
        Expression::Literal(literal) => match *literal {
            Literal::String(pattern) => Expression::string(format!("{prefix}{pattern})$")),
            other => Expression::Function(Box::new(Function::new(
                "concat".to_string(),
                vec![
                    Expression::string(prefix),
                    Expression::Literal(Box::new(other)),
                    Expression::string(")$"),
                ],
            ))),
        },
        pattern => Expression::Function(Box::new(Function::new(
            "concat".to_string(),
            vec![
                Expression::string(prefix),
                pattern,
                Expression::string(")$"),
            ],
        ))),
    };
    regexp.flags = None;
    Ok(Expression::RegexpLike(Box::new(regexp)))
}

fn snowflake_regex_inline_flags(flags: Option<&Expression>) -> std::result::Result<String, String> {
    let Some(flags) = flags else {
        // ClickHouse enables dot-all matching by default, unlike Snowflake.
        return Ok("(?-s)".to_string());
    };
    let Expression::Literal(literal) = flags else {
        return Err("with dynamic parameters".to_string());
    };
    let Literal::String(flags) = literal.as_ref() else {
        return Err("with non-string parameters".to_string());
    };

    let mut case_insensitive = false;
    let mut multiline = false;
    let mut dot_matches_newline = false;
    for flag in flags.chars() {
        match flag.to_ascii_lowercase() {
            'c' => case_insensitive = false,
            'i' => case_insensitive = true,
            'm' => multiline = true,
            's' => dot_matches_newline = true,
            // `e` controls submatch extraction and does not change the boolean result.
            'e' => {}
            unsupported => {
                return Err(format!("parameter `{unsupported}`"));
            }
        }
    }

    let mut enabled = String::new();
    if case_insensitive {
        enabled.push('i');
    }
    if multiline {
        enabled.push('m');
    }
    if dot_matches_newline {
        enabled.push('s');
    }

    if dot_matches_newline {
        Ok(format!("(?{enabled})"))
    } else if enabled.is_empty() {
        Ok("(?-s)".to_string())
    } else {
        Ok(format!("(?{enabled}-s)"))
    }
}

pub(super) fn cast_expr(this: Expression, to: DataType) -> Expression {
    Expression::Cast(Box::new(Cast {
        this,
        to,
        trailing_comments: Vec::new(),
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StaticNumericValue {
    Zero,
    Finite(f64),
    NonFinite,
    Unknown,
}

fn static_numeric_value(expression: &Expression) -> StaticNumericValue {
    match expression {
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::Number(number) => {
                let mantissa = number
                    .trim_start_matches(['+', '-'])
                    .split(['e', 'E'])
                    .next()
                    .unwrap_or(number);
                let is_exact_zero = mantissa.chars().filter(|ch| *ch != '.').all(|ch| ch == '0');

                number
                    .parse::<f64>()
                    .map(|value| {
                        if is_exact_zero {
                            StaticNumericValue::Zero
                        } else if value == 0.0 || !value.is_finite() {
                            StaticNumericValue::NonFinite
                        } else {
                            StaticNumericValue::Finite(value)
                        }
                    })
                    .unwrap_or(StaticNumericValue::Unknown)
            }
            Literal::HexNumber(number) => {
                let number = number
                    .strip_prefix("0x")
                    .or_else(|| number.strip_prefix("0X"))
                    .unwrap_or(number);
                u128::from_str_radix(number, 16)
                    .map(|value| {
                        if value == 0 {
                            StaticNumericValue::Zero
                        } else {
                            StaticNumericValue::Finite(value as f64)
                        }
                    })
                    .unwrap_or(StaticNumericValue::Unknown)
            }
            _ => StaticNumericValue::Unknown,
        },
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            static_numeric_value(&cast.this)
        }
        Expression::Paren(paren) => static_numeric_value(&paren.this),
        Expression::Alias(alias) => static_numeric_value(&alias.this),
        Expression::Neg(unary) => match static_numeric_value(&unary.this) {
            StaticNumericValue::Finite(value) => StaticNumericValue::Finite(-value),
            other => other,
        },
        _ => StaticNumericValue::Unknown,
    }
}

fn canonical_integer_type(data_type: &DataType) -> Option<(u8, DataType)> {
    match data_type {
        DataType::TinyInt { .. } => Some((0, DataType::TinyInt { length: None })),
        DataType::SmallInt { .. } => Some((1, DataType::SmallInt { length: None })),
        DataType::Int { .. } => Some((
            2,
            DataType::Int {
                length: None,
                integer_spelling: true,
            },
        )),
        DataType::BigInt { .. } => Some((3, DataType::BigInt { length: None })),
        DataType::Nullable { inner } => canonical_integer_type(inner),
        DataType::Custom { name } => match name.to_ascii_uppercase().as_str() {
            "TINYINT" | "INT8" => Some((0, DataType::TinyInt { length: None })),
            "SMALLINT" | "INT16" => Some((1, DataType::SmallInt { length: None })),
            "INT" | "INTEGER" | "INT32" => Some((
                2,
                DataType::Int {
                    length: None,
                    integer_spelling: true,
                },
            )),
            "BIGINT" | "INT64" => Some((3, DataType::BigInt { length: None })),
            _ => None,
        },
        _ => None,
    }
}

fn expression_integer_type(expression: &Expression, source: DialectType) -> Option<(u8, DataType)> {
    match expression {
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            canonical_integer_type(&cast.to)
        }
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::Number(number) if !number.contains(['.', 'e', 'E']) => {
                if number.parse::<i32>().is_ok() {
                    canonical_integer_type(&DataType::Int {
                        length: None,
                        integer_spelling: true,
                    })
                } else if !matches!(source, DialectType::TSQL) && number.parse::<i64>().is_ok() {
                    canonical_integer_type(&DataType::BigInt { length: None })
                } else {
                    None
                }
            }
            _ => None,
        },
        Expression::Paren(paren) => expression_integer_type(&paren.this, source),
        Expression::Alias(alias) => expression_integer_type(&alias.this, source),
        Expression::Neg(unary) => expression_integer_type(&unary.this, source),
        Expression::Add(binary)
        | Expression::Sub(binary)
        | Expression::Mul(binary)
        | Expression::Mod(binary) => {
            let left = expression_integer_type(&binary.left, source)?;
            let right = expression_integer_type(&binary.right, source)?;
            Some(if left.0 >= right.0 { left } else { right })
        }
        _ => expression.inferred_type().and_then(canonical_integer_type),
    }
}

fn cast_integer_operand(expression: Expression, data_type: &DataType) -> Expression {
    if matches!(
        &expression,
        Expression::Cast(cast) if canonical_integer_type(&cast.to).is_some_and(|(_, current)| current == *data_type)
    ) {
        expression
    } else {
        cast_expr(expression, data_type.clone())
    }
}

fn clickhouse_float_type(data_type: &DataType, source: DialectType) -> Option<DataType> {
    match data_type {
        DataType::Double { .. } => Some(DataType::Double {
            precision: None,
            scale: None,
        }),
        DataType::Float {
            real_spelling: true,
            ..
        } => Some(DataType::Float {
            precision: None,
            scale: None,
            real_spelling: true,
        }),
        DataType::Float { .. } if matches!(source, DialectType::TSQL | DialectType::Redshift) => {
            Some(DataType::Double {
                precision: None,
                scale: None,
            })
        }
        DataType::Float { .. } => Some(DataType::Float {
            precision: None,
            scale: None,
            real_spelling: false,
        }),
        DataType::Nullable { inner } => clickhouse_float_type(inner, source),
        DataType::Custom { name } => match name.to_ascii_uppercase().as_str() {
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT64" => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            "REAL" | "FLOAT4" | "FLOAT32" => Some(DataType::Float {
                precision: None,
                scale: None,
                real_spelling: true,
            }),
            "FLOAT"
                if matches!(
                    source,
                    DialectType::PostgreSQL | DialectType::Redshift | DialectType::TSQL
                ) =>
            {
                Some(DataType::Double {
                    precision: None,
                    scale: None,
                })
            }
            _ => None,
        },
        _ => None,
    }
}

fn normalize_clickhouse_float_operand(expression: Expression, source: DialectType) -> Expression {
    match expression {
        Expression::Cast(mut cast) => {
            if let Some(data_type) = clickhouse_float_type(&cast.to, source) {
                cast.to = data_type;
            }
            Expression::Cast(cast)
        }
        Expression::TryCast(mut cast) => {
            if let Some(data_type) = clickhouse_float_type(&cast.to, source) {
                cast.to = data_type;
            }
            Expression::TryCast(cast)
        }
        Expression::SafeCast(mut cast) => {
            if let Some(data_type) = clickhouse_float_type(&cast.to, source) {
                cast.to = data_type;
            }
            Expression::SafeCast(cast)
        }
        Expression::Paren(mut paren) => {
            paren.this = normalize_clickhouse_float_operand(paren.this, source);
            Expression::Paren(paren)
        }
        Expression::Alias(mut alias) => {
            alias.this = normalize_clickhouse_float_operand(alias.this, source);
            Expression::Alias(alias)
        }
        Expression::Neg(mut unary) => {
            unary.this = normalize_clickhouse_float_operand(unary.this, source);
            Expression::Neg(unary)
        }
        other => other,
    }
}

fn normalize_source_division_to_clickhouse(
    mut division: BinaryOp,
    context: &NormalizationContext,
) -> Result<Expression> {
    let left_kind = super::scalar::expression_numeric_kind(&division.left);
    let right_kind = super::scalar::expression_numeric_kind(&division.right);

    if left_kind == super::scalar::NumericKind::Integer
        && right_kind == super::scalar::NumericKind::Integer
    {
        if let (Some(left_type), Some(right_type)) = (
            expression_integer_type(&division.left, context.source),
            expression_integer_type(&division.right, context.source),
        ) {
            let result_type = if left_type.0 >= right_type.0 {
                left_type.1
            } else {
                right_type.1
            };
            return Ok(Expression::IntDiv(Box::new(BinaryFunc {
                this: cast_integer_operand(division.left, &result_type),
                expression: cast_integer_operand(division.right, &result_type),
                original_name: None,
                inferred_type: Some(result_type),
            })));
        }
    }

    if !context.strict {
        return Ok(Expression::Div(Box::new(division)));
    }

    if left_kind == super::scalar::NumericKind::Integer
        && right_kind == super::scalar::NumericKind::Integer
    {
        return Err(crate::error::Error::unsupported(
            format!(
                "{} integer division with unresolved operand widths cannot be preserved for ClickHouse",
                context.source
            ),
            context.target.to_string(),
        ));
    }

    if left_kind == super::scalar::NumericKind::Unknown
        || right_kind == super::scalar::NumericKind::Unknown
    {
        return Err(crate::error::Error::unsupported(
            format!(
                "{} division with unresolved operand types cannot be preserved for ClickHouse",
                context.source
            ),
            context.target.to_string(),
        ));
    }

    if matches!(
        (left_kind, right_kind),
        (
            super::scalar::NumericKind::Decimal,
            super::scalar::NumericKind::Decimal | super::scalar::NumericKind::Integer
        ) | (
            super::scalar::NumericKind::Integer,
            super::scalar::NumericKind::Decimal
        )
    ) {
        return Err(crate::error::Error::unsupported(
            format!(
                "{} decimal division precision, scale, rounding, and overflow semantics cannot be preserved for ClickHouse",
                context.source
            ),
            context.target.to_string(),
        ));
    }

    let left_value = static_numeric_value(&division.left);
    let right_value = static_numeric_value(&division.right);
    let statically_safe_float_division = matches!(
        (left_value, right_value),
        (StaticNumericValue::Zero, StaticNumericValue::Finite(_))
            | (StaticNumericValue::Finite(_), StaticNumericValue::Finite(_))
    ) && match (left_value, right_value) {
        (StaticNumericValue::Zero, StaticNumericValue::Finite(divisor)) => divisor != 0.0,
        (StaticNumericValue::Finite(dividend), StaticNumericValue::Finite(divisor)) => {
            divisor != 0.0 && (dividend / divisor).is_finite()
        }
        _ => false,
    };

    if statically_safe_float_division {
        division.left = normalize_clickhouse_float_operand(division.left, context.source);
        division.right = normalize_clickhouse_float_operand(division.right, context.source);
        Ok(Expression::Div(Box::new(division)))
    } else {
        Err(crate::error::Error::unsupported(
            format!(
                "{} floating-point division may error on zero or overflow, while ClickHouse returns a non-finite Float64 value",
                context.source
            ),
            context.target.to_string(),
        ))
    }
}

pub(super) fn lower_expr(this: Expression) -> Expression {
    Expression::Function(Box::new(Function::new("LOWER".to_string(), vec![this])))
}

pub(super) fn build_tsql_regex_patindex_predicate(
    this: Expression,
    pattern: Expression,
    case_insensitive: bool,
) -> Expression {
    let (this, pattern) = if case_insensitive {
        (lower_expr(this), lower_expr(pattern))
    } else {
        (this, pattern)
    };
    let patindex = Expression::Function(Box::new(Function::new(
        "PATINDEX".to_string(),
        vec![pattern, this],
    )));

    Expression::Gt(Box::new(BinaryOp::new(patindex, Expression::number(0))))
}

pub(super) fn similar_to_can_lower_to_tsql_like(f: &crate::expressions::SimilarToExpr) -> bool {
    match &f.pattern {
        Expression::Literal(literal) if literal.is_string() => {
            similar_to_literal_pattern_is_like_compatible(literal.value_str())
        }
        _ => false,
    }
}

pub(super) fn similar_to_literal_pattern_is_like_compatible(pattern: &str) -> bool {
    if pattern.contains('\\') {
        return false;
    }

    !pattern
        .chars()
        .any(|ch| matches!(ch, '|' | '*' | '+' | '?' | '{' | '}' | '(' | ')'))
}

pub(super) fn build_tsql_div_func(
    left: Expression,
    right: Expression,
    target: DialectType,
) -> Expression {
    let cast_left = cast_expr(
        left,
        DataType::Double {
            precision: None,
            scale: None,
        },
    );
    let divided = Expression::Div(Box::new(BinaryOp::new(cast_left, right)));
    let cast_int = cast_expr(
        divided,
        DataType::Int {
            length: None,
            integer_spelling: true,
        },
    );
    let numeric_type = match target {
        DialectType::Fabric => DataType::Custom {
            name: "DECIMAL".to_string(),
        },
        _ => DataType::Custom {
            name: "NUMERIC".to_string(),
        },
    };
    cast_expr(cast_int, numeric_type)
}

pub(super) fn build_tsql_cbrt_power(this: Expression) -> Expression {
    let base = cast_expr(
        this,
        DataType::Double {
            precision: None,
            scale: None,
        },
    );
    let exponent = Expression::Div(Box::new(BinaryOp::new(
        Expression::Literal(Box::new(Literal::Number("1.0".to_string()))),
        Expression::Literal(Box::new(Literal::Number("3.0".to_string()))),
    )));

    Expression::Power(Box::new(crate::expressions::BinaryFunc {
        this: base,
        expression: exponent,
        original_name: None,
        inferred_type: None,
    }))
}

fn presto_regexp_replacement_to_duckdb(replacement: &str) -> Result<String> {
    let mut chars = replacement.chars().peekable();
    let mut converted = String::with_capacity(replacement.len());
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if let Some(literal) = chars.next() {
                    // Trino quotes the following character; RE2 only needs
                    // quoting for a literal backslash in replacement text.
                    if literal == '\\' {
                        converted.push('\\');
                    }
                    converted.push(literal);
                } else {
                    return Err(crate::error::Error::unsupported(
                        "REGEXP_REPLACE replacement ends in an escape",
                        "duckdb",
                    ));
                }
            }
            '$' => match chars.next() {
                Some(group) if group.is_ascii_digit() => {
                    if chars.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                        return Err(crate::error::Error::unsupported(
                            "DuckDB REGEXP_REPLACE does not support multi-digit capture references",
                            "duckdb",
                        ));
                    }
                    converted.push('\\');
                    converted.push(group);
                }
                _ => {
                    return Err(crate::error::Error::unsupported(
                        "DuckDB REGEXP_REPLACE requires numbered capture references",
                        "duckdb",
                    ))
                }
            },
            _ => converted.push(ch),
        }
    }
    Ok(converted)
}
