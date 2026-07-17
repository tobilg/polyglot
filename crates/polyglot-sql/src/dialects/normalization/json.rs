use super::{types, NormalizationContext, RewriteOutcome};
use crate::dialects::DialectType;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    TsqlJsonConstructorReturnType,
    PostgresJsonBuildObjectToJsonObject,
    PostgresJsonAggToJsonArrayAgg,
    JsonExtractToGetJsonObject,
    JsonExtractScalarToGetJsonObject,
    JsonQueryValueConvert,
    JsonLiteralToJsonParse,
    DuckDBCastJsonToVariant,
    DuckDBTryCastJsonToTryJsonParse,
    DuckDBJsonFuncToJsonParse,
    DuckDBJsonValidToIsJson,
    CastToJsonForSpark,
    CastJsonToFromJson,
    ToJsonConvert,
    JsonToGetPath,
    JsonObjectAggConvert,
    JsonbExistsConvert,
    JsonExtractToArrow,
    JsonExtractToTsql,
    JsonExtractToClickHouse,
    JsonExtractScalarConvert,
    JsonPathNormalize,
    JsonKeysConvert,
    ParseJsonStrip,
}

pub(super) fn rewrite(
    action: Action,
    expression: Expression,
    context: &NormalizationContext,
) -> Result<RewriteOutcome> {
    let target = context.target;
    let e = expression;
    let expression = (|| -> Result<Expression> {
        match action {
            Action::TsqlJsonConstructorReturnType => rewrite_tsql_json_constructor_return_type(e),
            Action::PostgresJsonBuildObjectToJsonObject => match e {
                Expression::Function(f) => Ok(Expression::JsonObject(
                    build_json_object_from_pairs(f.args)
                        .expect("action selected only for even key/value argument lists"),
                )),
                _ => Ok(e),
            },
            Action::PostgresJsonAggToJsonArrayAgg => match e {
                Expression::Function(mut f) if f.args.len() == 1 && !f.distinct => Ok(
                    Expression::JsonArrayAgg(Box::new(crate::expressions::JsonArrayAggFunc {
                        this: f.args.remove(0),
                        order_by: None,
                        null_handling: Some(JsonNullHandling::NullOnNull),
                        filter: None,
                    })),
                ),
                Expression::Function(mut f) => {
                    f.name = "JSON_ARRAYAGG".to_string();
                    Ok(Expression::Function(f))
                }
                Expression::AggregateFunction(mut af)
                    if af.args.len() == 1
                        && !af.distinct
                        && af.filter.is_none()
                        && af.limit.is_none()
                        && af.ignore_nulls.is_none() =>
                {
                    for ordered in &mut af.order_by {
                        if ordered.nulls_first.is_none() {
                            // PostgreSQL treats NULL as larger than every non-NULL value.
                            ordered.nulls_first = Some(ordered.desc);
                        }
                    }
                    let order_by = if af.order_by.is_empty() {
                        None
                    } else {
                        Some(std::mem::take(&mut af.order_by))
                    };
                    Ok(Expression::JsonArrayAgg(Box::new(JsonArrayAggFunc {
                        this: af.args.remove(0),
                        order_by,
                        null_handling: Some(JsonNullHandling::NullOnNull),
                        filter: None,
                    })))
                }
                Expression::AggregateFunction(mut af) => {
                    af.name = "JSON_ARRAYAGG".to_string();
                    Ok(Expression::AggregateFunction(af))
                }
                _ => Ok(e),
            },
            Action::JsonExtractToGetJsonObject => {
                if let Expression::JsonExtract(f) = e {
                    if matches!(target, DialectType::PostgreSQL | DialectType::Redshift) {
                        // JSON_EXTRACT(x, '$.key') -> JSON_EXTRACT_PATH(x, 'key') for PostgreSQL
                        // Use proper decomposition that handles brackets
                        let keys: Vec<Expression> = if let Expression::Literal(lit) = f.path {
                            if let Literal::String(ref s) = lit.as_ref() {
                                let parts = decompose_json_path(s);
                                parts.into_iter().map(|k| Expression::string(&k)).collect()
                            } else {
                                vec![]
                            }
                        } else {
                            vec![f.path]
                        };
                        let func_name = if matches!(target, DialectType::Redshift) {
                            "JSON_EXTRACT_PATH_TEXT"
                        } else {
                            "JSON_EXTRACT_PATH"
                        };
                        let mut args = vec![f.this];
                        args.extend(keys);
                        Ok(Expression::Function(Box::new(Function::new(
                            func_name.to_string(),
                            args,
                        ))))
                    } else {
                        // GET_JSON_OBJECT(x, '$.path') for Hive/Spark
                        // Convert bracket double quotes to single quotes
                        let path = if let Expression::Literal(ref lit) = f.path {
                            if let Literal::String(ref s) = lit.as_ref() {
                                let normalized = bracket_to_single_quotes(s);
                                if normalized != *s {
                                    Expression::string(&normalized)
                                } else {
                                    f.path.clone()
                                }
                            } else {
                                f.path.clone()
                            }
                        } else {
                            f.path.clone()
                        };
                        Ok(Expression::Function(Box::new(Function::new(
                            "GET_JSON_OBJECT".to_string(),
                            vec![f.this, path],
                        ))))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::JsonExtractScalarToGetJsonObject => {
                // JSON_EXTRACT_SCALAR(x, '$.path') -> GET_JSON_OBJECT(x, '$.path') for Hive/Spark
                if let Expression::JsonExtractScalar(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "GET_JSON_OBJECT".to_string(),
                        vec![f.this, f.path],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::JsonQueryValueConvert => {
                // JsonQuery/JsonValue -> target-specific
                let (f, is_query) = match e {
                    Expression::JsonQuery(f) => (f, true),
                    Expression::JsonValue(f) => (f, false),
                    _ => return Ok(e),
                };
                match target {
                    DialectType::TSQL | DialectType::Fabric => {
                        // ISNULL(JSON_QUERY(...), JSON_VALUE(...))
                        let json_query = Expression::Function(Box::new(Function::new(
                            "JSON_QUERY".to_string(),
                            vec![f.this.clone(), f.path.clone()],
                        )));
                        let json_value = Expression::Function(Box::new(Function::new(
                            "JSON_VALUE".to_string(),
                            vec![f.this, f.path],
                        )));
                        Ok(Expression::Function(Box::new(Function::new(
                            "ISNULL".to_string(),
                            vec![json_query, json_value],
                        ))))
                    }
                    DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                        Ok(Expression::Function(Box::new(Function::new(
                            "GET_JSON_OBJECT".to_string(),
                            vec![f.this, f.path],
                        ))))
                    }
                    DialectType::PostgreSQL | DialectType::Redshift => {
                        Ok(Expression::Function(Box::new(Function::new(
                            "JSON_EXTRACT_PATH_TEXT".to_string(),
                            vec![f.this, f.path],
                        ))))
                    }
                    DialectType::DuckDB | DialectType::SQLite => {
                        // json -> path arrow syntax
                        Ok(Expression::JsonExtract(Box::new(
                            crate::expressions::JsonExtractFunc {
                                this: f.this,
                                path: f.path,
                                returning: f.returning,
                                arrow_syntax: true,
                                hash_arrow_syntax: false,
                                wrapper_option: f.wrapper_option,
                                quotes_option: f.quotes_option,
                                on_scalar_string: f.on_scalar_string,
                                on_error: f.on_error,
                            },
                        )))
                    }
                    DialectType::Snowflake => {
                        // GET_PATH(PARSE_JSON(json), 'path')
                        // Strip $. prefix from path
                        // Only wrap in PARSE_JSON if not already a PARSE_JSON call or ParseJson expression
                        let json_expr = match &f.this {
                            Expression::Function(ref inner_f)
                                if inner_f.name.eq_ignore_ascii_case("PARSE_JSON") =>
                            {
                                f.this
                            }
                            Expression::ParseJson(_) => {
                                // Already a ParseJson expression, which generates as PARSE_JSON(...)
                                f.this
                            }
                            _ => Expression::Function(Box::new(Function::new(
                                "PARSE_JSON".to_string(),
                                vec![f.this],
                            ))),
                        };
                        let path_str = match &f.path {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                let stripped = s.strip_prefix("$.").unwrap_or(s);
                                Expression::Literal(Box::new(Literal::String(stripped.to_string())))
                            }
                            other => other.clone(),
                        };
                        Ok(Expression::Function(Box::new(Function::new(
                            "GET_PATH".to_string(),
                            vec![json_expr, path_str],
                        ))))
                    }
                    _ => {
                        // Default: keep as JSON_QUERY/JSON_VALUE function
                        let func_name = if is_query { "JSON_QUERY" } else { "JSON_VALUE" };
                        Ok(Expression::Function(Box::new(Function::new(
                            func_name.to_string(),
                            vec![f.this, f.path],
                        ))))
                    }
                }
            }

            Action::JsonLiteralToJsonParse => {
                // CAST('x' AS JSON) -> JSON_PARSE('x') for Presto, PARSE_JSON for Snowflake
                // Also DuckDB CAST(x AS JSON) -> JSON_PARSE(x) for Trino/Presto/Athena
                if let Expression::Cast(c) = e {
                    let func_name = if matches!(target, DialectType::Snowflake) {
                        "PARSE_JSON"
                    } else {
                        "JSON_PARSE"
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        func_name.to_string(),
                        vec![c.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::DuckDBCastJsonToVariant => {
                if let Expression::Cast(c) = e {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: c.this,
                        to: DataType::Custom {
                            name: "VARIANT".to_string(),
                        },
                        trailing_comments: c.trailing_comments,
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::DuckDBTryCastJsonToTryJsonParse => {
                // DuckDB TRY_CAST(x AS JSON) -> TRY(JSON_PARSE(x)) for Trino/Presto/Athena
                if let Expression::TryCast(c) = e {
                    let json_parse = Expression::Function(Box::new(Function::new(
                        "JSON_PARSE".to_string(),
                        vec![c.this],
                    )));
                    Ok(Expression::Function(Box::new(Function::new(
                        "TRY".to_string(),
                        vec![json_parse],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::DuckDBJsonFuncToJsonParse => {
                // DuckDB json(x) -> JSON_PARSE(x) for Trino/Presto/Athena
                if let Expression::Function(f) = e {
                    let args = f.args;
                    Ok(Expression::Function(Box::new(Function::new(
                        "JSON_PARSE".to_string(),
                        args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::DuckDBJsonValidToIsJson => {
                // DuckDB json_valid(x) -> x IS JSON (SQL:2016 predicate) for Trino/Presto/Athena
                if let Expression::Function(mut f) = e {
                    let arg = f.args.remove(0);
                    Ok(Expression::IsJson(Box::new(crate::expressions::IsJson {
                        this: arg,
                        json_type: None,
                        unique_keys: None,
                        negated: false,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::CastToJsonForSpark => {
                // CAST(x AS JSON) -> TO_JSON(x) for Spark
                if let Expression::Cast(c) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_JSON".to_string(),
                        vec![c.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::CastJsonToFromJson => {
                // CAST(ParseJson(literal) AS ARRAY/MAP/STRUCT) -> FROM_JSON(literal, type_string) for Spark
                if let Expression::Cast(c) = e {
                    // Extract the string literal from ParseJson
                    let literal_expr = if let Expression::ParseJson(pj) = c.this {
                        pj.this
                    } else {
                        c.this
                    };
                    // Convert the target DataType to Spark's type string format
                    let type_str = types::data_type_to_spark_string(&c.to);
                    Ok(Expression::Function(Box::new(Function::new(
                        "FROM_JSON".to_string(),
                        vec![
                            literal_expr,
                            Expression::Literal(Box::new(Literal::String(type_str))),
                        ],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ToJsonConvert => {
                // TO_JSON(x) -> target-specific conversion
                if let Expression::ToJson(f) = e {
                    let arg = f.this;
                    match target {
                        DialectType::Presto | DialectType::Trino => {
                            // JSON_FORMAT(CAST(x AS JSON))
                            let cast_json = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Custom {
                                    name: "JSON".to_string(),
                                },
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "JSON_FORMAT".to_string(),
                                vec![cast_json],
                            ))))
                        }
                        DialectType::BigQuery => Ok(Expression::Function(Box::new(Function::new(
                            "TO_JSON_STRING".to_string(),
                            vec![arg],
                        )))),
                        DialectType::DuckDB => {
                            // CAST(TO_JSON(x) AS TEXT)
                            let to_json =
                                Expression::ToJson(Box::new(crate::expressions::UnaryFunc {
                                    this: arg,
                                    original_name: None,
                                    inferred_type: None,
                                }));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: to_json,
                                to: DataType::Text,
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        _ => Ok(Expression::ToJson(Box::new(
                            crate::expressions::UnaryFunc {
                                this: arg,
                                original_name: None,
                                inferred_type: None,
                            },
                        ))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::JsonToGetPath => {
                // JSON_EXTRACT(x, '$.key') -> GET_PATH(PARSE_JSON(x), 'key')
                if let Expression::JsonExtract(je) = e {
                    // Convert to PARSE_JSON() wrapper:
                    // - JSON(x) -> PARSE_JSON(x)
                    // - PARSE_JSON(x) -> keep as-is
                    // - anything else -> wrap in PARSE_JSON()
                    let this = match &je.this {
                        Expression::Function(f)
                            if f.name.eq_ignore_ascii_case("JSON") && f.args.len() == 1 =>
                        {
                            Expression::Function(Box::new(Function::new(
                                "PARSE_JSON".to_string(),
                                f.args.clone(),
                            )))
                        }
                        Expression::Function(f) if f.name.eq_ignore_ascii_case("PARSE_JSON") => {
                            je.this.clone()
                        }
                        // GET_PATH result is already JSON, don't wrap
                        Expression::Function(f) if f.name.eq_ignore_ascii_case("GET_PATH") => {
                            je.this.clone()
                        }
                        other => {
                            // Wrap non-JSON expressions in PARSE_JSON()
                            Expression::Function(Box::new(Function::new(
                                "PARSE_JSON".to_string(),
                                vec![other.clone()],
                            )))
                        }
                    };
                    // Convert path: extract key from JSONPath or strip $. prefix from string
                    let path = match &je.path {
                        Expression::JSONPath(jp) => {
                            // Extract the key from JSONPath: $root.key -> 'key'
                            let mut key_parts = Vec::new();
                            for expr in &jp.expressions {
                                match expr {
                                    Expression::JSONPathRoot(_) => {} // skip root
                                    Expression::JSONPathKey(k) => {
                                        if let Expression::Literal(lit) = &*k.this {
                                            if let Literal::String(s) = lit.as_ref() {
                                                key_parts.push(s.clone());
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            if !key_parts.is_empty() {
                                Expression::Literal(Box::new(Literal::String(key_parts.join("."))))
                            } else {
                                je.path.clone()
                            }
                        }
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s.starts_with("$.")) =>
                        {
                            let Literal::String(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            let stripped = strip_json_wildcards(&s[2..].to_string());
                            Expression::Literal(Box::new(Literal::String(stripped)))
                        }
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s.starts_with('$')) =>
                        {
                            let Literal::String(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            let stripped = strip_json_wildcards(&s[1..].to_string());
                            Expression::Literal(Box::new(Literal::String(stripped)))
                        }
                        _ => je.path.clone(),
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        "GET_PATH".to_string(),
                        vec![this, path],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::JsonObjectAggConvert => {
                // JSON_OBJECT_AGG/JSONB_OBJECT_AGG -> JSON_GROUP_OBJECT for DuckDB
                match e {
                    Expression::Function(f) => Ok(Expression::Function(Box::new(Function::new(
                        "JSON_GROUP_OBJECT".to_string(),
                        f.args,
                    )))),
                    Expression::AggregateFunction(af) => {
                        // AggregateFunction stores all args in the `args` vec
                        Ok(Expression::Function(Box::new(Function::new(
                            "JSON_GROUP_OBJECT".to_string(),
                            af.args,
                        ))))
                    }
                    other => Ok(other),
                }
            }

            Action::JsonbExistsConvert => {
                // JSONB_EXISTS('json', 'key') -> JSON_EXISTS('json', '$.key') for DuckDB
                if let Expression::Function(f) = e {
                    if f.args.len() == 2 {
                        let json_expr = f.args[0].clone();
                        let key = match &f.args[1] {
                            Expression::Literal(lit)
                                if matches!(
                                    lit.as_ref(),
                                    crate::expressions::Literal::String(_)
                                ) =>
                            {
                                let crate::expressions::Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                format!("$.{}", s)
                            }
                            _ => return Ok(Expression::Function(f)),
                        };
                        Ok(Expression::Function(Box::new(Function::new(
                            "JSON_EXISTS".to_string(),
                            vec![json_expr, Expression::string(&key)],
                        ))))
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::JsonExtractToArrow => {
                // JSON_EXTRACT(x, path) -> x -> path for SQLite/DuckDB (set arrow_syntax = true)
                if let Expression::JsonExtract(mut f) = e {
                    f.arrow_syntax = true;
                    // Transform path: convert bracket notation to dot notation
                    // SQLite strips wildcards, DuckDB preserves them
                    if let Expression::Literal(ref lit) = f.path {
                        if let Literal::String(ref s) = lit.as_ref() {
                            let mut transformed = s.clone();
                            if matches!(target, DialectType::SQLite) {
                                transformed = strip_json_wildcards(&transformed);
                            }
                            transformed = bracket_to_dot_notation(&transformed);
                            if transformed != *s {
                                f.path = Expression::string(&transformed);
                            }
                        }
                    }
                    Ok(Expression::JsonExtract(f))
                } else {
                    Ok(e)
                }
            }

            Action::JsonExtractToTsql => {
                // PostgreSQL JSON operators need rooted T-SQL JSON paths. Generic
                // JSON_EXTRACT keeps the historical SQLGlot-style query/value fallback.
                let (this, path, func_name) = match e {
                    Expression::JsonExtract(f) => {
                        let path = normalize_tsql_json_path_expr(f.path, false, false);
                        if f.arrow_syntax {
                            (f.this, path, Some("JSON_QUERY"))
                        } else {
                            (f.this, path, None)
                        }
                    }
                    Expression::JsonExtractScalar(f) => {
                        let path = normalize_tsql_json_path_expr(
                            f.path,
                            f.hash_arrow_syntax,
                            f.hash_arrow_syntax,
                        );
                        if f.arrow_syntax || f.hash_arrow_syntax {
                            (f.this, path, Some("JSON_VALUE"))
                        } else {
                            (f.this, path, None)
                        }
                    }
                    Expression::JsonExtractPath(f) => {
                        let path = normalize_tsql_json_path_parts(f.paths);
                        (f.this, path, Some("JSON_QUERY"))
                    }
                    _ => return Ok(e),
                };

                if let Some(func_name) = func_name {
                    return Ok(build_tsql_json_function(func_name, this, path));
                }

                // Transform path: strip wildcards, convert bracket notation to dot notation,
                // and make sure the path is rooted for T-SQL JSON functions.
                let transformed_path = if let Expression::Literal(ref lit) = path {
                    if let Literal::String(ref s) = lit.as_ref() {
                        let stripped = strip_json_wildcards(s);
                        let dotted = bracket_to_dot_notation(&stripped);
                        normalize_tsql_json_path_expr(Expression::string(&dotted), false, false)
                    } else {
                        path.clone()
                    }
                } else {
                    path
                };
                let json_query = Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    vec![this.clone(), transformed_path.clone()],
                )));
                let json_value = Expression::Function(Box::new(Function::new(
                    "JSON_VALUE".to_string(),
                    vec![this, transformed_path],
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ISNULL".to_string(),
                    vec![json_query, json_value],
                ))))
            }

            Action::JsonExtractToClickHouse => {
                // JSON_EXTRACT/JSON_EXTRACT_SCALAR -> JSONExtractString(x, 'key1', idx, 'key2') for ClickHouse
                let (this, path) = match e {
                    Expression::JsonExtract(f) => (f.this, f.path),
                    Expression::JsonExtractScalar(f) => (f.this, f.path),
                    _ => return Ok(e),
                };
                let args: Vec<Expression> = if let Expression::Literal(lit) = path {
                    if let Literal::String(ref s) = lit.as_ref() {
                        let parts = decompose_json_path(s);
                        let mut result = vec![this];
                        for part in parts {
                            // ClickHouse uses 1-based integer indices for array access
                            if let Ok(idx) = part.parse::<i64>() {
                                result.push(Expression::number(idx + 1));
                            } else {
                                result.push(Expression::string(&part));
                            }
                        }
                        result
                    } else {
                        vec![]
                    }
                } else {
                    vec![this, path]
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "JSONExtractString".to_string(),
                    args,
                ))))
            }

            Action::JsonExtractScalarConvert => {
                // JSON_EXTRACT_SCALAR -> target-specific
                if let Expression::JsonExtractScalar(f) = e {
                    match target {
                        DialectType::PostgreSQL | DialectType::Redshift => {
                            // JSON_EXTRACT_SCALAR(x, '$.path') -> JSON_EXTRACT_PATH_TEXT(x, 'key1', 'key2')
                            let keys: Vec<Expression> = if let Expression::Literal(lit) = f.path {
                                if let Literal::String(ref s) = lit.as_ref() {
                                    let parts = decompose_json_path(s);
                                    parts.into_iter().map(|k| Expression::string(&k)).collect()
                                } else {
                                    vec![]
                                }
                            } else {
                                vec![f.path]
                            };
                            let mut args = vec![f.this];
                            args.extend(keys);
                            Ok(Expression::Function(Box::new(Function::new(
                                "JSON_EXTRACT_PATH_TEXT".to_string(),
                                args,
                            ))))
                        }
                        DialectType::Snowflake => {
                            // JSON_EXTRACT_SCALAR(x, '$.path') -> JSON_EXTRACT_PATH_TEXT(x, 'stripped_path')
                            let stripped_path = if let Expression::Literal(ref lit) = f.path {
                                if let Literal::String(ref s) = lit.as_ref() {
                                    let stripped = strip_json_dollar_prefix(s);
                                    Expression::string(&stripped)
                                } else {
                                    f.path.clone()
                                }
                            } else {
                                f.path
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                "JSON_EXTRACT_PATH_TEXT".to_string(),
                                vec![f.this, stripped_path],
                            ))))
                        }
                        DialectType::SQLite | DialectType::DuckDB => {
                            // JSON_EXTRACT_SCALAR(x, '$.path') -> x ->> '$.path'
                            Ok(Expression::JsonExtractScalar(Box::new(
                                crate::expressions::JsonExtractFunc {
                                    this: f.this,
                                    path: f.path,
                                    returning: f.returning,
                                    arrow_syntax: true,
                                    hash_arrow_syntax: false,
                                    wrapper_option: None,
                                    quotes_option: None,
                                    on_scalar_string: false,
                                    on_error: None,
                                },
                            )))
                        }
                        _ => Ok(Expression::JsonExtractScalar(f)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::JsonPathNormalize => {
                // Normalize JSON path format for BigQuery, MySQL, etc.
                if let Expression::JsonExtract(mut f) = e {
                    if let Expression::Literal(ref lit) = f.path {
                        if let Literal::String(ref s) = lit.as_ref() {
                            let mut normalized = s.clone();
                            // Convert bracket notation and handle wildcards per dialect
                            match target {
                                DialectType::BigQuery => {
                                    // BigQuery strips wildcards and uses single quotes in brackets
                                    normalized = strip_json_wildcards(&normalized);
                                    normalized = bracket_to_single_quotes(&normalized);
                                }
                                DialectType::MySQL => {
                                    // MySQL preserves wildcards, converts brackets to dot notation
                                    normalized = bracket_to_dot_notation(&normalized);
                                }
                                _ => {}
                            }
                            if normalized != *s {
                                f.path = Expression::string(&normalized);
                            }
                        }
                    }
                    Ok(Expression::JsonExtract(f))
                } else {
                    Ok(e)
                }
            }

            Action::JsonKeysConvert => {
                // JSON_KEYS(x) -> JSON_OBJECT_KEYS/OBJECT_KEYS
                if let Expression::JsonKeys(uf) = e {
                    match target {
                        DialectType::Spark | DialectType::Databricks => Ok(Expression::Function(
                            Box::new(Function::new("JSON_OBJECT_KEYS".to_string(), vec![uf.this])),
                        )),
                        DialectType::Snowflake => Ok(Expression::Function(Box::new(
                            Function::new("OBJECT_KEYS".to_string(), vec![uf.this]),
                        ))),
                        _ => Ok(Expression::JsonKeys(uf)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::ParseJsonStrip => {
                // PARSE_JSON(x) -> x (strip wrapper for SQLite/Doris)
                if let Expression::ParseJson(uf) = e {
                    Ok(uf.this)
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

#[derive(Debug)]
enum TsqlJsonReturnType {
    Json,
    Character(DataType),
    Unsupported,
}

pub(super) fn has_json_constructor_return_type(expression: &Expression) -> bool {
    match expression {
        Expression::JsonObject(function) => function.returning_type.is_some(),
        Expression::JSONObject(function) => function.return_type.is_some(),
        Expression::JSONArray(function) => function.return_type.is_some(),
        Expression::JSONArrayAgg(function) => function.return_type.is_some(),
        Expression::JSONObjectAgg(function) => function.return_type.is_some(),
        _ => false,
    }
}

pub(super) fn unsupported_tsql_json_constructor_return_type(
    expression: &Expression,
) -> Option<String> {
    let return_type = match expression {
        Expression::JsonObject(function) => function.returning_type.as_ref(),
        Expression::JSONObject(function) => expression_data_type(function.return_type.as_deref()),
        Expression::JSONArray(function) => expression_data_type(function.return_type.as_deref()),
        Expression::JSONArrayAgg(function) => expression_data_type(function.return_type.as_deref()),
        Expression::JSONObjectAgg(function) => {
            expression_data_type(function.return_type.as_deref())
        }
        _ => return None,
    };

    match return_type {
        Some(data_type)
            if matches!(
                classify_tsql_json_return_type(data_type),
                TsqlJsonReturnType::Unsupported
            ) =>
        {
            Some(format!("{data_type:?}"))
        }
        Some(_) => None,
        None if has_json_constructor_return_type(expression) => {
            Some("non-data-type expression".to_string())
        }
        None => None,
    }
}

fn expression_data_type(expression: Option<&Expression>) -> Option<&DataType> {
    match expression {
        Some(Expression::DataType(data_type)) => Some(data_type),
        Some(Expression::JSONFormat(format)) => expression_data_type(format.this.as_deref()),
        _ => None,
    }
}

fn classify_tsql_json_return_type(data_type: &DataType) -> TsqlJsonReturnType {
    match data_type {
        DataType::Json | DataType::JsonB => TsqlJsonReturnType::Json,
        DataType::Char { .. }
        | DataType::VarChar { .. }
        | DataType::String { .. }
        | DataType::Text
        | DataType::TextWithLength { .. } => TsqlJsonReturnType::Character(data_type.clone()),
        _ => TsqlJsonReturnType::Unsupported,
    }
}

fn rewrite_tsql_json_constructor_return_type(expression: Expression) -> Result<Expression> {
    let (constructor, return_type) = match expression {
        Expression::JsonObject(mut function) => {
            for (key, value) in &mut function.pairs {
                rewrite_nested_json_expression(key)?;
                rewrite_nested_json_expression(value)?;
            }
            let return_type = function.returning_type.take();
            function.format_json = false;
            function.encoding = None;
            (Expression::JsonObject(function), return_type)
        }
        Expression::JSONObject(mut function) => {
            rewrite_nested_json_expressions(&mut function.expressions)?;
            let return_type = take_expression_data_type(&mut function.return_type);
            function.encoding = None;
            (Expression::JSONObject(function), return_type)
        }
        Expression::JSONArray(mut function) => {
            rewrite_nested_json_expressions(&mut function.expressions)?;
            let return_type = take_expression_data_type(&mut function.return_type);
            (Expression::JSONArray(function), return_type)
        }
        Expression::JSONArrayAgg(mut function) => {
            function.this = Box::new(rewrite_nested_tsql_json_constructor_return_types(
                *function.this,
            )?);
            let return_type = take_expression_data_type(&mut function.return_type);
            (Expression::JSONArrayAgg(function), return_type)
        }
        Expression::JSONObjectAgg(mut function) => {
            rewrite_nested_json_expressions(&mut function.expressions)?;
            let return_type = take_expression_data_type(&mut function.return_type);
            function.encoding = None;
            (Expression::JSONObjectAgg(function), return_type)
        }
        other => return Ok(other),
    };

    match return_type.map(|data_type| classify_tsql_json_return_type(&data_type)) {
        Some(TsqlJsonReturnType::Character(data_type)) => Ok(Expression::Cast(Box::new(Cast {
            this: constructor,
            to: data_type,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))),
        Some(TsqlJsonReturnType::Json | TsqlJsonReturnType::Unsupported) | None => Ok(constructor),
    }
}

fn rewrite_nested_json_expressions(expressions: &mut [Expression]) -> Result<()> {
    for expression in expressions {
        rewrite_nested_json_expression(expression)?;
    }
    Ok(())
}

fn rewrite_nested_json_expression(expression: &mut Expression) -> Result<()> {
    let owned = std::mem::replace(expression, Expression::Null(Null));
    *expression = rewrite_nested_tsql_json_constructor_return_types(owned)?;
    Ok(())
}

fn rewrite_nested_tsql_json_constructor_return_types(expression: Expression) -> Result<Expression> {
    crate::dialects::transform_recursive(expression, &|node| {
        if has_json_constructor_return_type(&node) {
            rewrite_tsql_json_constructor_return_type(node)
        } else {
            Ok(node)
        }
    })
}

fn take_expression_data_type(return_type: &mut Option<Box<Expression>>) -> Option<DataType> {
    into_expression_data_type(return_type.take().map(|expression| *expression))
}

fn into_expression_data_type(expression: Option<Expression>) -> Option<DataType> {
    match expression {
        Some(Expression::DataType(data_type)) => Some(data_type),
        Some(Expression::JSONFormat(format)) => {
            into_expression_data_type(format.this.map(|expression| *expression))
        }
        _ => None,
    }
}

pub(super) fn decompose_json_path(path: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with('$') {
        &path[1..]
    } else {
        path
    };
    if path.is_empty() {
        return parts;
    }
    let mut current = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
            }
            '[' => {
                if !current.is_empty() {
                    parts.push(current.clone());
                    current.clear();
                }
                i += 1;
                let mut bracket_content = String::new();
                while i < chars.len() && chars[i] != ']' {
                    if chars[i] == '"' || chars[i] == '\'' {
                        let quote = chars[i];
                        i += 1;
                        while i < chars.len() && chars[i] != quote {
                            bracket_content.push(chars[i]);
                            i += 1;
                        }
                        if i < chars.len() {
                            i += 1;
                        }
                    } else {
                        bracket_content.push(chars[i]);
                        i += 1;
                    }
                }
                if i < chars.len() {
                    i += 1;
                }
                if bracket_content != "*" {
                    parts.push(bracket_content);
                }
            }
            _ => {
                current.push(chars[i]);
                i += 1;
            }
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

pub(super) fn normalize_tsql_json_path_expr(
    path: Expression,
    postgres_path_array_literal: bool,
    string_numbers_are_indexes: bool,
) -> Expression {
    match path {
        Expression::Literal(lit) => match lit.as_ref() {
            Literal::String(s) => {
                let normalized = if postgres_path_array_literal {
                    if let Some(parts) = parse_postgres_json_path_array_literal(s) {
                        tsql_json_path_from_segments(&parts, true)
                    } else {
                        normalize_tsql_json_path_string(s, string_numbers_are_indexes)
                    }
                } else {
                    normalize_tsql_json_path_string(s, string_numbers_are_indexes)
                };
                Expression::string(normalized)
            }
            Literal::Number(n) => Expression::string(format!("$[{n}]")),
            _ => Expression::Literal(lit),
        },
        other => other,
    }
}

pub(super) fn normalize_tsql_json_path_parts(paths: Vec<Expression>) -> Expression {
    if paths.len() == 1 {
        return normalize_tsql_json_path_expr(
            paths.into_iter().next().expect("checked len"),
            true,
            true,
        );
    }

    let mut segments = Vec::new();
    for path in paths {
        match path {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::String(s) => {
                    if let Some(parts) = parse_postgres_json_path_array_literal(s) {
                        segments.extend(parts);
                    } else {
                        segments.push(s.clone());
                    }
                }
                Literal::Number(n) => segments.push(n.clone()),
                _ => return Expression::Literal(lit),
            },
            other => return other,
        }
    }

    Expression::string(tsql_json_path_from_segments(&segments, true))
}

pub(super) fn normalize_tsql_json_path_string(
    path: &str,
    string_numbers_are_indexes: bool,
) -> String {
    let trimmed = path.trim();
    let lower = trimmed.to_ascii_lowercase();

    if trimmed.starts_with('$') || lower.starts_with("lax $") || lower.starts_with("strict $") {
        return bracket_to_dot_notation(trimmed);
    }

    if trimmed.starts_with('[') {
        return format!("${}", bracket_to_dot_notation(trimmed));
    }

    if string_numbers_are_indexes && is_json_array_index(trimmed) {
        return format!("$[{trimmed}]");
    }

    let mut result = "$".to_string();
    push_tsql_json_path_segment(&mut result, trimmed, false);
    result
}

pub(super) fn parse_postgres_json_path_array_literal(path: &str) -> Option<Vec<String>> {
    let trimmed = path.trim();
    if !(trimmed.starts_with('{') && trimmed.ends_with('}')) {
        return None;
    }

    let inner = &trimmed[1..trimmed.len().saturating_sub(1)];
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = inner.chars().peekable();
    let mut in_quotes = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if in_quotes {
            match ch {
                '\\' => escaped = true,
                '"' => {
                    if matches!(chars.peek(), Some('"')) {
                        current.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                }
                _ => current.push(ch),
            }
            continue;
        }

        match ch {
            '"' => in_quotes = true,
            ',' => {
                parts.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    parts.push(current.trim().to_string());
    Some(parts)
}

pub(super) fn tsql_json_path_from_segments(
    segments: &[String],
    numeric_strings_are_indexes: bool,
) -> String {
    let mut path = "$".to_string();
    for segment in segments {
        push_tsql_json_path_segment(&mut path, segment, numeric_strings_are_indexes);
    }
    path
}

pub(super) fn push_tsql_json_path_segment(
    path: &mut String,
    segment: &str,
    numeric_string_is_index: bool,
) {
    if numeric_string_is_index && is_json_array_index(segment) {
        path.push('[');
        path.push_str(segment);
        path.push(']');
        return;
    }

    if is_simple_json_path_key(segment) {
        path.push('.');
        path.push_str(segment);
    } else {
        path.push_str(".\"");
        path.push_str(&segment.replace('\\', "\\\\").replace('"', "\\\""));
        path.push('"');
    }
}

pub(super) fn is_json_array_index(segment: &str) -> bool {
    !segment.is_empty() && segment.chars().all(|c| c.is_ascii_digit())
}

pub(super) fn is_simple_json_path_key(segment: &str) -> bool {
    !segment.is_empty()
        && !segment.starts_with('$')
        && segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(super) fn build_tsql_json_function(
    name: &str,
    this: Expression,
    path: Expression,
) -> Expression {
    let (this, path) = collapse_nested_tsql_json_query(this, path);
    Expression::Function(Box::new(crate::expressions::Function::new(
        name.to_string(),
        vec![this, path],
    )))
}

pub(super) fn collapse_nested_tsql_json_query(
    this: Expression,
    path: Expression,
) -> (Expression, Expression) {
    if let Expression::Function(f) = &this {
        if f.name.eq_ignore_ascii_case("JSON_QUERY") && f.args.len() == 2 {
            if let (Some(prefix), Some(suffix)) = (
                literal_string_value(&f.args[1]),
                literal_string_value(&path),
            ) {
                if let Some(combined) = join_tsql_json_paths(prefix, suffix) {
                    return (f.args[0].clone(), Expression::string(combined));
                }
            }
        }
    }

    (this, path)
}

pub(super) fn literal_string_value(expr: &Expression) -> Option<&str> {
    match expr {
        Expression::Literal(lit) => match lit.as_ref() {
            Literal::String(s) => Some(s.as_str()),
            _ => None,
        },
        _ => None,
    }
}

pub(super) fn join_tsql_json_paths(prefix: &str, suffix: &str) -> Option<String> {
    if prefix == "$" {
        return Some(suffix.to_string());
    }
    if suffix == "$" {
        return Some(prefix.to_string());
    }

    if let Some(rest) = suffix.strip_prefix("$.") {
        Some(format!("{prefix}.{rest}"))
    } else {
        suffix
            .strip_prefix("$[")
            .map(|rest| format!("{prefix}[{rest}"))
    }
}

pub(super) fn strip_json_dollar_prefix(path: &str) -> String {
    if path.starts_with("$.") {
        path[2..].to_string()
    } else if path.starts_with('$') {
        path[1..].to_string()
    } else {
        path.to_string()
    }
}

pub(super) fn strip_json_wildcards(path: &str) -> String {
    path.replace("[*]", "")
        .replace("..", ".") // Clean double dots from `$.y[*].z` -> `$.y..z`
        .trim_end_matches('.')
        .to_string()
}

pub(super) fn bracket_to_dot_notation(path: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '[' {
            // Read bracket content
            i += 1;
            let mut bracket_content = String::new();
            let mut is_quoted = false;
            let mut _quote_char = '"';
            while i < chars.len() && chars[i] != ']' {
                if chars[i] == '"' || chars[i] == '\'' {
                    is_quoted = true;
                    _quote_char = chars[i];
                    i += 1;
                    while i < chars.len() && chars[i] != _quote_char {
                        bracket_content.push(chars[i]);
                        i += 1;
                    }
                    if i < chars.len() {
                        i += 1;
                    }
                } else {
                    bracket_content.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                i += 1;
            } // skip ]
            if bracket_content == "*" {
                // Keep wildcard as-is
                result.push_str("[*]");
            } else if is_quoted {
                // Quoted bracket -> dot notation with quotes
                result.push('.');
                result.push('"');
                result.push_str(&bracket_content);
                result.push('"');
            } else {
                // Numeric index -> keep as bracket
                result.push('[');
                result.push_str(&bracket_content);
                result.push(']');
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

pub(super) fn bracket_to_single_quotes(path: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '[' && i + 1 < chars.len() && chars[i + 1] == '"' {
            result.push('[');
            result.push('\'');
            i += 2; // skip [ and "
            while i < chars.len() && chars[i] != '"' {
                result.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            } // skip closing "
            result.push('\'');
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

pub(super) fn build_json_object_from_pairs(
    args: Vec<Expression>,
) -> Option<Box<crate::expressions::JsonObjectFunc>> {
    if args.len() % 2 != 0 {
        return None;
    }

    let mut pairs = Vec::with_capacity(args.len() / 2);
    let mut iter = args.into_iter();
    while let Some(key) = iter.next() {
        let value = iter.next()?;
        pairs.push((key, value));
    }

    Some(Box::new(crate::expressions::JsonObjectFunc {
        pairs,
        null_handling: None,
        with_unique_keys: false,
        returning_type: None,
        format_json: false,
        encoding: None,
        star: false,
    }))
}
