//! StarRocks Dialect
//!
//! StarRocks-specific transformations based on sqlglot patterns.
//! StarRocks is MySQL-compatible with OLAP extensions (similar to Doris).

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, Case, Cast, Expression, Function, Lateral, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// StarRocks dialect
pub struct StarRocksDialect;

impl DialectImpl for StarRocksDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::StarRocks
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        use crate::tokens::TokenType;
        let mut config = TokenizerConfig::default();
        // StarRocks uses backticks for identifiers (MySQL-style)
        config.identifiers.insert('`', '`');
        // Remove double quotes from identifiers (MySQL-style)
        config.identifiers.remove(&'"');
        config.quotes.insert("\"".to_string(), "\"".to_string());
        config.nested_comments = false;
        // LARGEINT maps to INT128
        config.keywords.insert("LARGEINT".to_string(), TokenType::Int128);
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::StarRocks),
            // StarRocks: INSERT OVERWRITE (without TABLE keyword)
            insert_overwrite: " OVERWRITE",
            // StarRocks: PROPERTIES prefix for WITH properties
            with_properties_prefix: "PROPERTIES",
            // StarRocks uses MySQL-style settings
            null_ordering_supported: false,
            limit_only_literals: true,
            semi_anti_join_with_side: false,
            supports_table_alias_columns: false,
            values_as_table: false,
            tablesample_requires_parens: false,
            tablesample_with_method: false,
            aggregate_filter_supported: false,
            try_supported: false,
            supports_convert_timezone: false,
            supports_uescape: false,
            supports_between_flags: false,
            query_hints: false,
            parameter_token: "?",
            supports_window_exclude: false,
            supports_exploding_projections: false,
            // StarRocks: COMMENT 'value' (naked property, no = sign)
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL is native in StarRocks (MySQL-style)
            Expression::IfNull(f) => Ok(Expression::IfNull(f)),

            // NVL -> IFNULL in StarRocks
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // TryCast -> not directly supported, use CAST
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST in StarRocks
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

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

            // RAND is native in StarRocks
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // JSON arrow syntax: preserve -> for StarRocks (arrow_json_extract_sql)
            Expression::JsonExtract(mut f) => {
                // Set arrow_syntax to true to preserve -> operator
                f.arrow_syntax = true;
                Ok(Expression::JsonExtract(f))
            }

            Expression::JsonExtractScalar(mut f) => {
                // Set arrow_syntax to true to preserve ->> operator
                f.arrow_syntax = true;
                Ok(Expression::JsonExtractScalar(f))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // Handle LATERAL UNNEST - StarRocks requires column alias "unnest" by default
            Expression::Lateral(mut l) => {
                self.transform_lateral(&mut l)?;
                Ok(Expression::Lateral(l))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl StarRocksDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // NVL -> IFNULL
            "NVL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // ISNULL -> IFNULL
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "IFNULL".to_string(),
                f.args,
            )))),

            // COALESCE is native in StarRocks
            "COALESCE" => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NOW is native in StarRocks
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> NOW in StarRocks
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GROUP_CONCAT is native in StarRocks
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

            // SUBSTR is native in StarRocks
            "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // SUBSTRING is native in StarRocks
            "SUBSTRING" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native in StarRocks
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LENGTH".to_string(),
                f.args,
            )))),

            // CHARINDEX -> INSTR in StarRocks (with swapped args)
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

            // DATE_TRUNC is native in StarRocks
            "DATE_TRUNC" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_AGG is native in StarRocks
            "ARRAY_AGG" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_LIST -> ARRAY_AGG
            "COLLECT_LIST" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("ARRAY_AGG".to_string(), f.args),
            ))),

            // ARRAY_JOIN is native in StarRocks
            "ARRAY_JOIN" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_FLATTEN is native in StarRocks
            "ARRAY_FLATTEN" => Ok(Expression::Function(Box::new(f))),

            // FLATTEN -> ARRAY_FLATTEN
            "FLATTEN" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_FLATTEN".to_string(),
                f.args,
            )))),

            // TO_DATE is native in StarRocks
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT is native in StarRocks
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

            // TO_CHAR -> DATE_FORMAT
            "TO_CHAR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT -> arrow operator in StarRocks
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // GET_JSON_OBJECT -> JSON_EXTRACT
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT".to_string(), f.args),
            ))),

            // REGEXP is native in StarRocks
            "REGEXP" => Ok(Expression::Function(Box::new(f))),

            // RLIKE is native in StarRocks
            "RLIKE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_LIKE -> REGEXP
            "REGEXP_LIKE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP".to_string(),
                f.args,
            )))),

            // ARRAY_INTERSECTION -> ARRAY_INTERSECT
            "ARRAY_INTERSECTION" => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_INTERSECT".to_string(),
                f.args,
            )))),

            // ST_MAKEPOINT -> ST_POINT
            "ST_MAKEPOINT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "ST_POINT".to_string(),
                f.args,
            )))),

            // ST_DISTANCE(a, b) -> ST_DISTANCE_SPHERE(ST_X(a), ST_Y(a), ST_X(b), ST_Y(b))
            "ST_DISTANCE" if f.args.len() == 2 => {
                let a = f.args[0].clone();
                let b = f.args[1].clone();
                Ok(Expression::Function(Box::new(Function::new(
                    "ST_DISTANCE_SPHERE".to_string(),
                    vec![
                        Expression::Function(Box::new(Function::new("ST_X".to_string(), vec![a.clone()]))),
                        Expression::Function(Box::new(Function::new("ST_Y".to_string(), vec![a]))),
                        Expression::Function(Box::new(Function::new("ST_X".to_string(), vec![b.clone()]))),
                        Expression::Function(Box::new(Function::new("ST_Y".to_string(), vec![b]))),
                    ],
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

            // APPROX_COUNT_DISTINCT is native in StarRocks
            "APPROX_COUNT_DISTINCT" => Ok(Expression::AggregateFunction(f)),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // StarRocks: CAST(x AS TIMESTAMP/TIMESTAMPTZ) -> TIMESTAMP(x) function
        // Similar to MySQL behavior
        match &c.to {
            crate::expressions::DataType::Timestamp { .. } => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    vec![c.this],
                ))))
            }
            crate::expressions::DataType::Custom { name } if name.to_uppercase() == "TIMESTAMPTZ"
                || name.to_uppercase() == "TIMESTAMPLTZ" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    vec![c.this],
                ))))
            }
            // StarRocks type mappings are handled in the generator
            _ => Ok(Expression::Cast(Box::new(c))),
        }
    }

    /// Transform LATERAL UNNEST for StarRocks
    /// StarRocks requires UNNEST to have a default column alias of "unnest" if not specified.
    /// Python reference: starrocks.py _parse_unnest
    fn transform_lateral(&self, l: &mut Box<Lateral>) -> Result<()> {
        // Check if the lateral expression contains UNNEST
        if let Expression::Unnest(_) = &*l.this {
            // If there's a table alias but no column aliases, add "unnest" as default column
            if l.alias.is_some() && l.column_aliases.is_empty() {
                l.column_aliases.push("unnest".to_string());
            }
            // If there's no alias at all, add both table alias "unnest" and column alias "unnest"
            else if l.alias.is_none() {
                l.alias = Some("unnest".to_string());
                l.column_aliases.push("unnest".to_string());
            }
        }
        Ok(())
    }
}
