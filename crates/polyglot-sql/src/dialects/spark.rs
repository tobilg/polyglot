//! Spark SQL Dialect
//!
//! Spark SQL-specific transformations based on sqlglot patterns.
//! Key features (extends Hive with modern SQL):
//! - TRY_CAST is supported (Spark 3+)
//! - ILIKE is supported (Spark 3+)
//! - Uses backticks for identifiers
//! - ARRAY_AGG, COLLECT_LIST for array aggregation
//! - STRING_AGG / LISTAGG supported (Spark 4+)
//! - DATE_ADD with unit parameter (Spark 3+)
//! - TIMESTAMPADD, TIMESTAMPDIFF (Spark 3+)
//! - More PostgreSQL-like syntax than Hive

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{CeilFunc, CurrentTimestamp, DataType, DateTimeField, Expression, ExtractFunc, Function, Literal, StructField, UnaryFunc, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Spark SQL dialect
pub struct SparkDialect;

impl DialectImpl for SparkDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Spark
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Spark uses backticks for identifiers (NOT double quotes)
        config.identifiers.clear();
        config.identifiers.insert('`', '`');
        // Spark (like Hive) uses double quotes as string delimiters (QUOTES = ["'", '"'])
        config.quotes.insert("\"".to_string(), "\"".to_string());
        // Spark (like Hive) uses backslash escapes in strings (STRING_ESCAPES = ["\\"])
        config.string_escapes.push('\\');
        // Spark supports DIV keyword for integer division (inherited from Hive)
        config.keywords.insert("DIV".to_string(), crate::tokens::TokenType::Div);
        // Spark numeric literal suffixes (same as Hive): 1L -> BIGINT, 1S -> SMALLINT, etc.
        config.numeric_literals.insert("L".to_string(), "BIGINT".to_string());
        config.numeric_literals.insert("S".to_string(), "SMALLINT".to_string());
        config.numeric_literals.insert("Y".to_string(), "TINYINT".to_string());
        config.numeric_literals.insert("D".to_string(), "DOUBLE".to_string());
        config.numeric_literals.insert("F".to_string(), "FLOAT".to_string());
        config.numeric_literals.insert("BD".to_string(), "DECIMAL".to_string());
        // Spark allows identifiers to start with digits (e.g., 1a, 1_a)
        config.identifiers_can_start_with_digit = true;
        // Spark: STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = False
        // Backslashes in raw strings are always literal (no escape processing)
        config.string_escapes_allowed_in_raw_strings = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Spark),
            // Spark uses colon separator in STRUCT field definitions: STRUCT<field_name: TYPE>
            struct_field_sep: ": ",
            // Spark doesn't use AS before RETURN in function definitions
            create_function_return_as: false,
            // Spark places alias after the TABLESAMPLE clause
            alias_post_tablesample: true,
            tablesample_seed_keyword: "REPEATABLE",
            join_hints: false,
            identifiers_can_start_with_digit: true,
            // Spark uses COMMENT 'value' without = sign
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Spark
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL is supported in Spark (from Hive), but COALESCE is standard
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Cast: normalize VARCHAR(n) -> STRING, CHAR(n) -> STRING for Spark
            Expression::Cast(mut c) => {
                c.to = Self::normalize_spark_type(c.to);
                Ok(Expression::Cast(c))
            }

            // TryCast stays as TryCast in Spark (Spark supports TRY_CAST natively)
            Expression::TryCast(mut c) => {
                c.to = Self::normalize_spark_type(c.to);
                Ok(Expression::TryCast(c))
            }

            // SafeCast -> TRY_CAST
            Expression::SafeCast(mut c) => {
                c.to = Self::normalize_spark_type(c.to);
                Ok(Expression::TryCast(c))
            }

            // TRIM: non-standard comma syntax -> standard FROM syntax
            // TRIM('SL', 'SSparkSQLS') -> TRIM('SL' FROM 'SSparkSQLS')
            Expression::Trim(mut t) => {
                if !t.sql_standard_syntax && t.characters.is_some() {
                    // Convert comma syntax to standard SQL syntax
                    // Fields already have correct semantics: this=string, characters=chars
                    t.sql_standard_syntax = true;
                }
                Ok(Expression::Trim(t))
            }

            // ILIKE is supported in Spark 3+
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // UNNEST -> EXPLODE in Spark (Hive compatibility)
            Expression::Unnest(f) => Ok(Expression::Explode(Box::new(UnaryFunc::new(f.this)))),

            // EXPLODE is native to Spark
            Expression::Explode(f) => Ok(Expression::Explode(f)),

            // ExplodeOuter is supported in Spark
            Expression::ExplodeOuter(f) => Ok(Expression::ExplodeOuter(f)),

            // RANDOM -> RAND in Spark
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None, lower: None, upper: None,
            }))),

            // Rand is native to Spark
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // || (Concat) -> CONCAT in Spark
            Expression::Concat(op) => Ok(Expression::Function(Box::new(Function::new(
                "CONCAT".to_string(),
                vec![op.left, op.right],
            )))),

            // ParseJson: handled by generator (emits just the string literal for Spark)

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // $N parameters -> ${N} in Spark (DollarBrace style)
            Expression::Parameter(mut p) if p.style == crate::expressions::ParameterStyle::Dollar => {
                p.style = crate::expressions::ParameterStyle::DollarBrace;
                // Convert index to name for DollarBrace format
                if let Some(idx) = p.index {
                    p.name = Some(idx.to_string());
                }
                Ok(Expression::Parameter(p))
            }

            // JSONExtract with variant_extract (Databricks colon syntax) -> GET_JSON_OBJECT
            Expression::JSONExtract(je) if je.variant_extract.is_some() => {
                // Convert path: 'item[1].price' -> '$.item[1].price'
                let path = match *je.expression {
                    Expression::Literal(Literal::String(s)) => {
                        Expression::Literal(Literal::String(format!("$.{}", s)))
                    }
                    other => other,
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "GET_JSON_OBJECT".to_string(),
                    vec![*je.this, path],
                ))))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl SparkDialect {
    /// Normalize a data type for Spark:
    /// - VARCHAR/CHAR without length -> STRING
    /// - VARCHAR(n)/CHAR(n) with length -> keep as-is
    /// - TEXT -> STRING
    fn normalize_spark_type(dt: DataType) -> DataType {
        match dt {
            DataType::VarChar { length: None, .. } | DataType::Char { length: None } | DataType::Text => {
                DataType::Custom { name: "STRING".to_string() }
            }
            // VARCHAR(n) and CHAR(n) with length are kept as-is
            DataType::VarChar { .. } | DataType::Char { .. } => dt,
            // Also normalize struct fields recursively
            DataType::Struct { fields, nested } => {
                let normalized_fields: Vec<StructField> = fields.into_iter().map(|mut f| {
                    f.data_type = Self::normalize_spark_type(f.data_type);
                    f
                }).collect();
                DataType::Struct { fields: normalized_fields, nested }
            }
            _ => dt,
        }
    }

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

            // GROUP_CONCAT -> CONCAT_WS + COLLECT_LIST in older Spark
            // In Spark 4+, STRING_AGG is available
            "GROUP_CONCAT" if !f.args.is_empty() => {
                // For simplicity, use COLLECT_LIST (array aggregation)
                Ok(Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    f.args,
                ))))
            }

            // STRING_AGG is supported in Spark 4+
            // For older versions, fall back to CONCAT_WS + COLLECT_LIST
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // LISTAGG -> STRING_AGG in Spark 4+ (or COLLECT_LIST for older)
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // SUBSTRING is native to Spark
            "SUBSTRING" | "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native to Spark
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None, lower: None, upper: None,
            }))),

            // RAND is native to Spark
            "RAND" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None, lower: None, upper: None,
            }))),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // CURRENT_TIMESTAMP is native
            "CURRENT_TIMESTAMP" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // CURRENT_DATE is native
            "CURRENT_DATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // TO_DATE is native to Spark; strip default format 'yyyy-MM-dd'
            "TO_DATE" if f.args.len() == 2 => {
                let is_default_format = matches!(&f.args[1], Expression::Literal(crate::expressions::Literal::String(s)) if s == "yyyy-MM-dd");
                if is_default_format {
                    Ok(Expression::Function(Box::new(Function::new("TO_DATE".to_string(), vec![f.args.into_iter().next().unwrap()]))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP is native to Spark
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT is native to Spark
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT
            "STRFTIME" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // TO_CHAR -> DATE_FORMAT
            "TO_CHAR" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // DATE_TRUNC is native to Spark
            "DATE_TRUNC" => Ok(Expression::Function(Box::new(f))),

            // TRUNC is native to Spark
            "TRUNC" => Ok(Expression::Function(Box::new(f))),

            // EXTRACT is native to Spark
            "EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // DATEPART -> EXTRACT
            "DATEPART" => Ok(Expression::Function(Box::new(Function::new(
                "EXTRACT".to_string(),
                f.args,
            )))),

            // UNIX_TIMESTAMP is native to Spark
            // When called with no args, add CURRENT_TIMESTAMP() as default
            "UNIX_TIMESTAMP" => {
                if f.args.is_empty() {
                    Ok(Expression::Function(Box::new(Function::new(
                        "UNIX_TIMESTAMP".to_string(),
                        vec![Expression::CurrentTimestamp(CurrentTimestamp {
                            precision: None,
                            sysdate: false,
                        })],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // FROM_UNIXTIME is native to Spark
            "FROM_UNIXTIME" => Ok(Expression::Function(Box::new(f))),

            // STR_TO_MAP is native to Spark
            // When called with only one arg, add default delimiters ',' and ':'
            "STR_TO_MAP" => {
                if f.args.len() == 1 {
                    let mut args = f.args;
                    args.push(Expression::Literal(crate::expressions::Literal::String(",".to_string())));
                    args.push(Expression::Literal(crate::expressions::Literal::String(":".to_string())));
                    Ok(Expression::Function(Box::new(Function::new(
                        "STR_TO_MAP".to_string(),
                        args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // POSITION is native to Spark (POSITION(substr IN str))
            "POSITION" => Ok(Expression::Function(Box::new(f))),

            // LOCATE is native to Spark
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // STRPOS -> Use expression form or LOCATE
            "STRPOS" if f.args.len() == 2 => {
                let mut args = f.args;
                let first = args.remove(0);
                let second = args.remove(0);
                // LOCATE(substr, str) in Spark
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    vec![second, first],
                ))))
            }

            // CHARINDEX -> LOCATE
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                let mut locate_args = vec![substring, string];
                if !args.is_empty() {
                    locate_args.push(args.remove(0));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    locate_args,
                ))))
            }

            // INSTR is native to Spark
            "INSTR" => Ok(Expression::Function(Box::new(f))),

            // CEILING -> CEIL
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // CEIL is native to Spark
            "CEIL" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // UNNEST -> EXPLODE
            "UNNEST" => Ok(Expression::Function(Box::new(Function::new(
                "EXPLODE".to_string(),
                f.args,
            )))),

            // FLATTEN -> FLATTEN is native to Spark (for nested arrays)
            "FLATTEN" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_AGG -> COLLECT_LIST
            "ARRAY_AGG" => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // COLLECT_LIST is native to Spark
            "COLLECT_LIST" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_SET is native to Spark
            "COLLECT_SET" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_LENGTH -> SIZE in Spark
            "ARRAY_LENGTH" | "CARDINALITY" => Ok(Expression::Function(Box::new(Function::new(
                "SIZE".to_string(),
                f.args,
            )))),

            // SIZE is native to Spark
            "SIZE" => Ok(Expression::Function(Box::new(f))),

            // SPLIT is native to Spark
            "SPLIT" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_REPLACE: Spark supports up to 4 args (subject, pattern, replacement, position)
            // Strip extra Snowflake args (occurrence, params) if present
            "REGEXP_REPLACE" if f.args.len() > 4 => {
                let mut args = f.args;
                args.truncate(4);
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    args,
                ))))
            }
            "REGEXP_REPLACE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_EXTRACT is native to Spark
            "REGEXP_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_EXTRACT_ALL is native to Spark
            "REGEXP_EXTRACT_ALL" => Ok(Expression::Function(Box::new(f))),

            // RLIKE is native to Spark
            "RLIKE" | "REGEXP_LIKE" => Ok(Expression::Function(Box::new(Function::new(
                "RLIKE".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT -> GET_JSON_OBJECT (Hive style) or :: operator
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "GET_JSON_OBJECT".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_SCALAR -> GET_JSON_OBJECT
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(Function::new(
                "GET_JSON_OBJECT".to_string(),
                f.args,
            )))),

            // GET_JSON_OBJECT is native to Spark
            "GET_JSON_OBJECT" => Ok(Expression::Function(Box::new(f))),

            // FROM_JSON is native to Spark
            "FROM_JSON" => Ok(Expression::Function(Box::new(f))),

            // TO_JSON is native to Spark
            "TO_JSON" => Ok(Expression::Function(Box::new(f))),

            // PARSE_JSON -> strip for Spark (just keep the string argument)
            "PARSE_JSON" if f.args.len() == 1 => Ok(f.args.into_iter().next().unwrap()),
            "PARSE_JSON" => Ok(Expression::Function(Box::new(Function::new(
                "FROM_JSON".to_string(),
                f.args,
            )))),

            // DATEDIFF is native to Spark (supports unit in Spark 3+)
            "DATEDIFF" | "DATE_DIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                f.args,
            )))),

            // DATE_ADD is native to Spark
            "DATE_ADD" | "DATEADD" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_ADD".to_string(),
                f.args,
            )))),

            // DATE_SUB is native to Spark
            "DATE_SUB" => Ok(Expression::Function(Box::new(f))),

            // TIMESTAMPADD is native to Spark 3+
            "TIMESTAMPADD" => Ok(Expression::Function(Box::new(f))),

            // TIMESTAMPDIFF is native to Spark 3+
            "TIMESTAMPDIFF" => Ok(Expression::Function(Box::new(f))),

            // ADD_MONTHS is native to Spark
            "ADD_MONTHS" => Ok(Expression::Function(Box::new(f))),

            // MONTHS_BETWEEN is native to Spark
            "MONTHS_BETWEEN" => Ok(Expression::Function(Box::new(f))),

            // NVL is native to Spark
            "NVL" => Ok(Expression::Function(Box::new(f))),

            // NVL2 is native to Spark
            "NVL2" => Ok(Expression::Function(Box::new(f))),

            // MAP is native to Spark
            "MAP" => Ok(Expression::Function(Box::new(f))),

            // ARRAY is native to Spark
            "ARRAY" => Ok(Expression::Function(Box::new(f))),

            // ROW -> STRUCT for Spark (cross-dialect, no auto-naming)
            "ROW" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "STRUCT".to_string(),
                    f.args,
                ))))
            }

            // STRUCT is native to Spark - auto-name unnamed args as col1, col2, etc.
            "STRUCT" => {
                let mut col_idx = 1usize;
                let named_args: Vec<Expression> = f.args.into_iter().map(|arg| {
                    let current_idx = col_idx;
                    col_idx += 1;
                    // Check if arg already has an alias (AS name) or is Star
                    match &arg {
                        Expression::Alias(_) => arg, // already named
                        Expression::Star(_) => arg, // STRUCT(*) - keep as-is
                        Expression::Column(c) if c.table.is_none() => {
                            // Column reference: use column name as the struct field name
                            let name = c.name.name.clone();
                            Expression::Alias(Box::new(crate::expressions::Alias {
                                this: arg,
                                alias: crate::expressions::Identifier::new(&name),
                                column_aliases: Vec::new(),
                                pre_alias_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                            }))
                        }
                        _ => {
                            // Unnamed literal/expression: auto-name as colN
                            let name = format!("col{}", current_idx);
                            Expression::Alias(Box::new(crate::expressions::Alias {
                                this: arg,
                                alias: crate::expressions::Identifier::new(&name),
                                column_aliases: Vec::new(),
                                pre_alias_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                            }))
                        }
                    }
                }).collect();
                Ok(Expression::Function(Box::new(Function {
                    name: "STRUCT".to_string(),
                    args: named_args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                    no_parens: false,
                    quoted: false,
                })))
            }

            // NAMED_STRUCT is native to Spark
            "NAMED_STRUCT" => Ok(Expression::Function(Box::new(f))),

            // MAP_FROM_ARRAYS is native to Spark
            "MAP_FROM_ARRAYS" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_SORT is native to Spark
            "ARRAY_SORT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_DISTINCT is native to Spark
            "ARRAY_DISTINCT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_UNION is native to Spark
            "ARRAY_UNION" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_INTERSECT is native to Spark
            "ARRAY_INTERSECT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_EXCEPT is native to Spark
            "ARRAY_EXCEPT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_CONTAINS is native to Spark
            "ARRAY_CONTAINS" => Ok(Expression::Function(Box::new(f))),

            // ELEMENT_AT is native to Spark
            "ELEMENT_AT" => Ok(Expression::Function(Box::new(f))),

            // TRY_ELEMENT_AT is native to Spark 3+
            "TRY_ELEMENT_AT" => Ok(Expression::Function(Box::new(f))),

            // TRANSFORM is native to Spark (array transformation)
            "TRANSFORM" => Ok(Expression::Function(Box::new(f))),

            // FILTER is native to Spark (array filtering)
            "FILTER" => Ok(Expression::Function(Box::new(f))),

            // AGGREGATE is native to Spark (array reduction)
            "AGGREGATE" => Ok(Expression::Function(Box::new(f))),

            // SEQUENCE is native to Spark (generate array)
            "SEQUENCE" => Ok(Expression::Function(Box::new(f))),

            // GENERATE_SERIES -> SEQUENCE
            "GENERATE_SERIES" => Ok(Expression::Function(Box::new(Function::new(
                "SEQUENCE".to_string(),
                f.args,
            )))),

            // STARTSWITH is native to Spark 3+
            "STARTSWITH" | "STARTS_WITH" => Ok(Expression::Function(Box::new(Function::new(
                "STARTSWITH".to_string(),
                f.args,
            )))),

            // ENDSWITH is native to Spark 3+
            "ENDSWITH" | "ENDS_WITH" => Ok(Expression::Function(Box::new(Function::new(
                "ENDSWITH".to_string(),
                f.args,
            )))),

            // ARRAY_CONSTRUCT_COMPACT(1, null, 2) -> ARRAY_COMPACT(ARRAY(1, NULL, 2))
            "ARRAY_CONSTRUCT_COMPACT" => {
                let inner = Expression::Function(Box::new(Function::new("ARRAY".to_string(), f.args)));
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_COMPACT".to_string(),
                    vec![inner],
                ))))
            }

            // ARRAY_TO_STRING -> ARRAY_JOIN
            "ARRAY_TO_STRING" => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_JOIN".to_string(),
                f.args,
            )))),

            // TO_ARRAY(x) -> IF(x IS NULL, NULL, ARRAY(x))
            "TO_ARRAY" if f.args.len() == 1 => {
                let x = f.args[0].clone();
                // Check if arg is already an array constructor (bracket notation)
                // In that case: TO_ARRAY(['test']) -> ARRAY('test')
                match &x {
                    Expression::ArrayFunc(arr) => {
                        // Just convert to ARRAY(...) function
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY".to_string(),
                            arr.expressions.clone(),
                        ))))
                    }
                    _ => {
                        Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                            condition: Expression::IsNull(Box::new(crate::expressions::IsNull {
                                this: x.clone(),
                                not: false,
                                postfix_form: false,
                            })),
                            true_value: Expression::Null(crate::expressions::Null),
                            false_value: Some(Expression::Function(Box::new(Function::new(
                                "ARRAY".to_string(),
                                vec![x],
                            )))),
                            original_name: Some("IF".to_string()),
                        })))
                    }
                }
            }

            // REGEXP_SUBSTR -> REGEXP_EXTRACT (strip extra args)
            "REGEXP_SUBSTR" if f.args.len() >= 2 => {
                let subject = f.args[0].clone();
                let pattern = f.args[1].clone();
                // For Spark: REGEXP_EXTRACT(subject, pattern, group)
                // group defaults to 0 for full match, but sqlglot uses last arg if present
                let group = if f.args.len() >= 6 {
                    let g = &f.args[5];
                    // If group is literal 1 (default), omit it
                    if matches!(g, Expression::Literal(Literal::Number(n)) if n == "1") {
                        None
                    } else {
                        Some(g.clone())
                    }
                } else {
                    None
                };
                let mut args = vec![subject, pattern];
                if let Some(g) = group {
                    args.push(g);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT".to_string(),
                    args,
                ))))
            }

            // UUID_STRING -> UUID()
            "UUID_STRING" => Ok(Expression::Function(Box::new(Function::new(
                "UUID".to_string(),
                vec![],
            )))),

            // OBJECT_CONSTRUCT -> STRUCT in Spark
            "OBJECT_CONSTRUCT" if f.args.len() >= 2 && f.args.len() % 2 == 0 => {
                // Convert key-value pairs to named struct fields
                // OBJECT_CONSTRUCT('Manitoba', 'Winnipeg', 'foo', 'bar')
                // -> STRUCT('Winnipeg' AS Manitoba, 'bar' AS foo)
                let mut struct_args = Vec::new();
                for pair in f.args.chunks(2) {
                    if let Expression::Literal(Literal::String(key)) = &pair[0] {
                        struct_args.push(Expression::Alias(Box::new(crate::expressions::Alias {
                            this: pair[1].clone(),
                            alias: crate::expressions::Identifier::new(key.clone()),
                            column_aliases: vec![],
                            pre_alias_comments: vec![],
                            trailing_comments: vec![],
                        })));
                    } else {
                        struct_args.push(pair[1].clone());
                    }
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "STRUCT".to_string(),
                    struct_args,
                ))))
            }

            // DATE_PART(part, expr) -> EXTRACT(part FROM expr)
            "DATE_PART" if f.args.len() == 2 => {
                let mut args = f.args;
                let part = args.remove(0);
                let expr = args.remove(0);
                if let Some(field) = expr_to_datetime_field(&part) {
                    Ok(Expression::Extract(Box::new(ExtractFunc {
                        this: expr,
                        field,
                    })))
                } else {
                    // Can't parse the field, keep as function
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PART".to_string(),
                        vec![part, expr],
                    ))))
                }
            }

            // GET_PATH(obj, path) -> GET_JSON_OBJECT(obj, json_path) in Spark
            "GET_PATH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                let json_path = match &path {
                    Expression::Literal(Literal::String(s)) => {
                        let normalized = if s.starts_with('$') {
                            s.clone()
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Literal::String(normalized))
                    }
                    _ => path,
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "GET_JSON_OBJECT".to_string(),
                    vec![this, json_path],
                ))))
            }

            // BITWISE_LEFT_SHIFT → SHIFTLEFT
            "BITWISE_LEFT_SHIFT" => Ok(Expression::Function(Box::new(Function::new(
                "SHIFTLEFT".to_string(),
                f.args,
            )))),

            // BITWISE_RIGHT_SHIFT → SHIFTRIGHT
            "BITWISE_RIGHT_SHIFT" => Ok(Expression::Function(Box::new(Function::new(
                "SHIFTRIGHT".to_string(),
                f.args,
            )))),

            // APPROX_DISTINCT → APPROX_COUNT_DISTINCT
            "APPROX_DISTINCT" => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                f.args,
            )))),

            // ARRAY_SLICE → SLICE
            "ARRAY_SLICE" => Ok(Expression::Function(Box::new(Function::new(
                "SLICE".to_string(),
                f.args,
            )))),

            // DATE_FROM_PARTS → MAKE_DATE
            "DATE_FROM_PARTS" => Ok(Expression::Function(Box::new(Function::new(
                "MAKE_DATE".to_string(),
                f.args,
            )))),

            // DAYOFWEEK_ISO → DAYOFWEEK
            "DAYOFWEEK_ISO" => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEK".to_string(),
                f.args,
            )))),

            // FORMAT → FORMAT_STRING
            "FORMAT" => Ok(Expression::Function(Box::new(Function::new(
                "FORMAT_STRING".to_string(),
                f.args,
            )))),

            // LOGICAL_AND → BOOL_AND
            "LOGICAL_AND" => Ok(Expression::Function(Box::new(Function::new(
                "BOOL_AND".to_string(),
                f.args,
            )))),

            // VARIANCE_POP → VAR_POP
            "VARIANCE_POP" => Ok(Expression::Function(Box::new(Function::new(
                "VAR_POP".to_string(),
                f.args,
            )))),

            // WEEK_OF_YEAR → WEEKOFYEAR
            "WEEK_OF_YEAR" => Ok(Expression::Function(Box::new(Function::new(
                "WEEKOFYEAR".to_string(),
                f.args,
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
            // GROUP_CONCAT -> COLLECT_LIST (then CONCAT_WS for string)
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("COLLECT_LIST".to_string(), f.args),
            ))),

            // STRING_AGG -> COLLECT_LIST (or STRING_AGG in Spark 4+)
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // LISTAGG -> COLLECT_LIST
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> COLLECT_LIST (preserve distinct and filter)
            "ARRAY_AGG" if !f.args.is_empty() => {
                let mut af = f;
                af.name = "COLLECT_LIST".to_string();
                Ok(Expression::AggregateFunction(af))
            }

            // LOGICAL_OR -> BOOL_OR in Spark
            "LOGICAL_OR" if !f.args.is_empty() => {
                let mut af = f;
                af.name = "BOOL_OR".to_string();
                Ok(Expression::AggregateFunction(af))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

/// Convert an expression (string literal or identifier) to a DateTimeField
fn expr_to_datetime_field(expr: &Expression) -> Option<DateTimeField> {
    let name = match expr {
        Expression::Literal(Literal::String(s)) => s.to_uppercase(),
        Expression::Identifier(id) => id.name.to_uppercase(),
        Expression::Column(col) if col.table.is_none() => col.name.name.to_uppercase(),
        _ => return None,
    };
    match name.as_str() {
        "YEAR" | "Y" | "YY" | "YYY" | "YYYY" | "YR" | "YEARS" | "YRS" => Some(DateTimeField::Year),
        "MONTH" | "MM" | "MON" | "MONS" | "MONTHS" => Some(DateTimeField::Month),
        "DAY" | "D" | "DD" | "DAYS" | "DAYOFMONTH" => Some(DateTimeField::Day),
        "HOUR" | "H" | "HH" | "HR" | "HOURS" | "HRS" => Some(DateTimeField::Hour),
        "MINUTE" | "MI" | "MIN" | "MINUTES" | "MINS" => Some(DateTimeField::Minute),
        "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => Some(DateTimeField::Second),
        "MILLISECOND" | "MS" | "MSEC" | "MILLISECONDS" => Some(DateTimeField::Millisecond),
        "MICROSECOND" | "US" | "USEC" | "MICROSECONDS" => Some(DateTimeField::Microsecond),
        "DOW" | "DAYOFWEEK" | "DAYOFWEEK_ISO" | "DW" => Some(DateTimeField::DayOfWeek),
        "DOY" | "DAYOFYEAR" => Some(DateTimeField::DayOfYear),
        "WEEK" | "W" | "WK" | "WEEKOFYEAR" | "WOY" => Some(DateTimeField::Week),
        "QUARTER" | "Q" | "QTR" | "QTRS" | "QUARTERS" => Some(DateTimeField::Quarter),
        "EPOCH" | "EPOCH_SECOND" | "EPOCH_SECONDS" => Some(DateTimeField::Epoch),
        "TIMEZONE" | "TIMEZONE_HOUR" | "TZH" => Some(DateTimeField::TimezoneHour),
        "TIMEZONE_MINUTE" | "TZM" => Some(DateTimeField::TimezoneMinute),
        _ => Some(DateTimeField::Custom(name)),
    }
}
