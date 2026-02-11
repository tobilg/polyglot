//! Microsoft Fabric Data Warehouse Dialect
//!
//! Fabric-specific SQL dialect based on sqlglot patterns.
//! Fabric inherits from T-SQL with specific differences.
//!
//! References:
//! - Data Types: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
//! - T-SQL Surface Area: https://learn.microsoft.com/en-us/fabric/data-warehouse/tsql-surface-area
//!
//! Key differences from T-SQL:
//! - Case-sensitive identifiers (unlike T-SQL which is case-insensitive)
//! - Limited data type support with mappings to supported alternatives
//! - Temporal types (DATETIME2, DATETIMEOFFSET, TIME) limited to 6 digits precision
//! - Certain legacy types (MONEY, SMALLMONEY, etc.) are not supported
//! - Unicode types (NCHAR, NVARCHAR) are mapped to non-unicode equivalents

use super::{DialectImpl, DialectType, TSQLDialect};
use crate::error::Result;
use crate::expressions::{BinaryOp, Cast, DataType, Expression, Function, Identifier, Literal};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Microsoft Fabric Data Warehouse dialect (based on T-SQL)
pub struct FabricDialect;

impl DialectImpl for FabricDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Fabric
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        // Inherit from T-SQL
        let tsql = TSQLDialect;
        tsql.tokenizer_config()
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        // Inherit from T-SQL with Fabric dialect type
        GeneratorConfig {
            // Use square brackets like T-SQL
            identifier_quote: '[',
            identifier_quote_style: IdentifierQuoteStyle::BRACKET,
            dialect: Some(DialectType::Fabric),
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        // Handle CreateTable specially - add default precision of 1 to VARCHAR/CHAR without length
        // Reference: Python sqlglot Fabric dialect parser._parse_create adds default precision
        if let Expression::CreateTable(mut ct) = expr {
            for col in &mut ct.columns {
                match &col.data_type {
                    DataType::VarChar { length: None, .. } => {
                        col.data_type = DataType::VarChar {
                            length: Some(1),
                            parenthesized_length: false,
                        };
                    }
                    DataType::Char { length: None } => {
                        col.data_type = DataType::Char { length: Some(1) };
                    }
                    _ => {}
                }
                // Also transform column data types through Fabric's type mappings
                if let Expression::DataType(new_dt) =
                    self.transform_fabric_data_type(col.data_type.clone())?
                {
                    col.data_type = new_dt;
                }
            }
            return Ok(Expression::CreateTable(ct));
        }

        // Handle DataType::Timestamp specially BEFORE T-SQL transform
        // because TSQL loses precision info when converting Timestamp to DATETIME2
        if let Expression::DataType(DataType::Timestamp { precision, .. }) = &expr {
            let p = FabricDialect::cap_precision(*precision, 6);
            return Ok(Expression::DataType(DataType::Custom {
                name: format!("DATETIME2({})", p),
            }));
        }

        // Handle DataType::Time specially BEFORE T-SQL transform
        // to ensure we get default precision of 6
        if let Expression::DataType(DataType::Time { precision, .. }) = &expr {
            let p = FabricDialect::cap_precision(*precision, 6);
            return Ok(Expression::DataType(DataType::Custom {
                name: format!("TIME({})", p),
            }));
        }

        // Handle DataType::Decimal specially BEFORE T-SQL transform
        // because TSQL converts DECIMAL to NUMERIC, but Fabric wants DECIMAL
        if let Expression::DataType(DataType::Decimal { precision, scale }) = &expr {
            let name = if let (Some(p), Some(s)) = (precision, scale) {
                format!("DECIMAL({}, {})", p, s)
            } else if let Some(p) = precision {
                format!("DECIMAL({})", p)
            } else {
                "DECIMAL".to_string()
            };
            return Ok(Expression::DataType(DataType::Custom { name }));
        }

        // Handle AT TIME ZONE with TIMESTAMPTZ cast
        // Reference: Python sqlglot Fabric dialect cast_sql and attimezone_sql methods
        // Input: CAST(x AS TIMESTAMPTZ) AT TIME ZONE 'Pacific Standard Time'
        // Output: CAST(CAST(x AS DATETIMEOFFSET(6)) AT TIME ZONE 'Pacific Standard Time' AS DATETIME2(6))
        if let Expression::AtTimeZone(ref at_tz) = expr {
            // Check if this contains a TIMESTAMPTZ cast
            if let Expression::Cast(ref inner_cast) = at_tz.this {
                if let DataType::Timestamp {
                    timezone: true,
                    precision,
                } = &inner_cast.to
                {
                    // Get precision, default 6, cap at 6
                    let capped_precision = FabricDialect::cap_precision(*precision, 6);

                    // Create inner DATETIMEOFFSET cast
                    let datetimeoffset_cast = Expression::Cast(Box::new(Cast {
                        this: inner_cast.this.clone(),
                        to: DataType::Custom {
                            name: format!("DATETIMEOFFSET({})", capped_precision),
                        },
                        trailing_comments: inner_cast.trailing_comments.clone(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }));

                    // Create new AT TIME ZONE with DATETIMEOFFSET
                    let new_at_tz = Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                        this: datetimeoffset_cast,
                        zone: at_tz.zone.clone(),
                    }));

                    // Wrap in outer DATETIME2 cast
                    return Ok(Expression::Cast(Box::new(Cast {
                        this: new_at_tz,
                        to: DataType::Custom {
                            name: format!("DATETIME2({})", capped_precision),
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })));
                }
            }
        }

        // Handle UnixToTime -> DATEADD(MICROSECONDS, CAST(ROUND(column * 1e6, 0) AS BIGINT), CAST('1970-01-01' AS DATETIME2(6)))
        // Reference: Python sqlglot Fabric dialect unixtotime_sql
        if let Expression::UnixToTime(ref f) = expr {
            // Build: column * 1e6
            let column_times_1e6 = Expression::Mul(Box::new(BinaryOp {
                left: (*f.this).clone(),
                right: Expression::Literal(Literal::Number("1e6".to_string())),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }));

            // Build: ROUND(column * 1e6, 0)
            let round_expr = Expression::Function(Box::new(Function::new(
                "ROUND".to_string(),
                vec![
                    column_times_1e6,
                    Expression::Literal(Literal::Number("0".to_string())),
                ],
            )));

            // Build: CAST(ROUND(...) AS BIGINT)
            let cast_to_bigint = Expression::Cast(Box::new(Cast {
                this: round_expr,
                to: DataType::BigInt { length: None },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
            }));

            // Build: CAST('1970-01-01' AS DATETIME2(6))
            let epoch_start = Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Literal::String("1970-01-01".to_string())),
                to: DataType::Custom {
                    name: "DATETIME2(6)".to_string(),
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
            }));

            // Build: DATEADD(MICROSECONDS, cast_to_bigint, epoch_start)
            let dateadd = Expression::Function(Box::new(Function::new(
                "DATEADD".to_string(),
                vec![
                    Expression::Identifier(Identifier::new("MICROSECONDS")),
                    cast_to_bigint,
                    epoch_start,
                ],
            )));

            return Ok(dateadd);
        }

        // Handle Function named UNIX_TO_TIME (parsed as generic function, not UnixToTime expression)
        // Reference: Python sqlglot Fabric dialect unixtotime_sql
        if let Expression::Function(ref f) = expr {
            if f.name.eq_ignore_ascii_case("UNIX_TO_TIME") && !f.args.is_empty() {
                let timestamp_input = f.args[0].clone();

                // Build: column * 1e6
                let column_times_1e6 = Expression::Mul(Box::new(BinaryOp {
                    left: timestamp_input,
                    right: Expression::Literal(Literal::Number("1e6".to_string())),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));

                // Build: ROUND(column * 1e6, 0)
                let round_expr = Expression::Function(Box::new(Function::new(
                    "ROUND".to_string(),
                    vec![
                        column_times_1e6,
                        Expression::Literal(Literal::Number("0".to_string())),
                    ],
                )));

                // Build: CAST(ROUND(...) AS BIGINT)
                let cast_to_bigint = Expression::Cast(Box::new(Cast {
                    this: round_expr,
                    to: DataType::BigInt { length: None },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));

                // Build: CAST('1970-01-01' AS DATETIME2(6))
                let epoch_start = Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String("1970-01-01".to_string())),
                    to: DataType::Custom {
                        name: "DATETIME2(6)".to_string(),
                    },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));

                // Build: DATEADD(MICROSECONDS, cast_to_bigint, epoch_start)
                let dateadd = Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![
                        Expression::Identifier(Identifier::new("MICROSECONDS")),
                        cast_to_bigint,
                        epoch_start,
                    ],
                )));

                return Ok(dateadd);
            }
        }

        // Delegate to T-SQL for other transformations
        let tsql = TSQLDialect;
        let transformed = tsql.transform_expr(expr)?;

        // Apply Fabric-specific transformations to the result
        self.transform_fabric_expr(transformed)
    }
}

impl FabricDialect {
    /// Fabric-specific expression transformations
    fn transform_fabric_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // Handle DataType expressions with Fabric-specific type mappings
            Expression::DataType(dt) => self.transform_fabric_data_type(dt),

            // Pass through everything else
            _ => Ok(expr),
        }
    }

    /// Transform data types according to Fabric TYPE_MAPPING
    /// Reference: https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
    fn transform_fabric_data_type(&self, dt: DataType) -> Result<Expression> {
        let transformed = match dt {
            // TIMESTAMP -> DATETIME2(6) with precision handling
            // Note: TSQL already converts this to DATETIME2, but without precision
            DataType::Timestamp { precision, .. } => {
                let p = Self::cap_precision(precision, 6);
                DataType::Custom {
                    name: format!("DATETIME2({})", p),
                }
            }

            // TIME -> TIME(6) default, capped at 6
            DataType::Time { precision, .. } => {
                let p = Self::cap_precision(precision, 6);
                DataType::Custom {
                    name: format!("TIME({})", p),
                }
            }

            // INT -> INT (override TSQL which may output INTEGER)
            DataType::Int { .. } => DataType::Custom {
                name: "INT".to_string(),
            },

            // DECIMAL -> DECIMAL (override TSQL which converts to NUMERIC)
            DataType::Decimal { precision, scale } => {
                if let (Some(p), Some(s)) = (&precision, &scale) {
                    DataType::Custom {
                        name: format!("DECIMAL({}, {})", p, s),
                    }
                } else if let Some(p) = &precision {
                    DataType::Custom {
                        name: format!("DECIMAL({})", p),
                    }
                } else {
                    DataType::Custom {
                        name: "DECIMAL".to_string(),
                    }
                }
            }

            // JSON -> VARCHAR
            DataType::Json => DataType::Custom {
                name: "VARCHAR".to_string(),
            },

            // UUID -> UNIQUEIDENTIFIER (already handled by TSQL, but ensure it's here)
            DataType::Uuid => DataType::Custom {
                name: "UNIQUEIDENTIFIER".to_string(),
            },

            // TinyInt -> SMALLINT
            DataType::TinyInt { .. } => DataType::Custom {
                name: "SMALLINT".to_string(),
            },

            // Handle Custom types for Fabric-specific mappings
            DataType::Custom { ref name } => {
                let upper = name.to_uppercase();

                // Parse out precision and scale if present: "TYPENAME(n)" or "TYPENAME(n, m)"
                let (base_name, precision, scale) = Self::parse_type_precision_and_scale(&upper);

                match base_name.as_str() {
                    // DATETIME -> DATETIME2(6)
                    "DATETIME" => DataType::Custom {
                        name: "DATETIME2(6)".to_string(),
                    },

                    // SMALLDATETIME -> DATETIME2(6)
                    "SMALLDATETIME" => DataType::Custom {
                        name: "DATETIME2(6)".to_string(),
                    },

                    // DATETIME2 -> DATETIME2(6) default, cap at 6
                    "DATETIME2" => {
                        let p = Self::cap_precision(precision, 6);
                        DataType::Custom {
                            name: format!("DATETIME2({})", p),
                        }
                    }

                    // DATETIMEOFFSET -> cap precision at 6
                    "DATETIMEOFFSET" => {
                        let p = Self::cap_precision(precision, 6);
                        DataType::Custom {
                            name: format!("DATETIMEOFFSET({})", p),
                        }
                    }

                    // TIME -> TIME(6) default, cap at 6
                    "TIME" => {
                        let p = Self::cap_precision(precision, 6);
                        DataType::Custom {
                            name: format!("TIME({})", p),
                        }
                    }

                    // TIMESTAMP -> DATETIME2(6)
                    "TIMESTAMP" => DataType::Custom {
                        name: "DATETIME2(6)".to_string(),
                    },

                    // TIMESTAMPNTZ -> DATETIME2(6) with precision
                    "TIMESTAMPNTZ" => {
                        let p = Self::cap_precision(precision, 6);
                        DataType::Custom {
                            name: format!("DATETIME2({})", p),
                        }
                    }

                    // TIMESTAMPTZ -> DATETIME2(6) with precision
                    "TIMESTAMPTZ" => {
                        let p = Self::cap_precision(precision, 6);
                        DataType::Custom {
                            name: format!("DATETIME2({})", p),
                        }
                    }

                    // IMAGE -> VARBINARY
                    "IMAGE" => DataType::Custom {
                        name: "VARBINARY".to_string(),
                    },

                    // MONEY -> DECIMAL
                    "MONEY" => DataType::Custom {
                        name: "DECIMAL".to_string(),
                    },

                    // SMALLMONEY -> DECIMAL
                    "SMALLMONEY" => DataType::Custom {
                        name: "DECIMAL".to_string(),
                    },

                    // NCHAR -> CHAR (with length preserved)
                    "NCHAR" => {
                        if let Some(len) = precision {
                            DataType::Custom {
                                name: format!("CHAR({})", len),
                            }
                        } else {
                            DataType::Custom {
                                name: "CHAR".to_string(),
                            }
                        }
                    }

                    // NVARCHAR -> VARCHAR (with length preserved)
                    "NVARCHAR" => {
                        if let Some(len) = precision {
                            DataType::Custom {
                                name: format!("VARCHAR({})", len),
                            }
                        } else {
                            DataType::Custom {
                                name: "VARCHAR".to_string(),
                            }
                        }
                    }

                    // TINYINT -> SMALLINT
                    "TINYINT" => DataType::Custom {
                        name: "SMALLINT".to_string(),
                    },

                    // UTINYINT -> SMALLINT
                    "UTINYINT" => DataType::Custom {
                        name: "SMALLINT".to_string(),
                    },

                    // VARIANT -> SQL_VARIANT
                    "VARIANT" => DataType::Custom {
                        name: "SQL_VARIANT".to_string(),
                    },

                    // XML -> VARCHAR
                    "XML" => DataType::Custom {
                        name: "VARCHAR".to_string(),
                    },

                    // NUMERIC -> DECIMAL (override TSQL's conversion)
                    // Fabric uses DECIMAL, not NUMERIC
                    "NUMERIC" => {
                        if let (Some(p), Some(s)) = (precision, scale) {
                            DataType::Custom {
                                name: format!("DECIMAL({}, {})", p, s),
                            }
                        } else if let Some(p) = precision {
                            DataType::Custom {
                                name: format!("DECIMAL({})", p),
                            }
                        } else {
                            DataType::Custom {
                                name: "DECIMAL".to_string(),
                            }
                        }
                    }

                    // Pass through other custom types unchanged
                    _ => dt,
                }
            }

            // Keep all other types as transformed by TSQL
            other => other,
        };

        Ok(Expression::DataType(transformed))
    }

    /// Cap precision to max value, defaulting to max if not specified
    fn cap_precision(precision: Option<u32>, max: u32) -> u32 {
        match precision {
            Some(p) if p > max => max,
            Some(p) => p,
            None => max, // Default to max if not specified
        }
    }

    /// Parse type name and optional precision/scale from strings like "DATETIME2(7)" or "NUMERIC(10, 2)"
    fn parse_type_precision_and_scale(name: &str) -> (String, Option<u32>, Option<u32>) {
        if let Some(paren_pos) = name.find('(') {
            let base = name[..paren_pos].to_string();
            let rest = &name[paren_pos + 1..];
            if let Some(close_pos) = rest.find(')') {
                let args = &rest[..close_pos];
                let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();

                let precision = parts.first().and_then(|s| s.parse::<u32>().ok());
                let scale = parts.get(1).and_then(|s| s.parse::<u32>().ok());

                return (base, precision, scale);
            }
            (base, None, None)
        } else {
            (name.to_string(), None, None)
        }
    }
}
