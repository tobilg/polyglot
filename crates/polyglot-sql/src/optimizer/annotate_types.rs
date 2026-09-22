//! Type Annotation for SQL Expressions
//!
//! This module provides type inference and annotation for SQL AST nodes.
//! It walks the expression tree and assigns data types to expressions based on:
//! - Literal values (strings, numbers, booleans)
//! - Column references (from schema)
//! - Function return types
//! - Operator result types (with coercion rules)
//!
//! Based on SQLGlot's optimizer/annotate_types.py

use std::collections::{HashMap, HashSet};

use crate::dialects::DialectType;
use crate::expressions::{
    BinaryOp, DataType, DateTimeField, DotAccess, Expression, Function, IfFunc, ListAggOverflow,
    Literal, Map, Nvl2Func, Struct, StructField, Subscript,
};
use crate::schema::{normalize_name, Schema, SchemaError, SchemaResult, TABLE_PARTS};
use crate::traversal::ExpressionWalk;

/// Type coercion class for determining result types in binary operations.
/// Higher-priority classes win during coercion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TypeCoercionClass {
    /// Text types (CHAR, VARCHAR, TEXT)
    Text = 0,
    /// Numeric types (INT, FLOAT, DECIMAL, etc.)
    Numeric = 1,
    /// Time-like types (DATE, TIME, TIMESTAMP, INTERVAL)
    Timelike = 2,
}

impl TypeCoercionClass {
    /// Get the coercion class for a data type
    pub fn from_data_type(dt: &DataType) -> Option<Self> {
        match dt {
            // Text types
            DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::Text
            | DataType::Binary { .. }
            | DataType::VarBinary { .. }
            | DataType::Blob => Some(TypeCoercionClass::Text),

            // Numeric types
            DataType::Boolean
            | DataType::TinyInt { .. }
            | DataType::SmallInt { .. }
            | DataType::Int { .. }
            | DataType::BigInt { .. }
            | DataType::Int128
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::UInt128
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::Decimal { .. } => Some(TypeCoercionClass::Numeric),

            // Timelike types
            DataType::Date
            | DataType::Time { .. }
            | DataType::Timestamp { .. }
            | DataType::Interval { .. } => Some(TypeCoercionClass::Timelike),

            // Other types don't have a coercion class
            _ => None,
        }
    }
}

/// Type annotation configuration and state
pub struct TypeAnnotator<'a> {
    /// Schema for looking up column types
    _schema: Option<&'a dyn Schema>,
    /// Catalogue and visible CTE definitions used to bind nested query inputs.
    /// This is separate from the selected bindings used for column lookup.
    query_schema: Option<&'a dyn Schema>,
    /// Dialect for dialect-specific type rules
    _dialect: Option<DialectType>,
    /// Whether to annotate types for all expressions
    annotate_aggregates: bool,
    /// Function return type mappings
    function_return_types: HashMap<String, DataType>,
    used_unknown_function_fallback: bool,
}

impl<'a> TypeAnnotator<'a> {
    /// Create a new type annotator
    pub fn new(schema: Option<&'a dyn Schema>, dialect: Option<DialectType>) -> Self {
        let mut annotator = Self {
            _schema: schema,
            query_schema: schema,
            _dialect: dialect,
            annotate_aggregates: true,
            function_return_types: HashMap::new(),
            used_unknown_function_fallback: false,
        };
        annotator.init_function_return_types();
        annotator
    }

    /// Initialize function return type mappings
    fn init_function_return_types(&mut self) {
        // Aggregate functions
        self.function_return_types
            .insert("COUNT".to_string(), DataType::BigInt { length: None });
        self.function_return_types.insert(
            "SUM".to_string(),
            DataType::Decimal {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "AVG".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );

        // String functions
        if self._dialect == Some(DialectType::DuckDB) {
            // DuckDB's list-to-string and date-name functions always return
            // VARCHAR, even when the argument's type is unresolved. Keep these
            // rules dialect-specific: BigQuery's ARRAY_TO_STRING also has a
            // BYTES overload.
            for name in [
                "ARRAY_TO_STRING",
                "ARRAY_TO_STRING_COMMA_DEFAULT",
                "MONTHNAME",
                "DAYNAME",
            ] {
                self.function_return_types.insert(
                    name.to_string(),
                    DataType::VarChar {
                        length: None,
                        parenthesized_length: false,
                    },
                );
            }
        }
        self.function_return_types.insert(
            "CONCAT".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "UPPER".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "LOWER".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "TRIM".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "LTRIM".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "RTRIM".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "SUBSTRING".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "SUBSTR".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "REPLACE".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "LENGTH".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "CHAR_LENGTH".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );

        // Date/Time functions
        self.function_return_types.insert(
            "NOW".to_string(),
            DataType::Timestamp {
                precision: None,
                timezone: false,
            },
        );
        self.function_return_types.insert(
            "CURRENT_TIMESTAMP".to_string(),
            DataType::Timestamp {
                precision: None,
                timezone: false,
            },
        );
        self.function_return_types
            .insert("CURRENT_DATE".to_string(), DataType::Date);
        self.function_return_types.insert(
            "CURRENT_TIME".to_string(),
            DataType::Time {
                precision: None,
                timezone: false,
            },
        );
        self.function_return_types
            .insert("DATE".to_string(), DataType::Date);
        self.function_return_types.insert(
            "YEAR".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "MONTH".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "DAY".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "HOUR".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "MINUTE".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "SECOND".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "EXTRACT".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "DATE_DIFF".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "DATEDIFF".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );

        // Math functions
        self.function_return_types.insert(
            "ABS".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "ROUND".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "DATE_FORMAT".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "FORMAT_DATE".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "TIME_TO_STR".to_string(),
            DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
        );
        self.function_return_types.insert(
            "SQRT".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "POWER".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "MOD".to_string(),
            DataType::Int {
                length: None,
                integer_spelling: false,
            },
        );
        self.function_return_types.insert(
            "LOG".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "LN".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );
        self.function_return_types.insert(
            "EXP".to_string(),
            DataType::Double {
                precision: None,
                scale: None,
            },
        );

        // Null-handling functions return Unknown (infer from args)
        self.function_return_types
            .insert("COALESCE".to_string(), DataType::Unknown);
        self.function_return_types
            .insert("NULLIF".to_string(), DataType::Unknown);
        self.function_return_types
            .insert("GREATEST".to_string(), DataType::Unknown);
        self.function_return_types
            .insert("LEAST".to_string(), DataType::Unknown);
    }

    /// Annotate types for an expression tree
    pub fn annotate(&mut self, expr: &Expression) -> Option<DataType> {
        match expr {
            // Literals
            Expression::Literal(lit) => Self::annotate_literal(lit),
            Expression::Boolean(_) => Some(DataType::Boolean),
            Expression::Null(_) => None, // NULL has no type

            // Arithmetic binary operations
            Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Mod(op) => self.annotate_arithmetic(op, false),
            Expression::Div(op) => self.annotate_arithmetic(op, true),

            // Comparison operations - always boolean
            Expression::Eq(_)
            | Expression::Neq(_)
            | Expression::Lt(_)
            | Expression::Lte(_)
            | Expression::Gt(_)
            | Expression::Gte(_)
            | Expression::Like(_)
            | Expression::ILike(_) => Some(DataType::Boolean),

            // Logical operations - always boolean
            Expression::And(_) | Expression::Or(_) | Expression::Not(_) => Some(DataType::Boolean),

            // Predicates - always boolean
            Expression::Between(_)
            | Expression::In(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Is(_)
            | Expression::Exists(_) => Some(DataType::Boolean),

            // String concatenation
            Expression::Concat(_) => Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            }),

            // Bitwise operations - integer
            Expression::BitwiseAnd(_)
            | Expression::BitwiseOr(_)
            | Expression::BitwiseXor(_)
            | Expression::BitwiseNot(_) => Some(DataType::BigInt { length: None }),

            // Negation preserves type
            Expression::Neg(op) => self.annotate(&op.this),

            // Transparent wrappers preserve the type of their inner expression.
            Expression::Paren(paren) => self.annotate(&paren.this),
            Expression::Annotated(annotated) => self.annotate(&annotated.this),

            // Functions
            Expression::Function(func) => self.annotate_function(func),
            Expression::IfFunc(if_func) => self.annotate_if_func(if_func),
            Expression::Nvl2(nvl2) => self.annotate_nvl2(nvl2),
            Expression::Coalesce(coalesce) => self.coerce_arg_types(&coalesce.expressions),

            // Typed aggregate functions
            Expression::Count(_) => Some(DataType::BigInt { length: None }),
            Expression::Sum(agg) => self.annotate_sum(&agg.this),
            Expression::SumIf(f) => self.annotate_sum(&f.this),
            Expression::Avg(_) => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            Expression::Min(agg) => self.annotate(&agg.this),
            Expression::Max(agg) => self.annotate(&agg.this),
            Expression::Median(agg) => {
                if self._dialect == Some(DialectType::DuckDB) {
                    self.annotate_duckdb_median(&agg.this)
                } else {
                    None
                }
            }
            Expression::GroupConcat(_) | Expression::StringAgg(_) | Expression::ListAgg(_) => {
                Some(DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                })
            }

            // Generic aggregate function
            Expression::AggregateFunction(agg) => {
                if !self.annotate_aggregates {
                    return None;
                }
                let func_name = agg.name.to_uppercase();
                self.get_aggregate_return_type(&func_name, &agg.args)
            }

            // Column references - look up type from schema if available
            Expression::Column(col) => {
                if let Some(schema) = &self._schema {
                    let table_name = col
                        .table
                        .as_ref()
                        .map(crate::binding::identifier_name)
                        .unwrap_or_default();
                    schema
                        .get_column_type(&table_name, &crate::binding::identifier_name(&col.name))
                        .ok()
                } else {
                    None
                }
            }

            // Cast expressions
            Expression::Cast(cast) => Some(cast.to.clone()),
            Expression::SafeCast(cast) => Some(cast.to.clone()),
            Expression::TryCast(cast) => Some(cast.to.clone()),

            // Only a single-column relation can supply a scalar subquery type.
            Expression::Subquery(subq) => subq.inferred_type.clone().or_else(|| {
                let mut query = subq.this.clone();
                let columns = annotate_scoped_expression_with_outer(
                    &mut query,
                    self.query_schema,
                    self._dialect,
                    self._schema,
                );
                match columns.as_slice() {
                    [(_, data_type)] => Some(data_type.clone()),
                    _ => None,
                }
            }),

            // CASE expression - common type of all result branches
            Expression::Case(case) => self.coerce_expression_types(
                case.whens
                    .iter()
                    .map(|(_, result)| result)
                    .chain(case.else_.iter()),
            ),

            // Array expressions. Bracket constructors such as ARRAY[...] and LIST[...]
            // parse as ArrayFunc, while some normalized expressions use Array.
            Expression::Array(arr) => self.annotate_array(&arr.expressions),
            Expression::ArrayFunc(arr) => self.annotate_array(&arr.expressions),
            Expression::ArrayTransform(array) => {
                let element = match &array.transform {
                    Expression::Lambda(lambda) => self.annotate(&lambda.body),
                    _ => None,
                }
                .unwrap_or(DataType::Unknown);
                Some(DataType::Array {
                    element_type: Box::new(element),
                    dimension: None,
                })
            }
            Expression::ArrayFilter(array) => self.annotate(&array.this),

            // Interval expressions
            Expression::Interval(_) => Some(DataType::Interval {
                unit: None,
                to: None,
            }),

            // Window functions inherit type from their function
            Expression::WindowFunction(window) => self.annotate(&window.this),

            // Date/time expressions
            Expression::CurrentDate(_) => Some(DataType::Date),
            Expression::CurrentTime(_) => Some(DataType::Time {
                precision: None,
                timezone: false,
            }),
            Expression::CurrentTimestamp(_) | Expression::CurrentTimestampLTZ(_) => {
                Some(DataType::Timestamp {
                    precision: None,
                    timezone: false,
                })
            }

            // Date functions
            Expression::DateAdd(_)
            | Expression::DateSub(_)
            | Expression::ToDate(_)
            | Expression::Date(_) => Some(DataType::Date),
            Expression::DateDiff(_) => Some(DataType::Int {
                length: None,
                integer_spelling: false,
            }),
            Expression::Extract(extract) if matches!(self._dialect, Some(DialectType::DuckDB)) => {
                // DuckDB returns fractional epoch seconds and Julian days as DOUBLE.
                let fractional = match &extract.field {
                    DateTimeField::Epoch => true,
                    DateTimeField::Custom(name) => {
                        name.eq_ignore_ascii_case("epoch") || name.eq_ignore_ascii_case("julian")
                    }
                    _ => false,
                };
                Some(if fractional {
                    DataType::Double {
                        precision: None,
                        scale: None,
                    }
                } else {
                    DataType::BigInt { length: None }
                })
            }
            Expression::Extract(_) => Some(DataType::Int {
                length: None,
                integer_spelling: false,
            }),
            Expression::ToTimestamp(_) => Some(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),

            // String functions
            Expression::Upper(_)
            | Expression::Lower(_)
            | Expression::Trim(_)
            | Expression::LTrim(_)
            | Expression::RTrim(_)
            | Expression::Replace(_)
            | Expression::Substring(_)
            | Expression::Reverse(_)
            | Expression::Left(_)
            | Expression::Right(_)
            | Expression::Repeat(_)
            | Expression::Lpad(_)
            | Expression::Rpad(_)
            | Expression::ConcatWs(_)
            | Expression::Overlay(_) => Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            }),
            Expression::Length(_) => Some(DataType::Int {
                length: None,
                integer_spelling: false,
            }),

            // Math functions
            Expression::Abs(_)
            | Expression::Sqrt(_)
            | Expression::Cbrt(_)
            | Expression::Ln(_)
            | Expression::Exp(_)
            | Expression::Power(_)
            | Expression::Log(_) => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            Expression::Round(_) => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            Expression::Floor(f) => self.annotate_math_function(&f.this),
            Expression::Ceil(f) => self.annotate_math_function(&f.this),
            Expression::Sign(s) => self.annotate(&s.this),
            Expression::DateFormat(_) | Expression::FormatDate(_) | Expression::TimeToStr(_) => {
                Some(DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                })
            }

            // Greatest/Least - coerce argument types
            Expression::Greatest(v) | Expression::Least(v) => self.coerce_arg_types(&v.expressions),

            // Alias - type of the inner expression
            Expression::Alias(alias) => self.annotate(&alias.this),

            // SELECT expressions - no scalar type
            Expression::Select(_) => None,

            // ============================================
            // 3.1.8: Array/Map Indexing (Subscript/Bracket)
            // ============================================
            Expression::Subscript(sub) => self.annotate_subscript(sub),

            // Dot access (struct.field) - resolve the field from the base STRUCT type
            Expression::Dot(dot) => self.annotate_dot(dot),

            // ============================================
            // 3.1.9: STRUCT Construction
            // ============================================
            Expression::Struct(s) => self.annotate_struct(s),

            // ============================================
            // 3.1.10: MAP Construction
            // ============================================
            Expression::Map(map) => self.annotate_map(map),
            Expression::MapFromEntries(mfe) => {
                // MAP_FROM_ENTRIES(array_of_pairs) - infer from array element type
                if let Some(DataType::Array { element_type, .. }) = self.annotate(&mfe.this) {
                    if let DataType::Struct { fields, .. } = *element_type {
                        if fields.len() >= 2 {
                            return Some(DataType::Map {
                                key_type: Box::new(fields[0].data_type.clone()),
                                value_type: Box::new(fields[1].data_type.clone()),
                            });
                        }
                    }
                }
                Some(DataType::Map {
                    key_type: Box::new(DataType::Unknown),
                    value_type: Box::new(DataType::Unknown),
                })
            }

            // ============================================
            // 3.1.11: SetOperation Type Coercion
            // ============================================
            Expression::Union(union) => self.annotate_set_operation(&union.left, &union.right),
            Expression::Intersect(intersect) => {
                self.annotate_set_operation(&intersect.left, &intersect.right)
            }
            Expression::Except(except) => self.annotate_set_operation(&except.left, &except.right),

            // ============================================
            // 3.1.12: UDTF Type Handling
            // ============================================
            Expression::Lateral(lateral) => {
                // LATERAL subquery - type is the subquery's type
                self.annotate(&lateral.this)
            }
            Expression::LateralView(lv) => {
                // LATERAL VIEW - returns the exploded type
                self.annotate_lateral_view(lv)
            }
            Expression::Unnest(unnest) => {
                // UNNEST(array) - returns the element type of the array
                if let Some(DataType::Array { element_type, .. }) = self.annotate(&unnest.this) {
                    Some(*element_type)
                } else {
                    None
                }
            }
            Expression::Explode(explode) => {
                // EXPLODE(array) - returns the element type
                if let Some(DataType::Array { element_type, .. }) = self.annotate(&explode.this) {
                    Some(*element_type)
                } else if let Some(DataType::Map {
                    key_type,
                    value_type,
                }) = self.annotate(&explode.this)
                {
                    // EXPLODE(map) returns struct(key, value)
                    Some(DataType::Struct {
                        fields: vec![
                            StructField::new("key".to_string(), *key_type),
                            StructField::new("value".to_string(), *value_type),
                        ],
                        nested: false,
                    })
                } else {
                    None
                }
            }
            Expression::ExplodeOuter(explode) => {
                // EXPLODE_OUTER - same as EXPLODE but preserves nulls
                if let Some(DataType::Array { element_type, .. }) = self.annotate(&explode.this) {
                    Some(*element_type)
                } else {
                    None
                }
            }
            Expression::GenerateSeries(gs) => {
                // GENERATE_SERIES returns the type of start/end
                if let Some(ref start) = gs.start {
                    self.annotate(start)
                } else if let Some(ref end) = gs.end {
                    self.annotate(end)
                } else {
                    Some(DataType::Int {
                        length: None,
                        integer_spelling: false,
                    })
                }
            }

            // Other expressions - unknown
            _ => None,
        }
    }

    /// Annotate types in-place on the expression tree (bottom-up).
    ///
    /// First recurses into children, then computes this node's type using the
    /// read-only `annotate` method, and finally stores the result via
    /// `set_inferred_type`.
    pub fn annotate_in_place(&mut self, expr: &mut Expression) {
        if matches!(
            expr,
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
        ) {
            annotate_scoped_expression_with_outer(
                expr,
                self.query_schema,
                self._dialect,
                self._schema,
            );
            return;
        }
        // 1. Recurse into children (bottom-up)
        self.annotate_children_in_place(expr);

        // 2. Compute this node's type using the read-only method
        //    (children already have their types set, but `annotate` re-derives
        //    from structure, which is fine since the structure hasn't changed)
        let dt = self.annotate(expr);

        // 3. Store on the node
        if let Some(data_type) = dt {
            expr.set_inferred_type(data_type);
        }
    }

    /// Recursively annotate children of an expression in-place.
    fn annotate_children_in_place(&mut self, expr: &mut Expression) {
        match expr {
            // Binary operations
            Expression::And(op)
            | Expression::Or(op)
            | Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op)
            | Expression::Mod(op)
            | Expression::Eq(op)
            | Expression::Neq(op)
            | Expression::Lt(op)
            | Expression::Lte(op)
            | Expression::Gt(op)
            | Expression::Gte(op)
            | Expression::Concat(op)
            | Expression::BitwiseAnd(op)
            | Expression::BitwiseOr(op)
            | Expression::BitwiseXor(op)
            | Expression::Adjacent(op)
            | Expression::TsMatch(op)
            | Expression::PropertyEQ(op)
            | Expression::ArrayContainsAll(op)
            | Expression::ArrayContainedBy(op)
            | Expression::ArrayOverlaps(op)
            | Expression::JSONBContainsAllTopKeys(op)
            | Expression::JSONBContainsAnyTopKeys(op)
            | Expression::JSONBDeleteAtPath(op)
            | Expression::ExtendsLeft(op)
            | Expression::ExtendsRight(op)
            | Expression::Is(op)
            | Expression::MemberOf(op)
            | Expression::Match(op)
            | Expression::NullSafeEq(op)
            | Expression::NullSafeNeq(op)
            | Expression::Glob(op)
            | Expression::BitwiseLeftShift(op)
            | Expression::BitwiseRightShift(op) => {
                self.annotate_in_place(&mut op.left);
                self.annotate_in_place(&mut op.right);
            }

            // Like operations
            Expression::Like(op) | Expression::ILike(op) => {
                self.annotate_in_place(&mut op.left);
                self.annotate_in_place(&mut op.right);
            }

            // Unary operations
            Expression::Not(op) | Expression::Neg(op) | Expression::BitwiseNot(op) => {
                self.annotate_in_place(&mut op.this);
            }

            // Cast
            Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
                self.annotate_in_place(&mut c.this);
            }

            // Transparent wrappers
            Expression::Paren(paren) => {
                self.annotate_in_place(&mut paren.this);
            }
            Expression::Annotated(annotated) => {
                self.annotate_in_place(&mut annotated.this);
            }

            // Case
            Expression::Case(c) => {
                if let Some(ref mut operand) = c.operand {
                    self.annotate_in_place(operand);
                }
                for (cond, then_expr) in &mut c.whens {
                    self.annotate_in_place(cond);
                    self.annotate_in_place(then_expr);
                }
                if let Some(ref mut else_expr) = c.else_ {
                    self.annotate_in_place(else_expr);
                }
            }

            // Array constructors
            Expression::Array(a) => {
                for expression in &mut a.expressions {
                    self.annotate_in_place(expression);
                }
            }
            Expression::ArrayFunc(a) => {
                for expression in &mut a.expressions {
                    self.annotate_in_place(expression);
                }
            }

            // Alias
            Expression::Alias(a) => {
                self.annotate_in_place(&mut a.this);
            }

            // Column - leaf node, no children to recurse
            Expression::Column(_) => {}

            // Dot access - the field is an identifier, so only the base is typed
            Expression::Dot(dot) => {
                self.annotate_in_place(&mut dot.this);
            }

            // Function
            Expression::Function(f) | Expression::HanaFunction(f) => {
                for arg in &mut f.args {
                    self.annotate_in_place(arg);
                }
            }
            Expression::Unnest(unnest) => {
                self.annotate_in_place(&mut unnest.this);
                for expression in &mut unnest.expressions {
                    self.annotate_in_place(expression);
                }
            }

            // Dedicated conditional functions
            Expression::IfFunc(f) => {
                self.annotate_in_place(&mut f.condition);
                self.annotate_in_place(&mut f.true_value);
                if let Some(false_value) = &mut f.false_value {
                    self.annotate_in_place(false_value);
                }
            }
            Expression::Nvl2(f) => {
                self.annotate_in_place(&mut f.this);
                self.annotate_in_place(&mut f.true_value);
                self.annotate_in_place(&mut f.false_value);
            }

            // AggregateFunction
            Expression::AggregateFunction(f) => {
                for arg in &mut f.args {
                    self.annotate_in_place(arg);
                }
            }

            // Dedicated aggregate / string functions
            Expression::Count(f) => {
                if let Some(this) = &mut f.this {
                    self.annotate_in_place(this);
                }
                if let Some(filter) = &mut f.filter {
                    self.annotate_in_place(filter);
                }
            }
            Expression::GroupConcat(f) => {
                self.annotate_in_place(&mut f.this);
                if let Some(separator) = &mut f.separator {
                    self.annotate_in_place(separator);
                }
                if let Some(order_by) = &mut f.order_by {
                    for ordered in order_by {
                        self.annotate_in_place(&mut ordered.this);
                    }
                }
                if let Some(filter) = &mut f.filter {
                    self.annotate_in_place(filter);
                }
            }
            Expression::StringAgg(f) => {
                self.annotate_in_place(&mut f.this);
                if let Some(separator) = &mut f.separator {
                    self.annotate_in_place(separator);
                }
                if let Some(order_by) = &mut f.order_by {
                    for ordered in order_by {
                        self.annotate_in_place(&mut ordered.this);
                    }
                }
                if let Some(filter) = &mut f.filter {
                    self.annotate_in_place(filter);
                }
                if let Some(limit) = &mut f.limit {
                    self.annotate_in_place(limit);
                }
            }
            Expression::ListAgg(f) => {
                self.annotate_in_place(&mut f.this);
                if let Some(separator) = &mut f.separator {
                    self.annotate_in_place(separator);
                }
                if let Some(order_by) = &mut f.order_by {
                    for ordered in order_by {
                        self.annotate_in_place(&mut ordered.this);
                    }
                }
                if let Some(filter) = &mut f.filter {
                    self.annotate_in_place(filter);
                }
                if let Some(ListAggOverflow::Truncate {
                    filler: Some(filler),
                    ..
                }) = &mut f.on_overflow
                {
                    self.annotate_in_place(filler);
                }
            }
            Expression::SumIf(f) => {
                self.annotate_in_place(&mut f.this);
                self.annotate_in_place(&mut f.condition);
                if let Some(filter) = &mut f.filter {
                    self.annotate_in_place(filter);
                }
            }

            // WindowFunction
            Expression::WindowFunction(w) => {
                self.annotate_in_place(&mut w.this);
            }

            // Subquery
            Expression::Subquery(s) => {
                let columns = annotate_scoped_expression_with_outer(
                    &mut s.this,
                    self.query_schema,
                    self._dialect,
                    self._schema,
                );
                s.inferred_type = match columns.as_slice() {
                    [(_, data_type)] => Some(data_type.clone()),
                    _ => None,
                };
            }

            Expression::Trim(f) => {
                self.annotate_in_place(&mut f.this);
                if let Some(characters) = &mut f.characters {
                    self.annotate_in_place(characters);
                }
            }

            // UnaryFunc variants
            Expression::Upper(f)
            | Expression::Lower(f)
            | Expression::Length(f)
            | Expression::LTrim(f)
            | Expression::RTrim(f)
            | Expression::Reverse(f)
            | Expression::Abs(f)
            | Expression::Sqrt(f)
            | Expression::Cbrt(f)
            | Expression::Ln(f)
            | Expression::Exp(f)
            | Expression::Sign(f)
            | Expression::Date(f)
            | Expression::Time(f)
            | Expression::Explode(f)
            | Expression::ExplodeOuter(f)
            | Expression::MapFromEntries(f)
            | Expression::MapKeys(f)
            | Expression::MapValues(f)
            | Expression::ArrayLength(f)
            | Expression::ArraySize(f)
            | Expression::Cardinality(f)
            | Expression::ArrayReverse(f)
            | Expression::ArrayDistinct(f)
            | Expression::ArrayFlatten(f)
            | Expression::ArrayCompact(f)
            | Expression::ToArray(f)
            | Expression::JsonArrayLength(f)
            | Expression::JsonKeys(f)
            | Expression::JsonType(f)
            | Expression::ParseJson(f)
            | Expression::ToJson(f)
            | Expression::Year(f)
            | Expression::Month(f)
            | Expression::Day(f)
            | Expression::Hour(f)
            | Expression::Minute(f)
            | Expression::Second(f)
            | Expression::Initcap(f)
            | Expression::Ascii(f)
            | Expression::Chr(f)
            | Expression::Soundex(f)
            | Expression::ByteLength(f)
            | Expression::Hex(f)
            | Expression::LowerHex(f)
            | Expression::Unicode(f)
            | Expression::Typeof(f)
            | Expression::BitwiseCount(f)
            | Expression::Epoch(f)
            | Expression::EpochMs(f)
            | Expression::Radians(f)
            | Expression::Degrees(f)
            | Expression::Sin(f)
            | Expression::Cos(f)
            | Expression::Tan(f)
            | Expression::Asin(f)
            | Expression::Acos(f)
            | Expression::Atan(f)
            | Expression::IsNan(f)
            | Expression::IsInf(f) => {
                self.annotate_in_place(&mut f.this);
            }

            // BinaryFunc variants
            Expression::Power(f)
            | Expression::NullIf(f)
            | Expression::IfNull(f)
            | Expression::Nvl(f)
            | Expression::Contains(f)
            | Expression::StartsWith(f)
            | Expression::EndsWith(f)
            | Expression::Levenshtein(f)
            | Expression::ModFunc(f)
            | Expression::IntDiv(f)
            | Expression::Atan2(f)
            | Expression::AddMonths(f)
            | Expression::MonthsBetween(f)
            | Expression::NextDay(f)
            | Expression::UnixToTimeStr(f)
            | Expression::ArrayContains(f)
            | Expression::ArrayPosition(f)
            | Expression::ArrayAppend(f)
            | Expression::ArrayPrepend(f)
            | Expression::ArrayUnion(f)
            | Expression::ArrayExcept(f)
            | Expression::ArrayRemove(f)
            | Expression::StarMap(f)
            | Expression::MapFromArrays(f)
            | Expression::MapContainsKey(f)
            | Expression::ElementAt(f)
            | Expression::JsonMergePatch(f) => {
                self.annotate_in_place(&mut f.this);
                self.annotate_in_place(&mut f.expression);
            }

            // VarArgFunc variants
            Expression::Coalesce(f)
            | Expression::Greatest(f)
            | Expression::Least(f)
            | Expression::ArrayConcat(f)
            | Expression::ArrayIntersect(f)
            | Expression::ArrayZip(f)
            | Expression::MapConcat(f)
            | Expression::JsonArray(f) => {
                for e in &mut f.expressions {
                    self.annotate_in_place(e);
                }
            }

            // AggFunc variants
            Expression::Sum(f)
            | Expression::Avg(f)
            | Expression::Min(f)
            | Expression::Max(f)
            | Expression::ArrayAgg(f)
            | Expression::CountIf(f)
            | Expression::Stddev(f)
            | Expression::StddevPop(f)
            | Expression::StddevSamp(f)
            | Expression::Variance(f)
            | Expression::VarPop(f)
            | Expression::VarSamp(f)
            | Expression::Median(f)
            | Expression::Mode(f)
            | Expression::First(f)
            | Expression::Last(f)
            | Expression::AnyValue(f)
            | Expression::ApproxDistinct(f)
            | Expression::ApproxCountDistinct(f)
            | Expression::LogicalAnd(f)
            | Expression::LogicalOr(f)
            | Expression::Skewness(f)
            | Expression::ArrayConcatAgg(f)
            | Expression::ArrayUniqueAgg(f)
            | Expression::BoolXorAgg(f)
            | Expression::BitwiseAndAgg(f)
            | Expression::BitwiseOrAgg(f)
            | Expression::BitwiseXorAgg(f) => {
                self.annotate_in_place(&mut f.this);
            }

            // Select - recurse into expressions
            Expression::Select(s) => {
                for e in &mut s.expressions {
                    self.annotate_in_place(e);
                }
            }

            // Keep less common clause/function nodes on the canonical visitor.
            _ => {
                crate::ast_children::for_each_child_mut(expr, |child| self.annotate_in_place(child))
            }
        }
    }

    /// Annotate math functions like FLOOR/CEIL that return Double for integer inputs
    /// and preserve the input type otherwise (matching sqlglot's _annotate_math_functions).
    fn annotate_math_function(&mut self, arg: &Expression) -> Option<DataType> {
        let input_type = self.annotate(arg)?;
        match input_type {
            DataType::TinyInt { .. }
            | DataType::SmallInt { .. }
            | DataType::Int { .. }
            | DataType::BigInt { .. } => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            other => Some(other),
        }
    }

    /// Annotate a subscript/bracket expression (array[index] or map[key])
    fn annotate_subscript(&mut self, sub: &Subscript) -> Option<DataType> {
        let base_type = self.annotate(&sub.this)?;

        match base_type {
            DataType::Array { element_type, .. } => Some(*element_type),
            DataType::Map { value_type, .. } => Some(*value_type),
            DataType::Json | DataType::JsonB => Some(DataType::Json), // JSON indexing returns JSON
            DataType::VarChar { .. } | DataType::Text => {
                // String indexing returns a character
                Some(DataType::VarChar {
                    length: Some(1),
                    parenthesized_length: false,
                })
            }
            _ => None,
        }
    }

    /// Annotate a named field lookup from the type of its base expression.
    fn annotate_dot(&mut self, dot: &DotAccess) -> Option<DataType> {
        let base_type = self.annotate(&dot.this)?;
        let DataType::Struct { fields, .. } = base_type else {
            return None;
        };
        let field_name = normalize_name(&dot.field.name, self._dialect, false, true);

        fields
            .into_iter()
            .find(|field| normalize_name(&field.name, self._dialect, false, true) == field_name)
            .map(|field| field.data_type)
    }

    /// Annotate a STRUCT literal
    fn annotate_struct(&mut self, s: &Struct) -> Option<DataType> {
        let fields: Vec<StructField> = s
            .fields
            .iter()
            .map(|(name, expr)| {
                let field_type = self.annotate(expr).unwrap_or(DataType::Unknown);
                StructField::new(name.clone().unwrap_or_default(), field_type)
            })
            .collect();
        Some(DataType::Struct {
            fields,
            nested: false,
        })
    }

    /// Annotate a MAP literal
    fn annotate_map(&mut self, map: &Map) -> Option<DataType> {
        let key_type = if let Some(first_key) = map.keys.first() {
            self.annotate(first_key).unwrap_or(DataType::Unknown)
        } else {
            DataType::Unknown
        };

        let value_type = if let Some(first_value) = map.values.first() {
            self.annotate(first_value).unwrap_or(DataType::Unknown)
        } else {
            DataType::Unknown
        };

        Some(DataType::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        })
    }

    /// Annotate a SetOperation (UNION/INTERSECT/EXCEPT)
    /// Returns None since set operations produce relation types, not scalar types
    fn annotate_set_operation(
        &mut self,
        _left: &Expression,
        _right: &Expression,
    ) -> Option<DataType> {
        // Set operations produce relations, not scalar types
        // The column types would be coerced between left and right
        // For now, return None as this is a relation-level type
        None
    }

    /// Annotate a LATERAL VIEW expression
    fn annotate_lateral_view(&mut self, lv: &crate::expressions::LateralView) -> Option<DataType> {
        // The type depends on the table-generating function
        self.annotate(&lv.this)
    }

    /// Annotate a literal value
    pub(super) fn annotate_literal(lit: &Literal) -> Option<DataType> {
        match lit {
            Literal::String(_)
            | Literal::NationalString(_)
            | Literal::TripleQuotedString(_, _)
            | Literal::EscapeString(_)
            | Literal::DollarString(_)
            | Literal::RawString(_) => Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            }),
            Literal::Number(n) => {
                // Try to determine if it's an integer or float
                if n.contains('.') || n.contains('e') || n.contains('E') {
                    Some(DataType::Double {
                        precision: None,
                        scale: None,
                    })
                } else {
                    // Check if it fits in an Int or needs BigInt
                    if let Ok(_) = n.parse::<i32>() {
                        Some(DataType::Int {
                            length: None,
                            integer_spelling: false,
                        })
                    } else {
                        Some(DataType::BigInt { length: None })
                    }
                }
            }
            Literal::HexString(_) | Literal::BitString(_) | Literal::ByteString(_) => {
                Some(DataType::VarBinary { length: None })
            }
            Literal::HexNumber(_) => Some(DataType::BigInt { length: None }),
            Literal::Date(_) => Some(DataType::Date),
            Literal::Time(_) => Some(DataType::Time {
                precision: None,
                timezone: false,
            }),
            Literal::Timestamp(_) => Some(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),
            Literal::Datetime(_) => Some(DataType::Custom {
                name: "DATETIME".to_string(),
            }),
        }
    }

    /// Annotate an arithmetic binary operation
    fn annotate_arithmetic(&mut self, op: &BinaryOp, division: bool) -> Option<DataType> {
        let left_type = self.annotate(&op.left);
        let right_type = self.annotate(&op.right);

        match (left_type, right_type) {
            (Some(l), Some(r)) => {
                if self._dialect == Some(DialectType::DuckDB) {
                    if division && (is_unsigned_integer(&l) || is_unsigned_integer(&r)) {
                        return Some(double_type());
                    }
                    if let Some(result) = duckdb_unsigned_integer_coercion(&l, &r, true) {
                        return Some(result);
                    }
                }
                self.coerce_types(&l, &r)
            }
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        }
    }

    /// Annotate a function call
    fn annotate_function(&mut self, func: &Function) -> Option<DataType> {
        if self._dialect == Some(DialectType::BigQuery)
            && !func.quoted
            && func.name.eq_ignore_ascii_case("ARRAY")
        {
            return self.annotate_array(&func.args);
        }
        if func.name.eq_ignore_ascii_case("TRANSFORM")
            || func.name.eq_ignore_ascii_case("ARRAY_TRANSFORM")
        {
            return Some(DataType::Array {
                element_type: Box::new(
                    func.args
                        .get(1)
                        .and_then(|arg| match arg {
                            Expression::Lambda(lambda) => self.annotate(&lambda.body),
                            _ => None,
                        })
                        .unwrap_or(DataType::Unknown),
                ),
                dimension: None,
            });
        }
        if func.name.eq_ignore_ascii_case("FILTER")
            || func.name.eq_ignore_ascii_case("ARRAY_FILTER")
        {
            return func.args.first().and_then(|arg| self.annotate(arg));
        }
        let func_name = func.name.to_uppercase();

        if self._dialect == Some(DialectType::DuckDB) && !func.quoted {
            match func_name.as_str() {
                // Builder-created Function nodes use the same inference as the
                // dedicated nodes produced when parsing these built-ins.
                "SUM" => return func.args.first().and_then(|arg| self.annotate_sum(arg)),
                "COALESCE" | "IFNULL" => return self.coerce_arg_types(&func.args),
                "DATE_TRUNC" => return self.annotate_duckdb_date_trunc(func),
                "REGEXP_EXTRACT_ALL" => return self.annotate_duckdb_regexp_extract_all(func),
                _ => {}
            }
        }

        // Check known function return types
        if let Some(return_type) = self.function_return_types.get(&func_name) {
            if *return_type != DataType::Unknown {
                return Some(return_type.clone());
            }
        }

        // For functions with Unknown return type, infer from arguments
        match func_name.as_str() {
            "UNNEST" => func.args.first().and_then(|arg| match self.annotate(arg) {
                Some(DataType::Array { element_type, .. }) => Some(*element_type),
                _ => None,
            }),
            "COALESCE" | "IFNULL" | "NVL" | "ISNULL" => {
                // Return type of first non-null argument
                for arg in &func.args {
                    if let Some(arg_type) = self.annotate(arg) {
                        return Some(arg_type);
                    }
                }
                None
            }
            "NULLIF" => {
                // Return type of first argument
                func.args.first().and_then(|arg| self.annotate(arg))
            }
            "GREATEST" | "LEAST" => {
                // Coerce all argument types
                self.coerce_arg_types(&func.args)
            }
            "IF" | "IIF" => {
                // Return type of THEN/ELSE branches
                if func.args.len() >= 2 {
                    self.annotate(&func.args[1])
                } else {
                    None
                }
            }
            _ => {
                self.used_unknown_function_fallback = true;
                func.args.first().and_then(|arg| self.annotate(arg))
            }
        }
    }

    /// Relation outputs cannot rely on the scalar annotator's legacy fallback
    /// that assigns an unknown function the type of its first argument.
    pub(super) fn function_result_is_known(func: &Function, dialect: DialectType) -> bool {
        let mut annotator = Self::new(None, Some(dialect));
        // Probe dispatch without traversing or cloning a query argument. The
        // original annotation already supplied the type for recognized rules.
        let mut probe = Function::new(func.name.clone(), Vec::new());
        probe.quoted = func.quoted;
        annotator.annotate_function(&probe);
        !annotator.used_unknown_function_fallback
    }

    /// Infer DuckDB's string-list or named-capture struct-list regex result.
    /// The result depends on the group argument, not the input string's type.
    fn annotate_duckdb_regexp_extract_all(&mut self, func: &Function) -> Option<DataType> {
        if !(2..=4).contains(&func.args.len()) {
            return None;
        }

        fn unwrap(mut expr: &Expression) -> &Expression {
            loop {
                expr = match expr {
                    Expression::Paren(p) => &p.this,
                    Expression::Annotated(a) => &a.this,
                    _ => return expr,
                };
            }
        }

        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let element_type = if let Some(group) = func.args.get(2).map(unwrap) {
            let names = match group {
                Expression::Array(a) => Some(&a.expressions),
                Expression::ArrayFunc(a) => Some(&a.expressions),
                _ => None,
            };
            if let Some(names) = names {
                if names.is_empty() {
                    return None;
                }
                let mut seen = HashSet::new();
                let mut fields = Vec::with_capacity(names.len());
                for name in names {
                    let Expression::Literal(literal) = unwrap(name) else {
                        return None;
                    };
                    if !literal.is_string() {
                        return None;
                    }
                    let name = literal.value_str();
                    // DuckDB rejects duplicate names using ASCII-insensitive
                    // comparison, but preserves the original spelling.
                    if !seen.insert(name.to_ascii_lowercase()) {
                        return None;
                    }
                    fields.push(StructField::new(name.to_string(), varchar.clone()));
                }
                DataType::Struct {
                    fields,
                    nested: false,
                }
            } else if matches!(group, Expression::Null(_))
                || matches!(
                    self.annotate(group),
                    Some(
                        DataType::TinyInt { .. } | DataType::SmallInt { .. } | DataType::Int { .. }
                    )
                )
            {
                varchar
            } else {
                // Unknown group expressions or nonliteral name lists cannot
                // determine the element type. Do not fall back to the input.
                return None;
            }
        } else {
            varchar
        };
        Some(DataType::Array {
            element_type: Box::new(element_type),
            dimension: None,
        })
    }

    /// Infer DuckDB's overloaded DATE_TRUNC return type from its temporal value argument.
    fn annotate_duckdb_date_trunc(&mut self, func: &Function) -> Option<DataType> {
        if func.args.len() != 2 {
            return None;
        }

        match self.annotate(&func.args[1])? {
            DataType::Date => Some(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),
            DataType::Timestamp { timezone, .. } => Some(DataType::Timestamp {
                precision: None,
                timezone,
            }),
            DataType::Interval { .. } => Some(DataType::Interval {
                unit: None,
                to: None,
            }),
            _ => None,
        }
    }

    /// Annotate IF/IIF/IFF conditional function
    fn annotate_if_func(&mut self, func: &IfFunc) -> Option<DataType> {
        let true_type = self.annotate(&func.true_value);
        let false_type = func
            .false_value
            .as_ref()
            .and_then(|expr| self.annotate(expr));

        match (true_type, false_type) {
            (Some(left), Some(right)) => self.coerce_types(&left, &right),
            (Some(dt), None) | (None, Some(dt)) => Some(dt),
            (None, None) => None,
        }
    }

    /// Annotate NVL2 conditional function from its true/false branches
    fn annotate_nvl2(&mut self, func: &Nvl2Func) -> Option<DataType> {
        let true_type = self.annotate(&func.true_value);
        let false_type = self.annotate(&func.false_value);

        match (true_type, false_type) {
            (Some(left), Some(right)) => self.coerce_types(&left, &right),
            (Some(dt), None) | (None, Some(dt)) => Some(dt),
            (None, None) => None,
        }
    }

    /// Get return type for aggregate functions
    fn get_aggregate_return_type(
        &mut self,
        func_name: &str,
        args: &[Expression],
    ) -> Option<DataType> {
        match func_name {
            "COUNT" | "COUNT_IF" => Some(DataType::BigInt { length: None }),
            "SUM_IF" => {
                if let Some(arg) = args.first() {
                    self.annotate_sum(arg)
                } else {
                    Some(DataType::Decimal {
                        precision: None,
                        scale: None,
                    })
                }
            }
            "SUM" => {
                if let Some(arg) = args.first() {
                    self.annotate_sum(arg)
                } else {
                    Some(DataType::Decimal {
                        precision: None,
                        scale: None,
                    })
                }
            }
            "AVG" => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            "MIN" | "MAX" => {
                // DuckDB's two-argument MIN/MAX aggregate overloads return the
                // bottom/top N values as a list. Other parsed aggregate forms
                // preserve the input type.
                let input_type = args.first().and_then(|arg| self.annotate(arg));
                if args.len() == 2 {
                    input_type.map(|element_type| DataType::Array {
                        element_type: Box::new(element_type),
                        dimension: None,
                    })
                } else {
                    input_type
                }
            }
            "STRING_AGG" | "GROUP_CONCAT" | "LISTAGG" | "ARRAY_AGG" => Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            }),
            "BOOL_AND" | "BOOL_OR" | "EVERY" | "ANY" | "SOME" => Some(DataType::Boolean),
            "BIT_AND" | "BIT_OR" | "BIT_XOR" => Some(DataType::BigInt { length: None }),
            "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
                Some(DataType::Double {
                    precision: None,
                    scale: None,
                })
            }
            "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                args.first().and_then(|arg| self.annotate(arg))
            }
            "MEDIAN" => args.first().and_then(|arg| {
                if self._dialect == Some(DialectType::DuckDB) {
                    self.annotate_duckdb_median(arg)
                } else {
                    self.annotate(arg)
                }
            }),
            _ => None,
        }
    }

    /// DuckDB defines MEDIAN as QUANTILE_CONT(value, 0.5). Integral inputs are
    /// promoted to DOUBLE so an interpolated midpoint can be represented, and
    /// DATE inputs become TIMESTAMP. Other supported quantitative and ordinal
    /// types preserve their input type.
    fn annotate_duckdb_median(&mut self, arg: &Expression) -> Option<DataType> {
        match self.annotate(arg)? {
            DataType::TinyInt { .. }
            | DataType::SmallInt { .. }
            | DataType::Int { .. }
            | DataType::BigInt { .. }
            | DataType::Int128 => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::UInt128 => Some(double_type()),
            DataType::Date => Some(DataType::Timestamp {
                precision: None,
                timezone: false,
            }),
            DataType::Unknown => None,
            other => Some(other),
        }
    }

    /// Annotate SUM function - promotes to at least BigInt
    fn annotate_sum(&mut self, arg: &Expression) -> Option<DataType> {
        let arg_type = self.annotate(arg);
        if self._dialect == Some(DialectType::DuckDB) {
            // These are DuckDB's bound aggregate result types, not the generic
            // integer-to-BIGINT promotion. UHUGEINT binds to the DOUBLE overload.
            return match arg_type? {
                DataType::Boolean
                | DataType::TinyInt { .. }
                | DataType::SmallInt { .. }
                | DataType::Int { .. }
                | DataType::BigInt { .. }
                | DataType::Int128
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => Some(DataType::Int128),
                DataType::UInt128 | DataType::Float { .. } | DataType::Double { .. } => {
                    Some(double_type())
                }
                DataType::Decimal { precision, scale } => Some(DataType::Decimal {
                    precision: Some(38),
                    // DuckDB DECIMAL defaults to (18,3); DECIMAL(p) defaults to (p,0).
                    scale: Some(scale.unwrap_or(if precision.is_some() { 0 } else { 3 })),
                }),
                // Do not invent a concrete return type for an unresolved input.
                _ => None,
            };
        }
        match arg_type {
            Some(DataType::TinyInt { .. })
            | Some(DataType::SmallInt { .. })
            | Some(DataType::Int { .. }) => Some(DataType::BigInt { length: None }),
            Some(DataType::BigInt { .. }) => Some(DataType::BigInt { length: None }),
            Some(DataType::Int128) => Some(DataType::Int128),
            Some(DataType::Float { .. }) | Some(DataType::Double { .. }) => {
                Some(DataType::Double {
                    precision: None,
                    scale: None,
                })
            }
            Some(DataType::Decimal { precision, scale }) => {
                Some(DataType::Decimal { precision, scale })
            }
            _ => Some(DataType::Decimal {
                precision: None,
                scale: None,
            }),
        }
    }

    /// Infer the type of an array constructor from all of its elements.
    fn annotate_array(&mut self, expressions: &[Expression]) -> Option<DataType> {
        if let [query] = expressions {
            if matches!(
                query,
                Expression::Select(_)
                    | Expression::Union(_)
                    | Expression::Intersect(_)
                    | Expression::Except(_)
                    | Expression::Subquery(_)
            ) {
                let mut query = query.clone();
                let columns = annotate_scoped_expression_with_outer(
                    &mut query,
                    self.query_schema,
                    self._dialect,
                    self._schema,
                );
                let element_type = match columns.as_slice() {
                    [(_, data_type)] if *data_type != DataType::Unknown => data_type.clone(),
                    _ => return None,
                };
                return Some(DataType::Array {
                    element_type: Box::new(element_type),
                    dimension: None,
                });
            }
        }
        let element_type = self
            .coerce_expression_types(expressions.iter())
            .unwrap_or(DataType::Unknown);
        Some(DataType::Array {
            element_type: Box::new(element_type),
            dimension: None,
        })
    }

    /// Coerce multiple argument types to a common type
    fn coerce_arg_types(&mut self, args: &[Expression]) -> Option<DataType> {
        self.coerce_expression_types(args.iter())
    }

    /// Coerce expression types from an iterator to a common type.
    fn coerce_expression_types<'b>(
        &mut self,
        args: impl IntoIterator<Item = &'b Expression>,
    ) -> Option<DataType> {
        let mut result_type: Option<DataType> = None;
        for arg in args {
            if let Some(arg_type) = self.annotate(arg) {
                result_type = match result_type {
                    Some(t) => self.coerce_types(&t, &arg_type),
                    None => Some(arg_type),
                };
            }
        }
        result_type
    }

    /// Coerce two types to a common type
    fn coerce_types(&self, left: &DataType, right: &DataType) -> Option<DataType> {
        // If types are the same, return that type
        if left == right {
            return Some(left.clone());
        }

        // Arrays coerce recursively based on their element types.
        if let (
            DataType::Array {
                element_type: left_element,
                dimension: left_dimension,
            },
            DataType::Array {
                element_type: right_element,
                dimension: right_dimension,
            },
        ) = (left, right)
        {
            return self
                .coerce_types(left_element, right_element)
                .map(|element_type| DataType::Array {
                    element_type: Box::new(element_type),
                    dimension: if left_dimension == right_dimension {
                        *left_dimension
                    } else {
                        None
                    },
                });
        }

        // Special case: Interval + Date/Timestamp
        match (left, right) {
            (DataType::Date, DataType::Interval { .. })
            | (DataType::Interval { .. }, DataType::Date) => return Some(DataType::Date),
            (
                DataType::Timestamp {
                    precision,
                    timezone,
                },
                DataType::Interval { .. },
            )
            | (
                DataType::Interval { .. },
                DataType::Timestamp {
                    precision,
                    timezone,
                },
            ) => {
                return Some(DataType::Timestamp {
                    precision: *precision,
                    timezone: *timezone,
                });
            }
            _ => {}
        }

        // Coerce based on class
        let left_class = TypeCoercionClass::from_data_type(left);
        let right_class = TypeCoercionClass::from_data_type(right);

        match (left_class, right_class) {
            // Same class: use higher-precision type within class
            (Some(lc), Some(rc)) if lc == rc => {
                // For numeric, choose wider type
                if lc == TypeCoercionClass::Numeric {
                    Some(self.wider_numeric_type(left, right))
                } else {
                    // For text and timelike, left wins by default
                    Some(left.clone())
                }
            }
            // Different classes: higher-priority class wins
            (Some(lc), Some(rc)) => {
                if lc > rc {
                    Some(left.clone())
                } else {
                    Some(right.clone())
                }
            }
            // One unknown: use the known type
            (Some(_), None) => Some(left.clone()),
            (None, Some(_)) => Some(right.clone()),
            // Both unknown: return unknown
            (None, None) => Some(DataType::Unknown),
        }
    }

    /// Get the wider numeric type
    fn wider_numeric_type(&self, left: &DataType, right: &DataType) -> DataType {
        if self._dialect == Some(DialectType::DuckDB) {
            if let Some(result) = duckdb_unsigned_integer_coercion(left, right, false) {
                return result;
            }
        }
        let order = |dt: &DataType| -> u8 {
            match dt {
                DataType::Boolean => 0,
                DataType::TinyInt { .. } | DataType::UInt8 => 1,
                DataType::SmallInt { .. } | DataType::UInt16 => 2,
                DataType::Int { .. } | DataType::UInt32 => 3,
                DataType::BigInt { .. } | DataType::UInt64 => 4,
                DataType::Int128 | DataType::UInt128 => 5,
                DataType::Float { .. } => 6,
                DataType::Double { .. } => 7,
                DataType::Decimal { .. } => 8,
                _ => 0,
            }
        };

        if order(left) >= order(right) {
            left.clone()
        } else {
            right.clone()
        }
    }
}

fn double_type() -> DataType {
    DataType::Double {
        precision: None,
        scale: None,
    }
}

fn is_unsigned_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::UInt128
    )
}

/// Integer width and signedness, used only for DuckDB's unsigned coercion rules.
fn integer_shape(data_type: &DataType) -> Option<(u16, bool)> {
    Some(match data_type {
        DataType::Boolean => (0, false),
        DataType::TinyInt { .. } => (8, false),
        DataType::SmallInt { .. } => (16, false),
        DataType::Int { .. } => (32, false),
        DataType::BigInt { .. } => (64, false),
        DataType::Int128 => (128, false),
        DataType::UInt8 => (8, true),
        DataType::UInt16 => (16, true),
        DataType::UInt32 => (32, true),
        DataType::UInt64 => (64, true),
        DataType::UInt128 => (128, true),
        _ => return None,
    })
}

fn signed_integer_type(bits: u16) -> DataType {
    match bits {
        0..=8 => DataType::TinyInt { length: None },
        9..=16 => DataType::SmallInt { length: None },
        17..=32 => DataType::Int {
            length: None,
            integer_spelling: false,
        },
        33..=64 => DataType::BigInt { length: None },
        65..=128 => DataType::Int128,
        _ => double_type(),
    }
}

/// DuckDB uses function-overload binding for arithmetic, but combination casting
/// for CASE/COALESCE/arrays. In particular, small mixed arithmetic binds BIGINT.
pub(super) fn duckdb_unsigned_integer_coercion(
    left: &DataType,
    right: &DataType,
    arithmetic: bool,
) -> Option<DataType> {
    let (left_bits, left_unsigned) = integer_shape(left)?;
    let (right_bits, right_unsigned) = integer_shape(right)?;
    if !left_unsigned && !right_unsigned {
        return None;
    }
    if left_unsigned && right_unsigned {
        return Some(if left_bits >= right_bits { left } else { right }.clone());
    }
    let (signed, unsigned, signed_type, unsigned_type) = if left_unsigned {
        (right_bits, left_bits, right, left)
    } else {
        (left_bits, right_bits, left, right)
    };
    if signed == 0 && !arithmetic {
        return Some(unsigned_type.clone());
    }
    if signed > unsigned {
        return Some(signed_type.clone());
    }
    if unsigned == 128 {
        // DuckDB 1.5's combination caster advances the signed input one width;
        // arithmetic instead binds DOUBLE. Match the engine's reported type,
        // rather than claiming this is a lossless conversion for every value.
        return Some(if arithmetic {
            double_type()
        } else {
            signed_integer_type(signed + 1)
        });
    }
    let required_bits = unsigned + 1;
    Some(signed_integer_type(if arithmetic {
        required_bits.max(64)
    } else {
        required_bits
    }))
}

/// A schema layer whose entries are visible only while annotating one query
/// scope. Catalogue/CTE definitions, selected relations, and correlated outer
/// bindings are kept separate so merely declaring a CTE or supplying a table
/// schema does not make its columns visible to a query.
struct ScopedSchema<'a> {
    parent: Option<&'a dyn Schema>,
    tables: HashMap<String, HashMap<String, DataType>>,
    dialect: Option<DialectType>,
    /// Catalogue/CTE layers may delegate lookups; a SELECT's binding layer
    /// must only resolve columns from its selected relations.
    lookup_parent_columns: bool,
    /// Selected bindings of a genuinely enclosing correlated query, separate
    /// from the catalogue and available CTE definitions in `parent`.
    outer: Option<&'a dyn Schema>,
}

impl<'a> ScopedSchema<'a> {
    fn new(parent: Option<&'a dyn Schema>, dialect: Option<DialectType>) -> Self {
        Self {
            parent,
            tables: HashMap::new(),
            dialect,
            lookup_parent_columns: true,
            outer: None,
        }
    }

    fn normalized(&self, name: &str, is_table: bool) -> String {
        normalize_name(name, self.dialect, is_table, true)
    }

    fn local_column_type(&self, table: &str, column: &str) -> Option<DataType> {
        let table = self.normalized(table, true);
        let column = self.normalized(column, false);
        self.tables
            .get(&table)
            .and_then(|columns| columns.get(&column))
            .cloned()
    }

    fn local_tables_for_column(&self, column: &str) -> Vec<String> {
        let column = self.normalized(column, false);
        self.tables
            .iter()
            .filter_map(|(table, columns)| columns.contains_key(&column).then(|| table.clone()))
            .collect()
    }
}

impl Schema for ScopedSchema<'_> {
    fn dialect(&self) -> Option<DialectType> {
        self.dialect
            .or_else(|| self.parent.and_then(Schema::dialect))
    }

    fn add_table(
        &mut self,
        table: &str,
        columns: &[(String, DataType)],
        _dialect: Option<DialectType>,
    ) -> SchemaResult<()> {
        let table = self.normalized(table, true);
        let columns = columns
            .iter()
            .filter(|(name, _)| !name.is_empty())
            .map(|(name, data_type)| (self.normalized(name, false), data_type.clone()))
            .collect();
        self.tables.insert(table, columns);
        Ok(())
    }

    fn column_names(&self, table: &str) -> SchemaResult<Vec<String>> {
        let normalized_table = self.normalized(table, true);
        if let Some(columns) = self.tables.get(&normalized_table) {
            return Ok(columns
                .keys()
                .map(|name| {
                    if self.normalized(name, false) == *name {
                        name.clone()
                    } else {
                        crate::binding::identifier_name(&crate::expressions::Identifier::quoted(
                            name,
                        ))
                    }
                })
                .collect());
        }
        self.parent
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?
            .column_names(table)
    }

    fn get_column_type(&self, table: &str, column: &str) -> SchemaResult<DataType> {
        if table.is_empty() {
            let local_tables = self.local_tables_for_column(column);
            return match local_tables.as_slice() {
                [local_table] => {
                    Ok(self.tables[local_table][&self.normalized(column, false)].clone())
                }
                [] if self.lookup_parent_columns => self
                    .parent
                    .ok_or_else(|| SchemaError::ColumnNotFound {
                        table: String::new(),
                        column: column.to_string(),
                    })?
                    .get_column_type(table, column),
                [] => self
                    .outer
                    .ok_or_else(|| SchemaError::ColumnNotFound {
                        table: String::new(),
                        column: column.to_string(),
                    })?
                    .get_column_type(table, column),
                _ => Err(SchemaError::AmbiguousTable {
                    table: String::new(),
                    matches: local_tables.join(", "),
                }),
            };
        }

        let normalized_table = self.normalized(table, true);
        if self.tables.contains_key(&normalized_table) {
            return self.local_column_type(table, column).ok_or_else(|| {
                SchemaError::ColumnNotFound {
                    table: table.to_string(),
                    column: column.to_string(),
                }
            });
        }

        if !self.lookup_parent_columns {
            return self
                .outer
                .ok_or_else(|| SchemaError::ColumnNotFound {
                    table: table.to_string(),
                    column: column.to_string(),
                })?
                .get_column_type(table, column);
        }
        self.parent
            .ok_or_else(|| SchemaError::ColumnNotFound {
                table: table.to_string(),
                column: column.to_string(),
            })?
            .get_column_type(table, column)
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.get_column_type(table, column).is_ok()
    }

    fn supported_table_args(&self) -> &[&str] {
        TABLE_PARTS
    }

    fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.parent.is_none_or(Schema::is_empty)
    }

    fn depth(&self) -> usize {
        self.parent.map_or(1, |schema| schema.depth().max(1))
    }

    fn find_tables_for_column(&self, column: &str) -> Vec<String> {
        let mut tables = self.local_tables_for_column(column);
        if let Some(parent) = self.parent {
            tables.extend(parent.find_tables_for_column(column));
        }
        let mut seen = HashSet::new();
        tables.retain(|table| seen.insert(table.clone()));
        tables
    }
}

type OutputColumns = Vec<(String, DataType)>;

fn table_name(table: &crate::expressions::TableRef) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = &table.catalog {
        parts.push(crate::binding::identifier_name(catalog));
    }
    if let Some(schema) = &table.schema {
        parts.push(crate::binding::identifier_name(schema));
    }
    parts.push(crate::binding::identifier_name(&table.name));
    parts.join(".")
}

fn table_columns(schema: &dyn Schema, table: &str) -> OutputColumns {
    schema
        .column_names(table)
        .unwrap_or_default()
        .into_iter()
        .map(|column| {
            let data_type = schema
                .get_column_type(table, &column)
                .unwrap_or(DataType::Unknown);
            (column, data_type)
        })
        .collect()
}

fn apply_column_aliases(
    mut columns: OutputColumns,
    aliases: &[crate::expressions::Identifier],
) -> OutputColumns {
    for ((name, _), alias) in columns.iter_mut().zip(aliases) {
        *name = crate::binding::identifier_name(alias);
    }
    columns
}

fn projection_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Alias(alias) => Some(crate::binding::identifier_name(&alias.alias)),
        Expression::Column(column) => Some(crate::binding::identifier_name(&column.name)),
        Expression::Identifier(identifier) => Some(crate::binding::identifier_name(identifier)),
        _ => None,
    }
}

fn projection_type(expression: &Expression) -> DataType {
    if let Expression::Literal(literal) = expression {
        return TypeAnnotator::annotate_literal(literal).unwrap_or(DataType::Unknown);
    }
    if matches!(expression, Expression::Boolean(_)) {
        return DataType::Boolean;
    }
    expression
        .inferred_type()
        .or_else(|| match expression {
            Expression::Alias(alias) => alias.this.inferred_type(),
            _ => None,
        })
        .cloned()
        .unwrap_or(DataType::Unknown)
}

fn query_outputs(expressions: &[Expression], dialect: Option<DialectType>) -> OutputColumns {
    expressions
        .iter()
        .map(|expression| {
            let mut inner = expression;
            loop {
                inner = match inner {
                    Expression::Alias(alias) => &alias.this,
                    Expression::Paren(paren) => &paren.this,
                    Expression::Annotated(annotated) => &annotated.this,
                    _ => break,
                };
            }
            let unknown_function = matches!(inner, Expression::Function(func)
                if !TypeAnnotator::function_result_is_known(func, dialect.unwrap_or_default()));
            // Keep unnamed slots until CTE/derived-table column aliases have
            // been applied by ordinal. They are not registered as named columns.
            (
                projection_name(expression).unwrap_or_default(),
                if unknown_function {
                    DataType::Unknown
                } else {
                    projection_type(expression)
                },
            )
        })
        .collect()
}

fn array_element_type(data_type: Option<&DataType>) -> DataType {
    match data_type {
        Some(DataType::Array { element_type, .. }) => (**element_type).clone(),
        _ => DataType::Unknown,
    }
}

fn unnest_output_types(unnest: &crate::expressions::UnnestFunc) -> Vec<DataType> {
    let mut types = vec![array_element_type(unnest.this.inferred_type())];
    types.extend(
        unnest
            .expressions
            .iter()
            .map(|expression| array_element_type(expression.inferred_type())),
    );
    if unnest.with_ordinality || unnest.offset_alias.is_some() {
        types.push(DataType::BigInt { length: None });
    }
    types
}

fn virtual_output_columns(
    expression: &Expression,
    source_alias: &str,
    column_aliases: &[crate::expressions::Identifier],
) -> OutputColumns {
    let (types, offset_alias) = match expression {
        Expression::Unnest(unnest) => (unnest_output_types(unnest), unnest.offset_alias.as_ref()),
        Expression::Explode(explode) | Expression::ExplodeOuter(explode) => {
            (vec![array_element_type(explode.this.inferred_type())], None)
        }
        Expression::Function(function) if function.name.eq_ignore_ascii_case("UNNEST") => (
            function
                .args
                .iter()
                .map(|argument| array_element_type(argument.inferred_type()))
                .collect(),
            None,
        ),
        _ => return Vec::new(),
    };

    if column_aliases.is_empty() {
        let mut columns = Vec::new();
        if let Some(data_type) = types.first() {
            columns.push((source_alias.to_string(), data_type.clone()));
        }
        if let Some(offset_alias) = offset_alias {
            columns.push((offset_alias.name.clone(), DataType::BigInt { length: None }));
        }
        columns
    } else {
        column_aliases
            .iter()
            .zip(types)
            .map(|(alias, data_type)| (alias.name.clone(), data_type))
            .collect()
    }
}

fn annotate_derived_query(
    subquery: &mut crate::expressions::Subquery,
    schema: &ScopedSchema<'_>,
    dialect: Option<DialectType>,
) -> OutputColumns {
    // LATERAL sees preceding selected sources, not every table/CTE available
    // in the catalogue used to bind its own FROM clause.
    let mut selected = ScopedSchema::new(None, dialect);
    selected.tables = schema.tables.clone();
    selected.lookup_parent_columns = false;
    selected.outer = schema.outer;
    let outer = if subquery.lateral {
        Some(&selected as &dyn Schema)
    } else {
        schema.outer
    };
    annotate_scoped_expression_with_outer(&mut subquery.this, schema.parent, dialect, outer)
}

fn annotate_relation_source(
    expression: &mut Expression,
    schema: &mut ScopedSchema<'_>,
    dialect: Option<DialectType>,
) {
    match expression {
        Expression::Table(table) => {
            let source_table = table_name(table);
            let mut columns = table_columns(schema, &source_table);
            columns = apply_column_aliases(columns, &table.column_aliases);
            let visible_name = table.alias.as_ref().unwrap_or(&table.name);
            let _ = schema.add_table(
                &crate::binding::identifier_name(visible_name),
                &columns,
                dialect,
            );
        }
        Expression::Subquery(subquery) => {
            let mut columns = annotate_derived_query(subquery, schema, dialect);
            columns = apply_column_aliases(columns, &subquery.column_aliases);
            if let Some((_, first_type)) = columns.first() {
                subquery.inferred_type = Some(first_type.clone());
            }
            if let Some(alias) = &subquery.alias {
                let _ =
                    schema.add_table(&crate::binding::identifier_name(alias), &columns, dialect);
            }
        }
        Expression::Alias(alias) => {
            match &mut alias.this {
                Expression::Subquery(subquery) => {
                    let columns = annotate_derived_query(subquery, schema, dialect);
                    let columns = apply_column_aliases(columns, &alias.column_aliases);
                    let _ = schema.add_table(
                        &crate::binding::identifier_name(&alias.alias),
                        &columns,
                        dialect,
                    );
                    return;
                }
                _ => {
                    let mut annotator = TypeAnnotator::new(Some(schema), dialect);
                    annotator.annotate_in_place(&mut alias.this);
                }
            }
            let columns =
                virtual_output_columns(&alias.this, &alias.alias.name, &alias.column_aliases);
            if !columns.is_empty() {
                let _ = schema.add_table(
                    &crate::binding::identifier_name(&alias.alias),
                    &columns,
                    dialect,
                );
            }
        }
        Expression::Unnest(_) => {
            let mut annotator = TypeAnnotator::new(Some(schema), dialect);
            annotator.annotate_in_place(expression);
            if let Expression::Unnest(unnest) = expression {
                if let Some(alias) = &unnest.alias {
                    let alias_name = alias.name.clone();
                    let columns = unnest_output_types(unnest)
                        .into_iter()
                        .next()
                        .map(|data_type| vec![(alias_name.clone(), data_type)])
                        .unwrap_or_default();
                    let _ = schema.add_table(&alias_name, &columns, dialect);
                }
            }
        }
        Expression::Lateral(lateral) => {
            let mut annotator = TypeAnnotator::new(Some(schema), dialect);
            annotator.annotate_in_place(&mut lateral.this);
            if let Some(alias) = &lateral.alias {
                let aliases: Vec<_> = lateral
                    .column_aliases
                    .iter()
                    .map(crate::expressions::Identifier::new)
                    .collect();
                let columns = virtual_output_columns(&lateral.this, alias, &aliases);
                if !columns.is_empty() {
                    let _ = schema.add_table(alias, &columns, dialect);
                }
            }
        }
        Expression::Paren(paren) => annotate_relation_source(&mut paren.this, schema, dialect),
        _ => {
            let mut annotator = TypeAnnotator::new(Some(schema), dialect);
            annotator.annotate_in_place(expression);
        }
    }
}

fn annotate_with(
    with: &mut Option<crate::expressions::With>,
    schema: &mut ScopedSchema<'_>,
    dialect: Option<DialectType>,
    outer: Option<&dyn Schema>,
) {
    if let Some(with) = with {
        for cte in &mut with.ctes {
            let name = crate::binding::identifier_name(&cte.alias);
            // Bind the anchor before visiting a recursive arm. Only stable
            // output types are retained; widening through recursion is not a
            // regular UNION and needs engine-specific recursive-CTE validation.
            let recursive_anchor = if let Expression::Union(union) = &mut cte.this {
                let references_self = union.right.contains(|node| matches!(node,
                    Expression::Table(table) if table.schema.is_none() && table.catalog.is_none()
                        && normalize_name(&crate::binding::identifier_name(&table.name), dialect, false, true)
                            == normalize_name(&name, dialect, false, true)));
                if references_self {
                    let columns = annotate_scoped_expression_with_outer(
                        &mut union.left,
                        Some(schema),
                        dialect,
                        outer,
                    );
                    let columns = apply_column_aliases(columns, &cte.columns);
                    let _ = schema.add_table(&name, &columns, dialect);
                    Some(columns)
                } else {
                    None
                }
            } else {
                None
            };
            let mut columns =
                annotate_scoped_expression_with_outer(&mut cte.this, Some(schema), dialect, outer);
            if let Some(anchor) = recursive_anchor {
                for ((_, result), (_, initial)) in columns.iter_mut().zip(anchor) {
                    if *result != initial {
                        *result = DataType::Unknown;
                    }
                }
            }
            let columns = apply_column_aliases(columns, &cte.columns);
            let _ = schema.add_table(
                &crate::binding::identifier_name(&cte.alias),
                &columns,
                dialect,
            );
        }
    }
}

fn annotate_select(
    select: &mut crate::expressions::Select,
    parent: Option<&dyn Schema>,
    dialect: Option<DialectType>,
    outer: Option<&dyn Schema>,
) -> OutputColumns {
    let mut ctes = ScopedSchema::new(parent, dialect);
    annotate_with(&mut select.with, &mut ctes, dialect, outer);
    let mut schema = ScopedSchema::new(Some(&ctes), dialect);
    schema.outer = outer;

    if let Some(from) = &mut select.from {
        for source in &mut from.expressions {
            annotate_relation_source(source, &mut schema, dialect);
        }
    }
    for join in &mut select.joins {
        annotate_relation_source(&mut join.this, &mut schema, dialect);
    }

    schema.lookup_parent_columns = false;
    let mut annotator = TypeAnnotator::new(Some(&schema), dialect);
    annotator.query_schema = Some(&ctes);
    for expression in &mut select.expressions {
        annotator.annotate_in_place(expression);
    }
    // Clause expressions use selected inputs, not the whole catalogue.
    if let Some(expression) = &mut select.prewhere {
        annotator.annotate_in_place(expression);
    }
    if let Some(clause) = &mut select.qualify {
        annotator.annotate_in_place(&mut clause.this);
    }
    if let Some(clause) = &mut select.where_clause {
        annotator.annotate_in_place(&mut clause.this);
    }
    if let Some(clause) = &mut select.having {
        annotator.annotate_in_place(&mut clause.this);
    }
    if let Some(group) = &mut select.group_by {
        for expression in &mut group.expressions {
            annotator.annotate_in_place(expression);
        }
    }
    if let Some(order) = &mut select.order_by {
        for expression in &mut order.expressions {
            annotator.annotate_in_place(&mut expression.this);
        }
    }
    for join in &mut select.joins {
        if let Some(on) = &mut join.on {
            annotator.annotate_in_place(on);
        }
        if let Some(condition) = &mut join.match_condition {
            annotator.annotate_in_place(condition);
        }
    }
    query_outputs(&select.expressions, dialect)
}

fn annotate_scoped_expression(
    expression: &mut Expression,
    parent: Option<&dyn Schema>,
    dialect: Option<DialectType>,
) -> OutputColumns {
    annotate_scoped_expression_with_outer(expression, parent, dialect, None)
}

fn annotate_scoped_expression_with_outer(
    expression: &mut Expression,
    parent: Option<&dyn Schema>,
    dialect: Option<DialectType>,
    outer: Option<&dyn Schema>,
) -> OutputColumns {
    match expression {
        Expression::Select(select) => annotate_select(select, parent, dialect, outer),
        Expression::Subquery(subquery) => {
            let columns =
                annotate_scoped_expression_with_outer(&mut subquery.this, parent, dialect, outer);
            if let Some((_, first_type)) = columns.first() {
                subquery.inferred_type = Some(first_type.clone());
            }
            columns
        }
        Expression::Cte(cte) => {
            annotate_scoped_expression_with_outer(&mut cte.this, parent, dialect, outer)
        }
        Expression::Paren(paren) => {
            annotate_scoped_expression_with_outer(&mut paren.this, parent, dialect, outer)
        }
        Expression::Union(union) => {
            let mut schema = ScopedSchema::new(parent, dialect);
            annotate_with(&mut union.with, &mut schema, dialect, outer);
            annotate_scoped_expression_with_outer(&mut union.left, Some(&schema), dialect, outer);
            annotate_scoped_expression_with_outer(&mut union.right, Some(&schema), dialect, outer);
            super::set_operation_types::query_columns(expression, dialect)
        }
        Expression::Intersect(intersect) => {
            let mut schema = ScopedSchema::new(parent, dialect);
            annotate_with(&mut intersect.with, &mut schema, dialect, outer);
            annotate_scoped_expression_with_outer(
                &mut intersect.left,
                Some(&schema),
                dialect,
                outer,
            );
            annotate_scoped_expression_with_outer(
                &mut intersect.right,
                Some(&schema),
                dialect,
                outer,
            );
            super::set_operation_types::query_columns(expression, dialect)
        }
        Expression::Except(except) => {
            let mut schema = ScopedSchema::new(parent, dialect);
            annotate_with(&mut except.with, &mut schema, dialect, outer);
            annotate_scoped_expression_with_outer(&mut except.left, Some(&schema), dialect, outer);
            annotate_scoped_expression_with_outer(&mut except.right, Some(&schema), dialect, outer);
            super::set_operation_types::query_columns(expression, dialect)
        }
        _ => {
            if let Some(mut selected) = crate::binding::dml_scope(expression) {
                let mut ctes = ScopedSchema::new(parent, dialect);
                annotate_with(&mut selected.with, &mut ctes, dialect, outer);
                let mut inputs = ScopedSchema::new(Some(&ctes), dialect);
                inputs.outer = outer;
                if let Some(from) = &mut selected.from {
                    for source in &mut from.expressions {
                        annotate_relation_source(source, &mut inputs, dialect);
                    }
                }
                for join in &mut selected.joins {
                    annotate_relation_source(&mut join.this, &mut inputs, dialect);
                }
                inputs.lookup_parent_columns = false;
                let mut annotator = TypeAnnotator::new(Some(&inputs), dialect);
                annotator.query_schema = Some(&ctes);
                crate::ast_children::for_each_child_mut(expression, |child| {
                    annotator.annotate_in_place(child)
                });
                return Vec::new();
            }
            let mut annotator = TypeAnnotator::new(parent, dialect);
            annotator.annotate_in_place(expression);
            Vec::new()
        }
    }
}

/// Annotate types in-place on the expression tree.
///
/// Walks the AST bottom-up and sets `inferred_type` on each value-producing
/// node. After this call, `expr.inferred_type()` (and the same on any child
/// node) returns the inferred type.
pub fn annotate_types(
    expr: &mut Expression,
    schema: Option<&dyn Schema>,
    dialect: Option<DialectType>,
) {
    annotate_scoped_expression(expr, schema, dialect);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BooleanLiteral, Cast, ExtractFunc, Null};
    use crate::{parse_one, DialectType, MappingSchema, Schema};

    #[test]
    fn set_operation_types_cover_all_dialects_454() {
        use DialectType::*;
        // Exercise real query annotation, including the external-source binding,
        // rather than just mirroring the policy dispatch.
        for dialect in [
            Generic,
            PostgreSQL,
            MySQL,
            BigQuery,
            Snowflake,
            DuckDB,
            SQLite,
            Hive,
            Spark,
            Trino,
            Presto,
            Redshift,
            TSQL,
            Oracle,
            ClickHouse,
            Databricks,
            Athena,
            Teradata,
            Doris,
            StarRocks,
            Materialize,
            RisingWave,
            SingleStore,
            CockroachDB,
            TiDB,
            Druid,
            Solr,
            Tableau,
            Dune,
            Fabric,
            Drill,
            Dremio,
            Exasol,
            DataFusion,
        ] {
            for (left, right) in [("INT", "BIGINT"), ("BIGINT", "INT")] {
                let sql = format!("SELECT t.a FROM (SELECT CAST(1 AS {left}) AS a UNION ALL SELECT CAST(2 AS {right}) AS a) AS t");
                let mut expression = parse_one(&sql, dialect).unwrap();
                annotate_types(&mut expression, None, Some(dialect));
                let Expression::Select(select) = expression else {
                    panic!("select")
                };
                let actual = select.expressions[0].inferred_type();
                match dialect {
                    BigQuery => assert!(
                        matches!(actual, Some(DataType::Custom { name }) if name == "INT64"),
                        "{dialect:?}: {actual:?}"
                    ),
                    SQLite => assert!(
                        actual.is_none_or(|t| *t == DataType::Unknown),
                        "{dialect:?}: {actual:?}"
                    ),
                    Teradata if left == "INT" => assert!(
                        matches!(actual, Some(DataType::Int { .. })),
                        "{dialect:?}: {actual:?}"
                    ),
                    Oracle => assert!(
                        matches!(
                            actual,
                            Some(DataType::Oracle {
                                oracle_type: crate::expressions::OracleDataType::Number { .. }
                            })
                        ),
                        "{dialect:?}: {actual:?}"
                    ),
                    _ => assert!(
                        matches!(actual, Some(DataType::BigInt { .. })),
                        "{dialect:?}: {actual:?}"
                    ),
                }
            }
            let mut expression = parse_one(
                "SELECT t.a FROM (SELECT CAST(1 AS DECIMAL(10,2)) AS a UNION ALL SELECT missing_function() AS a) AS t", dialect
            ).unwrap();
            annotate_types(&mut expression, None, Some(dialect));
            let Expression::Select(select) = expression else {
                panic!("select")
            };
            assert!(
                select.expressions[0]
                    .inferred_type()
                    .is_none_or(|t| *t == DataType::Unknown),
                "{dialect:?}: {:?}",
                select.expressions[0].inferred_type()
            );
        }
    }

    #[test]
    fn set_operation_coercion_parameters_and_isolation_454() {
        use super::super::set_operation_types::common_type;
        let mut literal_query = parse_one(
            "SELECT t.a FROM (SELECT 1.5 AS a UNION ALL SELECT 2.5 AS a) AS t",
            DialectType::PostgreSQL,
        )
        .unwrap();
        annotate_types(&mut literal_query, None, Some(DialectType::PostgreSQL));
        let Expression::Select(select) = literal_query else {
            panic!("select")
        };
        assert!(matches!(
            select.expressions[0].inferred_type(),
            Some(DataType::Decimal {
                precision: None,
                scale: None
            })
        ));
        let dec = |p, s| DataType::Decimal {
            precision: Some(p),
            scale: Some(s),
        };
        let varchar = |n| DataType::VarChar {
            length: Some(n),
            parenthesized_length: false,
        };
        let array = |t| DataType::Array {
            element_type: Box::new(t),
            dimension: None,
        };
        let int = DataType::Int {
            length: None,
            integer_spelling: false,
        };
        let double = double_type();
        for dialect in [
            DialectType::TSQL,
            DialectType::DuckDB,
            DialectType::DataFusion,
            DialectType::Trino,
        ] {
            assert_eq!(
                common_type(&dec(10, 2), &dec(8, 4), dialect, 0),
                Some(dec(12, 4))
            );
            assert_eq!(
                common_type(&dec(8, 4), &dec(10, 2), dialect, 0),
                Some(dec(12, 4))
            );
            assert_eq!(
                common_type(&dec(10, 2), &DataType::Unknown, dialect, 0),
                None
            );
            assert_eq!(
                common_type(&array(int.clone()), &array(DataType::Unknown), dialect, 0),
                None
            );
        }
        assert_eq!(
            common_type(&varchar(10), &varchar(30), DialectType::TSQL, 0),
            Some(varchar(30))
        );
        assert_eq!(
            common_type(
                &array(int.clone()),
                &array(double.clone()),
                DialectType::DuckDB,
                0
            ),
            Some(array(double))
        );
        assert_eq!(
            common_type(
                &DataType::BigInt { length: None },
                &DataType::UInt64,
                DialectType::DuckDB,
                0
            ),
            Some(DataType::Int128)
        );
        assert_eq!(
            common_type(&dec(38, 20), &dec(38, 0), DialectType::TSQL, 0),
            Some(dec(38, 0))
        );
        assert_eq!(
            common_type(&dec(38, 20), &dec(38, 0), DialectType::Trino, 0),
            None
        );
        assert_eq!(
            common_type(&dec(10, 2), &double_type(), DialectType::DataFusion, 0),
            Some(dec(30, 15))
        );
        assert_eq!(
            common_type(
                &DataType::UInt64,
                &DataType::BigInt { length: None },
                DialectType::ClickHouse,
                0
            ),
            None
        );
        assert_eq!(
            common_type(
                &DataType::UInt64,
                &DataType::BigInt { length: None },
                DialectType::DataFusion,
                0
            ),
            Some(dec(20, 0))
        );
        let structure = |fields: Vec<(&str, DataType)>| DataType::Struct {
            fields: fields
                .into_iter()
                .map(|(name, t)| StructField::new(name.to_owned(), t))
                .collect(),
            nested: false,
        };
        let left = structure(vec![("x", int.clone()), ("y", varchar(10))]);
        let right = structure(vec![("y", varchar(30)), ("x", double_type())]);
        let combined = structure(vec![("x", double_type()), ("y", varchar(30))]);
        assert_eq!(
            common_type(&left, &right, DialectType::DataFusion, 0),
            Some(combined)
        );
        let right = structure(vec![("z", DataType::Boolean), ("x", double_type())]);
        assert_eq!(
            common_type(
                &structure(vec![("x", int.clone())]),
                &right,
                DialectType::DuckDB,
                0
            ),
            Some(structure(vec![
                ("x", double_type()),
                ("z", DataType::Boolean)
            ]))
        );
        assert_eq!(
            common_type(
                &array(int.clone()),
                &array(double_type()),
                DialectType::BigQuery,
                0
            ),
            None
        );
        let map = |value_type| DataType::Map {
            key_type: Box::new(DataType::Text),
            value_type: Box::new(value_type),
        };
        assert_eq!(
            common_type(
                &map(int.clone()),
                &map(double_type()),
                DialectType::DuckDB,
                0
            ),
            Some(map(double_type()))
        );
        let timestamp = |p| DataType::Timestamp {
            precision: Some(p),
            timezone: false,
        };
        assert_eq!(
            common_type(&timestamp(3), &timestamp(6), DialectType::TSQL, 0),
            Some(timestamp(6))
        );
        assert_eq!(
            common_type(&DataType::Date, &timestamp(6), DialectType::PostgreSQL, 0),
            Some(timestamp(6))
        );
        for dialect in [
            DialectType::BigQuery,
            DialectType::Hive,
            DialectType::Databricks,
            DialectType::Generic,
            DialectType::BigQuery,
        ] {
            assert_eq!(common_type(&varchar(10), &DataType::Date, dialect, 0), None);
        }
    }

    fn make_int_literal(val: i64) -> Expression {
        Expression::Literal(Box::new(Literal::Number(val.to_string())))
    }

    fn make_float_literal(val: f64) -> Expression {
        Expression::Literal(Box::new(Literal::Number(val.to_string())))
    }

    fn make_string_literal(val: &str) -> Expression {
        Expression::Literal(Box::new(Literal::String(val.to_string())))
    }

    fn make_bool_literal(val: bool) -> Expression {
        Expression::Boolean(BooleanLiteral { value: val })
    }

    #[test]
    fn test_annotate_cte_bindings_exclude_unselected_definitions() {
        for sql in [
            "WITH a AS (SELECT CAST('1' AS INT) AS n), b AS (SELECT n FROM a) SELECT n FROM b",
            "WITH unused AS (SELECT CAST('x' AS TEXT) AS n), a AS (SELECT CAST('1' AS INT) AS n) SELECT n FROM a",
            "WITH a(n) AS (SELECT CAST('1' AS INT)), b(m) AS (SELECT n FROM a) SELECT m FROM b",
        ] {
            let mut expression = parse_one(sql, DialectType::DuckDB).unwrap();
            annotate_types(&mut expression, None, Some(DialectType::DuckDB));
            let Expression::Select(select) = expression else { panic!("select") };
            assert!(matches!(select.expressions[0].inferred_type(), Some(DataType::Int { .. })), "{sql}");
        }
    }

    #[test]
    fn test_annotate_does_not_resolve_unselected_or_ambiguous_columns() {
        let mut schema = MappingSchema::new();
        schema
            .add_table("unrelated", &[("n".into(), DataType::Text)], None)
            .unwrap();
        for sql in [
            "SELECT n FROM missing",
            "SELECT unrelated.n FROM missing",
            "WITH a AS (SELECT 1 AS n), b AS (SELECT 2 AS n) SELECT n FROM a CROSS JOIN b",
            "WITH unused AS (SELECT 1 AS n) SELECT n FROM missing",
        ] {
            let mut expression = parse_one(sql, DialectType::DuckDB).unwrap();
            annotate_types(&mut expression, Some(&schema), Some(DialectType::DuckDB));
            let Expression::Select(select) = expression else {
                panic!("select")
            };
            assert!(select.expressions[0].inferred_type().is_none(), "{sql}");
        }
    }

    #[test]
    fn test_literal_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // Integer literal
        let int_expr = make_int_literal(42);
        assert_eq!(
            annotator.annotate(&int_expr),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );

        // Float literal
        let float_expr = make_float_literal(3.14);
        assert_eq!(
            annotator.annotate(&float_expr),
            Some(DataType::Double {
                precision: None,
                scale: None
            })
        );

        // String literal
        let string_expr = make_string_literal("hello");
        assert_eq!(
            annotator.annotate(&string_expr),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false
            })
        );

        // Boolean literal
        let bool_expr = make_bool_literal(true);
        assert_eq!(annotator.annotate(&bool_expr), Some(DataType::Boolean));

        // Null literal
        let null_expr = Expression::Null(Null);
        assert_eq!(annotator.annotate(&null_expr), None);
    }

    #[test]
    fn test_comparison_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // Comparison returns boolean
        let cmp = Expression::Gt(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        assert_eq!(annotator.annotate(&cmp), Some(DataType::Boolean));

        // Equality returns boolean
        let eq = Expression::Eq(Box::new(BinaryOp::new(
            make_string_literal("a"),
            make_string_literal("b"),
        )));
        assert_eq!(annotator.annotate(&eq), Some(DataType::Boolean));
    }

    #[test]
    fn test_arithmetic_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // Int + Int = Int
        let add_int = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        assert_eq!(
            annotator.annotate(&add_int),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );

        // Int + Float = Double (wider type)
        let add_mixed = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_float_literal(2.5), // Use 2.5 so the string has a decimal point
        )));
        assert_eq!(
            annotator.annotate(&add_mixed),
            Some(DataType::Double {
                precision: None,
                scale: None
            })
        );
    }

    #[test]
    fn test_string_concat_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // String || String = VarChar
        let concat = Expression::Concat(Box::new(BinaryOp::new(
            make_string_literal("hello"),
            make_string_literal(" world"),
        )));
        assert_eq!(
            annotator.annotate(&concat),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false
            })
        );
    }

    #[test]
    fn test_cast_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // CAST(1 AS VARCHAR)
        let cast = Expression::Cast(Box::new(Cast {
            this: make_int_literal(1),
            to: DataType::VarChar {
                length: Some(10),
                parenthesized_length: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&cast),
            Some(DataType::VarChar {
                length: Some(10),
                parenthesized_length: false
            })
        );
    }

    #[test]
    fn test_function_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // COUNT returns BigInt
        let count =
            Expression::Function(Box::new(Function::new("COUNT", vec![make_int_literal(1)])));
        assert_eq!(
            annotator.annotate(&count),
            Some(DataType::BigInt { length: None })
        );

        // UPPER returns VarChar
        let upper = Expression::Function(Box::new(Function::new(
            "UPPER",
            vec![make_string_literal("hello")],
        )));
        assert_eq!(
            annotator.annotate(&upper),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false
            })
        );

        // NOW returns Timestamp
        let now = Expression::Function(Box::new(Function::new("NOW", vec![])));
        assert_eq!(
            annotator.annotate(&now),
            Some(DataType::Timestamp {
                precision: None,
                timezone: false
            })
        );
    }

    #[test]
    fn test_duckdb_trim_stores_inferred_types() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "records",
                &[
                    ("name".to_string(), varchar.clone()),
                    ("chars".to_string(), varchar.clone()),
                    ("values_json".to_string(), DataType::Json),
                ],
                None,
            )
            .unwrap();

        for function in [
            "TRIM(name)",
            "trim(records.name)",
            "TRIM(name, chars)",
            "TRIM(BOTH chars FROM name)",
            "TRIM(LEADING chars FROM name)",
            "TRIM(TRAILING chars FROM name)",
            "TRIM(FROM name)",
            "TRIM('  hello  ')",
            "TRIM(NULL)",
            r#"TRIM(BOTH '"' FROM values_json->>0)"#,
        ] {
            let sql = format!("SELECT {function} AS normalized FROM records");
            for schema in [None, Some(&schema as &dyn Schema)] {
                let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
                let original_sql = expression.sql_for(DialectType::DuckDB);
                annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
                let Expression::Select(select) = &expression else {
                    panic!("Expected SELECT for {sql}");
                };
                let Expression::Alias(alias) = &select.expressions[0] else {
                    panic!("Expected alias for {sql}");
                };
                assert!(matches!(alias.this, Expression::Trim(_)), "{sql}");
                assert_eq!(alias.this.inferred_type(), Some(&varchar), "{sql}");
                assert_eq!(alias.inferred_type.as_ref(), Some(&varchar), "{sql}");
                assert_eq!(expression.sql_for(DialectType::DuckDB), original_sql);
            }
        }
    }

    #[test]
    fn test_int128_inferred_types_and_widening() {
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "measurements",
                &[("value".to_string(), DataType::Int128)],
                None,
            )
            .unwrap();
        for (projection, expected) in [
            ("CAST(value AS HUGEINT)", DataType::Int128),
            ("value", DataType::Int128),
            ("value + CAST(1 AS BIGINT)", DataType::Int128),
            ("CAST(1 AS BIGINT) + value", DataType::Int128),
            ("COALESCE(value, CAST(1 AS BIGINT))", DataType::Int128),
            ("COALESCE(CAST(1 AS BIGINT), value)", DataType::Int128),
            (
                "CASE WHEN TRUE THEN value ELSE CAST(1 AS BIGINT) END",
                DataType::Int128,
            ),
            ("SUM(value)", DataType::Int128),
            ("SUM(CAST(value AS BIGINT))", DataType::Int128),
            (
                "MEDIAN(value)",
                DataType::Double {
                    precision: None,
                    scale: None,
                },
            ),
            (
                "CAST(value AS HUGEINT[])",
                DataType::Array {
                    element_type: Box::new(DataType::Int128),
                    dimension: None,
                },
            ),
        ] {
            let sql = format!("SELECT {projection} AS widened FROM measurements");
            let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
            annotate_types(&mut expression, Some(&schema), Some(DialectType::DuckDB));
            let Expression::Select(select) = &expression else {
                panic!("expected SELECT")
            };
            let Expression::Alias(alias) = &select.expressions[0] else {
                panic!("expected alias")
            };
            assert_eq!(alias.this.inferred_type(), Some(&expected), "{sql}");
            assert_eq!(alias.inferred_type.as_ref(), Some(&expected), "{sql}");
        }
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Int128),
            Some(TypeCoercionClass::Numeric)
        );
    }

    #[test]
    fn test_duckdb_unsigned_integer_coercion_matrix() {
        // Verified against DuckDB 1.5.5: arithmetic overload binding and
        // combination casting do not select the same small integer widths.
        let signed = ["TINYINT", "SMALLINT", "INT", "BIGINT", "HUGEINT"];
        let unsigned = ["UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT"];
        let arithmetic = [
            ["BIGINT", "BIGINT", "BIGINT", "HUGEINT", "DOUBLE"],
            ["SMALLINT", "BIGINT", "BIGINT", "HUGEINT", "DOUBLE"],
            ["INT", "INT", "BIGINT", "HUGEINT", "DOUBLE"],
            ["BIGINT", "BIGINT", "BIGINT", "HUGEINT", "DOUBLE"],
            ["HUGEINT", "HUGEINT", "HUGEINT", "HUGEINT", "DOUBLE"],
        ];
        let combination = [
            ["SMALLINT", "INT", "BIGINT", "HUGEINT", "SMALLINT"],
            ["SMALLINT", "INT", "BIGINT", "HUGEINT", "INT"],
            ["INT", "INT", "BIGINT", "HUGEINT", "BIGINT"],
            ["BIGINT", "BIGINT", "BIGINT", "HUGEINT", "HUGEINT"],
            ["HUGEINT", "HUGEINT", "HUGEINT", "HUGEINT", "DOUBLE"],
        ];
        for (i, s) in signed.iter().enumerate() {
            for (j, u) in unsigned.iter().enumerate() {
                for (a, b) in [(s, u), (u, s)] {
                    let a = format!("CAST(1 AS {a})");
                    let b = format!("CAST(1 AS {b})");
                    for (sql, expected) in [
                        (format!("{a} + {b}"), arithmetic[i][j]),
                        (format!("{a} - {b}"), arithmetic[i][j]),
                        (format!("{a} * {b}"), arithmetic[i][j]),
                        (format!("{a} % {b}"), arithmetic[i][j]),
                        (format!("{a} / {b}"), "DOUBLE"),
                        (format!("COALESCE({a}, {b})"), combination[i][j]),
                        (
                            format!("CASE WHEN TRUE THEN {a} ELSE {b} END"),
                            combination[i][j],
                        ),
                    ] {
                        let expr = parse_one(&sql, DialectType::DuckDB).unwrap();
                        let expected =
                            crate::parse_data_type(expected, DialectType::DuckDB).unwrap();
                        let mut annotator = TypeAnnotator::new(None, Some(DialectType::DuckDB));
                        assert_eq!(annotator.annotate(&expr), Some(expected), "{sql}");
                    }
                }
            }
        }
        for (i, a) in unsigned.iter().enumerate() {
            for (j, b) in unsigned.iter().enumerate() {
                let sql = format!("CAST(1 AS {a}) + CAST(1 AS {b})");
                let expr = parse_one(&sql, DialectType::DuckDB).unwrap();
                let mut annotator = TypeAnnotator::new(None, Some(DialectType::DuckDB));
                assert_eq!(
                    annotator.annotate(&expr),
                    Some(crate::parse_data_type(unsigned[i.max(j)], DialectType::DuckDB).unwrap()),
                    "{sql}"
                );
            }
        }
    }

    #[test]
    fn test_duckdb_sum_return_types() {
        for (input, output) in [
            ("BOOLEAN", "INT128"),
            ("TINYINT", "INT128"),
            ("SMALLINT", "INT128"),
            ("INT", "INT128"),
            ("BIGINT", "INT128"),
            ("HUGEINT", "INT128"),
            ("UTINYINT", "INT128"),
            ("USMALLINT", "INT128"),
            ("UINTEGER", "INT128"),
            ("UBIGINT", "INT128"),
            ("UHUGEINT", "DOUBLE"),
            ("FLOAT", "DOUBLE"),
            ("DOUBLE", "DOUBLE"),
            ("DECIMAL(10,2)", "DECIMAL(38,2)"),
            ("DECIMAL", "DECIMAL(38,3)"),
            ("DECIMAL(10)", "DECIMAL(38,0)"),
        ] {
            let input_type = crate::parse_data_type(input, DialectType::DuckDB).unwrap();
            let expected = crate::parse_data_type(output, DialectType::DuckDB).unwrap();
            let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
            schema
                .add_table("t", &[("x".to_string(), input_type)], None)
                .unwrap();
            let cast = parse_one(&format!("CAST(1 AS {input})"), DialectType::DuckDB).unwrap();
            let function = Expression::Function(Box::new(Function::new("SUM", vec![cast])));
            assert_eq!(
                TypeAnnotator::new(None, Some(DialectType::DuckDB)).annotate(&function),
                Some(expected.clone()),
                "SUM({input}) builder"
            );
            for arg in ["x".to_string(), format!("CAST(1 AS {input})")] {
                for projection in [
                    format!("SUM({arg})"),
                    format!("SUM(DISTINCT {arg})"),
                    format!("SUM({arg}) OVER ()"),
                    format!("SUM({arg}) FILTER (WHERE TRUE)"),
                ] {
                    let sql = format!("SELECT {projection} AS total FROM t");
                    let mut expr = parse_one(&sql, DialectType::DuckDB).unwrap();
                    annotate_types(&mut expr, Some(&schema), Some(DialectType::DuckDB));
                    let Expression::Select(select) = expr else {
                        unreachable!()
                    };
                    let Expression::Alias(alias) = &select.expressions[0] else {
                        unreachable!()
                    };
                    assert_eq!(alias.inferred_type.as_ref(), Some(&expected), "{sql}");
                }
            }
        }
        let expr = parse_one("SUM(x)", DialectType::DuckDB).unwrap();
        assert_eq!(
            TypeAnnotator::new(None, Some(DialectType::DuckDB)).annotate(&expr),
            None
        );
        // The dialect-specific override must not change the generic/default rule.
        for dialect in [
            DialectType::Generic,
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
        ] {
            let expr = parse_one("SUM(CAST(1 AS BIGINT))", dialect).unwrap();
            assert_eq!(
                TypeAnnotator::new(None, Some(dialect)).annotate(&expr),
                Some(DataType::BigInt { length: None })
            );
        }
    }

    #[test]
    fn test_trim_annotates_both_arguments_and_nested_children() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "users",
                &[
                    ("name".to_string(), varchar.clone()),
                    ("chars".to_string(), varchar.clone()),
                ],
                None,
            )
            .unwrap();

        fn check_children(expr: &Expression, varchar: &DataType, has_schema: bool) -> usize {
            let mut count = 0;
            if matches!(
                expr,
                Expression::Trim(_)
                    | Expression::Lower(_)
                    | Expression::Upper(_)
                    | Expression::Column(_)
            ) {
                let expected = if matches!(expr, Expression::Column(_)) && !has_schema {
                    None
                } else {
                    Some(varchar)
                };
                assert_eq!(expr.inferred_type(), expected, "{}", expr.sql());
                count += 1;
            }
            crate::ast_children::for_each_child(expr, |_, child| {
                count += check_children(child, varchar, has_schema);
            });
            count
        }

        for schema in [None, Some(&schema as &dyn Schema)] {
            let mut expression = parse_one(
                "SELECT TRIM(LOWER(TRIM(name)), UPPER(chars)) AS normalized FROM users",
                DialectType::DuckDB,
            )
            .unwrap();
            annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
            assert_eq!(check_children(&expression, &varchar, schema.is_some()), 6);
        }
    }

    #[test]
    fn test_trim_type_metadata_roundtrip_across_dialects() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        for dialect in [
            DialectType::Generic,
            DialectType::DuckDB,
            DialectType::PostgreSQL,
            DialectType::Snowflake,
            DialectType::BigQuery,
            DialectType::MySQL,
        ] {
            let expression = parse_one("TRIM('  hello  ')", dialect).unwrap();
            assert!(matches!(expression, Expression::Trim(_)));
            assert_eq!(expression.inferred_type(), None);
            let unannotated = serde_json::to_value(&expression).unwrap();
            assert!(unannotated["trim"].get("inferred_type").is_none());

            // Old AST JSON without type metadata remains deserializable.
            let mut restored: Expression = serde_json::from_value(unannotated).unwrap();
            assert_eq!(restored, expression);
            annotate_types(&mut restored, None, Some(dialect));
            assert_eq!(restored.inferred_type(), Some(&varchar), "{dialect:?}");
            assert_eq!(restored.sql_for(dialect), expression.sql_for(dialect));

            let annotated = serde_json::to_value(&restored).unwrap();
            assert_eq!(annotated["trim"]["inferred_type"]["data_type"], "var_char");
            let roundtripped: Expression = serde_json::from_value(annotated).unwrap();
            assert_eq!(roundtripped, restored);
            assert_eq!(roundtripped.inferred_type(), Some(&varchar));
        }
    }

    #[test]
    fn test_duckdb_regexp_extract_all_overload_types() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let captures = DataType::Struct {
            fields: vec![
                StructField::new("Letter".to_string(), varchar.clone()),
                StructField::new("number".to_string(), varchar.clone()),
            ],
            nested: false,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table("documents", &[("body".to_string(), varchar.clone())], None)
            .unwrap();

        for (arguments, element_type) in [
            ("body, '[0-9]+'", &varchar),
            ("documents.body, '[0-9]+'", &varchar),
            ("'a1b22', '[0-9]+'", &varchar),
            ("'abc', '[0-9]+'", &varchar),
            ("NULL, '[0-9]+'", &varchar),
            ("body, '([a-z])([0-9]+)', 0", &varchar),
            ("body, '([a-z])([0-9]+)', 2", &varchar),
            ("body, '([a-z])([0-9]+)', (1), 'i'", &varchar),
            ("body, '([a-z])([0-9]+)', CAST(1 AS INTEGER)", &varchar),
            ("body, '([a-z])([0-9]+)', NULL", &varchar),
            ("body, '([a-z])([0-9]+)', ['Letter', 'number']", &captures),
            (
                "body, '([a-z])([0-9]+)', (['Letter', 'number']), 'i'",
                &captures,
            ),
            (
                "body, '([a-z])([0-9]+)', ARRAY['Letter', 'number']",
                &captures,
            ),
            (
                "NULL, '([a-z])([0-9]+)', LIST['Letter', 'number']",
                &captures,
            ),
        ] {
            let expected = DataType::Array {
                element_type: Box::new(element_type.clone()),
                dimension: None,
            };
            for name in ["REGEXP_EXTRACT_ALL", "ReGeXp_ExTrAcT_AlL"] {
                for schema in [None, Some(&schema as &dyn Schema)] {
                    let sql = format!("SELECT {name}({arguments}) AS matches FROM documents");
                    let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
                    let original_sql = expression.sql_for(DialectType::DuckDB);
                    annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
                    let Expression::Select(select) = &expression else {
                        panic!("Expected SELECT for {sql}");
                    };
                    let Expression::Alias(alias) = &select.expressions[0] else {
                        panic!("Expected alias for {sql}");
                    };
                    assert_eq!(alias.this.inferred_type(), Some(&expected), "{sql}");
                    assert_eq!(alias.inferred_type.as_ref(), Some(&expected), "{sql}");
                    assert_eq!(expression.sql_for(DialectType::DuckDB), original_sql);
                    let json = serde_json::to_value(&expression).unwrap();
                    let restored: Expression = serde_json::from_value(json).unwrap();
                    assert_eq!(restored, expression);

                    let sql = format!(
                        "SELECT UNNEST({name}({arguments})) AS single_match FROM documents"
                    );
                    let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
                    annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
                    let Expression::Select(select) = &expression else {
                        panic!("Expected SELECT for {sql}");
                    };
                    let Expression::Alias(alias) = &select.expressions[0] else {
                        panic!("Expected alias for {sql}");
                    };
                    assert_eq!(alias.this.inferred_type(), Some(element_type), "{sql}");
                    assert_eq!(alias.inferred_type.as_ref(), Some(element_type), "{sql}");
                }
            }
        }
    }

    #[test]
    fn test_duckdb_regexp_extract_all_unresolved_overloads() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "documents",
                &[(
                    "names".to_string(),
                    DataType::Array {
                        element_type: Box::new(varchar),
                        dimension: None,
                    },
                )],
                None,
            )
            .unwrap();

        // Do not fabricate a scalar or a precise struct for unknown capture
        // names, ambiguous overloads, or invalid literal name lists/arity.
        for arguments in [
            "",
            "'a1'",
            "'a1', '(a)(1)', 0, 'i', 1",
            "'a1', '(a)(1)', missing_group",
            "'a1', '(a)(1)', documents.names",
            "'a1', '(a)(1)', ['letter', missing_name]",
            "'a1', '(a)(1)', CAST(['letter', 'number'] AS VARCHAR[])",
            "'a1', '(a)(1)', []",
            "'a1', '(a)(1)', [NULL]",
            "'a1', '(a)(1)', ['letter', 'LETTER']",
            "'a1', '(a)(1)', 1.0",
            "'a1', '(a)(1)', CAST(1 AS BIGINT)",
        ] {
            let sql = format!("REGEXP_EXTRACT_ALL({arguments})");
            let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
            let mut annotator = TypeAnnotator::new(Some(&schema), Some(DialectType::DuckDB));
            assert_eq!(annotator.annotate(&expression), None, "{sql}");
            annotator.annotate_in_place(&mut expression);
            assert_eq!(expression.inferred_type(), None, "{sql}");
        }
    }

    #[test]
    fn test_regexp_extract_all_rule_is_duckdb_specific() {
        // Other dialects have different overloads, notably BigQuery BYTES.
        // Preserve their existing behavior rather than installing a global rule.
        for dialect in [
            None,
            Some(DialectType::Generic),
            Some(DialectType::BigQuery),
            Some(DialectType::Snowflake),
            Some(DialectType::Databricks),
        ] {
            for input in [
                make_string_literal("a1"),
                Expression::Literal(Box::new(Literal::ByteString("a1".to_string()))),
            ] {
                let mut annotator = TypeAnnotator::new(None, dialect);
                let expected = annotator.annotate(&input);
                let function = Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT_ALL",
                    vec![input, make_string_literal("[0-9]+")],
                )));
                assert_eq!(annotator.annotate(&function), expected, "{dialect:?}");
            }
        }
    }

    #[test]
    fn test_duckdb_date_name_types() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        for input_type in [
            DataType::Date,
            DataType::Timestamp {
                precision: None,
                timezone: false,
            },
            DataType::Timestamp {
                precision: None,
                timezone: true,
            },
        ] {
            let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
            schema
                .add_table(
                    "events",
                    &[("created_at".to_string(), input_type.clone())],
                    None,
                )
                .unwrap();
            for function in ["MONTHNAME", "MoNtHnAmE", "DAYNAME", "DaYnAmE"] {
                for input in [
                    "created_at",
                    "events.created_at",
                    "DATE '2026-01-01'",
                    "TIMESTAMP '2026-01-01 12:00:00'",
                    "TIMESTAMPTZ '2026-01-01 12:00:00+00'",
                    "NULL",
                ] {
                    let sql = format!("SELECT {function}({input}) AS label FROM events");
                    for schema in [None, Some(&schema as &dyn Schema)] {
                        let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
                        let original_sql = expression.sql_for(DialectType::DuckDB);
                        annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
                        let Expression::Select(select) = &expression else {
                            panic!("Expected SELECT for {sql}");
                        };
                        let Expression::Alias(alias) = &select.expressions[0] else {
                            panic!("Expected alias for {sql}");
                        };
                        assert_eq!(alias.this.inferred_type(), Some(&varchar), "{sql}");
                        assert_eq!(alias.inferred_type.as_ref(), Some(&varchar), "{sql}");
                        if let Expression::Function(function) = &alias.this {
                            if let Expression::Column(column) = &function.args[0] {
                                assert_eq!(
                                    column.inferred_type.as_ref(),
                                    schema.map(|_| &input_type),
                                    "Argument type must remain unchanged for {sql}"
                                );
                            }
                        }
                        assert_eq!(expression.sql_for(DialectType::DuckDB), original_sql);
                        let json = serde_json::to_value(&expression).unwrap();
                        let restored: Expression = serde_json::from_value(json).unwrap();
                        assert_eq!(restored, expression);
                    }
                }
            }
        }
    }

    #[test]
    fn test_date_name_fixed_return_types_are_duckdb_specific() {
        for dialect in [
            None,
            Some(DialectType::Generic),
            Some(DialectType::PostgreSQL),
            Some(DialectType::MySQL),
            Some(DialectType::Snowflake),
        ] {
            let annotator = TypeAnnotator::new(None, dialect);
            for function in ["MONTHNAME", "DAYNAME"] {
                assert!(
                    !annotator.function_return_types.contains_key(function),
                    "Unexpected fixed return type for {function} in {dialect:?}"
                );
            }
        }
    }

    #[test]
    fn test_duckdb_array_to_string_types() {
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "events",
                &[
                    ("label".to_string(), varchar.clone()),
                    (
                        "labels".to_string(),
                        DataType::Array {
                            element_type: Box::new(varchar.clone()),
                            dimension: None,
                        },
                    ),
                ],
                None,
            )
            .unwrap();

        for function in [
            "ARRAY_TO_STRING",
            "array_to_string",
            "ArRaY_To_StRiNg_CoMmA_DeFaUlT",
        ] {
            for input in [
                "labels",
                "events.labels",
                "ARRAY_AGG(label)",
                "ARRAY_AGG(events.label)",
                "['a', NULL, 'b']",
                "[1, 2]",
                "[]",
                "CAST(NULL AS VARCHAR[])",
                "NULL",
            ] {
                let arguments = if function.eq_ignore_ascii_case("ARRAY_TO_STRING") {
                    format!("{input}, ', '")
                } else {
                    input.to_string()
                };
                let sql = format!("SELECT {function}({arguments}) AS label_text FROM events");
                for schema in [None, Some(&schema as &dyn Schema)] {
                    let mut expression = parse_one(&sql, DialectType::DuckDB).unwrap();
                    annotate_types(&mut expression, schema, Some(DialectType::DuckDB));
                    let Expression::Select(select) = &expression else {
                        panic!("Expected SELECT for {sql}");
                    };
                    let Expression::Alias(alias) = &select.expressions[0] else {
                        panic!("Expected alias for {sql}");
                    };
                    assert_eq!(alias.this.inferred_type(), Some(&varchar), "{sql}");
                    assert_eq!(alias.inferred_type.as_ref(), Some(&varchar), "{sql}");
                }
            }
        }
    }

    #[test]
    fn test_array_to_string_fixed_return_types_are_duckdb_specific() {
        // In particular, BigQuery also has a BYTES-returning overload, so it
        // must not inherit DuckDB's unconditional VARCHAR return-type rules.
        for dialect in [
            None,
            Some(DialectType::Generic),
            Some(DialectType::BigQuery),
            Some(DialectType::PostgreSQL),
            Some(DialectType::Snowflake),
        ] {
            let annotator = TypeAnnotator::new(None, dialect);
            for function in ["ARRAY_TO_STRING", "ARRAY_TO_STRING_COMMA_DEFAULT"] {
                assert!(
                    !annotator.function_return_types.contains_key(function),
                    "Unexpected fixed return type for {function} in {dialect:?}"
                );
            }
        }
    }

    #[test]
    fn test_extract_types_are_dialect_and_field_specific() {
        let double = DataType::Double {
            precision: None,
            scale: None,
        };
        let bigint = DataType::BigInt { length: None };
        let int = DataType::Int {
            length: None,
            integer_spelling: false,
        };

        for (field, duckdb_type) in [
            (DateTimeField::Year, &bigint),
            (DateTimeField::Second, &bigint),
            (DateTimeField::Millisecond, &bigint),
            (DateTimeField::Microsecond, &bigint),
            (DateTimeField::Custom("yearweek".to_string()), &bigint),
            (DateTimeField::Epoch, &double),
            (DateTimeField::Custom("EpOcH".to_string()), &double),
            (DateTimeField::Custom("julian".to_string()), &double),
            (DateTimeField::Custom("JuLiAn".to_string()), &double),
        ] {
            let extract = Expression::Extract(Box::new(ExtractFunc {
                this: make_string_literal("2024-02-03 04:05:06.123456"),
                field,
            }));
            for dialect in [
                None,
                Some(DialectType::Generic),
                Some(DialectType::PostgreSQL),
                Some(DialectType::Snowflake),
                Some(DialectType::DuckDB),
            ] {
                let expected = if dialect == Some(DialectType::DuckDB) {
                    duckdb_type
                } else {
                    &int
                };
                assert_eq!(
                    TypeAnnotator::new(None, dialect)
                        .annotate(&extract)
                        .as_ref(),
                    Some(expected),
                    "unexpected type for {extract:?} in {dialect:?}"
                );
            }
        }
    }

    #[test]
    fn test_duckdb_date_trunc_overload_types() {
        fn cast_to(data_type: DataType) -> Expression {
            Expression::Cast(Box::new(Cast {
                this: make_string_literal("value"),
                to: data_type,
                format: None,
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                default: None,
                inferred_type: None,
            }))
        }

        fn date_trunc(value: Expression) -> Expression {
            Expression::Function(Box::new(Function::new(
                "DATE_TRUNC",
                vec![make_string_literal("month"), value],
            )))
        }

        let mut annotator = TypeAnnotator::new(None, Some(DialectType::DuckDB));

        for input_type in [
            DataType::Date,
            DataType::Timestamp {
                precision: Some(9),
                timezone: false,
            },
        ] {
            assert_eq!(
                annotator.annotate(&date_trunc(cast_to(input_type))),
                Some(DataType::Timestamp {
                    precision: None,
                    timezone: false,
                })
            );
        }

        assert_eq!(
            annotator.annotate(&date_trunc(cast_to(DataType::Timestamp {
                precision: Some(6),
                timezone: true,
            }))),
            Some(DataType::Timestamp {
                precision: None,
                timezone: true,
            })
        );
        assert_eq!(
            annotator.annotate(&date_trunc(cast_to(DataType::Interval {
                unit: Some("DAY".to_string()),
                to: Some("SECOND".to_string()),
            }))),
            Some(DataType::Interval {
                unit: None,
                to: None,
            })
        );
        assert_eq!(
            annotator.annotate(&date_trunc(Expression::Null(Null))),
            None
        );
    }

    #[test]
    fn test_coalesce_type_inference() {
        let mut annotator = TypeAnnotator::new(None, None);

        // COALESCE(NULL, 1) returns Int (type of first non-null arg)
        let coalesce = Expression::Function(Box::new(Function::new(
            "COALESCE",
            vec![Expression::Null(Null), make_int_literal(1)],
        )));
        assert_eq!(
            annotator.annotate(&coalesce),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );

        let specialized_coalesce = Expression::Coalesce(Box::new(crate::expressions::VarArgFunc {
            expressions: vec![Expression::Null(Null), make_int_literal(1)],
            original_name: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&specialized_coalesce),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );
    }

    #[test]
    fn test_type_coercion_class() {
        // Text types
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::VarChar {
                length: None,
                parenthesized_length: false
            }),
            Some(TypeCoercionClass::Text)
        );
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Text),
            Some(TypeCoercionClass::Text)
        );

        // Numeric types
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Int {
                length: None,
                integer_spelling: false
            }),
            Some(TypeCoercionClass::Numeric)
        );
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Double {
                precision: None,
                scale: None
            }),
            Some(TypeCoercionClass::Numeric)
        );

        // Timelike types
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Date),
            Some(TypeCoercionClass::Timelike)
        );
        assert_eq!(
            TypeCoercionClass::from_data_type(&DataType::Timestamp {
                precision: None,
                timezone: false
            }),
            Some(TypeCoercionClass::Timelike)
        );

        // Unknown types
        assert_eq!(TypeCoercionClass::from_data_type(&DataType::Json), None);
    }

    #[test]
    fn test_wider_numeric_type() {
        let annotator = TypeAnnotator::new(None, None);

        // Int vs BigInt -> BigInt
        let result = annotator.wider_numeric_type(
            &DataType::Int {
                length: None,
                integer_spelling: false,
            },
            &DataType::BigInt { length: None },
        );
        assert_eq!(result, DataType::BigInt { length: None });

        // Float vs Double -> Double
        let result = annotator.wider_numeric_type(
            &DataType::Float {
                precision: None,
                scale: None,
                real_spelling: false,
            },
            &DataType::Double {
                precision: None,
                scale: None,
            },
        );
        assert_eq!(
            result,
            DataType::Double {
                precision: None,
                scale: None
            }
        );

        // Int vs Double -> Double
        let result = annotator.wider_numeric_type(
            &DataType::Int {
                length: None,
                integer_spelling: false,
            },
            &DataType::Double {
                precision: None,
                scale: None,
            },
        );
        assert_eq!(
            result,
            DataType::Double {
                precision: None,
                scale: None
            }
        );
    }

    #[test]
    fn test_aggregate_return_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // SUM(int) returns BigInt
        let sum_type = annotator.get_aggregate_return_type("SUM", &[make_int_literal(1)]);
        assert_eq!(sum_type, Some(DataType::BigInt { length: None }));

        // AVG always returns Double
        let avg_type = annotator.get_aggregate_return_type("AVG", &[make_int_literal(1)]);
        assert_eq!(
            avg_type,
            Some(DataType::Double {
                precision: None,
                scale: None
            })
        );

        // MIN/MAX preserve input type
        let min_type = annotator.get_aggregate_return_type("MIN", &[make_string_literal("a")]);
        assert_eq!(
            min_type,
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false
            })
        );

        // DuckDB MIN/MAX(value, n) return a list of the input type.
        let top_n_type = annotator
            .get_aggregate_return_type("MAX", &[make_string_literal("a"), make_int_literal(2)]);
        assert_eq!(
            top_n_type,
            Some(DataType::Array {
                element_type: Box::new(DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                }),
                dimension: None,
            })
        );
    }

    #[test]
    fn test_date_literal_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // DATE literal
        let date_expr = Expression::Literal(Box::new(Literal::Date("2024-01-15".to_string())));
        assert_eq!(annotator.annotate(&date_expr), Some(DataType::Date));

        // TIME literal
        let time_expr = Expression::Literal(Box::new(Literal::Time("10:30:00".to_string())));
        assert_eq!(
            annotator.annotate(&time_expr),
            Some(DataType::Time {
                precision: None,
                timezone: false
            })
        );

        // TIMESTAMP literal
        let ts_expr = Expression::Literal(Box::new(Literal::Timestamp(
            "2024-01-15 10:30:00".to_string(),
        )));
        assert_eq!(
            annotator.annotate(&ts_expr),
            Some(DataType::Timestamp {
                precision: None,
                timezone: false
            })
        );
    }

    #[test]
    fn test_logical_operations() {
        let mut annotator = TypeAnnotator::new(None, None);

        // AND returns boolean
        let and_expr = Expression::And(Box::new(BinaryOp::new(
            make_bool_literal(true),
            make_bool_literal(false),
        )));
        assert_eq!(annotator.annotate(&and_expr), Some(DataType::Boolean));

        // OR returns boolean
        let or_expr = Expression::Or(Box::new(BinaryOp::new(
            make_bool_literal(true),
            make_bool_literal(false),
        )));
        assert_eq!(annotator.annotate(&or_expr), Some(DataType::Boolean));

        // NOT returns boolean
        let not_expr = Expression::Not(Box::new(crate::expressions::UnaryOp::new(
            make_bool_literal(true),
        )));
        assert_eq!(annotator.annotate(&not_expr), Some(DataType::Boolean));
    }

    // ========================================
    // Tests for newly implemented features
    // ========================================

    #[test]
    fn test_subscript_array_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // Array[index] returns element type
        let arr = Expression::Array(Box::new(crate::expressions::Array {
            expressions: vec![make_int_literal(1), make_int_literal(2)],
            inferred_type: None,
        }));
        let subscript = Expression::Subscript(Box::new(crate::expressions::Subscript {
            this: arr,
            index: make_int_literal(0),
        }));
        assert_eq!(
            annotator.annotate(&subscript),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );
    }

    #[test]
    fn test_subscript_map_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // Map[key] returns value type
        let map = Expression::Map(Box::new(crate::expressions::Map {
            keys: vec![make_string_literal("a")],
            values: vec![make_int_literal(1)],
        }));
        let subscript = Expression::Subscript(Box::new(crate::expressions::Subscript {
            this: map,
            index: make_string_literal("a"),
        }));
        assert_eq!(
            annotator.annotate(&subscript),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );
    }

    #[test]
    fn test_struct_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // STRUCT literal
        let struct_expr = Expression::Struct(Box::new(crate::expressions::Struct {
            fields: vec![
                (Some("name".to_string()), make_string_literal("Alice")),
                (Some("age".to_string()), make_int_literal(30)),
            ],
        }));
        let result = annotator.annotate(&struct_expr);
        assert!(matches!(result, Some(DataType::Struct { fields, .. }) if fields.len() == 2));
    }

    #[test]
    fn test_map_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // MAP literal
        let map_expr = Expression::Map(Box::new(crate::expressions::Map {
            keys: vec![make_string_literal("a"), make_string_literal("b")],
            values: vec![make_int_literal(1), make_int_literal(2)],
        }));
        let result = annotator.annotate(&map_expr);
        assert!(matches!(
            result,
            Some(DataType::Map { key_type, value_type })
            if matches!(*key_type, DataType::VarChar { .. })
               && matches!(*value_type, DataType::Int { .. })
        ));
    }

    #[test]
    fn test_explode_array_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // EXPLODE(array) returns element type
        let arr = Expression::Array(Box::new(crate::expressions::Array {
            expressions: vec![make_int_literal(1), make_int_literal(2)],
            inferred_type: None,
        }));
        let explode = Expression::Explode(Box::new(crate::expressions::UnaryFunc {
            this: arr,
            original_name: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&explode),
            Some(DataType::Int {
                length: None,
                integer_spelling: false
            })
        );
    }

    #[test]
    fn test_unnest_array_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // UNNEST(array) returns element type
        let arr = Expression::Array(Box::new(crate::expressions::Array {
            expressions: vec![make_string_literal("a"), make_string_literal("b")],
            inferred_type: None,
        }));
        let unnest = Expression::Unnest(Box::new(crate::expressions::UnnestFunc {
            this: arr,
            expressions: Vec::new(),
            with_ordinality: false,
            alias: None,
            offset_alias: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&unnest),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false
            })
        );
    }

    #[test]
    fn test_annotate_duckdb_array_case_unnest_types() {
        let timestamp = DataType::Timestamp {
            precision: None,
            timezone: false,
        };
        let array_timestamp = DataType::Array {
            element_type: Box::new(timestamp.clone()),
            dimension: None,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "events",
                &[
                    ("created_at".to_string(), timestamp.clone()),
                    ("closed_at".to_string(), timestamp.clone()),
                ],
                None,
            )
            .unwrap();

        let mut expr = parse_one(
            "SELECT UNNEST(CASE WHEN closed_at IS NULL THEN ARRAY[created_at] \
             ELSE ARRAY[created_at, closed_at] END) AS event_at FROM events",
            DialectType::DuckDB,
        )
        .unwrap();
        annotate_types(&mut expr, Some(&schema), Some(DialectType::DuckDB));

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };
        let Expression::Function(unnest) = &alias.this else {
            panic!("expected UNNEST function");
        };
        assert_eq!(unnest.name, "UNNEST");
        let Expression::Case(case) = &unnest.args[0] else {
            panic!("expected case");
        };
        let Expression::ArrayFunc(then_array) = &case.whens[0].1 else {
            panic!("expected ARRAY constructor in THEN branch");
        };
        let Expression::ArrayFunc(else_array) = case.else_.as_ref().expect("expected ELSE") else {
            panic!("expected ARRAY constructor in ELSE branch");
        };

        assert_eq!(then_array.inferred_type.as_ref(), Some(&array_timestamp));
        assert_eq!(else_array.inferred_type.as_ref(), Some(&array_timestamp));
        assert_eq!(case.inferred_type.as_ref(), Some(&array_timestamp));
        assert_eq!(unnest.inferred_type.as_ref(), Some(&timestamp));
        assert_eq!(alias.inferred_type.as_ref(), Some(&timestamp));
        assert!(then_array
            .expressions
            .iter()
            .all(|expression| { expression.inferred_type() == Some(&timestamp) }));
        assert!(else_array
            .expressions
            .iter()
            .all(|expression| { expression.inferred_type() == Some(&timestamp) }));
    }

    #[test]
    fn test_array_and_case_coerce_all_result_types() {
        let mut expr = parse_one(
            "SELECT CASE WHEN TRUE THEN ARRAY[1] ELSE ARRAY[2.5] END AS values",
            DialectType::DuckDB,
        )
        .unwrap();
        annotate_types(&mut expr, None, Some(DialectType::DuckDB));

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };
        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::Array {
                element_type: Box::new(DataType::Double {
                    precision: None,
                    scale: None,
                }),
                dimension: None,
            })
        );
    }

    #[test]
    fn test_transparent_wrappers_preserve_scalar_types() {
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table("flags", &[("flag".to_string(), DataType::Boolean)], None)
            .unwrap();
        let varchar = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };

        let mut parenthesized = parse_one(
            "SELECT (CASE WHEN flag THEN 'yes' ELSE 'no' END) AS label FROM flags",
            DialectType::DuckDB,
        )
        .unwrap();
        annotate_types(&mut parenthesized, Some(&schema), Some(DialectType::DuckDB));

        let Expression::Select(select) = &parenthesized else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };
        let Expression::Paren(paren) = &alias.this else {
            panic!("expected parenthesized expression");
        };
        let Expression::Case(case) = &paren.this else {
            panic!("expected case");
        };
        let Expression::Column(column) = &case.whens[0].0 else {
            panic!("expected column condition");
        };

        assert_eq!(alias.this.inferred_type(), Some(&varchar));
        assert_eq!(case.inferred_type.as_ref(), Some(&varchar));
        assert_eq!(column.inferred_type.as_ref(), Some(&DataType::Boolean));
        assert_eq!(alias.inferred_type.as_ref(), Some(&varchar));

        let mut annotated = parse_one(
            "SELECT CASE WHEN flag THEN 'yes' ELSE 'no' END /*tail*/ FROM flags",
            DialectType::DuckDB,
        )
        .unwrap();
        annotate_types(&mut annotated, Some(&schema), Some(DialectType::DuckDB));

        let Expression::Select(select) = &annotated else {
            panic!("expected select");
        };
        let Expression::Annotated(annotated) = &select.expressions[0] else {
            panic!("expected comment annotation wrapper");
        };
        let Expression::Case(case) = &annotated.this else {
            panic!("expected case");
        };
        let Expression::Column(column) = &case.whens[0].0 else {
            panic!("expected column condition");
        };

        assert_eq!(select.expressions[0].inferred_type(), Some(&varchar));
        assert_eq!(case.inferred_type.as_ref(), Some(&varchar));
        assert_eq!(column.inferred_type.as_ref(), Some(&DataType::Boolean));
    }

    #[test]
    fn test_set_operation_type() {
        let mut annotator = TypeAnnotator::new(None, None);

        // UNION/INTERSECT/EXCEPT return None (they produce relations, not scalars)
        let select = Expression::Select(Box::new(crate::expressions::Select::default()));
        let union = Expression::Union(Box::new(crate::expressions::Union {
            left: select.clone(),
            right: select.clone(),
            all: false,
            distinct: false,
            with: None,
            order_by: None,
            limit: None,
            offset: None,
            by_name: false,
            side: None,
            kind: None,
            corresponding: false,
            strict: false,
            on_columns: Vec::new(),
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
        }));
        assert_eq!(annotator.annotate(&union), None);
    }

    #[test]
    fn test_floor_ceil_input_dependent_types() {
        use crate::expressions::{CeilFunc, FloorFunc};

        let mut annotator = TypeAnnotator::new(None, None);

        // FLOOR/CEIL with integer literal → Double (integers get promoted)
        let floor_int = Expression::Floor(Box::new(FloorFunc {
            this: make_int_literal(42),
            scale: None,
            to: None,
        }));
        assert_eq!(
            annotator.annotate(&floor_int),
            Some(DataType::Double {
                precision: None,
                scale: None,
            })
        );

        let ceil_int = Expression::Ceil(Box::new(CeilFunc {
            this: make_int_literal(42),
            decimals: None,
            to: None,
        }));
        assert_eq!(
            annotator.annotate(&ceil_int),
            Some(DataType::Double {
                precision: None,
                scale: None,
            })
        );

        // FLOOR with float literal → Double (literals are always Double)
        let floor_float = Expression::Floor(Box::new(FloorFunc {
            this: make_float_literal(3.14),
            scale: None,
            to: None,
        }));
        assert_eq!(
            annotator.annotate(&floor_float),
            Some(DataType::Double {
                precision: None,
                scale: None,
            })
        );

        // FLOOR via Function("FLOOR") path → falls through to arg-based inference
        let floor_fn =
            Expression::Function(Box::new(Function::new("FLOOR", vec![make_int_literal(1)])));
        assert_eq!(
            annotator.annotate(&floor_fn),
            Some(DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_sign_preserves_input_type() {
        use crate::expressions::UnaryFunc;

        let mut annotator = TypeAnnotator::new(None, None);

        // SIGN with integer literal → Int (preserves input type)
        let sign_int = Expression::Sign(Box::new(UnaryFunc {
            this: make_int_literal(42),
            original_name: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&sign_int),
            Some(DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );

        // SIGN with float literal → Double (preserves input type)
        let sign_float = Expression::Sign(Box::new(UnaryFunc {
            this: make_float_literal(3.14),
            original_name: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&sign_float),
            Some(DataType::Double {
                precision: None,
                scale: None,
            })
        );

        // SIGN with a CAST to INT → Int (preserves input type)
        let sign_cast = Expression::Sign(Box::new(UnaryFunc {
            this: Expression::Cast(Box::new(Cast {
                this: make_int_literal(42),
                to: DataType::Int {
                    length: None,
                    integer_spelling: false,
                },
                format: None,
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                default: None,
                inferred_type: None,
            })),
            original_name: None,
            inferred_type: None,
        }));
        assert_eq!(
            annotator.annotate(&sign_cast),
            Some(DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_date_format_types() {
        use crate::expressions::{DateFormatFunc, TimeToStr};

        let mut annotator = TypeAnnotator::new(None, None);

        // DateFormat → VarChar
        let date_fmt = Expression::DateFormat(Box::new(DateFormatFunc {
            this: make_string_literal("2024-01-01"),
            format: make_string_literal("%Y-%m-%d"),
        }));
        assert_eq!(
            annotator.annotate(&date_fmt),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );

        // FormatDate → VarChar
        let format_date = Expression::FormatDate(Box::new(DateFormatFunc {
            this: make_string_literal("2024-01-01"),
            format: make_string_literal("%Y-%m-%d"),
        }));
        assert_eq!(
            annotator.annotate(&format_date),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );

        // TimeToStr → VarChar
        let time_to_str = Expression::TimeToStr(Box::new(TimeToStr {
            this: Box::new(make_string_literal("2024-01-01")),
            format: "%Y-%m-%d".to_string(),
            culture: None,
            zone: None,
        }));
        assert_eq!(
            annotator.annotate(&time_to_str),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );

        // DATE_FORMAT via Function path → VarChar (uses function_return_types)
        let date_fmt_fn = Expression::Function(Box::new(Function::new(
            "DATE_FORMAT",
            vec![
                make_string_literal("2024-01-01"),
                make_string_literal("%Y-%m-%d"),
            ],
        )));
        assert_eq!(
            annotator.annotate(&date_fmt_fn),
            Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );
    }

    // ===== In-place annotation tests (Step 9) =====

    #[test]
    fn test_annotate_in_place_sets_type_on_root() {
        // Literals don't have inferred_type field, so test with a BinaryOp
        let mut expr = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        annotate_types(&mut expr, None, None);
        assert_eq!(
            expr.inferred_type(),
            Some(&DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_annotate_in_place_sets_types_on_children() {
        // (a + b) + (c - d) where all are ints
        // This tests that inner BinaryOp children also get annotated
        let inner_add = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_float_literal(2.5),
        )));
        let inner_sub = Expression::Sub(Box::new(BinaryOp::new(
            make_int_literal(3),
            make_int_literal(4),
        )));
        let mut expr = Expression::Add(Box::new(BinaryOp::new(inner_add, inner_sub)));
        annotate_types(&mut expr, None, None);

        // Root (Add) should be Double (wider of Double and Int)
        assert_eq!(
            expr.inferred_type(),
            Some(&DataType::Double {
                precision: None,
                scale: None,
            })
        );

        // Children should also have types
        if let Expression::Add(op) = &expr {
            // Left child (1 + 2.5) should be Double
            assert_eq!(
                op.left.inferred_type(),
                Some(&DataType::Double {
                    precision: None,
                    scale: None,
                })
            );
            // Right child (3 - 4) should be Int
            assert_eq!(
                op.right.inferred_type(),
                Some(&DataType::Int {
                    length: None,
                    integer_spelling: false,
                })
            );
        } else {
            panic!("Expected Add expression");
        }
    }

    #[test]
    fn test_annotate_in_place_comparison() {
        let mut expr = Expression::Eq(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        annotate_types(&mut expr, None, None);
        assert_eq!(expr.inferred_type(), Some(&DataType::Boolean));
    }

    #[test]
    fn test_annotate_in_place_cast() {
        let mut expr = Expression::Cast(Box::new(Cast {
            this: make_int_literal(42),
            to: DataType::VarChar {
                length: None,
                parenthesized_length: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }));
        annotate_types(&mut expr, None, None);
        assert_eq!(
            expr.inferred_type(),
            Some(&DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );
    }

    #[test]
    fn test_annotate_in_place_nested_expression() {
        // (1 + 2) > 0  -> should be Boolean at root, Int for the Add
        let add = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        let mut expr = Expression::Gt(Box::new(BinaryOp::new(add, make_int_literal(0))));
        annotate_types(&mut expr, None, None);

        assert_eq!(expr.inferred_type(), Some(&DataType::Boolean));

        // The left child (Add) should be Int
        if let Expression::Gt(op) = &expr {
            assert_eq!(
                op.left.inferred_type(),
                Some(&DataType::Int {
                    length: None,
                    integer_spelling: false,
                })
            );
        }
    }

    #[test]
    fn test_annotate_in_place_parsed_sql() {
        use crate::parser::Parser;
        let mut expr =
            Parser::parse_sql("SELECT 1 + 2.0, 'hello', TRUE").expect("parse failed")[0].clone();
        annotate_types(&mut expr, None, None);

        // The expression tree should have types annotated throughout
        // We can't easily inspect deep inside a parsed Select, but at minimum
        // the root Select itself won't have a type (it's not value-producing)
        assert!(expr.inferred_type().is_none());
    }

    #[test]
    fn test_inferred_type_json_roundtrip() {
        let mut expr = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        annotate_types(&mut expr, None, None);

        // Serialize to JSON
        let json = serde_json::to_string(&expr).expect("serialize failed");
        // The JSON should contain the inferred_type
        assert!(json.contains("inferred_type"));

        // Deserialize back
        let deserialized: Expression = serde_json::from_str(&json).expect("deserialize failed");
        assert_eq!(
            deserialized.inferred_type(),
            Some(&DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_inferred_type_none_not_serialized() {
        // When inferred_type is None, it should not appear in JSON
        let expr = Expression::Add(Box::new(BinaryOp::new(
            make_int_literal(1),
            make_int_literal(2),
        )));
        let json = serde_json::to_string(&expr).expect("serialize failed");
        assert!(!json.contains("inferred_type"));
    }

    #[test]
    fn test_annotate_if_func_bigquery_node_and_alias_type() {
        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table(
                "t",
                &[("col1".to_string(), DataType::String { length: None })],
                None,
            )
            .unwrap();

        let mut expr = parse_one(
            "SELECT IF(col1 IS NOT NULL, 1, 0) AS x FROM t",
            DialectType::BigQuery,
        )
        .unwrap();
        annotate_types(&mut expr, Some(&schema), Some(DialectType::BigQuery));

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };

        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
        assert_eq!(
            select.expressions[0].inferred_type(),
            Some(&DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_annotate_nvl2_node_type() {
        let mut expr = parse_one("SELECT NVL2(a, 1, 0) AS x", DialectType::Generic).unwrap();
        annotate_types(&mut expr, None, None);

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };

        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::Int {
                length: None,
                integer_spelling: false,
            })
        );
    }

    #[test]
    fn test_annotate_count_node_type() {
        let mut expr = parse_one("SELECT COUNT(1) AS x", DialectType::Generic).unwrap();
        annotate_types(&mut expr, None, None);

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };

        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::BigInt { length: None })
        );
    }

    #[test]
    fn test_annotate_group_concat_node_type() {
        let mut expr = parse_one("SELECT GROUP_CONCAT(a) AS x", DialectType::Generic).unwrap();
        annotate_types(&mut expr, None, None);

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };

        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::VarChar {
                length: None,
                parenthesized_length: false,
            })
        );
    }

    #[test]
    fn test_annotate_sum_if_generic_aggregate_type() {
        let mut expr =
            parse_one("SELECT SUM_IF(1, a > 0) AS x FROM t", DialectType::Generic).unwrap();
        annotate_types(&mut expr, None, None);

        let Expression::Select(select) = &expr else {
            panic!("expected select");
        };
        let Expression::Alias(alias) = &select.expressions[0] else {
            panic!("expected alias");
        };

        assert_eq!(
            select.expressions[0].inferred_type(),
            Some(&DataType::BigInt { length: None })
        );
        assert_eq!(
            alias.this.inferred_type(),
            Some(&DataType::BigInt { length: None })
        );
    }

    #[test]
    fn test_annotate_duckdb_median_node_types() {
        let cases = [
            (
                "SELECT MEDIAN(CAST(1 AS INTEGER)) AS result",
                DataType::Double {
                    precision: None,
                    scale: None,
                },
            ),
            (
                "SELECT MEDIAN(CAST(1 AS FLOAT)) AS result",
                DataType::Float {
                    precision: None,
                    scale: None,
                    real_spelling: false,
                },
            ),
            (
                "SELECT MEDIAN(CAST(1 AS DECIMAL(10, 2))) AS result",
                DataType::Decimal {
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
            (
                "SELECT MEDIAN(CAST('2024-01-01' AS DATE)) AS result",
                DataType::Timestamp {
                    precision: None,
                    timezone: false,
                },
            ),
        ];

        for (sql, expected) in cases {
            let mut expr = parse_one(sql, DialectType::DuckDB).unwrap();
            annotate_types(&mut expr, None, Some(DialectType::DuckDB));

            let Expression::Select(select) = &expr else {
                panic!("expected select for {sql}");
            };
            let Expression::Alias(alias) = &select.expressions[0] else {
                panic!("expected alias for {sql}");
            };

            assert_eq!(alias.this.inferred_type(), Some(&expected), "{sql}");
            assert_eq!(
                select.expressions[0].inferred_type(),
                Some(&expected),
                "{sql}"
            );
        }
    }
}
