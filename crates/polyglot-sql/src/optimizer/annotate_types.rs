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

use std::collections::HashMap;

use crate::dialects::DialectType;
use crate::expressions::{
    BinaryOp, DataType, Expression, Function, IfFunc, ListAggOverflow, Literal, Map, Nvl2Func,
    Struct, StructField, Subscript,
};
use crate::schema::Schema;

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
    /// Dialect for dialect-specific type rules
    _dialect: Option<DialectType>,
    /// Whether to annotate types for all expressions
    annotate_aggregates: bool,
    /// Function return type mappings
    function_return_types: HashMap<String, DataType>,
}

impl<'a> TypeAnnotator<'a> {
    /// Create a new type annotator
    pub fn new(schema: Option<&'a dyn Schema>, dialect: Option<DialectType>) -> Self {
        let mut annotator = Self {
            _schema: schema,
            _dialect: dialect,
            annotate_aggregates: true,
            function_return_types: HashMap::new(),
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
            Expression::Literal(lit) => self.annotate_literal(lit),
            Expression::Boolean(_) => Some(DataType::Boolean),
            Expression::Null(_) => None, // NULL has no type

            // Arithmetic binary operations
            Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op)
            | Expression::Mod(op) => self.annotate_arithmetic(op),

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

            // Functions
            Expression::Function(func) => self.annotate_function(func),
            Expression::IfFunc(if_func) => self.annotate_if_func(if_func),
            Expression::Nvl2(nvl2) => self.annotate_nvl2(nvl2),

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
                    let table_name = col.table.as_ref().map(|t| t.name.as_str()).unwrap_or("");
                    schema.get_column_type(table_name, &col.name.name).ok()
                } else {
                    None
                }
            }

            // Cast expressions
            Expression::Cast(cast) => Some(cast.to.clone()),
            Expression::SafeCast(cast) => Some(cast.to.clone()),
            Expression::TryCast(cast) => Some(cast.to.clone()),

            // Subqueries - type is the type of the first SELECT expression
            Expression::Subquery(subq) => {
                if let Expression::Select(select) = &subq.this {
                    if let Some(first) = select.expressions.first() {
                        self.annotate(first)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            // CASE expression - type of the first THEN/ELSE
            Expression::Case(case) => {
                if let Some(else_expr) = &case.else_ {
                    self.annotate(else_expr)
                } else if let Some((_, then_expr)) = case.whens.first() {
                    self.annotate(then_expr)
                } else {
                    None
                }
            }

            // Array expressions
            Expression::Array(arr) => {
                if let Some(first) = arr.expressions.first() {
                    if let Some(elem_type) = self.annotate(first) {
                        Some(DataType::Array {
                            element_type: Box::new(elem_type),
                            dimension: None,
                        })
                    } else {
                        Some(DataType::Array {
                            element_type: Box::new(DataType::Unknown),
                            dimension: None,
                        })
                    }
                } else {
                    Some(DataType::Array {
                        element_type: Box::new(DataType::Unknown),
                        dimension: None,
                    })
                }
            }

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
            Expression::DateDiff(_) | Expression::Extract(_) => Some(DataType::Int {
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

            // Dot access (struct.field) - returns Unknown without schema
            Expression::Dot(_) => None,

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

            // Alias
            Expression::Alias(a) => {
                self.annotate_in_place(&mut a.this);
            }

            // Column - leaf node, no children to recurse
            Expression::Column(_) => {}

            // Function
            Expression::Function(f) => {
                for arg in &mut f.args {
                    self.annotate_in_place(arg);
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
                self.annotate_in_place(&mut s.this);
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

            // Everything else - no children to recurse or not value-producing
            _ => {}
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
    fn annotate_literal(&self, lit: &Literal) -> Option<DataType> {
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
    fn annotate_arithmetic(&mut self, op: &BinaryOp) -> Option<DataType> {
        let left_type = self.annotate(&op.left);
        let right_type = self.annotate(&op.right);

        match (left_type, right_type) {
            (Some(l), Some(r)) => self.coerce_types(&l, &r),
            (Some(t), None) | (None, Some(t)) => Some(t),
            (None, None) => None,
        }
    }

    /// Annotate a function call
    fn annotate_function(&mut self, func: &Function) -> Option<DataType> {
        let func_name = func.name.to_uppercase();

        // Check known function return types
        if let Some(return_type) = self.function_return_types.get(&func_name) {
            if *return_type != DataType::Unknown {
                return Some(return_type.clone());
            }
        }

        // For functions with Unknown return type, infer from arguments
        match func_name.as_str() {
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
                // Unknown function - try to infer from first argument
                func.args.first().and_then(|arg| self.annotate(arg))
            }
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
                // Preserves input type
                args.first().and_then(|arg| self.annotate(arg))
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
            "PERCENTILE_CONT" | "PERCENTILE_DISC" | "MEDIAN" => {
                args.first().and_then(|arg| self.annotate(arg))
            }
            _ => None,
        }
    }

    /// Annotate SUM function - promotes to at least BigInt
    fn annotate_sum(&mut self, arg: &Expression) -> Option<DataType> {
        match self.annotate(arg) {
            Some(DataType::TinyInt { .. })
            | Some(DataType::SmallInt { .. })
            | Some(DataType::Int { .. }) => Some(DataType::BigInt { length: None }),
            Some(DataType::BigInt { .. }) => Some(DataType::BigInt { length: None }),
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

    /// Coerce multiple argument types to a common type
    fn coerce_arg_types(&mut self, args: &[Expression]) -> Option<DataType> {
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
        let order = |dt: &DataType| -> u8 {
            match dt {
                DataType::Boolean => 0,
                DataType::TinyInt { .. } => 1,
                DataType::SmallInt { .. } => 2,
                DataType::Int { .. } => 3,
                DataType::BigInt { .. } => 4,
                DataType::Float { .. } => 5,
                DataType::Double { .. } => 6,
                DataType::Decimal { .. } => 7,
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
    let mut annotator = TypeAnnotator::new(schema, dialect);
    annotator.annotate_in_place(expr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BooleanLiteral, Cast, Null};
    use crate::{parse_one, DialectType, MappingSchema, Schema};

    fn make_int_literal(val: i64) -> Expression {
        Expression::Literal(Literal::Number(val.to_string()))
    }

    fn make_float_literal(val: f64) -> Expression {
        Expression::Literal(Literal::Number(val.to_string()))
    }

    fn make_string_literal(val: &str) -> Expression {
        Expression::Literal(Literal::String(val.to_string()))
    }

    fn make_bool_literal(val: bool) -> Expression {
        Expression::Boolean(BooleanLiteral { value: val })
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
    }

    #[test]
    fn test_date_literal_types() {
        let mut annotator = TypeAnnotator::new(None, None);

        // DATE literal
        let date_expr = Expression::Literal(Literal::Date("2024-01-15".to_string()));
        assert_eq!(annotator.annotate(&date_expr), Some(DataType::Date));

        // TIME literal
        let time_expr = Expression::Literal(Literal::Time("10:30:00".to_string()));
        assert_eq!(
            annotator.annotate(&time_expr),
            Some(DataType::Time {
                precision: None,
                timezone: false
            })
        );

        // TIMESTAMP literal
        let ts_expr = Expression::Literal(Literal::Timestamp("2024-01-15 10:30:00".to_string()));
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
        }));
        let unnest = Expression::Unnest(Box::new(crate::expressions::UnnestFunc {
            this: arr,
            expressions: Vec::new(),
            with_ordinality: false,
            alias: None,
            offset_alias: None,
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
}
