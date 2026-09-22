//! Set-operation output types. Rules and deliberately unresolved cases are
//! documented in docs/set-operation-types.md. These policies never mutate the
//! scalar coercion tables or assume that parser inheritance implies coercion.

use crate::dialects::DialectType;
use crate::expressions::{DataType, Expression, Literal, OracleDataType, StructField};
use crate::set_operation::set_operation_layout;
use std::collections::HashMap;

#[derive(Clone, Debug)]
enum OutputType {
    Known(DataType),
    Null,
    StringLiteral(String),
    Unknown,
}

#[derive(Clone, Debug)]
pub(crate) struct QueryOutput {
    pub name: String,
    value: OutputType,
    pub cast_type: Option<DataType>,
}

impl QueryOutput {
    pub fn data_type(&self) -> Option<&DataType> {
        match &self.value {
            OutputType::Known(value) if !contains_unknown(value) => Some(value),
            _ => None,
        }
    }
}

/// Cache belongs to one immutable annotated tree, never to an engine or thread.
pub(crate) struct OutputResolver {
    dialect: DialectType,
    cache: HashMap<*const Expression, Vec<QueryOutput>>,
}

impl OutputResolver {
    pub fn new(dialect: DialectType) -> Self {
        Self {
            dialect,
            cache: HashMap::new(),
        }
    }

    pub fn resolve(&mut self, query: &Expression) -> Vec<QueryOutput> {
        self.resolve_inner(query, 0)
    }

    fn resolve_inner(&mut self, query: &Expression, depth: usize) -> Vec<QueryOutput> {
        // Optional metadata must not bypass the parser's complexity protection.
        if depth > 128 {
            return Vec::new();
        }
        let key = query as *const Expression;
        if let Some(result) = self.cache.get(&key) {
            return result.clone();
        }
        let result = match query {
            Expression::Select(select) => select
                .expressions
                .iter()
                .map(|expr| projection_output(expr, self.dialect))
                .collect(),
            Expression::Subquery(subquery) => {
                let mut outputs = self.resolve_inner(&subquery.this, depth + 1);
                for (output, alias) in outputs.iter_mut().zip(&subquery.column_aliases) {
                    output.name = crate::binding::identifier_name(alias);
                }
                outputs
            }
            Expression::Paren(paren) => self.resolve_inner(&paren.this, depth + 1),
            Expression::Annotated(annotated) => self.resolve_inner(&annotated.this, depth + 1),
            Expression::Cte(cte) => self.resolve_inner(&cte.this, depth + 1),
            Expression::Union(op) => self.set_operation(
                query,
                &op.left,
                &op.right,
                op.by_name || op.corresponding,
                depth,
            ),
            Expression::Intersect(op) => self.set_operation(
                query,
                &op.left,
                &op.right,
                op.by_name || op.corresponding,
                depth,
            ),
            Expression::Except(op) => self.set_operation(
                query,
                &op.left,
                &op.right,
                op.by_name || op.corresponding,
                depth,
            ),
            _ => Vec::new(),
        };
        self.cache.insert(key, result.clone());
        result
    }

    fn set_operation(
        &mut self,
        query: &Expression,
        left: &Expression,
        right: &Expression,
        named: bool,
        depth: usize,
    ) -> Vec<QueryOutput> {
        let left = self.resolve_inner(left, depth + 1);
        let right = self.resolve_inner(right, depth + 1);
        let layout = set_operation_layout(query, Some(self.dialect));
        let pairs = match layout {
            Ok(Some(layout)) => layout
                .outputs
                .into_iter()
                .map(|output| {
                    (
                        crate::binding::identifier_name(&output.identifier),
                        output.left_ordinal,
                        output.right_ordinal,
                    )
                })
                .collect::<Vec<_>>(),
            Ok(None)
                if !named
                    && left.len() == right.len()
                    && !left.iter().chain(&right).any(|column| column.name == "*") =>
            {
                left.iter()
                    .enumerate()
                    .map(|(i, output)| (output.name.clone(), Some(i), Some(i)))
                    .collect()
            }
            _ => {
                return left
                    .into_iter()
                    .map(|mut output| {
                        output.value = OutputType::Unknown;
                        output.cast_type = None;
                        output
                    })
                    .collect()
            }
        };
        pairs
            .into_iter()
            .map(|(name, l, r)| {
                let left = l.and_then(|i| left.get(i));
                let right = r.and_then(|i| right.get(i));
                let missing_left = if l.is_none() {
                    OutputType::Null
                } else {
                    OutputType::Unknown
                };
                let missing_right = if r.is_none() {
                    OutputType::Null
                } else {
                    OutputType::Unknown
                };
                let value = combine(
                    left.map_or(&missing_left, |v| &v.value),
                    right.map_or(&missing_right, |v| &v.value),
                    self.dialect,
                    0,
                );
                let cast_type = left
                    .and_then(|v| v.cast_type.as_ref())
                    .zip(right.and_then(|v| v.cast_type.as_ref()))
                    .and_then(|(l, r)| match &value {
                        OutputType::Known(result)
                            if !contains_unknown(result)
                                && canonical(l, self.dialect) == canonical(r, self.dialect)
                                && canonical(l, self.dialect) == *result =>
                        {
                            Some(l.clone())
                        }
                        _ => None,
                    });
                QueryOutput {
                    name,
                    value,
                    cast_type,
                }
            })
            .collect()
    }
}

pub(crate) fn query_columns(
    query: &Expression,
    dialect: Option<DialectType>,
) -> Vec<(String, DataType)> {
    let dialect = dialect.unwrap_or_default();
    OutputResolver::new(dialect)
        .resolve(query)
        .into_iter()
        .map(|output| {
            let data_type = match &output.value {
                OutputType::Known(t) => t.clone(),
                OutputType::StringLiteral(_) => text_type(dialect),
                _ => DataType::Unknown,
            };
            (output.name, data_type)
        })
        .collect()
}

fn projection_output(expr: &Expression, dialect: DialectType) -> QueryOutput {
    let (name, mut inner) = match expr {
        Expression::Alias(alias) => (crate::binding::identifier_name(&alias.alias), &alias.this),
        Expression::Column(column) => (crate::binding::identifier_name(&column.name), expr),
        Expression::Identifier(id) => (crate::binding::identifier_name(id), expr),
        Expression::Star(_) => ("*".into(), expr),
        _ => (String::new(), expr),
    };
    loop {
        inner = match inner {
            Expression::Paren(p) => &p.this,
            Expression::Annotated(a) => &a.this,
            _ => break,
        };
    }
    let cast_type = match inner {
        Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
            Some(c.to.clone())
        }
        _ => None,
    };
    let value = match inner {
        Expression::Null(_) => OutputType::Null,
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::String(s) => OutputType::StringLiteral(s.clone()),
            Literal::Number(value)
                if matches!(
                    family(dialect),
                    Family::Postgres | Family::Trino | Family::Tsql | Family::MySql
                ) && value.contains('.')
                    && !value.contains(['e', 'E']) =>
            {
                let digits = value.trim_start_matches(['+', '-']);
                let scale = digits.split_once('.').map_or(0, |(_, s)| s.len() as u32);
                let precision = digits.bytes().filter(u8::is_ascii_digit).count().max(1) as u32;
                // PostgreSQL numeric literals have no declared typmod.
                let parameterized = family(dialect) != Family::Postgres;
                OutputType::Known(DataType::Decimal {
                    precision: parameterized.then_some(precision),
                    scale: parameterized.then_some(scale),
                })
            }
            _ => inferred_output(expr, inner, dialect),
        },
        _ => inferred_output(expr, inner, dialect),
    };
    QueryOutput {
        name,
        value,
        cast_type,
    }
}

fn inferred_output(expr: &Expression, inner: &Expression, dialect: DialectType) -> OutputType {
    if matches!(inner, Expression::Function(func)
        if !super::annotate_types::TypeAnnotator::function_result_is_known(func, dialect))
    {
        return OutputType::Unknown;
    }
    if let Expression::Literal(literal) = inner {
        return super::annotate_types::TypeAnnotator::annotate_literal(literal)
            .map(|t| OutputType::Known(canonical(&t, dialect)))
            .unwrap_or(OutputType::Unknown);
    }
    if matches!(inner, Expression::Boolean(_)) {
        return OutputType::Known(DataType::Boolean);
    }
    expr.inferred_type()
        .or_else(|| inner.inferred_type())
        .filter(|t| !contains_unknown(t))
        .map(|t| OutputType::Known(canonical(t, dialect)))
        .unwrap_or(OutputType::Unknown)
}

#[derive(Clone, Copy, PartialEq)]
enum Family {
    Standard,
    Postgres,
    MySql,
    BigQuery,
    Snowflake,
    DuckDB,
    SQLite,
    Hive,
    Spark,
    Databricks,
    Trino,
    Redshift,
    Tsql,
    Oracle,
    ClickHouse,
    Teradata,
    Arrow,
    Limited,
}

// Exhaustive: adding a dialect requires an explicit typing policy.
fn family(dialect: DialectType) -> Family {
    use DialectType::*;
    match dialect {
        Generic => Family::Standard,
        PostgreSQL | Materialize | RisingWave => Family::Postgres,
        CockroachDB => Family::Postgres,
        MySQL | TiDB | SingleStore => Family::MySql,
        BigQuery => Family::BigQuery,
        Snowflake => Family::Snowflake,
        DuckDB => Family::DuckDB,
        SQLite => Family::SQLite,
        Hive => Family::Hive,
        Spark => Family::Spark,
        Databricks => Family::Databricks,
        Trino | Presto | Athena | Dune => Family::Trino,
        Redshift => Family::Redshift,
        TSQL | Fabric => Family::Tsql,
        Oracle => Family::Oracle,
        ClickHouse => Family::ClickHouse,
        Teradata => Family::Teradata,
        DataFusion => Family::Arrow,
        Doris | StarRocks => Family::Standard,
        Drill | Dremio => Family::Limited,
        Exasol => Family::Standard,
        HANA => Family::Limited,
        Druid | Solr | Tableau => Family::Limited,
    }
}

fn combine(
    left: &OutputType,
    right: &OutputType,
    dialect: DialectType,
    depth: usize,
) -> OutputType {
    use OutputType::*;
    if depth > 64 || matches!(left, Unknown) || matches!(right, Unknown) {
        return Unknown;
    }
    match (left, right) {
        (Null, Null) => match family(dialect) {
            Family::Postgres => Known(DataType::Text),
            Family::BigQuery => Known(canonical(&DataType::BigInt { length: None }, dialect)),
            Family::DuckDB | Family::Tsql => Known(int_type(32)),
            _ => Null,
        },
        (Null, value) | (value, Null) => match value {
            _ if family(dialect) == Family::SQLite => Unknown,
            StringLiteral(_) => Known(text_type(dialect)),
            Known(t) if family(dialect) == Family::ClickHouse => Known(nullable(t.clone())),
            _ => value.clone(),
        },
        (StringLiteral(_), StringLiteral(_)) => Known(text_type(dialect)),
        (StringLiteral(value), Known(t)) | (Known(t), StringLiteral(value)) => {
            if is_text(t) {
                return Known(text_type(dialect));
            }
            // PostgreSQL untyped string literals and BigQuery temporal literals
            // differ from typed VARCHAR expressions. Do not generalize this to columns.
            if family(dialect) == Family::Postgres
                || (family(dialect) == Family::BigQuery
                    && temporal(t)
                    && temporal_literal(value, t))
            {
                return Known(t.clone());
            }
            common_type(&text_type(dialect), t, dialect, depth + 1)
                .map(Known)
                .unwrap_or(Unknown)
        }
        (Known(l), Known(r)) => common_type(l, r, dialect, depth + 1)
            .map(Known)
            .unwrap_or(Unknown),
        _ => Unknown,
    }
}

pub(crate) fn common_type(
    left: &DataType,
    right: &DataType,
    dialect: DialectType,
    depth: usize,
) -> Option<DataType> {
    if depth > 64 || contains_unknown(left) || contains_unknown(right) {
        return None;
    }
    let left = canonical(left, dialect);
    let right = canonical(right, dialect);
    let (l, r) = (&left, &right);
    let f = family(dialect);
    if l == r {
        return Some(left);
    }
    if f == Family::Oracle {
        if let Some(result) = oracle_common(l, r) {
            return Some(result);
        }
    }
    if f == Family::SQLite {
        return None;
    }
    if f == Family::Teradata {
        return ((numeric(l) && numeric(r))
            || (is_text(l) && is_text(r))
            || (temporal(l) && temporal(r))
            || (is_binary(l) && is_binary(r)))
        .then_some(left);
    }
    if let DataType::Nullable { inner } = l {
        let other = if let DataType::Nullable { inner } = r {
            inner.as_ref()
        } else {
            r
        };
        return common_type(inner, other, dialect, depth + 1).map(nullable);
    }
    if let DataType::Nullable { inner } = r {
        return common_type(l, inner, dialect, depth + 1).map(nullable);
    }
    if let (Some((lb, lu)), Some((rb, ru))) = (integer(l), integer(r)) {
        if lu == ru {
            return Some(if lb >= rb { left } else { right });
        }
        if f == Family::DuckDB {
            return super::annotate_types::duckdb_unsigned_integer_coercion(l, r, false);
        }
        if matches!(f, Family::ClickHouse | Family::Arrow) {
            if f == Family::Arrow && ((lu && lb == 64) || (ru && rb == 64)) {
                return Some(DataType::Decimal {
                    precision: Some(20),
                    scale: Some(0),
                });
            }
            let required = if lu {
                (lb + 1).max(rb)
            } else {
                (rb + 1).max(lb)
            };
            if f == Family::ClickHouse && required == 65 {
                return None;
            }
            return [8, 16, 32, 64, 128]
                .into_iter()
                .find(|width| *width >= required)
                .map(int_type);
        }
        return None;
    }
    if numeric(l) && numeric(r) {
        if f == Family::Arrow && (decimal(l) || decimal(r)) {
            return decimal_common(l, r, dialect);
        }
        if floating(l) || floating(r) {
            if f == Family::Limited {
                return None;
            }
            if f == Family::Spark
                && (matches!(l, DataType::Float { .. }) || matches!(r, DataType::Float { .. }))
            {
                return None; // ANSI vs legacy widening differs; options do not select the mode.
            }
            if f == Family::ClickHouse && (decimal(l) || decimal(r)) {
                return None;
            }
            if f == Family::ClickHouse {
                let bits = integer(l)
                    .or_else(|| integer(r))
                    .map_or(0, |(bits, _)| bits);
                if bits > 53 {
                    return None;
                }
                if bits > 24 {
                    return Some(double_type());
                }
            }
            if matches!(f, Family::Databricks | Family::MySql)
                || matches!(l, DataType::Double { .. })
                || matches!(r, DataType::Double { .. })
            {
                return Some(double_type());
            }
            return Some(if floating(l) { left } else { right });
        }
        if decimal(l) || decimal(r) {
            return decimal_common(l, r, dialect);
        }
    }
    if matches!(f, Family::DuckDB | Family::MySql) {
        if matches!(l, DataType::Boolean) && integer(r).is_some() {
            return Some(right);
        }
        if matches!(r, DataType::Boolean) && integer(l).is_some() {
            return Some(left);
        }
    }
    if is_text(l) && is_text(r) {
        return text_common(l, r, dialect);
    }
    if is_binary(l) && is_binary(r) {
        if f == Family::Tsql
            && matches!(l, DataType::Binary { .. })
            && matches!(r, DataType::Binary { .. })
        {
            return Some(DataType::Binary {
                length: max_length(type_length(l), type_length(r)),
            });
        }
        return Some(DataType::VarBinary {
            length: max_length(type_length(l), type_length(r)),
        });
    }
    if f == Family::DuckDB
        && (is_text(l) || is_text(r))
        && (numeric(l)
            || numeric(r)
            || temporal(l)
            || temporal(r)
            || matches!(l, DataType::Boolean)
            || matches!(r, DataType::Boolean))
    {
        return Some(DataType::VarChar {
            length: None,
            parenthesized_length: false,
        });
    }
    if matches!(f, Family::MySql | Family::Arrow)
        && ((is_text(l) && numeric(r)) || (is_text(r) && numeric(l)))
    {
        return Some(text_type(dialect));
    }
    if temporal(l) && temporal(r) {
        return temporal_common(l, r, dialect);
    }
    if let (
        DataType::Array {
            element_type: le,
            dimension: ld,
        },
        DataType::Array {
            element_type: re,
            dimension: rd,
        },
    ) = (l, r)
    {
        if !nested_supported(f) {
            return None;
        }
        // BigQuery's ARRAY supertype requires the same element type.
        if f == Family::BigQuery && le != re {
            return None;
        }
        return common_type(le, re, dialect, depth + 1).map(|element_type| DataType::Array {
            element_type: Box::new(element_type),
            dimension: if ld == rd { *ld } else { None },
        });
    }
    if let (DataType::List { element_type: le }, DataType::List { element_type: re }) = (l, r) {
        if dialect != DialectType::Materialize {
            return None;
        }
        return common_type(le, re, dialect, depth + 1).map(|element_type| DataType::List {
            element_type: Box::new(element_type),
        });
    }
    if let (
        DataType::Map {
            key_type: lk,
            value_type: lv,
        },
        DataType::Map {
            key_type: rk,
            value_type: rv,
        },
    ) = (l, r)
    {
        if !nested_supported(f) {
            return None;
        }
        return Some(DataType::Map {
            key_type: Box::new(common_type(lk, rk, dialect, depth + 1)?),
            value_type: Box::new(common_type(lv, rv, dialect, depth + 1)?),
        });
    }
    if let (DataType::Struct { fields: lf, nested }, DataType::Struct { fields: rf, .. }) = (l, r) {
        if !nested_supported(f) {
            return None;
        }
        let mut fields = Vec::new();
        if f == Family::DuckDB {
            for field in lf {
                let mut field = field.clone();
                if let Some(other) = rf.iter().find(|r| r.name.eq_ignore_ascii_case(&field.name)) {
                    field.data_type =
                        common_type(&field.data_type, &other.data_type, dialect, depth + 1)?;
                }
                fields.push(field);
            }
            fields.extend(
                rf.iter()
                    .filter(|r| !lf.iter().any(|l| l.name.eq_ignore_ascii_case(&r.name)))
                    .cloned(),
            );
        } else {
            if lf.len() != rf.len() {
                return None;
            }
            for (index, l) in lf.iter().enumerate() {
                let r = if f == Family::Arrow {
                    rf.iter().find(|r| r.name == l.name)?
                } else {
                    &rf[index]
                };
                if f == Family::BigQuery && l.data_type != r.data_type {
                    return None;
                }
                fields.push(StructField {
                    data_type: common_type(&l.data_type, &r.data_type, dialect, depth + 1)?,
                    ..l.clone()
                });
            }
        }
        return Some(DataType::Struct {
            fields,
            nested: *nested,
        });
    }
    None
}

fn nested_supported(f: Family) -> bool {
    matches!(
        f,
        Family::DuckDB
            | Family::BigQuery
            | Family::Postgres
            | Family::Trino
            | Family::Hive
            | Family::Spark
            | Family::Databricks
            | Family::Arrow
            | Family::ClickHouse
    )
}

fn decimal_common(l: &DataType, r: &DataType, dialect: DialectType) -> Option<DataType> {
    let f = family(dialect);
    if f == Family::Limited {
        return None;
    }
    if f == Family::BigQuery {
        let big = |t: &DataType| matches!(t, DataType::Custom { name } if name.eq_ignore_ascii_case("BIGNUMERIC") || name.eq_ignore_ascii_case("BIGDECIMAL"));
        return Some(if big(l) || big(r) {
            DataType::Custom {
                name: "BIGNUMERIC".into(),
            }
        } else {
            DataType::Decimal {
                precision: None,
                scale: None,
            }
        });
    }
    if f == Family::Postgres {
        return Some(DataType::Decimal {
            precision: None,
            scale: None,
        });
    }
    let (lp, ls) = decimal_shape(l, dialect)?;
    let (rp, rs) = decimal_shape(r, dialect)?;
    let scale = ls.max(rs);
    let integral = (lp - ls).max(rp - rs);
    let precision = integral.checked_add(scale)?;
    let limit = match dialect {
        DialectType::MySQL | DialectType::TiDB => 65,
        DialectType::Exasol => 36,
        DialectType::ClickHouse => 76,
        _ => 38,
    };
    let (precision, scale) = if precision <= limit {
        (precision, scale)
    } else if matches!(f, Family::Tsql | Family::DuckDB | Family::Databricks) && integral <= limit {
        (limit, limit - integral)
    } else {
        return None;
    };
    Some(DataType::Decimal {
        precision: Some(precision),
        scale: Some(scale),
    })
}

fn decimal_shape(t: &DataType, dialect: DialectType) -> Option<(u32, u32)> {
    if dialect == DialectType::DataFusion {
        match t {
            DataType::Float { .. } => return Some((14, 7)),
            DataType::Double { .. } => return Some((30, 15)),
            DataType::BigInt { .. } | DataType::UInt64 => return Some((20, 0)),
            _ => {}
        }
    }
    if let DataType::Decimal {
        precision: Some(p),
        scale,
    } = t
    {
        let s = scale.unwrap_or(0);
        return (s <= *p).then_some((*p, s));
    }
    if let DataType::Decimal {
        precision: None, ..
    } = t
    {
        return None;
    }
    let (width, unsigned) = integer(t)?;
    if dialect == DialectType::Snowflake {
        return Some((38, 0));
    }
    let digits = match (width, unsigned) {
        (8, _) => 3,
        (16, _) => 5,
        (32, _) => 10,
        (64, false) => 19,
        (64, true) => 20,
        (128, _) => 39,
        _ => return None,
    };
    Some((digits, 0))
}

fn temporal_common(l: &DataType, r: &DataType, dialect: DialectType) -> Option<DataType> {
    if matches!(family(dialect), Family::BigQuery | Family::Limited) {
        return None;
    }
    match (l, r) {
        (DataType::Date, DataType::Timestamp { .. }) => Some(r.clone()),
        (DataType::Timestamp { .. }, DataType::Date) => Some(l.clone()),
        (
            DataType::Timestamp {
                precision: lp,
                timezone: lz,
            },
            DataType::Timestamp {
                precision: rp,
                timezone: rz,
            },
        ) if lz == rz => Some(DataType::Timestamp {
            precision: max_length(*lp, *rp),
            timezone: *lz,
        }),
        (
            DataType::Time {
                precision: lp,
                timezone: lz,
            },
            DataType::Time {
                precision: rp,
                timezone: rz,
            },
        ) if lz == rz => Some(DataType::Time {
            precision: max_length(*lp, *rp),
            timezone: *lz,
        }),
        _ => None,
    }
}

fn canonical(t: &DataType, dialect: DialectType) -> DataType {
    if let DataType::Custom { name } = t {
        if dialect == DialectType::BigQuery && name.eq_ignore_ascii_case("FLOAT64") {
            return double_type();
        }
        if dialect == DialectType::ClickHouse {
            match name.to_ascii_uppercase().as_str() {
                "INT8" => return int_type(8),
                "INT16" => return int_type(16),
                "INT32" => return int_type(32),
                "INT64" => return int_type(64),
                "FLOAT32" => {
                    return DataType::Float {
                        precision: None,
                        scale: None,
                        real_spelling: false,
                    }
                }
                "FLOAT64" => return double_type(),
                _ => {}
            }
        }
    }
    match t {
        DataType::Array {
            element_type,
            dimension,
        } => DataType::Array {
            element_type: Box::new(canonical(element_type, dialect)),
            dimension: *dimension,
        },
        DataType::List { element_type } => DataType::List {
            element_type: Box::new(canonical(element_type, dialect)),
        },
        DataType::Map {
            key_type,
            value_type,
        } => DataType::Map {
            key_type: Box::new(canonical(key_type, dialect)),
            value_type: Box::new(canonical(value_type, dialect)),
        },
        DataType::Nullable { inner } => DataType::Nullable {
            inner: Box::new(canonical(inner, dialect)),
        },
        DataType::Struct { fields, nested } => DataType::Struct {
            fields: fields
                .iter()
                .map(|field| StructField {
                    data_type: canonical(&field.data_type, dialect),
                    ..field.clone()
                })
                .collect(),
            nested: *nested,
        },
        DataType::Float {
            precision,
            scale: None,
            real_spelling,
        } if matches!(
            family(dialect),
            Family::Postgres | Family::Tsql | Family::Redshift
        ) =>
        {
            if *real_spelling || precision.is_some_and(|p| p <= 24) {
                DataType::Float {
                    precision: None,
                    scale: None,
                    real_spelling: true,
                }
            } else if precision.is_none_or(|p| p <= 53) {
                double_type()
            } else {
                DataType::Unknown
            }
        }
        DataType::Float { .. } | DataType::Double { .. } if dialect == DialectType::Snowflake => {
            DataType::Float {
                precision: None,
                scale: None,
                real_spelling: false,
            }
        }
        DataType::Float { .. } if dialect == DialectType::BigQuery => double_type(),
        DataType::TinyInt { .. }
        | DataType::SmallInt { .. }
        | DataType::Int { .. }
        | DataType::BigInt { .. }
            if dialect == DialectType::BigQuery =>
        {
            DataType::Custom {
                name: "INT64".into(),
            }
        }
        DataType::Decimal { .. } if dialect == DialectType::Druid => double_type(),
        DataType::Float {
            real_spelling: true,
            ..
        } if dialect == DialectType::Druid => double_type(),
        DataType::String { .. } | DataType::VarChar { .. } | DataType::Text
            if dialect == DialectType::BigQuery =>
        {
            DataType::String { length: None }
        }
        DataType::Int { length, .. } => DataType::Int {
            length: *length,
            integer_spelling: false,
        },
        DataType::Float {
            precision, scale, ..
        } => DataType::Float {
            precision: *precision,
            scale: *scale,
            real_spelling: false,
        },
        DataType::VarChar { length, .. } => DataType::VarChar {
            length: *length,
            parenthesized_length: false,
        },
        DataType::Decimal { .. } if dialect == DialectType::BigQuery => DataType::Decimal {
            precision: None,
            scale: None,
        },
        _ => t.clone(),
    }
}

fn oracle_common(l: &DataType, r: &DataType) -> Option<DataType> {
    use crate::expressions::OracleCharacterKind;
    use OracleDataType::*;
    let rank = |t: &DataType| match t {
        DataType::Oracle {
            oracle_type: BinaryDouble,
        }
        | DataType::Double { .. } => Some(2),
        DataType::Oracle {
            oracle_type: BinaryFloat,
        } => Some(1),
        DataType::Oracle {
            oracle_type: Number { .. } | Float { .. },
        } => Some(0),
        _ if numeric(t) => Some(0),
        _ => None,
    };
    if let (Some(l), Some(r)) = (rank(l), rank(r)) {
        return Some(DataType::Oracle {
            oracle_type: match l.max(r) {
                2 => BinaryDouble,
                1 => BinaryFloat,
                _ => Number {
                    precision: None,
                    scale: None,
                },
            },
        });
    }
    if let (
        DataType::Oracle {
            oracle_type:
                Character {
                    kind: lk,
                    length: ll,
                    semantics: ls,
                },
        },
        DataType::Oracle {
            oracle_type:
                Character {
                    kind: rk,
                    length: rl,
                    semantics: rs,
                },
        },
    ) = (l, r)
    {
        let national = |k: &OracleCharacterKind| {
            matches!(
                k,
                OracleCharacterKind::NChar | OracleCharacterKind::NVarChar
            )
        };
        if national(lk) != national(rk) || ls != rs {
            return None;
        }
        let kind = if lk == rk && ll == rl {
            *lk
        } else if national(lk) {
            OracleCharacterKind::NVarChar
        } else {
            OracleCharacterKind::VarChar
        };
        return Some(DataType::Oracle {
            oracle_type: Character {
                kind,
                length: max_length(*ll, *rl),
                semantics: *ls,
            },
        });
    }
    None
}

fn contains_unknown(t: &DataType) -> bool {
    match t {
        DataType::Unknown => true,
        DataType::Array { element_type, .. } | DataType::List { element_type } => {
            contains_unknown(element_type)
        }
        DataType::Nullable { inner } => contains_unknown(inner),
        DataType::Map {
            key_type,
            value_type,
        } => contains_unknown(key_type) || contains_unknown(value_type),
        DataType::Struct { fields, .. } => fields
            .iter()
            .any(|field| contains_unknown(&field.data_type)),
        DataType::Object { fields, .. } => fields.iter().any(|(_, t, _)| contains_unknown(t)),
        DataType::Union { fields } => fields.iter().any(|(_, t)| contains_unknown(t)),
        DataType::Vector { element_type, .. } => {
            element_type.as_deref().is_some_and(contains_unknown)
        }
        _ => false,
    }
}

fn integer(t: &DataType) -> Option<(u16, bool)> {
    Some(match t {
        DataType::TinyInt { .. } => (8, false),
        DataType::SmallInt { .. } => (16, false),
        DataType::Int { .. } => (32, false),
        DataType::BigInt { .. } => (64, false),
        DataType::Custom { name } if name.eq_ignore_ascii_case("INT64") => (64, false),
        DataType::Int128 => (128, false),
        DataType::UInt8 => (8, true),
        DataType::UInt16 => (16, true),
        DataType::UInt32 => (32, true),
        DataType::UInt64 => (64, true),
        DataType::UInt128 => (128, true),
        _ => return None,
    })
}
fn int_type(bits: u16) -> DataType {
    match bits {
        0..=8 => DataType::TinyInt { length: None },
        9..=16 => DataType::SmallInt { length: None },
        17..=32 => DataType::Int {
            length: None,
            integer_spelling: false,
        },
        33..=64 => DataType::BigInt { length: None },
        _ => DataType::Int128,
    }
}
fn double_type() -> DataType {
    DataType::Double {
        precision: None,
        scale: None,
    }
}
fn decimal(t: &DataType) -> bool {
    matches!(t, DataType::Decimal { .. })
        || matches!(t, DataType::Custom { name } if name.eq_ignore_ascii_case("BIGNUMERIC") || name.eq_ignore_ascii_case("BIGDECIMAL"))
}
fn floating(t: &DataType) -> bool {
    matches!(t, DataType::Float { .. } | DataType::Double { .. })
}
fn numeric(t: &DataType) -> bool {
    integer(t).is_some() || floating(t) || decimal(t)
}
fn temporal(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Date | DataType::Time { .. } | DataType::Timestamp { .. }
    )
}
fn is_text(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::String { .. }
            | DataType::Text
            | DataType::TextWithLength { .. }
    )
}
fn is_binary(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Binary { .. } | DataType::VarBinary { .. } | DataType::Blob
    )
}
fn type_length(t: &DataType) -> Option<u32> {
    match t {
        DataType::Char { length }
        | DataType::VarChar { length, .. }
        | DataType::String { length }
        | DataType::Binary { length }
        | DataType::VarBinary { length } => *length,
        DataType::TextWithLength { length } => Some(*length),
        _ => None,
    }
}
fn max_length(l: Option<u32>, r: Option<u32>) -> Option<u32> {
    l.zip(r).map(|(l, r)| l.max(r))
}
fn text_type(dialect: DialectType) -> DataType {
    match family(dialect) {
        Family::BigQuery | Family::Hive | Family::Spark | Family::Databricks => {
            DataType::String { length: None }
        }
        Family::Postgres => DataType::Text,
        _ => DataType::VarChar {
            length: None,
            parenthesized_length: false,
        },
    }
}
fn text_common(l: &DataType, r: &DataType, dialect: DialectType) -> Option<DataType> {
    if family(dialect) == Family::Postgres {
        return Some(
            if matches!(l, DataType::Text) || matches!(r, DataType::Text) {
                DataType::Text
            } else {
                DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                }
            },
        );
    }
    if family(dialect) == Family::DuckDB {
        return Some(text_type(dialect));
    }
    if matches!(
        family(dialect),
        Family::BigQuery | Family::Spark | Family::Databricks
    ) || (family(dialect) == Family::Hive
        && (matches!(l, DataType::String { .. } | DataType::Text)
            || matches!(r, DataType::String { .. } | DataType::Text)))
    {
        return Some(text_type(dialect));
    }
    if matches!(family(dialect), Family::Tsql | Family::Trino | Family::Hive)
        && matches!(l, DataType::Char { .. })
        && matches!(r, DataType::Char { .. })
    {
        return Some(DataType::Char {
            length: max_length(type_length(l), type_length(r)),
        });
    }
    Some(DataType::VarChar {
        length: max_length(type_length(l), type_length(r)),
        parenthesized_length: false,
    })
}
fn nullable(t: DataType) -> DataType {
    if matches!(t, DataType::Nullable { .. }) {
        t
    } else {
        DataType::Nullable { inner: Box::new(t) }
    }
}
fn temporal_literal(value: &str, t: &DataType) -> bool {
    // Validate only unambiguous ISO literals; other formats remain unresolved.
    fn date(value: &str) -> bool {
        let parts = value
            .split('-')
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>();
        let Ok(parts) = parts else { return false };
        let [year, month, day] = parts.as_slice() else {
            return false;
        };
        let days = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
            2 => 28,
            _ => return false,
        };
        value.len() == 10 && *year > 0 && *year <= 9999 && *day > 0 && *day <= days
    }
    fn time(value: &str) -> bool {
        let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
        let parts = seconds
            .split(':')
            .map(str::parse::<u32>)
            .collect::<Result<Vec<_>, _>>();
        let Ok(parts) = parts else { return false };
        let [h, m, s] = parts.as_slice() else {
            return false;
        };
        seconds.len() == 8
            && *h < 24
            && *m < 60
            && *s < 60
            && fraction.len() <= 6
            && fraction.bytes().all(|b| b.is_ascii_digit())
    }
    match t {
        DataType::Date => date(value),
        DataType::Time { .. } => time(value),
        DataType::Timestamp { .. } => value
            .trim_end_matches('Z')
            .split_once([' ', 'T'])
            .is_some_and(|(d, t)| date(d) && time(t)),
        _ => false,
    }
}
