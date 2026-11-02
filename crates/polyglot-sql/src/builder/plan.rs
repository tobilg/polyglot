//! Binding-neutral, SQLGlot-compatible builder operations.
//!
//! The ordinary [`crate::builder`] module is an ergonomic Rust API. This module
//! provides a serializable, immutable expression plan so language bindings can
//! share coercion, parsing, and AST-editing semantics without duplicating them.

use crate::ast_mutation as engine;
use crate::builder::{self, Expr};
use crate::dialects::Dialect;
use crate::error::{Error, Result};
use crate::expressions::*;
use crate::generator::NotInStyle;
use crate::Expression;
use serde::{Deserialize, Serialize};
#[cfg(feature = "bindings")]
use ts_rs::TS;

pub const BUILDER_PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum BuilderValue {
    Null,
    Bool(bool),
    Integer(#[cfg_attr(feature = "bindings", ts(type = "number"))] i64),
    Float(f64),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    Ilike,
    Rlike,
    Is,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum UnaryOperator {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

impl std::convert::From<BinaryOperator> for engine::BinaryKind {
    fn from(value: BinaryOperator) -> Self {
        match value {
            BinaryOperator::Eq => Self::Eq,
            BinaryOperator::Neq => Self::Neq,
            BinaryOperator::Lt => Self::Lt,
            BinaryOperator::Lte => Self::Lte,
            BinaryOperator::Gt => Self::Gt,
            BinaryOperator::Gte => Self::Gte,
            BinaryOperator::And => Self::And,
            BinaryOperator::Or => Self::Or,
            BinaryOperator::Xor => Self::Xor,
            BinaryOperator::Add => Self::Add,
            BinaryOperator::Sub => Self::Sub,
            BinaryOperator::Mul => Self::Mul,
            BinaryOperator::Div => Self::Div,
            BinaryOperator::Mod => Self::Mod,
            BinaryOperator::Like => Self::Like,
            BinaryOperator::Ilike => Self::ILike,
            BinaryOperator::Rlike => Self::RLike,
            BinaryOperator::Is => Self::Is,
        }
    }
}

impl std::convert::From<UnaryOperator> for engine::UnaryKind {
    fn from(value: UnaryOperator) -> Self {
        match value {
            UnaryOperator::Not => Self::Not,
            UnaryOperator::Neg => Self::Neg,
            UnaryOperator::IsNull => Self::IsNull,
            UnaryOperator::IsNotNull => Self::IsNotNull,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    #[default]
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum LockType {
    Update,
    Share,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum BuiltinFunction {
    Count,
    CountStar,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    ApproxDistinct,
    Upper,
    Lower,
    Length,
    Trim,
    Ltrim,
    Rtrim,
    Reverse,
    Initcap,
    Substring,
    Replace,
    ConcatWs,
    Coalesce,
    NullIf,
    IfNull,
    Abs,
    Round,
    Floor,
    Ceil,
    Power,
    Sqrt,
    Ln,
    Exp,
    Sign,
    Greatest,
    Least,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildNode {
    Plan {
        plan: Box<BuilderPlan>,
    },
    Ast {
        expression: Box<Expression>,
    },
    Sql {
        sql: String,
    },
    Literal {
        value: BuilderValue,
    },
    Column {
        name: String,
    },
    Table {
        name: String,
    },
    Star,
    Function {
        name: String,
        args: Vec<BuildNode>,
    },
    Builtin {
        function: BuiltinFunction,
        #[serde(default)]
        args: Vec<BuildNode>,
    },
    Extract {
        field: String,
        expression: Box<BuildNode>,
    },
    Binary {
        op: BinaryOperator,
        left: Box<BuildNode>,
        right: Box<BuildNode>,
    },
    Unary {
        op: UnaryOperator,
        expression: Box<BuildNode>,
    },
    Alias {
        expression: Box<BuildNode>,
        alias: String,
    },
    Cast {
        expression: Box<BuildNode>,
        to: String,
    },
    Between {
        expression: Box<BuildNode>,
        low: Box<BuildNode>,
        high: Box<BuildNode>,
    },
    InList {
        expression: Box<BuildNode>,
        values: Vec<BuildNode>,
        #[serde(default)]
        negated: bool,
    },
    Ordered {
        expression: Box<BuildNode>,
        #[serde(default)]
        desc: bool,
    },
    Select {
        expressions: Vec<BuildNode>,
    },
    Case {
        operand: Option<Box<BuildNode>>,
    },
    Update {
        table: String,
        #[serde(default)]
        assignments: Vec<BuilderAssignment>,
        where_clause: Option<Box<BuildNode>>,
        from: Option<String>,
    },
    Delete {
        table: String,
        where_clause: Option<Box<BuildNode>>,
    },
    Insert {
        into: String,
        expression: Option<Box<BuildNode>>,
        #[serde(default)]
        columns: Vec<String>,
    },
    Merge {
        target: String,
    },
}

/// An immutable builder program. The base expression is evaluated once and the
/// operations are applied from left to right.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BuilderPlan {
    pub base: BuildNode,
    #[serde(default)]
    pub operations: Vec<BuildOperation>,
}

impl BuilderPlan {
    pub fn new(base: BuildNode) -> Self {
        Self {
            base,
            operations: Vec::new(),
        }
    }

    pub fn apply(mut self, operation: BuildOperation) -> Self {
        self.operations.push(operation);
        self
    }

    pub fn into_node(self) -> BuildNode {
        BuildNode::Plan {
            plan: Box::new(self),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BuilderAssignment {
    pub column: String,
    pub value: BuildNode,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildOperation {
    Select {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    From {
        source: BuildNode,
    },
    Join {
        source: BuildNode,
        on: Option<BuildNode>,
        #[serde(default)]
        join_type: JoinType,
    },
    Where {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    GroupBy {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    Having {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    OrderBy {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    SortBy {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    Limit {
        expression: BuildNode,
    },
    Offset {
        expression: BuildNode,
    },
    Distinct {
        #[serde(default = "default_true")]
        enabled: bool,
    },
    Qualify {
        expressions: Vec<BuildNode>,
        #[serde(default = "default_true")]
        append: bool,
    },
    LateralView {
        expression: BuildNode,
        table_alias: Option<String>,
        #[serde(default)]
        column_aliases: Vec<String>,
        #[serde(default)]
        outer: bool,
    },
    Window {
        name: String,
        #[serde(default)]
        partition_by: Vec<BuildNode>,
        #[serde(default)]
        order_by: Vec<BuildNode>,
    },
    Lock {
        lock_type: LockType,
    },
    Hint {
        text: String,
    },
    Ctas {
        table: String,
        #[serde(default)]
        replace: bool,
        #[serde(default)]
        temporary: bool,
    },
    Subquery {
        alias: Option<String>,
    },
    Union {
        other: BuildNode,
        #[serde(default = "default_true")]
        distinct: bool,
    },
    Intersect {
        other: BuildNode,
        #[serde(default = "default_true")]
        distinct: bool,
    },
    Except {
        other: BuildNode,
        #[serde(default = "default_true")]
        distinct: bool,
    },
    When {
        condition: BuildNode,
        result: BuildNode,
    },
    Else {
        result: BuildNode,
    },
    Set {
        assignments: Vec<BuilderAssignment>,
    },
    InsertColumns {
        columns: Vec<String>,
    },
    Values {
        rows: Vec<Vec<BuildNode>>,
        #[serde(default = "default_true")]
        append: bool,
    },
    Query {
        query: BuildNode,
    },
    MergeUsing {
        source: BuildNode,
        on: BuildNode,
    },
    WhenMatchedUpdate {
        assignments: Vec<BuilderAssignment>,
        condition: Option<BuildNode>,
    },
    WhenMatchedDelete {
        condition: Option<BuildNode>,
    },
    WhenNotMatchedInsert {
        columns: Vec<String>,
        values: Vec<BuildNode>,
        condition: Option<BuildNode>,
    },
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuilderOutput {
    Ast,
    Sql {
        #[serde(default = "generic_dialect")]
        dialect: String,
    },
}

fn generic_dialect() -> String {
    "generic".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BuildRequest {
    #[serde(default = "protocol_version")]
    pub version: u8,
    #[serde(default = "generic_dialect")]
    pub read_dialect: String,
    pub plan: BuilderPlan,
    #[serde(default = "ast_output")]
    pub output: BuilderOutput,
}

fn protocol_version() -> u8 {
    BUILDER_PROTOCOL_VERSION
}

fn ast_output() -> BuilderOutput {
    BuilderOutput::Ast
}

#[derive(Debug, Clone, PartialEq)]
pub enum BuildResult {
    Ast(Expression),
    Sql(String),
}

pub fn execute(request: &BuildRequest) -> Result<BuildResult> {
    if request.version != BUILDER_PROTOCOL_VERSION {
        return Err(Error::invalid_input(format!(
            "unsupported builder protocol version {}; expected {}",
            request.version, BUILDER_PROTOCOL_VERSION
        )));
    }
    let expression = evaluate(&request.plan, &request.read_dialect)?;
    match &request.output {
        BuilderOutput::Ast => Ok(BuildResult::Ast(expression)),
        BuilderOutput::Sql { dialect } => {
            let dialect = resolve_dialect(dialect)?;
            dialect
                .generate_with_overrides(&expression, |config| {
                    config.not_in_style = NotInStyle::Infix;
                })
                .map(BuildResult::Sql)
        }
    }
}

pub fn evaluate(plan: &BuilderPlan, read_dialect: &str) -> Result<Expression> {
    Evaluator::new(read_dialect)?.evaluate_plan(plan)
}

struct Evaluator {
    dialect: Dialect,
}

impl Evaluator {
    fn new(name: &str) -> Result<Self> {
        Ok(Self {
            dialect: resolve_dialect(name)?,
        })
    }

    fn evaluate_plan(&self, plan: &BuilderPlan) -> Result<Expression> {
        let mut expression = if matches!(
            plan.operations.first(),
            Some(
                BuildOperation::Union { .. }
                    | BuildOperation::Intersect { .. }
                    | BuildOperation::Except { .. }
            )
        ) {
            self.evaluate_query(&plan.base)?
        } else {
            self.evaluate_node(&plan.base)?
        };
        for operation in &plan.operations {
            if matches!(
                operation,
                BuildOperation::Union { .. }
                    | BuildOperation::Intersect { .. }
                    | BuildOperation::Except { .. }
            ) && !engine::is_query(&expression)
            {
                return Err(Error::invalid_input(
                    "set operations require query expressions",
                ));
            }
            expression = self.apply(expression, operation)?;
        }
        Ok(expression)
    }

    fn evaluate_node(&self, node: &BuildNode) -> Result<Expression> {
        match node {
            BuildNode::Plan { plan } => self.evaluate_plan(plan),
            BuildNode::Ast { expression } => Ok((**expression).clone()),
            BuildNode::Sql { sql } => self.parse_expression(sql),
            BuildNode::Literal { value } => Ok(match value {
                BuilderValue::Null => builder::null().into_inner(),
                BuilderValue::Bool(value) => builder::boolean(*value).into_inner(),
                BuilderValue::Integer(value) => builder::lit(*value).into_inner(),
                BuilderValue::Float(value) => builder::lit(*value).into_inner(),
                BuilderValue::String(value) => builder::lit(value.as_str()).into_inner(),
            }),
            BuildNode::Column { name } => Ok(builder::col(name).into_inner()),
            BuildNode::Table { name } => Ok(builder::table(name).into_inner()),
            BuildNode::Star => Ok(builder::star().into_inner()),
            BuildNode::Function { name, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.evaluate_node(arg).map(Expr))
                    .collect::<Result<Vec<_>>>()?;
                Ok(builder::func(name, args).into_inner())
            }
            BuildNode::Builtin { function, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.evaluate_node(arg).map(Expr))
                    .collect::<Result<Vec<_>>>()?;
                self.builtin(*function, args)
            }
            BuildNode::Extract { field, expression } => {
                Ok(builder::extract_(field, Expr(self.evaluate_node(expression)?)).into_inner())
            }
            BuildNode::Binary { op, left, right } => Ok(engine::binary(
                (*op).into(),
                self.evaluate_node(left)?,
                self.evaluate_node(right)?,
            )),
            BuildNode::Unary { op, expression } => {
                Ok(engine::unary((*op).into(), self.evaluate_node(expression)?))
            }
            BuildNode::Alias { expression, alias } => {
                Ok(builder::alias(Expr(self.evaluate_node(expression)?), alias).into_inner())
            }
            BuildNode::Cast { expression, to } => {
                Ok(builder::cast(Expr(self.evaluate_node(expression)?), to).into_inner())
            }
            BuildNode::Between {
                expression,
                low,
                high,
            } => Ok(Expr(self.evaluate_node(expression)?)
                .between(
                    Expr(self.evaluate_node(low)?),
                    Expr(self.evaluate_node(high)?),
                )
                .into_inner()),
            BuildNode::InList {
                expression,
                values,
                negated,
            } => {
                let values = values
                    .iter()
                    .map(|v| self.evaluate_node(v).map(Expr))
                    .collect::<Result<Vec<_>>>()?;
                let expr = Expr(self.evaluate_node(expression)?);
                Ok(if *negated {
                    expr.not_in(values)
                } else {
                    expr.in_list(values)
                }
                .into_inner())
            }
            BuildNode::Ordered { expression, desc } => {
                let expr = Expr(self.evaluate_node(expression)?);
                Ok(if *desc { expr.desc() } else { expr.asc() }.into_inner())
            }
            BuildNode::Select { expressions } => {
                let expressions = self.evaluate_expression_list(expressions)?;
                Ok(Expression::Select(Box::new(Select {
                    expressions,
                    ..Select::new()
                })))
            }
            BuildNode::Case { operand } => Ok(engine::case(
                operand
                    .as_deref()
                    .map(|value| self.evaluate_node(value))
                    .transpose()?,
            )),
            BuildNode::Update {
                table,
                assignments,
                where_clause,
                from,
            } => {
                let mut expression = builder::update(table).build();
                engine::append_update_assignments(
                    &mut expression,
                    self.evaluate_assignments(assignments)?,
                )?;
                if let Some(where_clause) = where_clause {
                    engine::apply_where(&mut expression, self.evaluate_node(where_clause)?, false)?;
                }
                if let Some(from) = from {
                    engine::set_from(&mut expression, self.parse_from(from)?)?;
                }
                Ok(expression)
            }
            BuildNode::Delete {
                table,
                where_clause,
            } => {
                let mut expression = builder::delete(table).build();
                if let Some(where_clause) = where_clause {
                    engine::apply_where(&mut expression, self.evaluate_node(where_clause)?, false)?;
                }
                Ok(expression)
            }
            BuildNode::Insert {
                into,
                expression,
                columns,
            } => {
                let mut result = builder::insert_into(into).columns(columns).build();
                if let Some(expression) = expression {
                    let source = match expression.as_ref() {
                        BuildNode::Sql { sql } => self.parse_statement(sql)?,
                        node => self.evaluate_node(node)?,
                    };
                    match source {
                        Expression::Values(values) => engine::apply_insert_values(
                            &mut result,
                            values
                                .expressions
                                .into_iter()
                                .map(|tuple| tuple.expressions)
                                .collect(),
                            false,
                        )?,
                        query => engine::set_insert_query(&mut result, query)?,
                    }
                }
                Ok(result)
            }
            BuildNode::Merge { target } => Ok(engine::merge(builder::table(target).into_inner())),
        }
    }

    fn apply(&self, mut expression: Expression, operation: &BuildOperation) -> Result<Expression> {
        match operation {
            BuildOperation::Select {
                expressions,
                append,
            } => {
                let values = self.evaluate_expression_list(expressions)?;
                engine::append_select(&mut expression, values, *append)?;
            }
            BuildOperation::From { source } => {
                let values = self.evaluate_source(source)?;
                engine::set_from(&mut expression, values)?;
            }
            BuildOperation::Join {
                source,
                on,
                join_type,
            } => {
                let source = self
                    .evaluate_source(source)?
                    .into_iter()
                    .next()
                    .ok_or_else(|| Error::invalid_input("join source is empty"))?;
                let on = on
                    .as_ref()
                    .map(|value| self.evaluate_node(value))
                    .transpose()?;
                let kind = match join_type {
                    JoinType::Inner => JoinKind::Inner,
                    JoinType::Left => JoinKind::Left,
                    JoinType::Right => JoinKind::Right,
                    JoinType::Full => JoinKind::Full,
                    JoinType::Cross => JoinKind::Cross,
                };
                engine::append_join(
                    &mut expression,
                    Join {
                        this: source,
                        on,
                        using: Vec::new(),
                        kind,
                        use_inner_keyword: false,
                        use_outer_keyword: false,
                        deferred_condition: false,
                        join_hint: None,
                        match_condition: None,
                        pivots: Vec::new(),
                        comments: Vec::new(),
                        nesting_group: 0,
                        directed: false,
                        nearest: None,
                    },
                )?;
            }
            BuildOperation::Where {
                expressions,
                append,
            } => {
                let condition = self.combine_conditions(expressions)?;
                engine::apply_where(&mut expression, condition, *append)?;
            }
            BuildOperation::GroupBy {
                expressions,
                append,
            } => {
                let values = self.evaluate_expression_list(expressions)?;
                engine::apply_group_by(&mut expression, values, *append)?;
            }
            BuildOperation::Having {
                expressions,
                append,
            } => {
                let condition = self.combine_conditions(expressions)?;
                engine::apply_having(&mut expression, condition, *append)?;
            }
            BuildOperation::OrderBy {
                expressions,
                append,
            } => {
                let values = self.evaluate_ordered_list(expressions)?;
                engine::apply_order_by(&mut expression, values, *append)?;
            }
            BuildOperation::SortBy {
                expressions,
                append,
            } => {
                let values = self.evaluate_ordered_list(expressions)?;
                engine::apply_sort_by(&mut expression, values, *append)?;
            }
            BuildOperation::Limit { expression: value } => {
                engine::apply_limit(&mut expression, self.evaluate_node(value)?)?
            }
            BuildOperation::Offset { expression: value } => {
                engine::apply_offset(&mut expression, self.evaluate_node(value)?)?
            }
            BuildOperation::Distinct { enabled } => {
                engine::apply_distinct(&mut expression, *enabled)?
            }
            BuildOperation::Qualify {
                expressions,
                append,
            } => {
                let condition = self.combine_conditions(expressions)?;
                engine::apply_qualify(&mut expression, condition, *append)?;
            }
            BuildOperation::LateralView {
                expression: value,
                table_alias,
                column_aliases,
                outer,
            } => {
                engine::append_lateral_view(
                    &mut expression,
                    self.evaluate_node(value)?,
                    table_alias.as_deref().map(Identifier::new),
                    column_aliases
                        .iter()
                        .map(|alias| Identifier::new(alias))
                        .collect(),
                    *outer,
                )?;
            }
            BuildOperation::Window {
                name,
                partition_by,
                order_by,
            } => {
                engine::append_window(
                    &mut expression,
                    Identifier::new(name),
                    self.evaluate_expression_list(partition_by)?,
                    self.evaluate_ordered_list(order_by)?,
                )?;
            }
            BuildOperation::Lock { lock_type } => {
                engine::append_lock(
                    &mut expression,
                    match lock_type {
                        LockType::Update => engine::LockKind::Update,
                        LockType::Share => engine::LockKind::Share,
                    },
                )?;
            }
            BuildOperation::Hint { text } => {
                engine::append_hint(&mut expression, text.clone())?;
            }
            BuildOperation::Ctas {
                table,
                replace,
                temporary,
            } => {
                let table = match builder::table(table).into_inner() {
                    Expression::Table(table) => *table,
                    _ => unreachable!("builder::table returns a table"),
                };
                expression = engine::create_table_as(expression, table, *replace, *temporary)?;
            }
            BuildOperation::Subquery { alias } => {
                expression =
                    engine::subquery(expression, alias.as_ref().map(Identifier::new), false)?;
            }
            BuildOperation::Union { other, distinct } => {
                expression = engine::set_operation(
                    engine::SetKind::Union,
                    expression,
                    self.evaluate_query(other)?,
                    *distinct,
                )?
            }
            BuildOperation::Intersect { other, distinct } => {
                expression = engine::set_operation(
                    engine::SetKind::Intersect,
                    expression,
                    self.evaluate_query(other)?,
                    *distinct,
                )?
            }
            BuildOperation::Except { other, distinct } => {
                expression = engine::set_operation(
                    engine::SetKind::Except,
                    expression,
                    self.evaluate_query(other)?,
                    *distinct,
                )?
            }
            BuildOperation::When { condition, result } => {
                let condition = self.evaluate_node(condition)?;
                let result = self.evaluate_node(result)?;
                engine::append_case_when(&mut expression, condition, result)?;
            }
            BuildOperation::Else { result } => {
                let result = self.evaluate_node(result)?;
                engine::set_case_else(&mut expression, result)?;
            }
            BuildOperation::Set { assignments } => {
                let values = self.evaluate_assignments(assignments)?;
                engine::append_update_assignments(&mut expression, values)?;
            }
            BuildOperation::InsertColumns { columns } => engine::set_insert_columns(
                &mut expression,
                columns.iter().map(Identifier::new).collect(),
            )?,
            BuildOperation::Values { rows, append } => {
                let evaluated = rows
                    .iter()
                    .map(|row| self.evaluate_expression_list(row))
                    .collect::<Result<Vec<_>>>()?;
                engine::apply_insert_values(&mut expression, evaluated, *append)?;
            }
            BuildOperation::Query { query } => {
                let query = self.evaluate_query(query)?;
                engine::set_insert_query(&mut expression, query)?;
            }
            BuildOperation::MergeUsing { source, on } => {
                engine::set_merge_using(
                    &mut expression,
                    self.evaluate_source(source)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| Error::invalid_input("merge source is empty"))?,
                    self.evaluate_node(on)?,
                )?;
            }
            BuildOperation::WhenMatchedUpdate {
                assignments,
                condition,
            } => {
                engine::append_merge_update(
                    &mut expression,
                    self.evaluate_assignments(assignments)?,
                    condition
                        .as_ref()
                        .map(|value| self.evaluate_node(value))
                        .transpose()?,
                )?;
            }
            BuildOperation::WhenMatchedDelete { condition } => {
                engine::append_merge_delete(
                    &mut expression,
                    condition
                        .as_ref()
                        .map(|value| self.evaluate_node(value))
                        .transpose()?,
                )?;
            }
            BuildOperation::WhenNotMatchedInsert {
                columns,
                values,
                condition,
            } => {
                engine::append_merge_insert(
                    &mut expression,
                    columns.iter().map(Identifier::new).collect(),
                    self.evaluate_expression_list(values)?,
                    condition
                        .as_ref()
                        .map(|value| self.evaluate_node(value))
                        .transpose()?,
                )?;
            }
        }
        Ok(expression)
    }

    fn builtin(&self, function: BuiltinFunction, mut args: Vec<Expr>) -> Result<Expression> {
        let name = format!("{function:?}");
        let exact = |args: &mut Vec<Expr>, count: usize| -> Result<Vec<Expr>> {
            if args.len() != count {
                return Err(Error::invalid_input(format!(
                    "{name} expects {count} argument(s), got {}",
                    args.len()
                )));
            }
            Ok(std::mem::take(args))
        };
        let one = |args: &mut Vec<Expr>| -> Result<Expr> {
            Ok(exact(args, 1)?.pop().expect("one validated argument"))
        };

        let result = match function {
            BuiltinFunction::Count => builder::count(one(&mut args)?),
            BuiltinFunction::CountStar => {
                exact(&mut args, 0)?;
                builder::count_star()
            }
            BuiltinFunction::CountDistinct => builder::count_distinct(one(&mut args)?),
            BuiltinFunction::Sum => builder::sum(one(&mut args)?),
            BuiltinFunction::Avg => builder::avg(one(&mut args)?),
            BuiltinFunction::Min => builder::min_(one(&mut args)?),
            BuiltinFunction::Max => builder::max_(one(&mut args)?),
            BuiltinFunction::ApproxDistinct => builder::approx_distinct(one(&mut args)?),
            BuiltinFunction::Upper => builder::upper(one(&mut args)?),
            BuiltinFunction::Lower => builder::lower(one(&mut args)?),
            BuiltinFunction::Length => builder::length(one(&mut args)?),
            BuiltinFunction::Trim => builder::trim(one(&mut args)?),
            BuiltinFunction::Ltrim => builder::ltrim(one(&mut args)?),
            BuiltinFunction::Rtrim => builder::rtrim(one(&mut args)?),
            BuiltinFunction::Reverse => builder::reverse(one(&mut args)?),
            BuiltinFunction::Initcap => builder::initcap(one(&mut args)?),
            BuiltinFunction::Substring => match args.len() {
                2 => {
                    let mut args = exact(&mut args, 2)?;
                    let start = args.pop().expect("start argument");
                    builder::substring(args.pop().expect("value argument"), start, None)
                }
                3 => {
                    let mut args = exact(&mut args, 3)?;
                    let length = args.pop().expect("length argument");
                    let start = args.pop().expect("start argument");
                    builder::substring(args.pop().expect("value argument"), start, Some(length))
                }
                count => {
                    return Err(Error::invalid_input(format!(
                        "Substring expects 2 or 3 arguments, got {count}"
                    )))
                }
            },
            BuiltinFunction::Replace => {
                let mut args = exact(&mut args, 3)?;
                let new = args.pop().expect("new argument");
                let old = args.pop().expect("old argument");
                builder::replace_(args.pop().expect("value argument"), old, new)
            }
            BuiltinFunction::ConcatWs => {
                if args.is_empty() {
                    return Err(Error::invalid_input(
                        "ConcatWs expects at least one argument",
                    ));
                }
                let values = args.split_off(1);
                builder::concat_ws(args.pop().expect("separator argument"), values)
            }
            BuiltinFunction::Coalesce => builder::coalesce(args),
            BuiltinFunction::NullIf => {
                let mut args = exact(&mut args, 2)?;
                let right = args.pop().expect("right argument");
                builder::null_if(args.pop().expect("left argument"), right)
            }
            BuiltinFunction::IfNull => {
                let mut args = exact(&mut args, 2)?;
                let fallback = args.pop().expect("fallback argument");
                builder::if_null(args.pop().expect("value argument"), fallback)
            }
            BuiltinFunction::Abs => builder::abs(one(&mut args)?),
            BuiltinFunction::Round => match args.len() {
                1 => builder::round(one(&mut args)?, None),
                2 => {
                    let mut args = exact(&mut args, 2)?;
                    let decimals = args.pop().expect("decimals argument");
                    builder::round(args.pop().expect("value argument"), Some(decimals))
                }
                count => {
                    return Err(Error::invalid_input(format!(
                        "Round expects 1 or 2 arguments, got {count}"
                    )))
                }
            },
            BuiltinFunction::Floor => builder::floor(one(&mut args)?),
            BuiltinFunction::Ceil => builder::ceil(one(&mut args)?),
            BuiltinFunction::Power => {
                let mut args = exact(&mut args, 2)?;
                let exponent = args.pop().expect("exponent argument");
                builder::power(args.pop().expect("base argument"), exponent)
            }
            BuiltinFunction::Sqrt => builder::sqrt(one(&mut args)?),
            BuiltinFunction::Ln => builder::ln(one(&mut args)?),
            BuiltinFunction::Exp => builder::exp_(one(&mut args)?),
            BuiltinFunction::Sign => builder::sign(one(&mut args)?),
            BuiltinFunction::Greatest => builder::greatest(args),
            BuiltinFunction::Least => builder::least(args),
            BuiltinFunction::CurrentDate => {
                exact(&mut args, 0)?;
                builder::current_date_()
            }
            BuiltinFunction::CurrentTime => {
                exact(&mut args, 0)?;
                builder::current_time_()
            }
            BuiltinFunction::CurrentTimestamp => {
                exact(&mut args, 0)?;
                builder::current_timestamp_()
            }
            BuiltinFunction::RowNumber => {
                exact(&mut args, 0)?;
                builder::row_number()
            }
            BuiltinFunction::Rank => {
                exact(&mut args, 0)?;
                builder::rank_()
            }
            BuiltinFunction::DenseRank => {
                exact(&mut args, 0)?;
                builder::dense_rank()
            }
        };
        Ok(result.into_inner())
    }

    fn parse_expression(&self, sql: &str) -> Result<Expression> {
        self.parse_expression_list(sql)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::invalid_input("SQL expression is empty"))
    }

    fn parse_statement(&self, sql: &str) -> Result<Expression> {
        let mut statements = self.dialect.parse(sql)?;
        if statements.len() != 1 {
            return Err(Error::invalid_input(
                "builder SQL must contain exactly one statement",
            ));
        }
        Ok(statements.remove(0))
    }

    fn evaluate_query(&self, node: &BuildNode) -> Result<Expression> {
        let expression = match node {
            BuildNode::Sql { sql } => self.parse_statement(sql)?,
            _ => self.evaluate_node(node)?,
        };
        if engine::is_query(&expression) {
            Ok(expression)
        } else {
            Err(Error::invalid_input(
                "set operations require query expressions",
            ))
        }
    }

    fn parse_expression_list(&self, sql: &str) -> Result<Vec<Expression>> {
        let statements = self.dialect.parse(&format!("SELECT {sql}"))?;
        match statements.into_iter().next() {
            Some(Expression::Select(select)) => Ok(select.expressions),
            _ => Err(Error::invalid_input(
                "failed to parse SQL expression fragment",
            )),
        }
    }

    fn parse_from(&self, sql: &str) -> Result<Vec<Expression>> {
        let statements = self.dialect.parse(&format!("SELECT * FROM {sql}"))?;
        match statements.into_iter().next() {
            Some(Expression::Select(select)) => select
                .from
                .map(|from| from.expressions)
                .ok_or_else(|| Error::invalid_input("failed to parse FROM fragment")),
            _ => Err(Error::invalid_input("failed to parse FROM fragment")),
        }
    }

    fn parse_ordered(&self, sql: &str) -> Result<Vec<Ordered>> {
        let statements = self.dialect.parse(&format!(
            "SELECT * FROM __polyglot_builder__ ORDER BY {sql}"
        ))?;
        match statements.into_iter().next() {
            Some(Expression::Select(select)) => select
                .order_by
                .map(|order| order.expressions)
                .ok_or_else(|| Error::invalid_input("failed to parse ORDER BY fragment")),
            _ => Err(Error::invalid_input("failed to parse ORDER BY fragment")),
        }
    }

    fn evaluate_expression_list(&self, values: &[BuildNode]) -> Result<Vec<Expression>> {
        let mut result = Vec::new();
        for value in values {
            match value {
                BuildNode::Sql { sql } => result.extend(self.parse_expression_list(sql)?),
                _ => result.push(self.evaluate_node(value)?),
            }
        }
        Ok(result)
    }

    fn evaluate_ordered_list(&self, values: &[BuildNode]) -> Result<Vec<Ordered>> {
        let mut result = Vec::new();
        for value in values {
            match value {
                BuildNode::Sql { sql } => result.extend(self.parse_ordered(sql)?),
                _ => match self.evaluate_node(value)? {
                    Expression::Ordered(ordered) => result.push(*ordered),
                    expression => result.push(engine::ordered(expression)),
                },
            }
        }
        Ok(result)
    }

    fn evaluate_source(&self, value: &BuildNode) -> Result<Vec<Expression>> {
        match value {
            BuildNode::Sql { sql } => self.parse_from(sql),
            BuildNode::Table { name } => Ok(vec![builder::table(name).into_inner()]),
            _ => Ok(vec![self.evaluate_node(value)?]),
        }
    }

    fn combine_conditions(&self, values: &[BuildNode]) -> Result<Expression> {
        let mut values = values.iter();
        let first = values
            .next()
            .ok_or_else(|| Error::invalid_input("at least one condition is required"))?;
        let mut result = self.evaluate_node(first)?;
        for value in values {
            result = engine::binary(engine::BinaryKind::And, result, self.evaluate_node(value)?);
        }
        Ok(result)
    }

    fn evaluate_assignments(
        &self,
        assignments: &[BuilderAssignment],
    ) -> Result<Vec<(Identifier, Expression)>> {
        assignments
            .iter()
            .map(|assignment| {
                Ok((
                    Identifier::new(&assignment.column),
                    self.evaluate_node(&assignment.value)?,
                ))
            })
            .collect()
    }
}

fn resolve_dialect(name: &str) -> Result<Dialect> {
    Dialect::get_by_name(if name.is_empty() { "generic" } else { name })
        .ok_or_else(|| Error::invalid_input(format!("unknown dialect: {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "bindings")]
    fn export_typescript_types_builder_protocol() {
        BuildRequest::export_all(&ts_rs::Config::default())
            .expect("failed to export builder protocol types");
    }

    fn sql(plan: BuilderPlan) -> String {
        match execute(&BuildRequest {
            version: 1,
            read_dialect: "generic".into(),
            plan,
            output: BuilderOutput::Sql {
                dialect: "generic".into(),
            },
        })
        .unwrap()
        {
            BuildResult::Sql(sql) => sql,
            _ => unreachable!(),
        }
    }

    #[test]
    fn builds_sqlglot_style_select() {
        let query = BuilderPlan::new(BuildNode::Select {
            expressions: vec![BuildNode::Sql {
                sql: "x, COUNT(*) AS n".into(),
            }],
        })
        .apply(BuildOperation::From {
            source: BuildNode::Sql {
                sql: "events".into(),
            },
        })
        .apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "active = TRUE".into(),
            }],
            append: true,
        });
        assert_eq!(
            sql(query),
            "SELECT x, COUNT(*) AS n FROM events WHERE active = TRUE"
        );
    }

    #[test]
    fn appends_conditions_without_mutating_the_original_plan() {
        let base = BuilderPlan::new(BuildNode::Select {
            expressions: vec![BuildNode::Sql { sql: "x".into() }],
        })
        .apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "x > 0".into(),
            }],
            append: true,
        });
        let appended = base.clone().apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "x < 9".into(),
            }],
            append: true,
        });
        assert_eq!(sql(base), "SELECT x WHERE x > 0");
        assert_eq!(sql(appended), "SELECT x WHERE x > 0 AND x < 9");
    }

    #[test]
    fn distinguishes_sql_and_literal_strings() {
        let comparison = BuildNode::Binary {
            op: BinaryOperator::Eq,
            left: Box::new(BuildNode::Column {
                name: "status".into(),
            }),
            right: Box::new(BuildNode::Literal {
                value: BuilderValue::String("active".into()),
            }),
        };
        assert_eq!(sql(BuilderPlan::new(comparison)), "status = 'active'");

        let not_in = BuildNode::InList {
            expression: Box::new(BuildNode::Column { name: "x".into() }),
            values: vec![
                BuildNode::Literal {
                    value: BuilderValue::Integer(1),
                },
                BuildNode::Literal {
                    value: BuilderValue::Integer(2),
                },
            ],
            negated: true,
        };
        assert_eq!(sql(BuilderPlan::new(not_in)), "x NOT IN (1, 2)");
    }

    #[test]
    fn parses_full_query_strings_in_query_contexts() {
        let union = BuilderPlan::new(BuildNode::Sql {
            sql: "SELECT id".into(),
        })
        .apply(BuildOperation::Union {
            other: BuildNode::Sql {
                sql: "SELECT id FROM archive".into(),
            },
            distinct: false,
        });
        assert_eq!(sql(union), "SELECT id UNION ALL SELECT id FROM archive");

        let insert = BuildNode::Insert {
            into: "users".into(),
            expression: Some(Box::new(BuildNode::Sql {
                sql: "SELECT id FROM staging".into(),
            })),
            columns: vec!["id".into()],
        };
        assert_eq!(
            sql(BuilderPlan::new(insert)),
            "INSERT INTO users (id) SELECT id FROM staging"
        );
    }

    #[test]
    fn supports_typed_builtins_and_advanced_query_operations() {
        let plan = BuilderPlan::new(BuildNode::Select {
            expressions: vec![
                BuildNode::Column {
                    name: "department".into(),
                },
                BuildNode::Alias {
                    expression: Box::new(BuildNode::Builtin {
                        function: BuiltinFunction::Count,
                        args: vec![BuildNode::Column { name: "id".into() }],
                    }),
                    alias: "employees".into(),
                },
            ],
        })
        .apply(BuildOperation::From {
            source: BuildNode::Table {
                name: "employees".into(),
            },
        })
        .apply(BuildOperation::Join {
            source: BuildNode::Table {
                name: "departments".into(),
            },
            on: Some(BuildNode::Sql {
                sql: "employees.department_id = departments.id".into(),
            }),
            join_type: JoinType::Full,
        })
        .apply(BuildOperation::Window {
            name: "w".into(),
            partition_by: vec![BuildNode::Column {
                name: "department".into(),
            }],
            order_by: vec![BuildNode::Ordered {
                expression: Box::new(BuildNode::Column {
                    name: "salary".into(),
                }),
                desc: true,
            }],
        })
        .apply(BuildOperation::Lock {
            lock_type: LockType::Share,
        });

        assert_eq!(
            sql(plan),
            "SELECT department, COUNT(id) AS employees FROM employees FULL JOIN departments ON employees.department_id = departments.id WINDOW w AS (PARTITION BY department ORDER BY salary DESC) FOR SHARE"
        );
    }

    #[test]
    fn repeated_clauses_append_by_default_and_can_replace() {
        let base = BuilderPlan::new(BuildNode::Select {
            expressions: vec![BuildNode::Column { name: "x".into() }],
        })
        .apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "x > 0".into(),
            }],
            append: true,
        });
        let appended = base.clone().apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "x < 10".into(),
            }],
            append: true,
        });
        let replaced = base.apply(BuildOperation::Where {
            expressions: vec![BuildNode::Sql {
                sql: "x = 5".into(),
            }],
            append: false,
        });

        assert_eq!(sql(appended), "SELECT x WHERE x > 0 AND x < 10");
        assert_eq!(sql(replaced), "SELECT x WHERE x = 5");
    }

    #[test]
    fn supports_conditional_merge_actions() {
        let plan = BuilderPlan::new(BuildNode::Merge {
            target: "target".into(),
        })
        .apply(BuildOperation::MergeUsing {
            source: BuildNode::Table {
                name: "source".into(),
            },
            on: BuildNode::Sql {
                sql: "target.id = source.id".into(),
            },
        })
        .apply(BuildOperation::WhenMatchedUpdate {
            assignments: vec![BuilderAssignment {
                column: "name".into(),
                value: BuildNode::Column {
                    name: "source.name".into(),
                },
            }],
            condition: Some(BuildNode::Sql {
                sql: "source.active".into(),
            }),
        })
        .apply(BuildOperation::WhenMatchedDelete {
            condition: Some(BuildNode::Sql {
                sql: "source.deleted".into(),
            }),
        })
        .apply(BuildOperation::WhenNotMatchedInsert {
            columns: vec!["id".into()],
            values: vec![BuildNode::Column {
                name: "source.id".into(),
            }],
            condition: Some(BuildNode::Sql {
                sql: "source.active".into(),
            }),
        });

        let sql = sql(plan);
        assert!(sql.contains("WHEN MATCHED AND source.active THEN UPDATE SET name = source.name"));
        assert!(sql.contains("WHEN MATCHED AND source.deleted THEN DELETE"));
        assert!(
            sql.contains("WHEN NOT MATCHED AND source.active THEN INSERT (id) VALUES (source.id)")
        );
    }
}
