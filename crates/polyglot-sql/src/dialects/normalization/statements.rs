use super::{NormalizationContext, RewriteOutcome};
use crate::dialects::{Dialect, DialectType};
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    SetToVariable,
    EnsureValuesDerivedTableColumnAliases,
    AlterTableRenameStripSchema,
    TempTableHash,
    TablesampleReservoir,
    CreateTableStripComment,
    AlterTableToSpRename,
    StraightJoinCase,
    TablesampleSnowflakeStrip,
    CreateTableLikeToCtas,
    CreateTableLikeToSelectInto,
    CreateTableLikeToAs,
}

pub(super) fn normalize_root(expression: Expression, context: &NormalizationContext) -> Expression {
    let source = context.source;
    let target = context.target;
    let expr = expression;
    // Handle SELECT INTO -> CREATE TABLE AS for DuckDB/Snowflake/etc.
    let expr = if matches!(source, DialectType::TSQL | DialectType::Fabric) {
        transform_select_into(expr, source, target)
    } else {
        expr
    };

    // Strip OFFSET ROWS for non-TSQL/Oracle targets
    let expr = if !matches!(
        target,
        DialectType::TSQL | DialectType::Oracle | DialectType::Fabric
    ) {
        if let Expression::Select(mut select) = expr {
            if let Some(ref mut offset) = select.offset {
                offset.rows = None;
            }
            Expression::Select(select)
        } else {
            expr
        }
    } else {
        expr
    };

    // Oracle: LIMIT -> FETCH FIRST, OFFSET -> OFFSET ROWS
    let expr = if matches!(target, DialectType::Oracle) {
        if let Expression::Select(mut select) = expr {
            if let Some(limit) = select.limit.take() {
                // Convert LIMIT to FETCH FIRST n ROWS ONLY
                select.fetch = Some(crate::expressions::Fetch {
                    direction: "FIRST".to_string(),
                    count: Some(limit.this),
                    percent: false,
                    rows: true,
                    with_ties: false,
                });
            }
            // Add ROWS to OFFSET if present
            if let Some(ref mut offset) = select.offset {
                offset.rows = Some(true);
            }
            Expression::Select(select)
        } else {
            expr
        }
    } else {
        expr
    };

    // Handle CreateTable WITH properties transformation before recursive transforms
    let expr = if let Expression::CreateTable(mut ct) = expr {
        transform_create_table_properties(&mut ct, source, target);

        // Handle Hive-style PARTITIONED BY (col_name type, ...) -> target-specific
        // When the PARTITIONED BY clause contains column definitions, merge them into the
        // main column list and adjust the PARTITIONED BY clause for the target dialect.
        if matches!(
            source,
            DialectType::Hive | DialectType::Spark | DialectType::Databricks
        ) {
            let mut partition_col_names: Vec<String> = Vec::new();
            let mut partition_col_defs: Vec<crate::expressions::ColumnDef> = Vec::new();
            let mut has_col_def_partitions = false;

            // Check if any PARTITIONED BY property contains ColumnDef expressions
            for prop in &ct.properties {
                if let Expression::PartitionedByProperty(ref pbp) = prop {
                    if let Expression::Tuple(ref tuple) = *pbp.this {
                        for expr in &tuple.expressions {
                            if let Expression::ColumnDef(ref cd) = expr {
                                has_col_def_partitions = true;
                                partition_col_names.push(cd.name.name.clone());
                                partition_col_defs.push(*cd.clone());
                            }
                        }
                    }
                }
            }

            if has_col_def_partitions && !matches!(target, DialectType::Hive) {
                // Merge partition columns into main column list
                for cd in partition_col_defs {
                    ct.columns.push(cd);
                }

                // Replace PARTITIONED BY property with column-name-only version
                ct.properties
                    .retain(|p| !matches!(p, Expression::PartitionedByProperty(_)));

                if matches!(
                    target,
                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                ) {
                    // Presto: WITH (PARTITIONED_BY=ARRAY['y', 'z'])
                    let array_elements: Vec<String> = partition_col_names
                        .iter()
                        .map(|n| format!("'{}'", n))
                        .collect();
                    let array_value = format!("ARRAY[{}]", array_elements.join(", "));
                    ct.with_properties
                        .push(("PARTITIONED_BY".to_string(), array_value));
                } else if matches!(target, DialectType::Spark | DialectType::Databricks) {
                    // Spark: PARTITIONED BY (y, z) - just column names
                    let name_exprs: Vec<Expression> = partition_col_names
                        .iter()
                        .map(|n| {
                            Expression::Column(Box::new(crate::expressions::Column {
                                name: crate::expressions::Identifier::new(n.clone()),
                                table: None,
                                join_mark: false,
                                trailing_comments: Vec::new(),
                                span: None,
                                inferred_type: None,
                            }))
                        })
                        .collect();
                    ct.properties.insert(
                        0,
                        Expression::PartitionedByProperty(Box::new(
                            crate::expressions::PartitionedByProperty {
                                this: Box::new(Expression::Tuple(Box::new(
                                    crate::expressions::Tuple {
                                        expressions: name_exprs,
                                    },
                                ))),
                            },
                        )),
                    );
                }
                // For DuckDB and other targets, just drop the PARTITIONED BY (already retained above)
            }

            // Note: Non-ColumnDef partitions (e.g., function expressions like MONTHS(y))
            // are handled by transform_create_table_properties which runs first
        }

        // Strip LOCATION property for Presto/Trino (not supported)
        if matches!(
            target,
            DialectType::Presto | DialectType::Trino | DialectType::Athena
        ) {
            ct.properties
                .retain(|p| !matches!(p, Expression::LocationProperty(_)));
        }

        // Strip table-level constraints for Spark/Hive/Databricks
        // Keep PRIMARY KEY and LIKE constraints but strip TSQL-specific modifiers; remove all others
        if matches!(
            target,
            DialectType::Spark | DialectType::Databricks | DialectType::Hive
        ) {
            ct.constraints.retain(|c| {
                matches!(
                    c,
                    crate::expressions::TableConstraint::PrimaryKey { .. }
                        | crate::expressions::TableConstraint::Like { .. }
                )
            });
            for constraint in &mut ct.constraints {
                if let crate::expressions::TableConstraint::PrimaryKey {
                    columns, modifiers, ..
                } = constraint
                {
                    // Strip ASC/DESC from column names
                    for col in columns.iter_mut() {
                        if col.name.ends_with(" ASC") {
                            col.name = col.name[..col.name.len() - 4].to_string();
                        } else if col.name.ends_with(" DESC") {
                            col.name = col.name[..col.name.len() - 5].to_string();
                        }
                    }
                    // Strip TSQL-specific modifiers
                    modifiers.clustered = None;
                    modifiers.with_options.clear();
                    modifiers.on_filegroup = None;
                }
            }
        }

        // Databricks: IDENTITY columns with INT/INTEGER -> BIGINT
        if matches!(target, DialectType::Databricks) {
            for col in &mut ct.columns {
                if col.auto_increment {
                    if matches!(col.data_type, crate::expressions::DataType::Int { .. }) {
                        col.data_type = crate::expressions::DataType::BigInt { length: None };
                    }
                }
            }
        }

        // Spark/Databricks: INTEGER -> INT in column definitions
        // Python sqlglot always outputs INT for Spark/Databricks
        if matches!(target, DialectType::Spark | DialectType::Databricks) {
            for col in &mut ct.columns {
                if let crate::expressions::DataType::Int {
                    integer_spelling, ..
                } = &mut col.data_type
                {
                    *integer_spelling = false;
                }
            }
        }

        // Strip explicit NULL constraints for Hive/Spark (B INTEGER NULL -> B INTEGER)
        if matches!(target, DialectType::Hive | DialectType::Spark) {
            for col in &mut ct.columns {
                // If nullable is explicitly true (NULL), change to None (omit it)
                if col.nullable == Some(true) {
                    col.nullable = None;
                }
                // Also remove from constraints if stored there
                col.constraints
                    .retain(|c| !matches!(c, crate::expressions::ColumnConstraint::Null));
            }
        }

        // Strip TSQL ON filegroup for non-TSQL/Fabric targets
        if ct.on_property.is_some() && !matches!(target, DialectType::TSQL | DialectType::Fabric) {
            ct.on_property = None;
        }

        // Snowflake: strip ARRAY type parameters (ARRAY<INT> -> ARRAY, ARRAY<ARRAY<INT>> -> ARRAY)
        // Snowflake doesn't support typed arrays in DDL
        if matches!(target, DialectType::Snowflake) {
            fn strip_array_type_params(dt: &mut crate::expressions::DataType) {
                if let crate::expressions::DataType::Array { .. } = dt {
                    *dt = crate::expressions::DataType::Custom {
                        name: "ARRAY".to_string(),
                    };
                }
            }
            for col in &mut ct.columns {
                strip_array_type_params(&mut col.data_type);
            }
        }

        // PostgreSQL target: ensure IDENTITY columns have NOT NULL
        // If NOT NULL was explicit in source (present in constraint_order), preserve original order.
        // If NOT NULL was not explicit, add it after IDENTITY (GENERATED BY DEFAULT AS IDENTITY NOT NULL).
        if matches!(target, DialectType::PostgreSQL) {
            for col in &mut ct.columns {
                if col.auto_increment && !col.constraint_order.is_empty() {
                    use crate::expressions::ConstraintType;
                    let has_explicit_not_null = col
                        .constraint_order
                        .iter()
                        .any(|ct| *ct == ConstraintType::NotNull);

                    if has_explicit_not_null {
                        // Source had explicit NOT NULL - preserve original order
                        // Just ensure nullable is set
                        if col.nullable != Some(false) {
                            col.nullable = Some(false);
                        }
                    } else {
                        // Source didn't have explicit NOT NULL - build order with
                        // AutoIncrement + NotNull first, then remaining constraints
                        let mut new_order = Vec::new();
                        // Put AutoIncrement (IDENTITY) first, followed by synthetic NotNull
                        new_order.push(ConstraintType::AutoIncrement);
                        new_order.push(ConstraintType::NotNull);
                        // Add remaining constraints in original order (except AutoIncrement)
                        for ct_type in &col.constraint_order {
                            if *ct_type != ConstraintType::AutoIncrement {
                                new_order.push(ct_type.clone());
                            }
                        }
                        col.constraint_order = new_order;
                        col.nullable = Some(false);
                    }
                }
            }
        }

        Expression::CreateTable(ct)
    } else {
        expr
    };

    // Handle CreateView column stripping for Presto/Trino target
    let expr = if let Expression::CreateView(mut cv) = expr {
        // Presto/Trino: drop column list when view has a SELECT body
        if matches!(target, DialectType::Presto | DialectType::Trino) && !cv.columns.is_empty() {
            if !matches!(&cv.query, Expression::Null(_)) {
                cv.columns.clear();
            }
        }
        Expression::CreateView(cv)
    } else {
        expr
    };

    // Wrap bare VALUES in CTE bodies with SELECT * FROM (...) AS _values for generic/non-Presto targets
    let expr = if !matches!(
        target,
        DialectType::Presto | DialectType::Trino | DialectType::Athena
    ) {
        if let Expression::Select(mut select) = expr {
            if let Some(ref mut with) = select.with {
                for cte in &mut with.ctes {
                    if let Expression::Values(ref vals) = cte.this {
                        // Build: SELECT * FROM (VALUES ...) AS _values
                        let values_subquery =
                            Expression::Subquery(Box::new(crate::expressions::Subquery {
                                this: Expression::Values(vals.clone()),
                                alias: Some(Identifier::new("_values".to_string())),
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
                            }));
                        let mut new_select = crate::expressions::Select::new();
                        new_select.expressions = vec![Expression::Star(crate::expressions::Star {
                            table: None,
                            except: None,
                            replace: None,
                            rename: None,
                            trailing_comments: Vec::new(),
                            span: None,
                        })];
                        new_select.from = Some(crate::expressions::From {
                            expressions: vec![values_subquery],
                        });
                        cte.this = Expression::Select(Box::new(new_select));
                    }
                }
            }
            Expression::Select(select)
        } else {
            expr
        }
    } else {
        expr
    };

    let expr = if matches!(target, DialectType::TSQL | DialectType::Fabric) {
        Dialect::wrap_tsql_top_level_values(expr)
    } else {
        expr
    };

    // PostgreSQL CREATE INDEX: add NULLS FIRST to index columns that don't have nulls ordering
    let expr = if matches!(target, DialectType::PostgreSQL) {
        if let Expression::CreateIndex(mut ci) = expr {
            for col in &mut ci.columns {
                if col.nulls_first.is_none() {
                    col.nulls_first = Some(true);
                }
            }
            Expression::CreateIndex(ci)
        } else {
            expr
        }
    } else {
        expr
    };

    expr
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
            Action::EnsureValuesDerivedTableColumnAliases => {
                if let Expression::Subquery(mut subquery) = e {
                    let column_count = match &subquery.this {
                        Expression::Values(values) => values
                            .expressions
                            .first()
                            .map(|row| row.expressions.len())
                            .unwrap_or(0),
                        _ => 0,
                    };
                    subquery.column_aliases = (1..=column_count)
                        .map(|index| Identifier::new(format!("column{index}")))
                        .collect();
                    Ok(Expression::Subquery(subquery))
                } else {
                    Ok(e)
                }
            }
            Action::SetToVariable => {
                // For DuckDB: SET a = 1 -> SET VARIABLE a = 1
                if let Expression::SetStatement(mut s) = e {
                    for item in &mut s.items {
                        if item.kind.is_none() {
                            // Check if name already has VARIABLE prefix (from DuckDB source parsing)
                            let already_variable = match &item.name {
                                Expression::Identifier(id) => id.name.starts_with("VARIABLE "),
                                _ => false,
                            };
                            if already_variable {
                                // Extract the actual name and set kind
                                if let Expression::Identifier(ref mut id) = item.name {
                                    let actual_name = id.name["VARIABLE ".len()..].to_string();
                                    id.name = actual_name;
                                }
                            }
                            item.kind = Some("VARIABLE".to_string());
                        }
                    }
                    Ok(Expression::SetStatement(s))
                } else {
                    Ok(e)
                }
            }

            Action::AlterTableRenameStripSchema => {
                if let Expression::AlterTable(mut at) = e {
                    if let Some(crate::expressions::AlterTableAction::RenameTable(
                        ref mut new_tbl,
                    )) = at.actions.first_mut()
                    {
                        new_tbl.schema = None;
                        new_tbl.catalog = None;
                    }
                    Ok(Expression::AlterTable(at))
                } else {
                    Ok(e)
                }
            }
            Action::TempTableHash => {
                match e {
                    Expression::CreateTable(mut ct) => {
                        // TSQL #table -> TEMPORARY TABLE with # stripped from name
                        let name = &ct.name.name.name;
                        if name.starts_with('#') {
                            ct.name.name.name = name.trim_start_matches('#').to_string();
                        }
                        // Set temporary flag
                        ct.temporary = true;
                        Ok(Expression::CreateTable(ct))
                    }
                    Expression::Table(mut tr) => {
                        // Strip # from table references
                        let name = &tr.name.name;
                        if name.starts_with('#') {
                            tr.name.name = name.trim_start_matches('#').to_string();
                        }
                        Ok(Expression::Table(tr))
                    }
                    Expression::DropTable(mut dt) => {
                        // Strip # from DROP TABLE names
                        for table_ref in &mut dt.names {
                            if table_ref.name.name.starts_with('#') {
                                table_ref.name.name =
                                    table_ref.name.name.trim_start_matches('#').to_string();
                            }
                        }
                        Ok(Expression::DropTable(dt))
                    }
                    _ => Ok(e),
                }
            }
            Action::TablesampleReservoir => {
                // TABLESAMPLE -> TABLESAMPLE RESERVOIR for DuckDB
                if let Expression::TableSample(mut ts) = e {
                    if let Some(ref mut sample) = ts.sample {
                        sample.method = crate::expressions::SampleMethod::Reservoir;
                        sample.explicit_method = true;
                    }
                    Ok(Expression::TableSample(ts))
                } else {
                    Ok(e)
                }
            }

            Action::CreateTableStripComment => {
                // Strip COMMENT column constraint, USING, PARTITIONED BY for DuckDB
                if let Expression::CreateTable(mut ct) = e {
                    for col in &mut ct.columns {
                        col.comment = None;
                        col.constraints.retain(|c| {
                            !matches!(c, crate::expressions::ColumnConstraint::Comment(_))
                        });
                        // Also remove Comment from constraint_order
                        col.constraint_order
                            .retain(|c| !matches!(c, crate::expressions::ConstraintType::Comment));
                    }
                    // Strip properties (USING, PARTITIONED BY, etc.)
                    ct.properties.clear();
                    Ok(Expression::CreateTable(ct))
                } else {
                    Ok(e)
                }
            }

            Action::AlterTableToSpRename => {
                // ALTER TABLE db.t1 RENAME TO db.t2 -> EXEC sp_rename 'db.t1', 't2'
                if let Expression::AlterTable(ref at) = e {
                    if let Some(crate::expressions::AlterTableAction::RenameTable(ref new_tbl)) =
                        at.actions.first()
                    {
                        // Build the old table name using TSQL bracket quoting
                        let old_name = if let Some(ref schema) = at.name.schema {
                            if at.name.name.quoted || schema.quoted {
                                format!("[{}].[{}]", schema.name, at.name.name.name)
                            } else {
                                format!("{}.{}", schema.name, at.name.name.name)
                            }
                        } else {
                            if at.name.name.quoted {
                                format!("[{}]", at.name.name.name)
                            } else {
                                at.name.name.name.clone()
                            }
                        };
                        let new_name = new_tbl.name.name.clone();
                        // EXEC sp_rename 'old_name', 'new_name'
                        let sql = format!("EXEC sp_rename '{}', '{}'", old_name, new_name);
                        Ok(Expression::Raw(crate::expressions::Raw { sql }))
                    } else {
                        Ok(e)
                    }
                } else {
                    Ok(e)
                }
            }

            Action::StraightJoinCase => {
                // straight_join: keep lowercase for DuckDB, quote for MySQL
                if let Expression::Column(col) = e {
                    if col.name.name == "STRAIGHT_JOIN" {
                        let mut new_col = col;
                        new_col.name.name = "straight_join".to_string();
                        if matches!(target, DialectType::MySQL) {
                            // MySQL: needs quoting since it's a reserved keyword
                            new_col.name.quoted = true;
                        }
                        Ok(Expression::Column(new_col))
                    } else {
                        Ok(Expression::Column(col))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TablesampleSnowflakeStrip => {
                // Strip method and PERCENT for Snowflake target from non-Snowflake source
                match e {
                    Expression::TableSample(mut ts) => {
                        if let Some(ref mut sample) = ts.sample {
                            sample.suppress_method_output = true;
                            sample.unit_after_size = false;
                            sample.is_percent = false;
                        }
                        Ok(Expression::TableSample(ts))
                    }
                    Expression::Table(mut t) => {
                        if let Some(ref mut sample) = t.table_sample {
                            sample.suppress_method_output = true;
                            sample.unit_after_size = false;
                            sample.is_percent = false;
                        }
                        Ok(Expression::Table(t))
                    }
                    _ => Ok(e),
                }
            }

            Action::CreateTableLikeToCtas => {
                // CREATE TABLE a LIKE b -> CREATE TABLE a AS SELECT * FROM b LIMIT 0
                if let Expression::CreateTable(ct) = e {
                    let like_source = ct.constraints.iter().find_map(|c| {
                        if let crate::expressions::TableConstraint::Like { source, .. } = c {
                            Some(source.clone())
                        } else {
                            None
                        }
                    });
                    if let Some(source_table) = like_source {
                        let mut new_ct = *ct;
                        new_ct.constraints.clear();
                        // Build: SELECT * FROM b LIMIT 0
                        let select = Expression::Select(Box::new(crate::expressions::Select {
                            expressions: vec![Expression::Star(crate::expressions::Star {
                                table: None,
                                except: None,
                                replace: None,
                                rename: None,
                                trailing_comments: Vec::new(),
                                span: None,
                            })],
                            from: Some(crate::expressions::From {
                                expressions: vec![Expression::Table(Box::new(source_table))],
                            }),
                            limit: Some(crate::expressions::Limit {
                                this: Expression::Literal(Box::new(Literal::Number(
                                    "0".to_string(),
                                ))),
                                percent: false,
                                comments: Vec::new(),
                            }),
                            ..Default::default()
                        }));
                        new_ct.as_select = Some(select);
                        Ok(Expression::CreateTable(Box::new(new_ct)))
                    } else {
                        Ok(Expression::CreateTable(ct))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::CreateTableLikeToSelectInto => {
                // CREATE TABLE a LIKE b -> SELECT TOP 0 * INTO a FROM b AS temp
                if let Expression::CreateTable(ct) = e {
                    let like_source = ct.constraints.iter().find_map(|c| {
                        if let crate::expressions::TableConstraint::Like { source, .. } = c {
                            Some(source.clone())
                        } else {
                            None
                        }
                    });
                    if let Some(source_table) = like_source {
                        let mut aliased_source = source_table;
                        aliased_source.alias = Some(Identifier::new("temp"));
                        // Build: SELECT TOP 0 * INTO a FROM b AS temp
                        let select = Expression::Select(Box::new(crate::expressions::Select {
                            expressions: vec![Expression::Star(crate::expressions::Star {
                                table: None,
                                except: None,
                                replace: None,
                                rename: None,
                                trailing_comments: Vec::new(),
                                span: None,
                            })],
                            from: Some(crate::expressions::From {
                                expressions: vec![Expression::Table(Box::new(aliased_source))],
                            }),
                            into: Some(crate::expressions::SelectInto {
                                this: Expression::Table(Box::new(ct.name.clone())),
                                temporary: false,
                                unlogged: false,
                                bulk_collect: false,
                                expressions: Vec::new(),
                            }),
                            top: Some(crate::expressions::Top {
                                this: Expression::Literal(Box::new(Literal::Number(
                                    "0".to_string(),
                                ))),
                                percent: false,
                                with_ties: false,
                                parenthesized: false,
                            }),
                            ..Default::default()
                        }));
                        Ok(select)
                    } else {
                        Ok(Expression::CreateTable(ct))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::CreateTableLikeToAs => {
                // CREATE TABLE a LIKE b -> CREATE TABLE a AS b (ClickHouse)
                if let Expression::CreateTable(ct) = e {
                    let like_source = ct.constraints.iter().find_map(|c| {
                        if let crate::expressions::TableConstraint::Like { source, .. } = c {
                            Some(source.clone())
                        } else {
                            None
                        }
                    });
                    if let Some(source_table) = like_source {
                        let mut new_ct = *ct;
                        new_ct.constraints.clear();
                        // AS b (just a table reference, not a SELECT)
                        new_ct.as_select = Some(Expression::Table(Box::new(source_table)));
                        Ok(Expression::CreateTable(Box::new(new_ct)))
                    } else {
                        Ok(Expression::CreateTable(ct))
                    }
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

pub(super) fn transform_select_into(
    expr: Expression,
    _source: DialectType,
    target: DialectType,
) -> Expression {
    use crate::expressions::{CreateTable, Expression, TableRef};

    // Handle INSERT INTO #temp -> INSERT INTO temp for non-TSQL targets
    if let Expression::Insert(ref insert) = expr {
        if insert.table.name.name.starts_with('#')
            && !matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            let mut new_insert = insert.clone();
            new_insert.table.name.name = insert.table.name.name.trim_start_matches('#').to_string();
            return Expression::Insert(new_insert);
        }
        return expr;
    }

    if let Expression::Select(ref select) = expr {
        if let Some(ref into) = select.into {
            let table_name_raw = match &into.this {
                Expression::Table(tr) => tr.name.name.clone(),
                Expression::Identifier(id) => id.name.clone(),
                _ => String::new(),
            };
            let is_temp = table_name_raw.starts_with('#') || into.temporary;
            let clean_name = table_name_raw.trim_start_matches('#').to_string();

            match target {
                DialectType::DuckDB | DialectType::Snowflake => {
                    // SELECT INTO -> CREATE TABLE AS SELECT
                    let mut new_select = select.clone();
                    new_select.into = None;
                    let ct = CreateTable {
                        name: TableRef::new(clean_name),
                        on_cluster: None,
                        columns: Vec::new(),
                        constraints: Vec::new(),
                        if_not_exists: false,
                        temporary: is_temp,
                        or_replace: false,
                        table_modifier: None,
                        as_select: Some(Expression::Select(new_select)),
                        as_select_parenthesized: false,
                        on_commit: None,
                        clone_source: None,
                        clone_at_clause: None,
                        shallow_clone: false,
                        deep_clone: false,
                        is_copy: false,
                        leading_comments: Vec::new(),
                        with_properties: Vec::new(),
                        teradata_post_name_options: Vec::new(),
                        with_data: None,
                        with_statistics: None,
                        teradata_indexes: Vec::new(),
                        with_cte: None,
                        properties: Vec::new(),
                        partition_of: None,
                        post_table_properties: Vec::new(),
                        mysql_table_options: Vec::new(),
                        tidb_table_options: Vec::new(),
                        inherits: Vec::new(),
                        on_property: None,
                        copy_grants: false,
                        using_template: None,
                        rollup: None,
                        uuid: None,
                        with_partition_columns: Vec::new(),
                        with_connection: None,
                    };
                    return Expression::CreateTable(Box::new(ct));
                }
                DialectType::PostgreSQL | DialectType::Redshift => {
                    // PostgreSQL: #foo -> INTO TEMPORARY foo
                    if is_temp && !into.temporary {
                        let mut new_select = select.clone();
                        let mut new_into = into.clone();
                        new_into.temporary = true;
                        new_into.unlogged = false;
                        new_into.this = Expression::Table(Box::new(TableRef::new(clean_name)));
                        new_select.into = Some(new_into);
                        Expression::Select(new_select)
                    } else {
                        expr
                    }
                }
                _ => expr,
            }
        } else {
            expr
        }
    } else {
        expr
    }
}

pub(super) fn transform_create_table_properties(
    ct: &mut crate::expressions::CreateTable,
    _source: DialectType,
    target: DialectType,
) {
    use crate::expressions::{
        BinaryOp, BooleanLiteral, Expression, FileFormatProperty, Identifier, Literal, Properties,
    };

    // Helper to convert a raw property value string to the correct Expression
    let value_to_expr = |v: &str| -> Expression {
        let trimmed = v.trim();
        // Check if it's a quoted string (starts and ends with ')
        if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            Expression::Literal(Box::new(Literal::String(
                trimmed[1..trimmed.len() - 1].to_string(),
            )))
        }
        // Check if it's a number
        else if trimmed.parse::<i64>().is_ok() || trimmed.parse::<f64>().is_ok() {
            Expression::Literal(Box::new(Literal::Number(trimmed.to_string())))
        }
        // Check if it's ARRAY[...] or ARRAY(...)
        else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("ARRAY") {
            // Convert ARRAY['y'] to ARRAY('y') for Hive/Spark
            let inner = trimmed
                .trim_start_matches(|c: char| c.is_alphabetic()) // Remove ARRAY
                .trim_start_matches('[')
                .trim_start_matches('(')
                .trim_end_matches(']')
                .trim_end_matches(')');
            let elements: Vec<Expression> = inner
                .split(',')
                .map(|e| {
                    let elem = e.trim().trim_matches('\'');
                    Expression::Literal(Box::new(Literal::String(elem.to_string())))
                })
                .collect();
            Expression::Function(Box::new(crate::expressions::Function::new(
                "ARRAY".to_string(),
                elements,
            )))
        }
        // Otherwise, just output as identifier (unquoted)
        else {
            Expression::Identifier(Identifier::new(trimmed.to_string()))
        }
    };

    if ct.with_properties.is_empty() && ct.properties.is_empty() {
        return;
    }

    // Handle Presto-style WITH properties
    if !ct.with_properties.is_empty() {
        // Extract FORMAT property and remaining properties
        let mut format_value: Option<String> = None;
        let mut partitioned_by: Option<String> = None;
        let mut other_props: Vec<(String, String)> = Vec::new();

        for (key, value) in ct.with_properties.drain(..) {
            if key.eq_ignore_ascii_case("FORMAT") {
                // Strip surrounding quotes from value if present
                format_value = Some(value.trim_matches('\'').to_string());
            } else if key.eq_ignore_ascii_case("PARTITIONED_BY") {
                partitioned_by = Some(value);
            } else {
                other_props.push((key, value));
            }
        }

        match target {
            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                // Presto: keep WITH properties but lowercase 'format' key
                if let Some(fmt) = format_value {
                    ct.with_properties
                        .push(("format".to_string(), format!("'{}'", fmt)));
                }
                if let Some(part) = partitioned_by {
                    // Convert (col1, col2) to ARRAY['col1', 'col2'] format
                    let trimmed = part.trim();
                    let inner = trimmed.trim_start_matches('(').trim_end_matches(')');
                    // Also handle ARRAY['...'] format - keep as-is
                    if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("ARRAY") {
                        ct.with_properties
                            .push(("PARTITIONED_BY".to_string(), part));
                    } else {
                        // Parse column names from the parenthesized list
                        let cols: Vec<&str> = inner
                            .split(',')
                            .map(|c| c.trim().trim_matches('"').trim_matches('\''))
                            .collect();
                        let array_val = format!(
                            "ARRAY[{}]",
                            cols.iter()
                                .map(|c| format!("'{}'", c))
                                .collect::<Vec<_>>()
                                .join(", ")
                        );
                        ct.with_properties
                            .push(("PARTITIONED_BY".to_string(), array_val));
                    }
                }
                ct.with_properties.extend(other_props);
            }
            DialectType::Hive => {
                // Hive: FORMAT -> STORED AS, other props -> TBLPROPERTIES
                if let Some(fmt) = format_value {
                    ct.properties.push(Expression::FileFormatProperty(Box::new(
                        FileFormatProperty {
                            this: Some(Box::new(Expression::Identifier(Identifier::new(fmt)))),
                            expressions: vec![],
                            hive_format: Some(Box::new(Expression::Boolean(BooleanLiteral {
                                value: true,
                            }))),
                        },
                    )));
                }
                if let Some(_part) = partitioned_by {
                    // PARTITIONED_BY handling is complex - move columns to partitioned by
                    // For now, the partition columns are extracted from the column list
                    apply_partitioned_by(ct, &_part, target);
                }
                if !other_props.is_empty() {
                    let eq_exprs: Vec<Expression> = other_props
                        .into_iter()
                        .map(|(k, v)| {
                            Expression::Eq(Box::new(BinaryOp::new(
                                Expression::Literal(Box::new(Literal::String(k))),
                                value_to_expr(&v),
                            )))
                        })
                        .collect();
                    ct.properties
                        .push(Expression::Properties(Box::new(Properties {
                            expressions: eq_exprs,
                        })));
                }
            }
            DialectType::Spark | DialectType::Databricks => {
                // Spark: FORMAT -> USING, other props -> TBLPROPERTIES
                if let Some(fmt) = format_value {
                    ct.properties.push(Expression::FileFormatProperty(Box::new(
                        FileFormatProperty {
                            this: Some(Box::new(Expression::Identifier(Identifier::new(fmt)))),
                            expressions: vec![],
                            hive_format: None, // None means USING syntax
                        },
                    )));
                }
                if let Some(_part) = partitioned_by {
                    apply_partitioned_by(ct, &_part, target);
                }
                if !other_props.is_empty() {
                    let eq_exprs: Vec<Expression> = other_props
                        .into_iter()
                        .map(|(k, v)| {
                            Expression::Eq(Box::new(BinaryOp::new(
                                Expression::Literal(Box::new(Literal::String(k))),
                                value_to_expr(&v),
                            )))
                        })
                        .collect();
                    ct.properties
                        .push(Expression::Properties(Box::new(Properties {
                            expressions: eq_exprs,
                        })));
                }
            }
            DialectType::DuckDB => {
                // DuckDB: strip all WITH properties (FORMAT, PARTITIONED_BY, etc.)
                // Keep nothing
            }
            _ => {
                // For other dialects, keep WITH properties as-is
                if let Some(fmt) = format_value {
                    ct.with_properties
                        .push(("FORMAT".to_string(), format!("'{}'", fmt)));
                }
                if let Some(part) = partitioned_by {
                    ct.with_properties
                        .push(("PARTITIONED_BY".to_string(), part));
                }
                ct.with_properties.extend(other_props);
            }
        }
    }

    // Handle STORED AS 'PARQUET' (quoted format name) -> STORED AS PARQUET (unquoted)
    // and Hive STORED AS -> Presto WITH (format=...) conversion
    if !ct.properties.is_empty() {
        let is_presto_target = matches!(
            target,
            DialectType::Presto | DialectType::Trino | DialectType::Athena
        );
        let is_duckdb_target = matches!(target, DialectType::DuckDB);

        if is_presto_target || is_duckdb_target {
            let mut new_properties = Vec::new();
            for prop in ct.properties.drain(..) {
                match &prop {
                    Expression::FileFormatProperty(ffp) => {
                        if is_presto_target {
                            // Convert STORED AS/USING to WITH (format=...)
                            if let Some(ref fmt_expr) = ffp.this {
                                let fmt_str = match fmt_expr.as_ref() {
                                    Expression::Identifier(id) => id.name.clone(),
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        let Literal::String(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        s.clone()
                                    }
                                    _ => {
                                        new_properties.push(prop);
                                        continue;
                                    }
                                };
                                ct.with_properties
                                    .push(("format".to_string(), format!("'{}'", fmt_str)));
                            }
                        }
                        // DuckDB: just strip file format properties
                    }
                    // Convert TBLPROPERTIES to WITH properties for Presto target
                    Expression::Properties(props) if is_presto_target => {
                        for expr in &props.expressions {
                            if let Expression::Eq(eq) = expr {
                                // Extract key and value from the Eq expression
                                let key = match &eq.left {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        let Literal::String(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        s.clone()
                                    }
                                    Expression::Identifier(id) => id.name.clone(),
                                    _ => continue,
                                };
                                let value = match &eq.right {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::String(_)) =>
                                    {
                                        let Literal::String(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        format!("'{}'", s)
                                    }
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::Number(_)) =>
                                    {
                                        let Literal::Number(n) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        n.clone()
                                    }
                                    Expression::Identifier(id) => id.name.clone(),
                                    _ => continue,
                                };
                                ct.with_properties.push((key, value));
                            }
                        }
                    }
                    // Convert PartitionedByProperty for Presto target
                    Expression::PartitionedByProperty(ref pbp) if is_presto_target => {
                        // Check if it contains ColumnDef expressions (Hive-style with types)
                        if let Expression::Tuple(ref tuple) = *pbp.this {
                            let mut col_names: Vec<String> = Vec::new();
                            let mut col_defs: Vec<crate::expressions::ColumnDef> = Vec::new();
                            let mut has_col_defs = false;
                            for expr in &tuple.expressions {
                                if let Expression::ColumnDef(ref cd) = expr {
                                    has_col_defs = true;
                                    col_names.push(cd.name.name.clone());
                                    col_defs.push(*cd.clone());
                                } else if let Expression::Column(ref col) = expr {
                                    col_names.push(col.name.name.clone());
                                } else if let Expression::Identifier(ref id) = expr {
                                    col_names.push(id.name.clone());
                                } else {
                                    // For function expressions like MONTHS(y), serialize to SQL
                                    let generic = Dialect::get(DialectType::Generic);
                                    if let Ok(sql) = generic.generate(expr) {
                                        col_names.push(sql);
                                    }
                                }
                            }
                            if has_col_defs {
                                // Merge partition column defs into the main column list
                                for cd in col_defs {
                                    ct.columns.push(cd);
                                }
                            }
                            if !col_names.is_empty() {
                                // Add PARTITIONED_BY property
                                let array_val = format!(
                                    "ARRAY[{}]",
                                    col_names
                                        .iter()
                                        .map(|n| format!("'{}'", n))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                );
                                ct.with_properties
                                    .push(("PARTITIONED_BY".to_string(), array_val));
                            }
                        }
                        // Skip - don't keep in properties
                    }
                    _ => {
                        if !is_duckdb_target {
                            new_properties.push(prop);
                        }
                    }
                }
            }
            ct.properties = new_properties;
        } else {
            // For Hive/Spark targets, unquote format names in STORED AS
            for prop in &mut ct.properties {
                if let Expression::FileFormatProperty(ref mut ffp) = prop {
                    if let Some(ref mut fmt_expr) = ffp.this {
                        if let Expression::Literal(lit) = fmt_expr.as_ref() {
                            if let Literal::String(s) = lit.as_ref() {
                                // Convert STORED AS 'PARQUET' to STORED AS PARQUET (unquote)
                                let unquoted = s.clone();
                                *fmt_expr =
                                    Box::new(Expression::Identifier(Identifier::new(unquoted)));
                            }
                        }
                    }
                }
            }
        }
    }
}

pub(super) fn apply_partitioned_by(
    ct: &mut crate::expressions::CreateTable,
    partitioned_by_value: &str,
    target: DialectType,
) {
    use crate::expressions::{Column, Expression, Identifier, PartitionedByProperty, Tuple};

    // Parse the ARRAY['col1', 'col2'] value to extract column names
    let mut col_names: Vec<String> = Vec::new();
    // The value looks like ARRAY['y', 'z'] or ARRAY('y', 'z')
    let inner = partitioned_by_value
        .trim()
        .trim_start_matches("ARRAY")
        .trim_start_matches('[')
        .trim_start_matches('(')
        .trim_end_matches(']')
        .trim_end_matches(')');
    for part in inner.split(',') {
        let col = part.trim().trim_matches('\'').trim_matches('"');
        if !col.is_empty() {
            col_names.push(col.to_string());
        }
    }

    if col_names.is_empty() {
        return;
    }

    if matches!(target, DialectType::Hive) {
        // Hive: PARTITIONED BY (col_name type, ...) - move columns out of column list
        let mut partition_col_defs = Vec::new();
        for col_name in &col_names {
            // Find and remove from columns
            if let Some(pos) = ct
                .columns
                .iter()
                .position(|c| c.name.name.eq_ignore_ascii_case(col_name))
            {
                let col_def = ct.columns.remove(pos);
                partition_col_defs.push(Expression::ColumnDef(Box::new(col_def)));
            }
        }
        if !partition_col_defs.is_empty() {
            ct.properties
                .push(Expression::PartitionedByProperty(Box::new(
                    PartitionedByProperty {
                        this: Box::new(Expression::Tuple(Box::new(Tuple {
                            expressions: partition_col_defs,
                        }))),
                    },
                )));
        }
    } else if matches!(target, DialectType::Spark | DialectType::Databricks) {
        // Spark: PARTITIONED BY (col1, col2) - just column names, keep in column list
        // Use quoted identifiers to match the quoting style of the original column definitions
        let partition_exprs: Vec<Expression> = col_names
            .iter()
            .map(|name| {
                // Check if the column exists in the column list and use its quoting
                let is_quoted = ct
                    .columns
                    .iter()
                    .any(|c| c.name.name.eq_ignore_ascii_case(name) && c.name.quoted);
                let ident = if is_quoted {
                    Identifier::quoted(name.clone())
                } else {
                    Identifier::new(name.clone())
                };
                Expression::boxed_column(Column {
                    name: ident,
                    table: None,
                    join_mark: false,
                    trailing_comments: Vec::new(),
                    span: None,
                    inferred_type: None,
                })
            })
            .collect();
        ct.properties
            .push(Expression::PartitionedByProperty(Box::new(
                PartitionedByProperty {
                    this: Box::new(Expression::Tuple(Box::new(Tuple {
                        expressions: partition_exprs,
                    }))),
                },
            )));
    }
    // DuckDB: strip partitioned_by entirely (already handled)
}
