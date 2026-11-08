//! Dialect-aware generation of structured EXPLAIN statements.
use super::*;
use crate::error::Error;

impl Generator {
    pub(super) fn generate_explain(&mut self, explain: &Explain) -> Result<()> {
        let dialect = self.config.dialect.unwrap_or_default();
        if matches!(
            dialect,
            DialectType::BigQuery | DialectType::Fabric | DialectType::Solr | DialectType::Tableau
        ) {
            return Err(Error::unsupported("EXPLAIN", dialect.to_string()));
        }
        let translated;
        let explain = if dialect != explain.source_dialect && dialect != DialectType::Generic {
            translated = self.translate_explain(explain, dialect)?;
            &translated
        } else {
            explain
        };
        for comment in &explain.leading_comments {
            self.write_formatted_comment(comment);
            self.write_space();
        }
        // MySQL's documented DESCRIBE synonym preserves existing canonical output.
        // It is still an Explain node, distinct from DESCRIBE table metadata.
        self.write_keyword(if dialect == DialectType::MySQL {
            "DESCRIBE"
        } else {
            "EXPLAIN"
        });
        self.generate_explain_options(&explain.options)?;
        if explain.for_target {
            self.write(" FOR");
        }
        match &explain.target {
            ExplainTarget::Statement(statement) => {
                self.write_space();
                self.generate_expression(statement)?;
            }
            ExplainTarget::Connection(connection) => {
                self.write(" CONNECTION ");
                self.generate_expression(connection)?;
            }
            ExplainTarget::PlanCache { entry, variant } => {
                self.write(" SQL PLAN CACHE ENTRY ");
                self.generate_expression(entry)?;
                if let Some(variant) = variant {
                    self.write(" VARIANT ");
                    self.generate_expression(variant)?;
                }
            }
            ExplainTarget::Object { kind, name } => {
                self.write_space();
                self.write_keyword(kind);
                self.write_space();
                self.generate_expression(name)?;
            }
            ExplainTarget::TableOverride { function, clauses } => {
                self.write_space();
                self.generate_expression(function)?;
                self.generate_explain_options(clauses)?;
            }
            ExplainTarget::CurrentTransaction | ExplainTarget::Cluster => {}
        }
        if explain.as_sql {
            self.write(" AS SQL");
        }
        if let Some(format) = &explain.output_format {
            self.write(" FORMAT ");
            self.write(&format.name);
        }
        Ok(())
    }

    fn generate_explain_options(&mut self, options: &[ExplainOption]) -> Result<()> {
        let mut index = 0;
        while index < options.len() {
            let option = &options[index];
            self.write_space();
            // The existing TTL syntax node owns its keyword and action clauses.
            if option.kind == ExplainOptionKind::Ttl {
                if let Some(value @ Expression::MergeTreeTTL(_)) = &option.value {
                    self.generate_expression(value)?;
                    index += 1;
                    continue;
                }
            }
            match option.syntax {
                ExplainOptionSyntax::Parenthesized | ExplainOptionSyntax::With => {
                    if option.syntax == ExplainOptionSyntax::With {
                        self.write_keyword("WITH ");
                    }
                    self.write("(");
                    let syntax = option.syntax;
                    loop {
                        let item = &options[index];
                        self.write_keyword(item.kind.name());
                        if let Some(value) = &item.value {
                            self.write(if syntax == ExplainOptionSyntax::With {
                                " = "
                            } else {
                                " "
                            });
                            self.generate_explain_option_value(value)?;
                        }
                        index += 1;
                        if index >= options.len() || options[index].syntax != syntax {
                            break;
                        }
                        self.write(", ");
                    }
                    self.write(")");
                    continue;
                }
                ExplainOptionSyntax::Using => self.write_keyword("USING"),
                ExplainOptionSyntax::As => self.write_keyword("AS"),
                ExplainOptionSyntax::Into => self.write_keyword("INTO"),
                ExplainOptionSyntax::Set => {
                    self.write_keyword("SET ");
                    self.write_keyword(option.kind.name());
                }
                _ => self.write_keyword(option.kind.name()),
            }
            if let Some(value) = &option.value {
                self.write(
                    if matches!(
                        option.syntax,
                        ExplainOptionSyntax::Equals | ExplainOptionSyntax::Set
                    ) {
                        "="
                    } else {
                        " "
                    },
                );
                self.generate_explain_option_value(value)?;
            }
            if option.syntax == ExplainOptionSyntax::CommaSeparated
                && options
                    .get(index + 1)
                    .is_some_and(|o| o.syntax == ExplainOptionSyntax::CommaSeparated)
            {
                self.write(",");
            }
            // ClickHouse's settings list requires commas, unlike keyword options.
            if matches!(option.kind, ExplainOptionKind::Other(_))
                && option.syntax == ExplainOptionSyntax::Equals
                && options
                    .get(index + 1)
                    .is_some_and(|o| o.syntax == ExplainOptionSyntax::Equals)
            {
                self.write(",");
            }
            index += 1;
        }
        Ok(())
    }

    fn generate_explain_option_value(&mut self, value: &Expression) -> Result<()> {
        if let Expression::Identifier(identifier) = value {
            if !identifier.quoted {
                // Option enum values (JSON, ON, etc.) are keywords, never quoted
                // just because a generator is configured to quote column names.
                self.write_keyword(&identifier.name);
                return Ok(());
            }
        }
        self.generate_expression(value)
    }

    /// Translate only options whose meaning has a supported target representation.
    /// Errors are intentional even in permissive mode: dropping an execution option
    /// or an engine-specific plan mode would change the requested operation.
    fn translate_explain(&self, explain: &Explain, dialect: DialectType) -> Result<Explain> {
        use ExplainOptionKind as K;
        use ExplainOptionSyntax as S;
        let unsupported =
            |feature: &str| Error::unsupported(format!("EXPLAIN {feature}"), dialect.to_string());
        if !matches!(explain.target, ExplainTarget::Statement(_)) {
            return Err(unsupported("native inspection target"));
        }
        if explain.execution_mode() == ExplainExecution::Unknown
            || matches!(
                explain.statement(),
                Some(Expression::Raw(_) | Expression::Command(_))
            )
            || explain.output_format.is_some()
            || explain.as_sql
        {
            return Err(unsupported("native options or opaque target"));
        }
        let mut translated = explain.clone();
        translated.source_dialect = dialect;
        let same_option_language =
            (matches!(
                explain.source_dialect,
                DialectType::Spark | DialectType::Databricks
            ) && matches!(dialect, DialectType::Spark | DialectType::Databricks))
                || (matches!(
                    explain.source_dialect,
                    DialectType::Trino | DialectType::Presto | DialectType::Dune
                ) && matches!(
                    dialect,
                    DialectType::Trino | DialectType::Presto | DialectType::Dune
                ));
        if same_option_language {
            return Ok(translated);
        }
        translated.options.clear();
        translated.for_target = matches!(
            dialect,
            DialectType::Oracle
                | DialectType::HANA
                | DialectType::Drill
                | DialectType::Dremio
                | DialectType::Druid
        );
        if translated.for_target {
            translated.options.push(ExplainOption {
                kind: K::Plan,
                value: None,
                syntax: S::Keyword,
            });
        }
        if dialect == DialectType::Exasol {
            translated.options.push(ExplainOption {
                kind: K::Virtual,
                value: None,
                syntax: S::Keyword,
            });
        }
        // TSQL's EXPLAIN is a Synapse facility; do not silently target all SQL Server
        // installations (or turn SHOWPLAN on around a potentially mutating query).
        if dialect == DialectType::TSQL {
            return Err(unsupported("requires a Synapse-specific target"));
        }
        let executes = explain.execution_mode() == ExplainExecution::Executes;
        if executes {
            if !matches!(
                dialect,
                DialectType::PostgreSQL
                    | DialectType::DuckDB
                    | DialectType::MySQL
                    | DialectType::TiDB
                    | DialectType::Trino
                    | DialectType::Presto
                    | DialectType::Athena
                    | DialectType::Dune
                    | DialectType::Hive
                    | DialectType::DataFusion
                    | DialectType::CockroachDB
                    | DialectType::RisingWave
                    | DialectType::ClickHouse
                    | DialectType::StarRocks
            ) {
                return Err(unsupported("ANALYZE execution"));
            }
            translated.options.push(ExplainOption {
                kind: K::Analyze,
                value: None,
                syntax: S::Keyword,
            });
        }
        for option in &explain.options {
            match option.kind {
                K::Analyze => {} // The effective execution mode above accounts for DuckDB FALSE.
                K::Plan
                    if matches!(
                        explain.source_dialect,
                        DialectType::Oracle
                            | DialectType::HANA
                            | DialectType::Drill
                            | DialectType::Dremio
                            | DialectType::Druid
                            | DialectType::ClickHouse
                    ) => {}
                K::Verbose => {
                    let enabled = option
                        .boolean_value()
                        .ok_or_else(|| unsupported("VERBOSE value"))?;
                    if enabled {
                        if !matches!(
                            dialect,
                            DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::DataFusion
                                | DialectType::Vertica
                                | DialectType::Doris
                                | DialectType::StarRocks
                                | DialectType::CockroachDB
                                | DialectType::RisingWave
                        ) && !(executes
                            && matches!(
                                dialect,
                                DialectType::Trino | DialectType::Presto | DialectType::Dune
                            ))
                        {
                            return Err(unsupported("VERBOSE"));
                        }
                        translated.options.push(ExplainOption {
                            kind: K::Verbose,
                            value: None,
                            syntax: if matches!(
                                dialect,
                                DialectType::CockroachDB | DialectType::RisingWave
                            ) {
                                S::Parenthesized
                            } else {
                                S::Keyword
                            },
                        });
                    }
                }
                K::Format => {
                    let name = match &option.value {
                        Some(Expression::Identifier(value)) => value.name.to_ascii_uppercase(),
                        Some(Expression::Literal(value)) => value.value_str().to_ascii_uppercase(),
                        _ => return Err(unsupported("FORMAT value")),
                    };
                    let (allowed, syntax): (&[&str], _) = match dialect {
                        DialectType::PostgreSQL => {
                            (&["TEXT", "JSON", "XML", "YAML"], S::Parenthesized)
                        }
                        DialectType::DuckDB => {
                            (&["TEXT", "JSON", "GRAPHVIZ", "HTML"], S::Parenthesized)
                        }
                        DialectType::RisingWave => {
                            (&["TEXT", "JSON", "XML", "YAML"], S::Parenthesized)
                        }
                        DialectType::Trino | DialectType::Presto | DialectType::Dune
                            if !executes =>
                        {
                            (&["TEXT", "JSON", "GRAPHVIZ"], S::Parenthesized)
                        }
                        DialectType::Athena => (&["TEXT", "JSON"], S::Parenthesized),
                        DialectType::MySQL if executes => (&["TREE"], S::Equals),
                        DialectType::MySQL => (&["JSON", "TREE", "TRADITIONAL"], S::Equals),
                        DialectType::TiDB => (
                            &[
                                "ROW",
                                "BRIEF",
                                "VERBOSE",
                                "DOT",
                                "HINT",
                                "BINARY",
                                "TIDB_JSON",
                            ],
                            S::Equals,
                        ),
                        DialectType::Snowflake => (&["TEXT", "JSON", "TABULAR"], S::Using),
                        DialectType::Materialize => (&["TEXT", "JSON"], S::As),
                        _ => return Err(unsupported("FORMAT")),
                    };
                    if !allowed.contains(&name.as_str()) {
                        return Err(unsupported(&format!("FORMAT {name}")));
                    }
                    translated.options.push(ExplainOption {
                        kind: K::Format,
                        value: Some(Expression::Identifier(Identifier::new(name))),
                        syntax,
                    });
                }
                _ => return Err(unsupported(option.kind.name())),
            }
        }
        // PostgreSQL's legacy syntax cannot precede a parenthesized option list.
        // Materialize AS requires a PLAN ... FOR wrapper.
        if dialect == DialectType::PostgreSQL
            && translated
                .options
                .iter()
                .any(|o| o.syntax == S::Parenthesized)
        {
            for option in &mut translated.options {
                option.syntax = S::Parenthesized;
            }
        }
        if dialect == DialectType::Materialize && !translated.options.is_empty() {
            translated.options.insert(
                0,
                ExplainOption {
                    kind: K::Plan,
                    value: None,
                    syntax: S::Keyword,
                },
            );
            translated.for_target = true;
        }
        Ok(translated)
    }
}
