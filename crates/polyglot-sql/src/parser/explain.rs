//! EXPLAIN is plan inspection, distinct from DESCRIBE's schema inspection.
use super::*;
use crate::dialects::DialectType;

impl Parser {
    fn explain_words(&mut self, words: &str) -> bool {
        if self.check_keyword_text(words) {
            self.skip();
            true
        } else {
            self.match_text_seq(&words.split_whitespace().collect::<Vec<_>>())
        }
    }

    fn mysql_explain_family(&self) -> bool {
        matches!(
            self.config.dialect,
            Some(
                DialectType::MySQL
                    | DialectType::TiDB
                    | DialectType::Doris
                    | DialectType::StarRocks
                    | DialectType::SingleStore
            )
        )
    }

    pub(super) fn is_mysql_explain_alias(&self) -> bool {
        self.mysql_explain_family()
            && self.peek_nth(1).is_some_and(|t| {
                if Self::text_match_excludes_token(t.token_type)
                    || self
                        .peek_nth(2)
                        .is_some_and(|next| next.token_type == TokenType::Dot)
                {
                    return false;
                }
                matches!(
                    t.token_type,
                    TokenType::Select
                        | TokenType::With
                        | TokenType::Insert
                        | TokenType::Update
                        | TokenType::Delete
                        | TokenType::Replace
                        | TokenType::LParen
                ) || matches!(
                    t.text(self.source.as_deref().unwrap_or_default())
                        .to_ascii_uppercase()
                        .as_str(),
                    "ANALYZE"
                        | "ANALYSE"
                        | "FORMAT"
                        | "FOR"
                        | "EXTENDED"
                        | "PARTITIONS"
                        | "VERBOSE"
                        | "COSTS"
                        | "LOGICAL"
                        | "JSON"
                        | "SHAPE"
                        | "ALL"
                        | "MEMO"
                        | "PARSED"
                        | "ANALYZED"
                        | "REWRITTEN"
                        | "OPTIMIZED"
                        | "DUPLICATE"
                        | "MBC"
                        | "MPL"
                        | "REPRO"
                        | "UI"
                )
            })
    }

    fn explain_value(&mut self) -> Result<Expression> {
        if self.is_at_end()
            || matches!(
                self.peek().token_type,
                TokenType::Comma | TokenType::RParen | TokenType::Semicolon
            )
        {
            return Err(self.parse_error("Expected EXPLAIN option value"));
        }
        match self.peek().token_type {
            TokenType::String
            | TokenType::Number
            | TokenType::True
            | TokenType::False
            | TokenType::QuotedIdentifier
            | TokenType::Identifier => self.parse_primary(),
            _ => Ok(Expression::Identifier(Identifier::new(
                self.advance_text()?.to_ascii_uppercase(),
            ))),
        }
    }

    fn explain_option_list(&mut self, syntax: ExplainOptionSyntax) -> Result<Vec<ExplainOption>> {
        self.expect(TokenType::LParen)?;
        let mut options = Vec::new();
        loop {
            if self.is_at_end() || self.check(TokenType::RParen) {
                return Err(self.parse_error("Expected EXPLAIN option"));
            }
            let mut name = self.advance_text()?;
            // Materialize WITH options include multiword names such as "node identifiers".
            if syntax == ExplainOptionSyntax::With {
                while !self.is_at_end()
                    && !matches!(
                        self.peek().token_type,
                        TokenType::Comma | TokenType::RParen | TokenType::Eq
                    )
                {
                    name.push(' ');
                    name.push_str(&self.advance_text()?);
                }
            }
            self.match_token(TokenType::Eq);
            let value = if self.check(TokenType::Comma) || self.check(TokenType::RParen) {
                None
            } else {
                Some(self.explain_value()?)
            };
            let option = ExplainOption {
                kind: ExplainOptionKind::from_name(&name),
                value,
                syntax,
            };
            if self.config.dialect == Some(DialectType::PostgreSQL)
                && matches!(
                    option.kind,
                    ExplainOptionKind::Analyze
                        | ExplainOptionKind::Verbose
                        | ExplainOptionKind::Costs
                        | ExplainOptionKind::Buffers
                        | ExplainOptionKind::Settings
                        | ExplainOptionKind::Wal
                        | ExplainOptionKind::Timing
                        | ExplainOptionKind::Summary
                        | ExplainOptionKind::Memory
                        | ExplainOptionKind::GenericPlan
                )
                && option.boolean_value().is_none()
            {
                return Err(self.parse_error("Expected a boolean EXPLAIN option value"));
            }
            options.push(option);
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        self.expect(TokenType::RParen)?;
        Ok(options)
    }

    pub(super) fn parse_explain(&mut self) -> Result<Expression> {
        let source_dialect = self.config.dialect.unwrap_or_default();
        if matches!(
            source_dialect,
            DialectType::BigQuery | DialectType::Fabric | DialectType::Solr | DialectType::Tableau
        ) {
            return Err(self.parse_error("EXPLAIN is not supported by this dialect"));
        }
        // MySQL's EXPLAIN table is schema inspection, just like DESCRIBE table.
        if self.mysql_explain_family() && !self.is_mysql_explain_alias() {
            return self.parse_describe();
        }
        let leading_comments = self.advance()?.comments;
        let mut explain = Explain::new(Expression::Null(Null), source_dialect);
        explain.leading_comments = leading_comments;
        let modes: &[&str] = match source_dialect {
            DialectType::Generic => &["ANALYZE", "ANALYSE", "VERBOSE"],
            DialectType::PostgreSQL
            | DialectType::DuckDB
            | DialectType::CockroachDB
            | DialectType::RisingWave
            | DialectType::Trino
            | DialectType::Presto
            | DialectType::Athena
            | DialectType::Dune
            | DialectType::DataFusion => &["ANALYZE", "ANALYSE", "VERBOSE"],
            DialectType::Redshift => &["VERBOSE"],
            DialectType::SQLite => &["QUERY PLAN"],
            DialectType::Spark | DialectType::Databricks => {
                &["EXTENDED", "FORMATTED", "COST", "CODEGEN"]
            }
            DialectType::Hive => &[
                "EXTENDED",
                "CBO",
                "AST",
                "DEPENDENCY",
                "AUTHORIZATION",
                "LOCKS",
                "VECTORIZATION",
                "ONLY",
                "SUMMARY",
                "OPERATOR",
                "EXPRESSION",
                "DETAIL",
                "ANALYZE",
            ],
            DialectType::ClickHouse => &[
                "TABLE OVERRIDE",
                "CURRENT TRANSACTION",
                "QUERY TREE",
                "SYNTAX",
                "AST",
                "PLAN",
                "PIPELINE",
                "ANALYZE",
                "ESTIMATE",
                "WHATIF",
            ],
            DialectType::MySQL | DialectType::TiDB => &["ANALYZE", "EXTENDED", "PARTITIONS"],
            DialectType::Doris => &[
                "SHAPE PLAN",
                "ALL PLAN",
                "MEMO PLAN",
                "PARSED PLAN",
                "ANALYZED PLAN",
                "REWRITTEN PLAN",
                "OPTIMIZED PLAN",
                "VERBOSE",
            ],
            DialectType::StarRocks => &["ANALYZE", "LOGICAL", "VERBOSE", "COSTS"],
            DialectType::SingleStore => {
                &["EXTENDED", "JSON", "DUPLICATE", "MBC", "MPL", "REPRO", "UI"]
            }
            DialectType::Materialize => &[
                "LOCALLY OPTIMIZED PLAN",
                "OPTIMIZED PLAN",
                "DECORRELATED PLAN",
                "RAW PLAN",
                "PHYSICAL PLAN",
                "PLAN",
                "TIMESTAMP",
                "FILTER PUSHDOWN",
                "ANALYZE",
                "CLUSTER",
                "CPU",
                "MEMORY",
                "HINTS",
                "WITH SKEW",
            ],
            DialectType::Oracle | DialectType::HANA | DialectType::Druid | DialectType::Dremio => {
                &["PLAN"]
            }
            DialectType::Drill => &[
                "PLAN",
                "INCLUDING ALL ATTRIBUTES",
                "INCLUDING ATTRIBUTES",
                "EXCLUDING ATTRIBUTES",
                "WITH IMPLEMENTATION",
                "WITHOUT IMPLEMENTATION",
                "WITH TYPE",
            ],
            DialectType::Teradata => &["IN XML", "NODDLTEXT", "STATIC", "DYNAMIC"],
            DialectType::Vertica => &["LOCAL", "VERBOSE", "JSON", "ANNOTATED"],
            DialectType::Exasol => &["VIRTUAL"],
            DialectType::TSQL => &["WITH_RECOMMENDATIONS"],
            _ => &[],
        };
        loop {
            if self.check(TokenType::LParen)
                && matches!(
                    source_dialect,
                    DialectType::Generic
                        | DialectType::PostgreSQL
                        | DialectType::DuckDB
                        | DialectType::CockroachDB
                        | DialectType::RisingWave
                        | DialectType::Trino
                        | DialectType::Presto
                        | DialectType::Athena
                        | DialectType::Dune
                )
                && !self.peek_nth(1).is_some_and(|t| {
                    matches!(
                        t.token_type,
                        TokenType::Select
                            | TokenType::With
                            | TokenType::Values
                            | TokenType::Table
                            | TokenType::From
                            | TokenType::LParen
                    )
                })
            {
                explain
                    .options
                    .extend(self.explain_option_list(ExplainOptionSyntax::Parenthesized)?);
            } else if let Some(mode) = modes.iter().find(|mode| {
                self.check_keyword_text(mode)
                    || self.check_text_seq(&mode.split_whitespace().collect::<Vec<_>>())
            }) {
                self.explain_words(mode);
                let syntax = if source_dialect == DialectType::Materialize
                    && matches!(*mode, "CPU" | "MEMORY")
                {
                    self.match_token(TokenType::Comma);
                    ExplainOptionSyntax::CommaSeparated
                } else {
                    ExplainOptionSyntax::Keyword
                };
                explain.options.push(ExplainOption {
                    kind: ExplainOptionKind::from_name(mode),
                    value: None,
                    syntax,
                });
            } else if source_dialect == DialectType::Snowflake && self.explain_words("USING") {
                explain.options.push(ExplainOption {
                    kind: ExplainOptionKind::Format,
                    value: Some(self.explain_value()?),
                    syntax: ExplainOptionSyntax::Using,
                });
            } else if self.mysql_explain_family() && self.explain_words("FORMAT") {
                self.expect(TokenType::Eq)?;
                explain.options.push(ExplainOption {
                    kind: ExplainOptionKind::Format,
                    value: Some(self.explain_value()?),
                    syntax: ExplainOptionSyntax::Equals,
                });
            } else if matches!(
                source_dialect,
                DialectType::Materialize | DialectType::Drill
            ) && self.explain_words("AS")
            {
                if source_dialect == DialectType::Materialize && self.explain_words("SQL") {
                    explain.as_sql = true;
                } else {
                    let value = if source_dialect == DialectType::Materialize
                        && self.explain_words("VERBOSE TEXT")
                    {
                        Expression::Identifier(Identifier::new("VERBOSE TEXT"))
                    } else {
                        self.explain_value()?
                    };
                    explain.options.push(ExplainOption {
                        kind: ExplainOptionKind::Format,
                        value: Some(value),
                        syntax: ExplainOptionSyntax::As,
                    });
                }
            } else if source_dialect == DialectType::Materialize
                && self.check_text_seq(&["WITH", "("])
            {
                self.skip();
                explain
                    .options
                    .extend(self.explain_option_list(ExplainOptionSyntax::With)?);
            } else if matches!(source_dialect, DialectType::Oracle | DialectType::HANA)
                && self.explain_words("SET")
            {
                let kind = ExplainOptionKind::from_name(&self.advance_text()?);
                if !matches!(
                    kind,
                    ExplainOptionKind::StatementId | ExplainOptionKind::StatementName
                ) {
                    return Err(self.parse_error("Expected STATEMENT_ID or STATEMENT_NAME"));
                }
                self.expect(TokenType::Eq)?;
                explain.options.push(ExplainOption {
                    kind,
                    value: Some(self.explain_value()?),
                    syntax: ExplainOptionSyntax::Set,
                });
            } else if source_dialect == DialectType::Oracle && self.explain_words("INTO") {
                explain.options.push(ExplainOption {
                    kind: ExplainOptionKind::Destination,
                    value: Some(Expression::Table(Box::new(self.parse_table_ref()?))),
                    syntax: ExplainOptionSyntax::Into,
                });
            } else if source_dialect == DialectType::SingleStore
                && self.explain_words("INTO OUTFILE")
            {
                explain.options.push(ExplainOption {
                    kind: ExplainOptionKind::Outfile,
                    value: Some(self.explain_value()?),
                    syntax: ExplainOptionSyntax::Keyword,
                });
            } else if source_dialect == DialectType::ClickHouse && self.check_next(TokenType::Eq) {
                let kind = ExplainOptionKind::Other(self.advance_text()?);
                self.skip();
                explain.options.push(ExplainOption {
                    kind,
                    value: Some(self.explain_value()?),
                    syntax: ExplainOptionSyntax::Equals,
                });
                self.match_token(TokenType::Comma);
            } else {
                break;
            }
        }
        explain.for_target = self.explain_words("FOR");
        explain.target = if explain.for_target
            && self.mysql_explain_family()
            && self.explain_words("CONNECTION")
        {
            ExplainTarget::Connection(Box::new(self.explain_value()?))
        } else if explain.for_target
            && source_dialect == DialectType::HANA
            && self.explain_words("SQL PLAN CACHE ENTRY")
        {
            let entry = Box::new(self.explain_value()?);
            let variant = if self.explain_words("VARIANT") {
                Some(Box::new(self.explain_value()?))
            } else {
                None
            };
            ExplainTarget::PlanCache { entry, variant }
        } else if source_dialect == DialectType::Materialize
            && (self.check_keyword_text("VIEW")
                || self.check_keyword_text("INDEX")
                || self.check_text_seq(&["MATERIALIZED", "VIEW"]))
        {
            let kind = if self.explain_words("MATERIALIZED VIEW") {
                "MATERIALIZED VIEW".to_string()
            } else {
                self.advance_text()?.to_ascii_uppercase()
            };
            ExplainTarget::Object {
                kind,
                name: Box::new(Expression::Table(Box::new(self.parse_table_ref()?))),
            }
        } else if source_dialect == DialectType::Materialize
            && explain
                .options
                .iter()
                .any(|o| o.kind == ExplainOptionKind::Cluster)
        {
            ExplainTarget::Cluster
        } else if explain
            .options
            .iter()
            .any(|o| o.kind == ExplainOptionKind::CurrentTransaction)
        {
            ExplainTarget::CurrentTransaction
        } else if explain
            .options
            .iter()
            .any(|o| o.kind == ExplainOptionKind::TableOverride)
        {
            let function = Box::new(self.parse_primary()?);
            let mut clauses = Vec::new();
            loop {
                let (name, value) = if self.explain_words("COLUMNS") {
                    (
                        "COLUMNS",
                        self.parse_schema()?
                            .ok_or_else(|| self.parse_error("Expected COLUMNS schema"))?,
                    )
                } else if self.explain_words("ORDER BY") {
                    ("ORDER BY", self.parse_expression()?)
                } else if self.explain_words("PARTITION BY") {
                    ("PARTITION BY", self.parse_expression()?)
                } else if self.explain_words("PRIMARY KEY") {
                    ("PRIMARY KEY", self.parse_expression()?)
                } else if self.explain_words("SAMPLE BY") {
                    ("SAMPLE BY", self.parse_expression()?)
                } else if self.explain_words("TTL") {
                    (
                        "TTL",
                        self.parse_ttl()?
                            .ok_or_else(|| self.parse_error("Expected TTL expression"))?,
                    )
                } else {
                    break;
                };
                clauses.push(ExplainOption {
                    kind: ExplainOptionKind::from_name(name),
                    value: Some(value),
                    syntax: ExplainOptionSyntax::Keyword,
                });
            }
            ExplainTarget::TableOverride { function, clauses }
        } else {
            ExplainTarget::Statement(Box::new(
                self.parse_explain_statement_target(&mut explain.output_format)?,
            ))
        };
        if source_dialect == DialectType::ClickHouse && self.explain_words("FORMAT") {
            explain.output_format = Some(self.expect_identifier_or_keyword_with_quoted()?);
        }
        if source_dialect == DialectType::Materialize && self.explain_words("AS SQL") {
            explain.as_sql = true;
        }
        if !matches!(explain.target, ExplainTarget::Statement(_))
            && !self.is_at_end()
            && !matches!(
                self.peek().token_type,
                TokenType::Semicolon | TokenType::RParen
            )
        {
            return Err(self.parse_error("Unexpected EXPLAIN target suffix"));
        }
        if matches!(
            source_dialect,
            DialectType::Oracle
                | DialectType::HANA
                | DialectType::Drill
                | DialectType::Druid
                | DialectType::Dremio
        ) && (!explain.for_target
            || !explain
                .options
                .iter()
                .any(|o| o.kind == ExplainOptionKind::Plan))
        {
            return Err(self.parse_error("Expected EXPLAIN PLAN ... FOR statement"));
        }
        if source_dialect == DialectType::Exasol
            && !explain
                .options
                .iter()
                .any(|o| o.kind == ExplainOptionKind::Virtual)
        {
            return Err(self.parse_error("Expected EXPLAIN VIRTUAL statement"));
        }
        if source_dialect == DialectType::Materialize {
            let plan = explain
                .options
                .iter()
                .any(|o| o.kind.name().ends_with("PLAN"));
            if plan && !explain.for_target {
                return Err(self.parse_error("Materialize EXPLAIN PLAN requires FOR"));
            }
        }
        Ok(Expression::Explain(Box::new(explain)))
    }

    /// Bound the target to its own statement. A command fallback must never consume
    /// the closing parenthesis or outer query of an EXPLAIN relation.
    fn parse_explain_statement_target(
        &mut self,
        output_format: &mut Option<Identifier>,
    ) -> Result<Expression> {
        let start = self.current;
        let mut end = start;
        let mut depth = 0usize;
        while end < self.tokens.len() {
            match self.tokens[end].token_type {
                TokenType::LParen => depth += 1,
                TokenType::RParen if depth == 0 => break,
                TokenType::RParen => depth -= 1,
                TokenType::Semicolon if depth == 0 => break,
                _ => {}
            }
            end += 1;
        }
        if start == end {
            return Err(self.parse_error("Expected EXPLAIN statement"));
        }
        let mut target_end = end;
        if self.config.dialect == Some(DialectType::ClickHouse)
            && end >= start + 2
            && !Self::text_match_excludes_token(self.tokens[end - 2].token_type)
            && self.tokens[end - 2]
                .text(self.source.as_deref().unwrap_or_default())
                .eq_ignore_ascii_case("FORMAT")
        {
            *output_format = Some(Identifier::new(
                self.tokens[end - 1].text(self.source.as_deref().unwrap_or_default()),
            ));
            target_end -= 2;
        }
        if target_end == start {
            return Err(self.parse_error("Expected EXPLAIN statement before FORMAT"));
        }
        let mut tokens: Vec<Token> = self.tokens[start..target_end]
            .iter()
            .map(|t| self.materialize_token(t))
            .collect();
        // TABLE relation is SELECT * FROM relation. Reuse the FROM-first query
        // parser for this shorthand, including a parenthesized target. The
        // bounded token copy preserves the original source and error spans.
        if matches!(
            self.config.dialect,
            Some(
                DialectType::PostgreSQL
                    | DialectType::DuckDB
                    | DialectType::CockroachDB
                    | DialectType::RisingWave
                    | DialectType::Materialize
            )
        ) {
            if let Some(token) = tokens
                .iter_mut()
                .find(|t| t.token_type != TokenType::LParen)
            {
                if token.token_type == TokenType::Table {
                    token.token_type = TokenType::From;
                    token.text = "FROM".to_string();
                }
            }
        }
        let mut child = Parser::with_config(tokens, self.config.clone());
        child.source = self.source.clone();
        child.recursion = Arc::clone(&self.recursion);
        child.guards_checked = self.guards_checked;
        let result = child.parse_statement();
        self.current = end;
        match result {
            Ok(statement) if child.is_at_end() && statement.is_statement() => Ok(statement),
            Err(error) if is_guard_error(&error) || self.recursion.error().is_some() => Err(error),
            // ClickHouse's corpus includes extensions outside our statement grammar.
            // Keep only that target opaque; the EXPLAIN node and options stay explicit.
            _ if self.config.dialect == Some(DialectType::ClickHouse) => {
                let sql = if self.source.is_some() {
                    self.source_text_range(
                        self.tokens[start].span.start,
                        self.tokens[target_end - 1].span.end,
                    )
                } else {
                    self.tokens_to_sql(start, target_end)
                };
                Ok(Expression::Raw(Raw { sql }))
            }
            Ok(_) => Err(child.parse_error("Unexpected tokens after EXPLAIN target")),
            Err(error) => Err(error),
        }
    }
}
