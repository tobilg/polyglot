use super::*;

impl Generator {
    fn generate_pipe_fields(&mut self, expressions: &[Expression]) -> Result<()> {
        for (index, expression) in expressions.iter().enumerate() {
            if index > 0 {
                self.write(", ");
            }
            self.generate_expression(expression)?;
        }
        Ok(())
    }

    pub(super) fn generate_pipe_query(&mut self, query: &PipeQuery) -> Result<()> {
        if !matches!(self.config.dialect, None | Some(DialectType::ClickHouse)) {
            return Err(crate::error::Error::unsupported(
                "ClickHouse pipe query",
                "target dialect",
            ));
        }
        if let Some(with) = &query.with {
            self.generate_with(with)?;
            self.write_space();
        }
        self.generate_expression(&query.this)?;
        for stage in &query.stages {
            self.write(" |> ");
            match stage {
                PipeStage::Select {
                    expressions,
                    distinct,
                } => {
                    self.write_keyword(if *distinct {
                        "SELECT DISTINCT "
                    } else {
                        "SELECT "
                    });
                    self.generate_pipe_fields(expressions)?;
                }
                PipeStage::Where(condition) => {
                    self.write_keyword("WHERE ");
                    self.generate_expression(condition)?;
                }
                PipeStage::Aggregate {
                    expressions,
                    group_by,
                } => {
                    self.write_keyword("AGGREGATE ");
                    self.generate_pipe_fields(expressions)?;
                    if !group_by.is_empty() {
                        self.write_keyword(" GROUP BY ");
                        self.generate_pipe_fields(group_by)?;
                    }
                }
                PipeStage::Extend(fields) | PipeStage::Set(fields) | PipeStage::Drop(fields) => {
                    self.write_keyword(match stage {
                        PipeStage::Extend(_) => "EXTEND ",
                        PipeStage::Set(_) => "SET ",
                        _ => "DROP ",
                    });
                    self.generate_pipe_fields(fields)?;
                }
                PipeStage::Distinct => self.write_keyword("DISTINCT"),
                PipeStage::OrderBy(order) => self.generate_order_by(order)?,
                PipeStage::Limit { count, offset } => {
                    self.write_keyword("LIMIT ");
                    self.generate_expression(count)?;
                    if let Some(offset) = offset {
                        self.write_keyword(" OFFSET ");
                        self.generate_expression(offset)?;
                    }
                }
                PipeStage::Offset(offset) => {
                    self.write_keyword("OFFSET ");
                    self.generate_expression(offset)?;
                }
                PipeStage::Alias(alias) => {
                    self.write_keyword("AS ");
                    self.generate_identifier(alias)?;
                }
                PipeStage::Joins(joins) => {
                    // generate_join supplies its own leading separator.
                    self.output.pop();
                    for join in joins {
                        self.generate_join(join)?;
                    }
                }
                PipeStage::Cross(tables) => {
                    self.write(", ");
                    self.generate_pipe_fields(tables)?;
                }
                PipeStage::SetOperation {
                    kind,
                    distinct,
                    query,
                } => {
                    self.write_keyword(kind);
                    if let Some(distinct) = distinct {
                        self.write_keyword(if *distinct { " DISTINCT" } else { " ALL" });
                    }
                    self.write_space();
                    self.generate_expression(query)?;
                }
            }
        }
        if !query.settings.is_empty() {
            self.write_keyword(" SETTINGS ");
            self.generate_pipe_fields(&query.settings)?;
        }
        if let Some(format) = &query.format {
            self.write_keyword(" FORMAT ");
            self.generate_identifier(format)?;
        }
        Ok(())
    }
}
