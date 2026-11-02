use super::*;

impl Parser {
    /// Each pipe stage operates on the complete preceding query. Retain these
    /// boundaries rather than merging predicates or projections across stages.
    pub(super) fn parse_clickhouse_pipe_tail(&mut self, query: Expression) -> Result<Expression> {
        if self.config.dialect != Some(crate::dialects::DialectType::ClickHouse)
            || !self.check(TokenType::PipeGt)
        {
            return Ok(query);
        }
        let mut stages = Vec::new();
        while self.match_token(TokenType::PipeGt) {
            let stage = if self.match_token(TokenType::Select) {
                let distinct = self.match_token(TokenType::Distinct);
                PipeStage::Select {
                    expressions: self.parse_select_expressions()?,
                    distinct,
                }
            } else if self.match_token(TokenType::Where) {
                PipeStage::Where(self.parse_expression()?)
            } else if self.match_identifier("AGGREGATE") {
                let expressions = self.parse_select_expressions()?;
                let group_by = if self.match_keywords(&[TokenType::Group, TokenType::By]) {
                    self.parse_select_expressions()?
                } else {
                    Vec::new()
                };
                PipeStage::Aggregate {
                    expressions,
                    group_by,
                }
            } else if self.match_identifier("EXTEND") {
                PipeStage::Extend(self.parse_select_expressions()?)
            } else if self.match_token(TokenType::Set) {
                PipeStage::Set(self.parse_expression_list()?)
            } else if self.match_token(TokenType::Drop) {
                PipeStage::Drop(self.parse_expression_list()?)
            } else if self.match_token(TokenType::Distinct) {
                PipeStage::Distinct
            } else if self.match_keywords(&[TokenType::Order, TokenType::By]) {
                PipeStage::OrderBy(self.parse_order_by()?)
            } else if self.match_token(TokenType::Limit) {
                let count = self.parse_expression()?;
                let offset = if self.match_token(TokenType::Offset) {
                    Some(self.parse_expression()?)
                } else {
                    None
                };
                PipeStage::Limit { count, offset }
            } else if self.match_token(TokenType::Offset) {
                PipeStage::Offset(self.parse_expression()?)
            } else if self.match_token(TokenType::As) {
                PipeStage::Alias(self.expect_identifier_or_keyword_with_quoted()?)
            } else if self.match_token(TokenType::Comma) {
                PipeStage::Cross(self.parse_from()?.expressions)
            } else if self.check_join_keyword() {
                PipeStage::Joins(self.parse_joins()?)
            } else if self.check(TokenType::Union)
                || self.check(TokenType::Intersect)
                || self.check(TokenType::Except)
            {
                let kind = self.advance_text()?.to_ascii_uppercase();
                let distinct = if self.match_token(TokenType::All) {
                    Some(false)
                } else if self.match_token(TokenType::Distinct) {
                    Some(true)
                } else {
                    None
                };
                let query = self.parse_statement()?;
                PipeStage::SetOperation {
                    kind,
                    distinct,
                    query,
                }
            } else {
                return Err(self.parse_error("Expected a pipe operator after |>"));
            };
            stages.push(stage);
        }
        let settings = if self.match_token(TokenType::Settings) {
            self.parse_expression_list()?
        } else {
            Vec::new()
        };
        let format = if self.match_token(TokenType::Format) {
            Some(self.expect_identifier_or_keyword_with_quoted()?)
        } else {
            None
        };
        Ok(Expression::PipeQuery(Box::new(PipeQuery {
            this: query,
            stages,
            with: None,
            settings,
            format,
        })))
    }
}
