//! Trino SQL routines: https://trino.io/docs/current/udf/sql.html
use super::*;

impl Parser {
    pub(super) fn parse_inline_functions(&mut self) -> Result<Expression> {
        self.expect(TokenType::With)?;
        let mut functions = Vec::new();
        loop {
            self.expect(TokenType::Function)?;
            let name = self.expect_identifier_or_keyword_with_quoted()?;
            self.expect(TokenType::LParen)?;
            let parameters = self.parse_function_parameters()?;
            self.expect(TokenType::RParen)?;
            self.expect(TokenType::Returns)?;
            let return_type = self.parse_data_type()?;
            let mut characteristics = Vec::new();
            loop {
                let characteristic = if self.match_token(TokenType::Language) {
                    RoutineCharacteristic::Language(self.expect_identifier_or_keyword()?)
                } else if self.match_identifier("DETERMINISTIC") {
                    RoutineCharacteristic::Deterministic(true)
                } else if self.match_text_seq(&["NOT", "DETERMINISTIC"]) {
                    RoutineCharacteristic::Deterministic(false)
                } else if self.match_text_seq(&["RETURNS", "NULL", "ON", "NULL", "INPUT"]) {
                    RoutineCharacteristic::ReturnsNullOnNullInput(true)
                } else if self.match_text_seq(&["CALLED", "ON", "NULL", "INPUT"]) {
                    RoutineCharacteristic::ReturnsNullOnNullInput(false)
                } else if self.match_identifier("SECURITY") {
                    let security = if self.match_identifier("DEFINER") {
                        FunctionSecurity::Definer
                    } else if self.match_identifier("INVOKER") {
                        FunctionSecurity::Invoker
                    } else {
                        return Err(self.parse_error("Expected DEFINER or INVOKER"));
                    };
                    RoutineCharacteristic::Security(security)
                } else if self.match_token(TokenType::Comment) {
                    RoutineCharacteristic::Comment(self.expect_string()?)
                } else if self.match_token(TokenType::With) {
                    self.expect(TokenType::LParen)?;
                    let properties = self.parse_expression_list()?;
                    self.expect(TokenType::RParen)?;
                    RoutineCharacteristic::Properties(properties)
                } else {
                    break;
                };
                characteristics.push(characteristic);
            }
            let body = self.parse_routine_statement()?;
            if !matches!(
                body,
                RoutineStatement::Block(_) | RoutineStatement::Return(_)
            ) {
                return Err(self.parse_error("Expected RETURN or BEGIN function body"));
            }
            functions.push(InlineFunction {
                name,
                parameters,
                return_type,
                characteristics,
                body,
            });
            if !self.match_token(TokenType::Comma) {
                break;
            }
        }
        let this = self.parse_statement()?;
        Ok(Expression::WithFunction(Box::new(WithFunction {
            functions,
            this,
        })))
    }

    fn parse_routine_statements(&mut self, terminators: &[&str]) -> Result<Vec<RoutineStatement>> {
        let mut statements = Vec::new();
        while !self.is_at_end() && !terminators.iter().any(|word| self.check_keyword_text(word)) {
            statements.push(self.parse_routine_statement()?);
            self.expect(TokenType::Semicolon)?;
        }
        Ok(statements)
    }

    fn parse_routine_statement(&mut self) -> Result<RoutineStatement> {
        self.with_recursive_stack(|parser| parser.parse_routine_statement_inner())
    }

    fn parse_routine_statement_inner(&mut self) -> Result<RoutineStatement> {
        let label = if self.check_next(TokenType::Colon) {
            let label = self.expect_identifier_or_keyword_with_quoted()?;
            self.expect(TokenType::Colon)?;
            Some(label)
        } else {
            None
        };
        if label.is_some()
            && !["WHILE", "LOOP", "REPEAT"]
                .iter()
                .any(|s| self.check_keyword_text(s))
        {
            return Err(self.parse_error("Routine labels require a loop"));
        }
        if self.match_token(TokenType::Begin) {
            let statements = self.parse_routine_statements(&["END"])?;
            self.expect(TokenType::End)?;
            Ok(RoutineStatement::Block(statements))
        } else if self.match_token(TokenType::Declare) {
            let mut names = vec![self.expect_identifier_or_keyword_with_quoted()?];
            while self.match_token(TokenType::Comma) {
                names.push(self.expect_identifier_or_keyword_with_quoted()?);
            }
            let data_type = self.parse_data_type()?;
            let default = if self.match_token(TokenType::Default) {
                Some(self.parse_expression()?)
            } else {
                None
            };
            Ok(RoutineStatement::Declare {
                names,
                data_type,
                default,
            })
        } else if self.match_token(TokenType::Set) {
            let name = self.expect_identifier_or_keyword_with_quoted()?;
            self.expect(TokenType::Eq)?;
            let value = self.parse_expression()?;
            Ok(RoutineStatement::Set { name, value })
        } else if self.match_identifier("RETURN") {
            Ok(RoutineStatement::Return(self.parse_expression()?))
        } else if self.match_token(TokenType::If) {
            let mut branches = Vec::new();
            loop {
                let condition = self.parse_expression()?;
                self.expect(TokenType::Then)?;
                let statements = self.parse_routine_statements(&["ELSEIF", "ELSE", "END"])?;
                branches.push(RoutineBranch {
                    condition,
                    statements,
                });
                if !self.match_identifier("ELSEIF") {
                    break;
                }
            }
            let otherwise = if self.match_token(TokenType::Else) {
                self.parse_routine_statements(&["END"])?
            } else {
                Vec::new()
            };
            self.expect(TokenType::End)?;
            self.expect(TokenType::If)?;
            Ok(RoutineStatement::If {
                branches,
                otherwise,
            })
        } else if self.match_token(TokenType::Case) {
            let operand = if self.check(TokenType::When) {
                None
            } else {
                Some(self.parse_expression()?)
            };
            let mut branches = Vec::new();
            while self.match_token(TokenType::When) {
                let condition = self.parse_expression()?;
                self.expect(TokenType::Then)?;
                let statements = self.parse_routine_statements(&["WHEN", "ELSE", "END"])?;
                branches.push(RoutineBranch {
                    condition,
                    statements,
                });
            }
            if branches.is_empty() {
                return Err(self.parse_error("Expected WHEN in routine CASE"));
            }
            let otherwise = if self.match_token(TokenType::Else) {
                self.parse_routine_statements(&["END"])?
            } else {
                Vec::new()
            };
            self.expect(TokenType::End)?;
            self.expect(TokenType::Case)?;
            Ok(RoutineStatement::Case {
                operand,
                branches,
                otherwise,
            })
        } else if self.match_identifier("WHILE") {
            let condition = self.parse_expression()?;
            if !self.match_identifier("DO") {
                return Err(self.parse_error("Expected DO"));
            }
            let statements = self.parse_routine_statements(&["END"])?;
            if !self.match_text_seq(&["END", "WHILE"]) {
                return Err(self.parse_error("Expected END WHILE"));
            }
            Ok(RoutineStatement::While {
                label,
                condition,
                statements,
            })
        } else if self.match_identifier("LOOP") {
            let statements = self.parse_routine_statements(&["END"])?;
            if !self.match_text_seq(&["END", "LOOP"]) {
                return Err(self.parse_error("Expected END LOOP"));
            }
            Ok(RoutineStatement::Loop { label, statements })
        } else if self.match_identifier("REPEAT") {
            let statements = self.parse_routine_statements(&["UNTIL"])?;
            if !self.match_identifier("UNTIL") {
                return Err(self.parse_error("Expected UNTIL"));
            }
            let until = self.parse_expression()?;
            if !self.match_text_seq(&["END", "REPEAT"]) {
                return Err(self.parse_error("Expected END REPEAT"));
            }
            Ok(RoutineStatement::Repeat {
                label,
                statements,
                until,
            })
        } else if self.match_identifier("LEAVE") {
            Ok(RoutineStatement::Leave(
                self.expect_identifier_or_keyword_with_quoted()?,
            ))
        } else if self.match_identifier("ITERATE") {
            Ok(RoutineStatement::Iterate(
                self.expect_identifier_or_keyword_with_quoted()?,
            ))
        } else {
            Err(self.parse_error("Expected SQL routine statement"))
        }
    }
}
