use super::*;

impl Generator {
    pub(super) fn generate_inline_functions(&mut self, query: &WithFunction) -> Result<()> {
        if !matches!(self.config.dialect, None | Some(DialectType::Trino)) {
            return Err(crate::error::Error::unsupported(
                "inline SQL routines",
                "target dialect",
            ));
        }
        self.write_keyword("WITH ");
        for (i, function) in query.functions.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write_keyword("FUNCTION ");
            self.generate_identifier(&function.name)?;
            self.write("(");
            for (j, parameter) in function.parameters.iter().enumerate() {
                if j > 0 {
                    self.write(", ");
                }
                if let Some(name) = &parameter.name {
                    self.generate_identifier(name)?;
                    self.write_space();
                }
                self.generate_data_type(&parameter.data_type)?;
            }
            self.write_keyword(") RETURNS ");
            self.generate_data_type(&function.return_type)?;
            for characteristic in &function.characteristics {
                self.write_space();
                match characteristic {
                    RoutineCharacteristic::Language(name) => {
                        self.write_keyword("LANGUAGE ");
                        self.write(name);
                    }
                    RoutineCharacteristic::Deterministic(true) => {
                        self.write_keyword("DETERMINISTIC")
                    }
                    RoutineCharacteristic::Deterministic(false) => {
                        self.write_keyword("NOT DETERMINISTIC")
                    }
                    RoutineCharacteristic::ReturnsNullOnNullInput(true) => {
                        self.write_keyword("RETURNS NULL ON NULL INPUT")
                    }
                    RoutineCharacteristic::ReturnsNullOnNullInput(false) => {
                        self.write_keyword("CALLED ON NULL INPUT")
                    }
                    RoutineCharacteristic::Security(security) => {
                        self.write_keyword(match security {
                            FunctionSecurity::Definer => "SECURITY DEFINER",
                            FunctionSecurity::Invoker => "SECURITY INVOKER",
                            FunctionSecurity::None => "SECURITY NONE",
                        })
                    }
                    RoutineCharacteristic::Comment(comment) => {
                        self.write_keyword("COMMENT ");
                        self.generate_string_literal(comment)?;
                    }
                    RoutineCharacteristic::Properties(properties) => {
                        self.write_keyword("WITH (");
                        for (k, property) in properties.iter().enumerate() {
                            if k > 0 {
                                self.write(", ");
                            }
                            if let Expression::Eq(eq) = property {
                                self.generate_expression(&eq.left)?;
                                self.write("=");
                                self.generate_expression(&eq.right)?;
                            } else {
                                self.generate_expression(property)?;
                            }
                        }
                        self.write(")");
                    }
                }
            }
            self.write_space();
            self.generate_routine_statement(&function.body)?;
        }
        self.write_space();
        self.generate_expression(&query.this)
    }

    fn generate_routine_statements(&mut self, statements: &[RoutineStatement]) -> Result<()> {
        for statement in statements {
            self.write_space();
            self.generate_routine_statement(statement)?;
            self.write(";");
        }
        Ok(())
    }

    fn generate_routine_label(&mut self, label: &Option<Identifier>) -> Result<()> {
        if let Some(label) = label {
            self.generate_identifier(label)?;
            self.write(": ");
        }
        Ok(())
    }

    fn generate_routine_statement(&mut self, statement: &RoutineStatement) -> Result<()> {
        match statement {
            RoutineStatement::Block(statements) => {
                self.write_keyword("BEGIN");
                self.generate_routine_statements(statements)?;
                self.write_keyword(" END");
            }
            RoutineStatement::Declare {
                names,
                data_type,
                default,
            } => {
                self.write_keyword("DECLARE ");
                for (i, name) in names.iter().enumerate() {
                    if i > 0 {
                        self.write(", ");
                    }
                    self.generate_identifier(name)?;
                }
                self.write_space();
                self.generate_data_type(data_type)?;
                if let Some(default) = default {
                    self.write_keyword(" DEFAULT ");
                    self.generate_expression(default)?;
                }
            }
            RoutineStatement::Set { name, value } => {
                self.write_keyword("SET ");
                self.generate_identifier(name)?;
                self.write(" = ");
                self.generate_expression(value)?;
            }
            RoutineStatement::Return(value) => {
                self.write_keyword("RETURN ");
                self.generate_expression(value)?;
            }
            RoutineStatement::If {
                branches,
                otherwise,
            } => {
                for (i, branch) in branches.iter().enumerate() {
                    self.write_keyword(if i == 0 { "IF " } else { " ELSEIF " });
                    self.generate_expression(&branch.condition)?;
                    self.write_keyword(" THEN");
                    self.generate_routine_statements(&branch.statements)?;
                }
                if !otherwise.is_empty() {
                    self.write_keyword(" ELSE");
                    self.generate_routine_statements(otherwise)?;
                }
                self.write_keyword(" END IF");
            }
            RoutineStatement::Case {
                operand,
                branches,
                otherwise,
            } => {
                self.write_keyword("CASE");
                if let Some(operand) = operand {
                    self.write_space();
                    self.generate_expression(operand)?;
                }
                for branch in branches {
                    self.write_keyword(" WHEN ");
                    self.generate_expression(&branch.condition)?;
                    self.write_keyword(" THEN");
                    self.generate_routine_statements(&branch.statements)?;
                }
                if !otherwise.is_empty() {
                    self.write_keyword(" ELSE");
                    self.generate_routine_statements(otherwise)?;
                }
                self.write_keyword(" END CASE");
            }
            RoutineStatement::While {
                label,
                condition,
                statements,
            } => {
                self.generate_routine_label(label)?;
                self.write_keyword("WHILE ");
                self.generate_expression(condition)?;
                self.write_keyword(" DO");
                self.generate_routine_statements(statements)?;
                self.write_keyword(" END WHILE");
            }
            RoutineStatement::Loop { label, statements } => {
                self.generate_routine_label(label)?;
                self.write_keyword("LOOP");
                self.generate_routine_statements(statements)?;
                self.write_keyword(" END LOOP");
            }
            RoutineStatement::Repeat {
                label,
                statements,
                until,
            } => {
                self.generate_routine_label(label)?;
                self.write_keyword("REPEAT");
                self.generate_routine_statements(statements)?;
                self.write_keyword(" UNTIL ");
                self.generate_expression(until)?;
                self.write_keyword(" END REPEAT");
            }
            RoutineStatement::Leave(label) => {
                self.write_keyword("LEAVE ");
                self.generate_identifier(label)?;
            }
            RoutineStatement::Iterate(label) => {
                self.write_keyword("ITERATE ");
                self.generate_identifier(label)?;
            }
        }
        Ok(())
    }
}
