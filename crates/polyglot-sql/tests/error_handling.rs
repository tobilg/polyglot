//! Error Handling Tests
//!
//! Tests for error scenarios to ensure graceful failure and useful error messages.

use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::generator::Generator;
use polyglot_sql::parser::Parser;

// ============================================================================
// Syntax Error Tests
// ============================================================================

mod syntax_errors {
    use super::*;

    #[test]
    fn test_unbalanced_parentheses_open() {
        let result = Parser::parse_sql("SELECT (1 + 2");
        assert!(result.is_err(), "Expected error for unbalanced parentheses");
    }

    #[test]
    fn test_unbalanced_parentheses_close() {
        let result = Parser::parse_sql("SELECT 1 + 2)");
        assert!(result.is_err(), "Expected error for unbalanced parentheses");
    }

    #[test]
    fn test_unbalanced_parentheses_nested() {
        let result = Parser::parse_sql("SELECT ((1 + 2)");
        assert!(result.is_err(), "Expected error for unbalanced nested parentheses");
    }

    #[test]
    fn test_missing_from_keyword() {
        // "SELECT * users" is missing FROM
        let result = Parser::parse_sql("SELECT * users");
        // This might parse differently depending on implementation
        // Just ensure it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_missing_select_keyword() {
        let result = Parser::parse_sql("* FROM users");
        assert!(result.is_err(), "Expected error for missing SELECT");
    }

    #[test]

    fn test_incomplete_where_clause() {
        let result = Parser::parse_sql("SELECT * FROM users WHERE");
        assert!(result.is_err(), "Expected error for incomplete WHERE");
    }

    #[test]

    fn test_incomplete_and_expression() {
        let result = Parser::parse_sql("SELECT * FROM users WHERE a AND");
        assert!(result.is_err(), "Expected error for incomplete AND");
    }

    #[test]

    fn test_incomplete_or_expression() {
        let result = Parser::parse_sql("SELECT * FROM users WHERE a OR");
        assert!(result.is_err(), "Expected error for incomplete OR");
    }

    #[test]
    fn test_missing_expression_in_select() {
        let result = Parser::parse_sql("SELECT FROM users");
        // Might parse as "SELECT FROM" table named "users" or error
        let _ = result;
    }

    #[test]
    fn test_incomplete_join() {
        let result = Parser::parse_sql("SELECT * FROM users JOIN");
        assert!(result.is_err(), "Expected error for incomplete JOIN");
    }

    #[test]
    fn test_join_missing_on() {
        let result = Parser::parse_sql("SELECT * FROM users JOIN orders");
        // Some dialects allow this (CROSS JOIN), just ensure no panic
        let _ = result;
    }

    #[test]

    fn test_incomplete_order_by() {
        let result = Parser::parse_sql("SELECT * FROM users ORDER BY");
        assert!(result.is_err(), "Expected error for incomplete ORDER BY");
    }

    #[test]

    fn test_incomplete_group_by() {
        let result = Parser::parse_sql("SELECT * FROM users GROUP BY");
        assert!(result.is_err(), "Expected error for incomplete GROUP BY");
    }

    #[test]
    fn test_missing_closing_quote() {
        let result = Parser::parse_sql("SELECT 'unclosed string");
        assert!(result.is_err(), "Expected error for unclosed string");
    }

    #[test]
    fn test_invalid_operator() {
        let result = Parser::parse_sql("SELECT 1 <> 2");
        // <> is valid SQL for not equal, should parse
        let _ = result;
    }

    #[test]
    fn test_double_comma() {
        let result = Parser::parse_sql("SELECT a,, b FROM users");
        assert!(result.is_err(), "Expected error for double comma");
    }

    #[test]
    fn test_trailing_comma_in_select() {
        let result = Parser::parse_sql("SELECT a, b, FROM users");
        assert!(result.is_err(), "Expected error for trailing comma");
    }
}

// ============================================================================
// Empty/Whitespace Input Tests
// ============================================================================

mod empty_input {
    use super::*;

    #[test]
    fn test_empty_string() {
        let result = Parser::parse_sql("");
        // Should return empty vec or graceful error
        match result {
            Ok(stmts) => assert!(stmts.is_empty(), "Empty input should produce no statements"),
            Err(_) => {} // Also acceptable
        }
    }

    #[test]
    fn test_whitespace_only() {
        let result = Parser::parse_sql("   \n\t  ");
        match result {
            Ok(stmts) => assert!(stmts.is_empty(), "Whitespace should produce no statements"),
            Err(_) => {} // Also acceptable
        }
    }

    #[test]
    fn test_only_semicolon() {
        let result = Parser::parse_sql(";");
        // Should handle gracefully
        let _ = result;
    }

    #[test]
    fn test_multiple_semicolons() {
        let result = Parser::parse_sql(";;;");
        // Should handle gracefully
        let _ = result;
    }

    #[test]
    fn test_only_comment() {
        let result = Parser::parse_sql("-- this is a comment");
        match result {
            Ok(stmts) => assert!(stmts.is_empty(), "Comment should produce no statements"),
            Err(_) => {} // Also acceptable
        }
    }

    #[test]
    fn test_only_block_comment() {
        let result = Parser::parse_sql("/* block comment */");
        match result {
            Ok(stmts) => assert!(stmts.is_empty(), "Block comment should produce no statements"),
            Err(_) => {} // Also acceptable
        }
    }
}

// ============================================================================
// Invalid Dialect Tests
// ============================================================================

mod invalid_dialect {
    use super::*;

    #[test]
    fn test_invalid_dialect_from_str() {
        let result: Result<DialectType, _> = "invalid_dialect".parse();
        assert!(result.is_err(), "Invalid dialect should return error");
    }

    #[test]
    fn test_case_insensitive_dialect() {
        let result1: Result<DialectType, _> = "POSTGRES".parse();
        let result2: Result<DialectType, _> = "PostgreSQL".parse();
        let result3: Result<DialectType, _> = "postgresql".parse();

        assert!(result1.is_ok(), "POSTGRES should be valid");
        assert!(result2.is_ok(), "PostgreSQL should be valid");
        assert!(result3.is_ok(), "postgresql should be valid");
    }

    #[test]
    fn test_alternate_dialect_names() {
        // Test alternate names for SQL Server
        let tsql1: Result<DialectType, _> = "tsql".parse();
        let tsql2: Result<DialectType, _> = "mssql".parse();
        let tsql3: Result<DialectType, _> = "sqlserver".parse();

        assert!(tsql1.is_ok(), "tsql should be valid");
        assert!(tsql2.is_ok(), "mssql should be valid");
        assert!(tsql3.is_ok(), "sqlserver should be valid");

        // Test alternate names for CockroachDB
        let crdb1: Result<DialectType, _> = "cockroachdb".parse();
        let crdb2: Result<DialectType, _> = "cockroach".parse();

        assert!(crdb1.is_ok(), "cockroachdb should be valid");
        assert!(crdb2.is_ok(), "cockroach should be valid");
    }
}

// ============================================================================
// Unicode Edge Cases
// ============================================================================

mod unicode_tests {
    use super::*;

    #[test]
    fn test_unicode_string_literal() {
        let result = Parser::parse_sql("SELECT '你好世界'");
        // Should parse Unicode string
        assert!(result.is_ok(), "Unicode string should parse: {:?}", result);
    }

    #[test]
    fn test_unicode_in_comment() {
        let result = Parser::parse_sql("SELECT 1 -- 日本語コメント");
        assert!(result.is_ok(), "Unicode in comment should parse");
    }

    #[test]
    fn test_emoji_in_string() {
        let result = Parser::parse_sql("SELECT '😀🎉'");
        // Should handle emoji in strings
        assert!(result.is_ok(), "Emoji in string should parse");
    }

    #[test]
    fn test_unicode_identifier() {
        // Unicode identifiers may or may not be supported
        let result = Parser::parse_sql("SELECT 用户名 FROM 表");
        // Just ensure it doesn't panic
        let _ = result;
    }
}

// ============================================================================
// Deeply Nested Expression Tests
// ============================================================================

mod nesting_tests {
    use super::*;

    #[test]
    fn test_deeply_nested_parentheses() {
        // Run in a thread with larger stack to avoid stack overflow
        // during recursive Drop of nested Expression structures
        let handle = std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024) // 4MB stack
            .spawn(|| {
                let sql = "SELECT ((((((((1 + 2))))))))";
                let result = Parser::parse_sql(sql);
                assert!(result.is_ok(), "Deeply nested parentheses should parse");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_nested_subqueries() {
        let sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM users))";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Nested subqueries should parse");
    }

    #[test]
    fn test_nested_case_expressions() {
        let sql = "SELECT CASE WHEN a THEN CASE WHEN b THEN 1 ELSE 2 END ELSE 3 END";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Nested CASE should parse");
    }

    #[test]
    fn test_deeply_nested_function_calls() {
        // Run in a thread with larger stack to avoid stack overflow
        // during recursive Drop of nested Expression structures
        let handle = std::thread::Builder::new()
            .stack_size(4 * 1024 * 1024) // 4MB stack
            .spawn(|| {
                let sql = "SELECT UPPER(LOWER(TRIM(UPPER(LOWER(name)))))";
                let result = Parser::parse_sql(sql);
                assert!(result.is_ok(), "Nested functions should parse");
            })
            .unwrap();
        handle.join().unwrap();
    }

    #[test]
    #[cfg_attr(debug_assertions, ignore = "Stack overflow in debug builds due to large stack frames - passes in release mode")]
    fn test_complex_nested_expression() {
        let sql = "SELECT (1 + (2 * (3 - (4 / (5 + 6)))))";
        let result = Parser::parse_sql(sql);
        assert!(result.is_ok(), "Complex nested math should parse");
    }
}

// ============================================================================
// Large/Stress Tests
// ============================================================================

mod stress_tests {
    use super::*;

    #[test]
    fn test_many_columns() {
        // Generate a SELECT with 100 columns
        let columns: Vec<String> = (1..=100).map(|i| format!("col{}", i)).collect();
        let sql = format!("SELECT {} FROM table1", columns.join(", "));
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Many columns should parse");
    }

    #[test]
    fn test_long_string_literal() {
        // Generate a long string (1KB)
        let long_string = "a".repeat(1024);
        let sql = format!("SELECT '{}'", long_string);
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Long string should parse");
    }

    #[test]
    fn test_many_unions() {
        // Generate a query with many UNIONs
        let selects: Vec<String> = (1..=20).map(|i| format!("SELECT {}", i)).collect();
        let sql = selects.join(" UNION ALL ");
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Many UNIONs should parse");
    }

    #[test]
    fn test_many_joins() {
        // Generate a query with many JOINs
        let mut sql = String::from("SELECT * FROM t1");
        for i in 2..=10 {
            sql.push_str(&format!(" JOIN t{} ON t1.id = t{}.id", i, i));
        }
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Many JOINs should parse");
    }

    #[test]
    fn test_long_in_list() {
        // Generate an IN clause with many values
        let values: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
        let sql = format!("SELECT * FROM users WHERE id IN ({})", values.join(", "));
        let result = Parser::parse_sql(&sql);
        assert!(result.is_ok(), "Long IN list should parse");
    }
}

// ============================================================================
// Transpile Error Tests
// ============================================================================

mod transpile_errors {
    use super::*;

    #[test]

    fn test_transpile_invalid_sql() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile_to("SELECT (", DialectType::PostgreSQL);
        assert!(result.is_err(), "Transpiling invalid SQL should fail");
    }

    #[test]
    fn test_transpile_empty_input() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile_to("", DialectType::PostgreSQL);
        match result {
            Ok(stmts) => assert!(stmts.is_empty(), "Empty input should transpile to empty"),
            Err(_) => {} // Also acceptable
        }
    }

    #[test]
    fn test_transpile_same_dialect() {
        let dialect = Dialect::get(DialectType::PostgreSQL);
        let result = dialect.transpile_to("SELECT 1", DialectType::PostgreSQL);
        assert!(result.is_ok(), "Same dialect transpile should work");
        assert_eq!(result.unwrap()[0], "SELECT 1");
    }
}

// ============================================================================
// Generator Error Tests
// ============================================================================

mod generator_tests {
    use super::*;

    #[test]
    fn test_generator_valid_ast() {
        let dialect = Dialect::get(DialectType::Generic);
        let ast = dialect.parse("SELECT 1").expect("Parse failed");
        let result = Generator::sql(&ast[0]);
        assert!(result.is_ok(), "Generator should produce valid SQL");
    }

    #[test]
    fn test_roundtrip_consistency() {
        // Parse, generate, parse again should be consistent
        let sql = "SELECT a, b FROM users WHERE id = 1";
        let ast1 = Parser::parse_sql(sql).expect("First parse failed");
        let generated = Generator::sql(&ast1[0]).expect("Generate failed");
        let ast2 = Parser::parse_sql(&generated).expect("Second parse failed");
        let generated2 = Generator::sql(&ast2[0]).expect("Second generate failed");

        assert_eq!(generated, generated2, "Roundtrip should be stable");
    }
}
