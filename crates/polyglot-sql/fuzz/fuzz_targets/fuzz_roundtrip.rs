#![no_main]

use libfuzzer_sys::fuzz_target;
use polyglot_sql::dialects::{Dialect, DialectType};

fuzz_target!(|data: &[u8]| {
    // Convert input bytes to string
    if let Ok(sql) = std::str::from_utf8(data) {
        // Test round-trip parsing (parse -> generate -> parse)
        let dialects = [
            DialectType::Generic,
            DialectType::PostgreSQL,
            DialectType::MySQL,
            DialectType::BigQuery,
            DialectType::Snowflake,
            DialectType::DuckDB,
        ];

        for dialect_type in dialects {
            let dialect = Dialect::get(dialect_type);

            // First parse
            if let Ok(ast) = dialect.parse(sql) {
                // Generate SQL from AST
                for expr in &ast {
                    if let Ok(generated) = dialect.generate(expr) {
                        // Second parse should not panic
                        let _ = dialect.parse(&generated);
                    }
                }
            }
        }
    }
});
