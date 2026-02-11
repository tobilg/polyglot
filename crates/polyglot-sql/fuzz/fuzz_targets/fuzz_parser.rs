#![no_main]

use libfuzzer_sys::fuzz_target;
use polyglot_sql::dialects::{Dialect, DialectType};

fuzz_target!(|data: &[u8]| {
    // Convert input bytes to string
    if let Ok(sql) = std::str::from_utf8(data) {
        // Try parsing with various dialects
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
            // The parser should never panic, regardless of input
            let _ = dialect.parse(sql);
        }
    }
});
