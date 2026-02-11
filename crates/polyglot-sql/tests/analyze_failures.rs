//! Analyze identity test failures and categorize them

mod common;

use common::{identity_test, IdentityFixtures};
use std::collections::HashMap;
use std::fs;

const FIXTURES_PATH: &str = "tests/fixtures";

/// Analyze identity test failures and categorize them.
///
/// Note: This test runs in a separate thread with a larger stack size (16MB)
/// to handle deeply nested SQL expressions without stack overflow.
#[test]
fn analyze_identity_failures() {
    // Run in a thread with larger stack to avoid stack overflow on deeply nested expressions
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024) // 16MB stack
        .spawn(|| {
            analyze_identity_failures_impl();
        })
        .expect("Failed to spawn test thread")
        .join()
        .expect("Test thread panicked");
}

fn analyze_identity_failures_impl() {
    let path = format!("{}/identity.json", FIXTURES_PATH);
    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            println!("Failed to read fixtures: {}", e);
            return;
        }
    };

    let fixtures: IdentityFixtures = match serde_json::from_str(&content) {
        Ok(f) => f,
        Err(e) => {
            println!("Failed to parse fixtures: {}", e);
            return;
        }
    };

    let mut categories: HashMap<String, Vec<(usize, String, String)>> = HashMap::new();

    for test in &fixtures.tests {
        let result = identity_test(&test.sql);
        if let Err(msg) = result {
            let category = categorize_error(&msg, &test.sql);
            categories
                .entry(category)
                .or_default()
                .push((test.line, test.sql.clone(), msg));
        }
    }

    // Sort categories by count (descending)
    let mut sorted: Vec<_> = categories.iter().collect();
    sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    println!("\n\n=== FAILURE ANALYSIS BY CATEGORY ===\n");

    for (category, failures) in sorted {
        println!("## {} ({} failures)", category, failures.len());
        println!();
        // Show ALL examples for syntax errors, first 5 for others
        let show_count = if category == "Syntax error (expected token)" { failures.len() } else { 5 };
        for (line, sql, msg) in failures.iter().take(show_count) {
            println!("- Line {}: `{}`", line, truncate(sql, 80));
            if category == "Syntax error (expected token)" {
                println!("  Error: {}", truncate(msg, 100));
            }
        }
        if failures.len() > show_count {
            println!("- ... and {} more", failures.len() - show_count);
        }
        println!();
    }

    // Print summary table
    println!("\n=== SUMMARY TABLE ===\n");
    println!("| Category | Count |");
    println!("|----------|-------|");

    let mut sorted: Vec<_> = categories.iter().collect();
    sorted.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

    for (category, failures) in &sorted {
        println!("| {} | {} |", category, failures.len());
    }

    let total_failures: usize = sorted.iter().map(|(_, v)| v.len()).sum();
    println!("| **TOTAL** | **{}** |", total_failures);
}

fn categorize_error(msg: &str, sql: &str) -> String {
    // Parse errors by token type
    if msg.contains("Unexpected token: Amp") {
        return "Bitwise AND operator (&)".to_string();
    }
    if msg.contains("Unexpected token: Pipe") {
        return "Bitwise OR operator (|)".to_string();
    }
    if msg.contains("Unexpected token: Caret") {
        return "Bitwise XOR operator (^)".to_string();
    }
    if msg.contains("Unexpected token: Tilde") {
        return "Bitwise NOT operator (~)".to_string();
    }
    if msg.contains("Unexpected token: Lt") && msg.contains("Parse error") {
        return "Bit shift operator (<<)".to_string();
    }
    if msg.contains("Unexpected token: Gt") && msg.contains("Parse error") {
        return "Bit shift operator (>>)".to_string();
    }
    if msg.contains("Unexpected token: Update") {
        return "FOR UPDATE clause".to_string();
    }
    if msg.contains("Unexpected token: End") {
        return "END keyword in expression".to_string();
    }
    if msg.contains("Unexpected token: At") {
        return "AT TIME ZONE / @ operator".to_string();
    }
    if msg.contains("Unexpected token: Hash") {
        return "Hash operator (#)".to_string();
    }
    if msg.contains("Unexpected token: Arrow") {
        return "Arrow operator (->)".to_string();
    }
    if msg.contains("Unexpected token: DArrow") {
        return "Double arrow operator (->>)".to_string();
    }
    if msg.contains("Unexpected token: Colon") {
        return "Colon in expression".to_string();
    }
    if msg.contains("Unexpected token: Dollar") {
        return "Dollar sign ($)".to_string();
    }
    if msg.contains("Unexpected token: Question") {
        return "Question mark operator (?)".to_string();
    }
    if msg.contains("Unexpected token: Parameter") {
        return "Parameter placeholders".to_string();
    }
    if msg.contains("Unexpected token: Values") {
        return "VALUES expression".to_string();
    }
    if msg.contains("Unexpected token: With") {
        return "WITH clause parsing".to_string();
    }
    if msg.contains("Unexpected token: Extract") {
        return "EXTRACT function".to_string();
    }
    if msg.contains("Unexpected token: Interval") {
        return "INTERVAL expressions".to_string();
    }
    if msg.contains("Unexpected token: Date") {
        return "DATE literals/functions".to_string();
    }
    if msg.contains("Unexpected token: Timestamp") {
        return "TIMESTAMP literals/functions".to_string();
    }
    if msg.contains("Unexpected token: Time") {
        return "TIME literals/functions".to_string();
    }
    if msg.contains("Unexpected token: Array") {
        return "ARRAY expressions".to_string();
    }
    if msg.contains("Unexpected token: Map") {
        return "MAP expressions".to_string();
    }
    if msg.contains("Unexpected token: Struct") {
        return "STRUCT expressions".to_string();
    }
    if msg.contains("Unexpected token: Lambda") {
        return "Lambda expressions".to_string();
    }
    if msg.contains("Unexpected token: If") {
        return "IF expressions".to_string();
    }
    if msg.contains("Unexpected token: Into") {
        return "INTO clause".to_string();
    }
    if msg.contains("Unexpected token: Using") {
        return "USING clause".to_string();
    }
    if msg.contains("Unexpected token: Over") {
        return "OVER clause parsing".to_string();
    }
    if msg.contains("Unexpected token: Partition") {
        return "PARTITION BY parsing".to_string();
    }
    if msg.contains("Unexpected token: Rows") || msg.contains("Unexpected token: Range") {
        return "Window frame specification".to_string();
    }
    if msg.contains("Unexpected token: Preceding") || msg.contains("Unexpected token: Following") {
        return "Window frame bounds".to_string();
    }
    if msg.contains("Unexpected token: Nulls") {
        return "NULLS FIRST/LAST".to_string();
    }
    if msg.contains("Unexpected token: Filter") {
        return "FILTER clause".to_string();
    }
    if msg.contains("Unexpected token: Within") {
        return "WITHIN GROUP".to_string();
    }
    if msg.contains("Unexpected token: Format") {
        return "FORMAT clause".to_string();
    }
    if msg.contains("Unexpected token: Trim") {
        return "TRIM function".to_string();
    }
    if msg.contains("Unexpected token: Substring") {
        return "SUBSTRING function".to_string();
    }
    if msg.contains("Unexpected token: Position") {
        return "POSITION function".to_string();
    }
    if msg.contains("Unexpected token: Overlay") {
        return "OVERLAY function".to_string();
    }
    if msg.contains("Unexpected token: Collate") {
        return "COLLATE clause".to_string();
    }
    if msg.contains("Unexpected token: Set") {
        return "SET operations".to_string();
    }
    if msg.contains("Unexpected token: Escape") {
        return "ESCAPE clause".to_string();
    }
    if msg.contains("Unexpected token: Recursive") {
        return "RECURSIVE CTE".to_string();
    }
    if msg.contains("Unexpected token: Lateral") {
        return "LATERAL keyword".to_string();
    }
    if msg.contains("Unexpected token: Tablesample") {
        return "TABLESAMPLE clause".to_string();
    }
    if msg.contains("Unexpected token: Natural") {
        return "NATURAL JOIN".to_string();
    }
    if msg.contains("Unexpected token: Global") {
        return "GLOBAL keyword".to_string();
    }
    if msg.contains("Unexpected token: Match") {
        return "MATCH expressions".to_string();
    }
    if msg.contains("Unexpected token: Glob") {
        return "GLOB operator".to_string();
    }
    if msg.contains("Unexpected token: Regexp") || msg.contains("Unexpected token: Rlike") {
        return "REGEXP/RLIKE operators".to_string();
    }
    if msg.contains("Unexpected token: Similar") {
        return "SIMILAR TO operator".to_string();
    }
    if msg.contains("Unexpected token: Ilike") {
        return "ILIKE operator".to_string();
    }
    if msg.contains("Unexpected token: Explain") {
        return "EXPLAIN statement".to_string();
    }
    if msg.contains("Unexpected token: Describe") {
        return "DESCRIBE statement".to_string();
    }
    if msg.contains("Unexpected token: Show") {
        return "SHOW statement".to_string();
    }
    if msg.contains("Unexpected token: Pragma") {
        return "PRAGMA statement".to_string();
    }
    if msg.contains("Unexpected token: Fetch") {
        return "FETCH clause".to_string();
    }
    if msg.contains("Unexpected token: Lock") {
        return "LOCK clause".to_string();
    }

    // Mismatch errors
    if msg.contains("Mismatch") {
        if sql.contains("\"\"\"") || sql.contains("'''") {
            return "Triple-quoted strings".to_string();
        }
        if sql.contains("$$") {
            return "Dollar-quoted strings".to_string();
        }
        if sql.contains("\\") {
            return "Escape sequences in strings".to_string();
        }
        if sql.contains("::") {
            return "PostgreSQL cast operator (::)".to_string();
        }
        if sql.to_uppercase().contains("DISTINCT ON") {
            return "DISTINCT ON clause".to_string();
        }
        if sql.to_uppercase().contains("GROUP BY ALL") {
            return "GROUP BY ALL".to_string();
        }
        if sql.to_uppercase().contains("ORDER BY ALL") {
            return "ORDER BY ALL".to_string();
        }
        if sql.contains("/*") || sql.contains("--") {
            return "Comment handling".to_string();
        }
        return "Output mismatch (other)".to_string();
    }

    // Generic parse errors
    if msg.contains("Unexpected token:") {
        let token = msg.split("Unexpected token:").nth(1).unwrap_or("Unknown");
        let token = token.trim().split_whitespace().next().unwrap_or("Unknown");
        return format!("Unexpected token: {}", token);
    }

    if msg.contains("Expected") {
        return "Syntax error (expected token)".to_string();
    }

    "Other error".to_string()
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
