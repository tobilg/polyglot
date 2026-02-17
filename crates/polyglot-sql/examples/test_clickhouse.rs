use std::fs;
use std::path::Path;

use polyglot_sql::{parse, DialectType};

fn main() {
    let dir = Path::new("../ClickHouse/tests/queries/0_stateless");

    let mut sql_files: Vec<_> = fs::read_dir(dir)
        .expect("Cannot read directory")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "sql"))
        .map(|e| e.path())
        .collect();

    sql_files.sort();

    let mut total_files = 0;
    let mut successful_files = 0;
    let mut failed_files = 0;
    let mut total_statements = 0;
    let mut successful_statements = 0;
    let mut failed_statements = 0;
    let mut errors: Vec<(String, String, String)> = Vec::new();

    for path in &sql_files {
        total_files += 1;
        let content = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Cannot read {}: {}", path.display(), e);
                failed_files += 1;
                continue;
            }
        };

        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        let mut file_ok = true;

        // Parse the whole file at once (the parser handles multiple statements)
        match parse(&content, DialectType::ClickHouse) {
            Ok(exprs) => {
                total_statements += exprs.len().max(1);
                successful_statements += exprs.len().max(1);
            }
            Err(e) => {
                // Count statements roughly by semicolons
                let stmt_count = content
                    .split(';')
                    .filter(|s| {
                        s.trim()
                            .lines()
                            .any(|l| {
                                let t = l.trim();
                                !t.is_empty() && !t.starts_with("--")
                            })
                    })
                    .count()
                    .max(1);
                total_statements += stmt_count;
                failed_statements += stmt_count;
                file_ok = false;
                let error_msg = format!("{}", e);
                let display_content: String = content.chars().take(300).collect();
                errors.push((file_name.clone(), display_content, error_msg));
            }
        }

        if file_ok {
            successful_files += 1;
        } else {
            failed_files += 1;
        }
    }

    println!("=== ClickHouse SQL Parsing Test Results ===");
    println!();
    println!(
        "Files:      {} total, {} OK, {} with errors",
        total_files, successful_files, failed_files
    );
    println!(
        "Statements: {} total, ~{} OK, ~{} errors",
        total_statements, successful_statements, failed_statements
    );
    println!();
    println!(
        "Success rate (files):      {:.1}%",
        100.0 * successful_files as f64 / total_files as f64
    );
    println!(
        "Success rate (statements): {:.1}%",
        100.0 * successful_statements as f64 / total_statements as f64
    );
    println!();

    if !errors.is_empty() {
        // Count errors by category
        let mut error_categories: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (_, _, err) in &errors {
            // Normalize error message for grouping
            let key = if let Some(pos) = err.find(" near [") {
                err[..pos].to_string()
            } else {
                err.clone()
            };
            *error_categories.entry(key).or_insert(0) += 1;
        }
        let mut categories: Vec<_> = error_categories.into_iter().collect();
        categories.sort_by(|a, b| b.1.cmp(&a.1));
        println!("=== Error categories ===");
        for (msg, count) in &categories {
            println!("  {:4}  {}", count, msg);
        }

        println!();
        println!("=== First 30 errors ===");
        for (i, (file, stmt, err)) in errors.iter().take(30).enumerate() {
            println!();
            println!("--- Error #{} in {} ---", i + 1, file);
            println!("SQL: {}", stmt);
            println!("Error: {}", err);
        }

        if errors.len() > 30 {
            println!();
            println!("... and {} more errors", errors.len() - 30);
        }
    }
}
