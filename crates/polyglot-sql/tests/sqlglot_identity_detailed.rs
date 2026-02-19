//! Detailed identity test output - shows all failures with input and output

mod common;

use common::{identity_test, IdentityFixtures};
use once_cell::sync::Lazy;
use std::fs;

const FIXTURES_PATH: &str = "tests/sqlglot_fixtures";

static IDENTITY_FIXTURES: Lazy<Option<IdentityFixtures>> = Lazy::new(|| {
    let path = format!("{}/identity.json", FIXTURES_PATH);
    match fs::read_to_string(&path) {
        Ok(content) => match serde_json::from_str(&content) {
            Ok(fixtures) => Some(fixtures),
            Err(e) => {
                eprintln!("Warning: Failed to parse identity.json: {}", e);
                None
            }
        },
        Err(e) => {
            eprintln!("Warning: Failed to read identity.json: {}", e);
            None
        }
    }
});

/// Note: This test runs in a separate thread with a larger stack size (16MB)
/// to handle deeply nested SQL expressions without stack overflow.
#[test]
fn test_sqlglot_identity_detailed() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024) // 16MB stack
        .spawn(|| {
            test_sqlglot_identity_detailed_impl();
        })
        .expect("Failed to spawn test thread")
        .join()
        .expect("Test thread panicked");
}

fn test_sqlglot_identity_detailed_impl() {
    let fixtures = match IDENTITY_FIXTURES.as_ref() {
        Some(f) => f,
        None => {
            println!("Skipping tests - fixtures not available");
            return;
        }
    };

    let mut mismatches = Vec::new();

    for test in &fixtures.tests {
        let result = identity_test(&test.sql);
        if let Err(msg) = result {
            if msg.contains("Mismatch") {
                mismatches.push((test.line, test.sql.clone(), msg));
            }
        }
    }

    println!("\n=== ALL MISMATCHES ({} total) ===\n", mismatches.len());

    for (line, sql, error) in &mismatches {
        println!("Line {}: {}", line, sql);
        if let Some(output_start) = error.find("output:") {
            let output_part = &error[output_start + 7..].trim();
            println!("  Output: {}", output_part);
        }
        println!();
    }

    println!("Total mismatches: {}", mismatches.len());
}
