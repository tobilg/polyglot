//! Basic usage examples for polyglot-sql.
//!
//! Run with: cargo run --example basic_usage -p polyglot-sql

use polyglot_sql::builder::{col, delete, func, insert_into, lit, null, select, star, update};
use polyglot_sql::{parse_one, transpile, validate, DialectType, Generator};

fn main() {
    parsing();
    generation();
    transpilation();
    validation();
    builder_api();
}

/// Parse SQL into an AST and inspect it.
fn parsing() {
    println!("=== Parsing ===\n");

    let expr = parse_one(
        "SELECT id, name FROM users WHERE active = TRUE",
        DialectType::Generic,
    )
    .expect("parse failed");

    println!("Parsed AST (debug): {:#?}\n", expr);
}

/// Parse SQL then regenerate it (round-trip).
fn generation() {
    println!("=== Generation ===\n");

    let expr =
        parse_one("select id,name from users where id=1", DialectType::Generic).expect("parse");

    // Default generation normalizes to uppercase keywords
    let sql = Generator::sql(&expr).expect("generate");
    println!("Normalized: {}\n", sql);

    // Generate for a specific dialect
    let pg_sql = polyglot_sql::generate(&expr, DialectType::PostgreSQL).expect("generate pg");
    println!("PostgreSQL: {}\n", pg_sql);
}

/// Transpile SQL between dialects.
fn transpilation() {
    println!("=== Transpilation ===\n");

    // DuckDB -> PostgreSQL
    let result = transpile(
        "SELECT EPOCH_MS(1618088028295)",
        DialectType::DuckDB,
        DialectType::PostgreSQL,
    )
    .expect("transpile");
    println!("DuckDB -> PostgreSQL: {}", result[0]);

    // BigQuery -> Snowflake
    let result = transpile(
        "SELECT SAFE_DIVIDE(a, b) FROM t",
        DialectType::BigQuery,
        DialectType::Snowflake,
    )
    .expect("transpile");
    println!("BigQuery -> Snowflake: {}", result[0]);

    // MySQL -> PostgreSQL
    let result = transpile(
        "SELECT IF(a > 0, 'positive', 'negative')",
        DialectType::MySQL,
        DialectType::PostgreSQL,
    )
    .expect("transpile");
    println!("MySQL -> PostgreSQL:   {}\n", result[0]);
}

/// Validate SQL syntax.
fn validation() {
    println!("=== Validation ===\n");

    let good = validate("SELECT 1", DialectType::Generic);
    println!("Valid SQL:   valid={}", good.valid);

    let bad = validate("SELECT FROM WHERE", DialectType::Generic);
    println!("Invalid SQL: valid={}", bad.valid);
    for error in &bad.errors {
        println!("  Error: {}", error.message);
    }
    println!();
}

/// Build SQL programmatically using the fluent builder API.
fn builder_api() {
    println!("=== Builder API ===\n");

    // SELECT with WHERE, ORDER BY, LIMIT
    let query = select(["id", "name", "email"])
        .from("users")
        .where_(col("age").gt(lit(18)).and(col("active").eq(lit(true))))
        .order_by(["name"])
        .limit(10)
        .build();
    println!("Select:  {}", Generator::sql(&query).unwrap());

    // SELECT with JOIN
    let query = select(["u.name", "o.total"])
        .from("users AS u")
        .join("orders AS o", col("u.id").eq(col("o.user_id")))
        .where_(col("o.total").gt(lit(100)))
        .build();
    println!("Join:    {}", Generator::sql(&query).unwrap());

    // SELECT with aggregation
    let query = select(["department"])
        .select_cols([func("COUNT", [star()]).alias("cnt")])
        .from("employees")
        .group_by(["department"])
        .having(func("COUNT", [star()]).gt(lit(5)))
        .build();
    println!("Agg:     {}", Generator::sql(&query).unwrap());

    // INSERT
    let query = insert_into("users")
        .columns(["name", "email"])
        .values([lit("Alice"), lit("alice@example.com")])
        .build();
    println!("Insert:  {}", Generator::sql(&query).unwrap());

    // UPDATE
    let query = update("users")
        .set("name", lit("Bob"))
        .set("updated_at", func("NOW", std::iter::empty::<polyglot_sql::builder::Expr>()))
        .where_(col("id").eq(lit(1)))
        .build();
    println!("Update:  {}", Generator::sql(&query).unwrap());

    // DELETE
    let query = delete("users")
        .where_(col("id").eq(lit(1)).or(col("name").is_null()))
        .build();
    println!("Delete:  {}", Generator::sql(&query).unwrap());

    // NOT IN
    let query = select([star()])
        .from("users")
        .where_(col("id").not().in_list([lit(1), lit(2), lit(3)]))
        .build();
    println!("Not In:  {}", Generator::sql(&query).unwrap());

    // NULL alias
    let query = select(["name"])
        .select_cols([null().alias("placeholder")])
        .from("users")
        .build();
    println!("Null:    {}", Generator::sql(&query).unwrap());
}
