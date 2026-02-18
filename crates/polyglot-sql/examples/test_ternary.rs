use polyglot_sql::{parse, DialectType};
use std::fs;

fn main() {
    let files = [
        "../ClickHouse/tests/queries/0_stateless/01623_constraints_column_swap.sql",
        "../ClickHouse/tests/queries/0_stateless/01275_parallel_mv.gen.sql",
        "../ClickHouse/tests/queries/0_stateless/01686_rocksdb.sql",
        "../ClickHouse/tests/queries/0_stateless/03279_join_choose_build_table.sql",
    ];
    for file in &files {
        let content = match fs::read_to_string(file) {
            Ok(c) => c,
            Err(e) => { println!("SKIP {}: {}", file, e); continue; }
        };
        let fname = file.rsplit('/').next().unwrap();
        // Binary search: try parsing progressively more of the file
        let stmts: Vec<&str> = content.split(';').collect();
        let mut good = 0;
        for i in 1..=stmts.len() {
            let partial: String = stmts[..i].join(";");
            if parse(&partial, DialectType::ClickHouse).is_err() {
                let failing_stmt = stmts[i-1].trim();
                println!("ERR: {} at stmt #{}: {}", fname, i,
                    &failing_stmt[..failing_stmt.len().min(200)]);
                break;
            }
            good = i;
        }
        if good == stmts.len() {
            println!("OK: {}", fname);
        }
    }
}
