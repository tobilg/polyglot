use polyglot_sql::{parse, DialectType};

fn test(sql: &str) {
    match parse(sql, DialectType::ClickHouse) {
        Ok(_exprs) => println!("OK: {}", &sql[..sql.len().min(120)]),
        Err(e) => println!("ERR: {} -> {}", &sql[..sql.len().min(120)], e),
    }
}

fn main() {
    // GRANT role TO user
    test("GRANT r1_01292, r2_01292 TO u1_01292, u2_01292, u3_01292, u4_01292, u5_01292, u6_01292");
    test("ALTER USER u2_01292 DEFAULT ROLE ALL EXCEPT r2_01292");
    test("REVOKE r1_01292, r2_01292 FROM u1_01292, u2_01292");
    test("GRANT NONE TO test_user_01999 WITH REPLACE OPTION");

    // Complex GRANT with multiple targets
    test("GRANT SELECT ON db1.table1 TO sqllt_user");
    test("GRANT SELECT ON db1.table1, SELECT ON db2.table2, SELECT ON db3.table3, SELECT(col1) ON db4.table4 TO sqllt_user");
}
