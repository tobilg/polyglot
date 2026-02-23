#include <stdio.h>
#include <stddef.h>
#include "polyglot_sql.h"

static void print_result(const char *label, polyglot_result_t result) {
    if (result.status == 0) {
        printf("%s: %s\n", label, result.data);
    } else {
        printf("%s error (%d): %s\n", label, result.status, result.error);
    }
    polyglot_free_result(result);
}

int main(void) {
    print_result(
        "Transpiled",
        polyglot_transpile(
            "SELECT IFNULL(a, b) FROM t",
            "mysql",
            "postgres"
        )
    );

    print_result(
        "Parsed AST",
        polyglot_parse("SELECT a, b FROM t", "generic")
    );

    polyglot_result_t parsed = polyglot_parse("SELECT 1", "generic");
    if (parsed.status == 0) {
        print_result("Generated SQL", polyglot_generate(parsed.data, "generic"));
    } else {
        print_result("Generated SQL input parse", parsed);
        parsed.data = NULL;
        parsed.error = NULL;
    }
    polyglot_free_result(parsed);

    print_result(
        "Formatted",
        polyglot_format("SELECT a,b,c FROM t WHERE x=1 AND y=2", "postgres")
    );

    polyglot_validation_result_t validation = polyglot_validate(
        "SELECT FROM",
        "generic"
    );
    printf(
        "Validation status=%d valid=%d errors=%s\n",
        validation.status,
        validation.valid,
        validation.errors_json ? validation.errors_json : "[]"
    );
    polyglot_free_validation_result(validation);

    print_result(
        "Optimized",
        polyglot_optimize("SELECT a FROM t WHERE NOT (NOT (b = 1))", "generic")
    );

    print_result(
        "Lineage",
        polyglot_lineage(
            "total",
            "SELECT o.total FROM orders o",
            "generic"
        )
    );

    print_result(
        "Source Tables",
        polyglot_source_tables(
            "total",
            "SELECT o.total FROM orders o",
            "generic"
        )
    );

    print_result(
        "Diff",
        polyglot_diff(
            "SELECT a FROM t",
            "SELECT b FROM t",
            "generic"
        )
    );

    char *dialects = polyglot_dialect_list();
    printf("Dialects (%d): %s\n", polyglot_dialect_count(), dialects);
    polyglot_free_string(dialects);

    printf("Version: %s\n", polyglot_version());
    return 0;
}
