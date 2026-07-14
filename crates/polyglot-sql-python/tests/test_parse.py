from concurrent.futures import ThreadPoolExecutor

import pytest

import polyglot_sql


def test_parse_single_select_returns_list_of_expressions():
    ast_list = polyglot_sql.parse("SELECT 1", dialect="postgres")
    assert isinstance(ast_list, list)
    assert len(ast_list) == 1
    assert isinstance(ast_list[0], polyglot_sql.Expression)
    assert isinstance(ast_list[0], polyglot_sql.Select)
    assert ast_list[0].kind == "select"


def test_parse_one_returns_expression():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert isinstance(ast, polyglot_sql.Expression)
    assert isinstance(ast, polyglot_sql.Select)
    assert ast.kind == "select"


def test_parse_one_returns_typed_subclass():
    ast = polyglot_sql.parse_one("SELECT 1", dialect="postgres")
    assert type(ast).__name__ == "Select"
    assert isinstance(ast, polyglot_sql.Select)
    assert isinstance(ast, polyglot_sql.Expression)
    # Not a different subclass
    assert not isinstance(ast, polyglot_sql.Insert)


def test_parse_returns_typed_subclasses():
    results = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
    assert all(isinstance(r, polyglot_sql.Select) for r in results)
    assert all(isinstance(r, polyglot_sql.Expression) for r in results)


def test_parse_tsql_try_catch_returns_typed_subclass():
    ast = polyglot_sql.parse_one(
        """
        BEGIN TRY
            INSERT INTO orders (id, amount) VALUES (1, 100.00);
        END TRY
        BEGIN CATCH
            INSERT INTO error_log (msg) VALUES (ERROR_MESSAGE());
        END CATCH
        """,
        dialect="tsql",
    )

    assert isinstance(ast, polyglot_sql.TryCatch)
    assert isinstance(ast, polyglot_sql.Expression)
    assert ast.kind == "try_catch"
    assert ast.sql("tsql").startswith("BEGIN TRY")


def test_parse_postgres_prepare_returns_typed_subclass():
    ast = polyglot_sql.parse_one(
        "PREPARE leak (int) AS SELECT id FROM sensitive_table WHERE id = $1",
        dialect="postgres",
    )

    assert isinstance(ast, polyglot_sql.Prepare)
    assert isinstance(ast, polyglot_sql.Expression)
    assert ast.kind == "prepare"
    assert ast.sql("postgres").startswith("PREPARE leak (INT) AS SELECT")


def test_parse_postgres_execute_prepared_statement():
    ast = polyglot_sql.parse_one("EXECUTE leak(1)", dialect="postgres")

    assert isinstance(ast, polyglot_sql.Execute)
    assert ast.kind == "execute"
    assert ast.sql("postgres") == "EXECUTE leak(1)"


def test_parse_multiple_statements_returns_n_entries():
    ast_list = polyglot_sql.parse("SELECT 1; SELECT 2", dialect="postgres")
    assert len(ast_list) == 2
    assert ast_list[0].kind == "select"
    assert ast_list[1].kind == "select"


def test_parse_one_multiple_statements_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.parse_one("SELECT 1; SELECT 2", dialect="postgres")


def test_parse_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.parse("SELECT 1", dialect="not_a_dialect")


def test_parse_one_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.parse_one("SELECT 1", dialect="not_a_dialect")


def test_parse_data_type_returns_data_type_expression():
    data_type = polyglot_sql.parse_data_type("DECIMAL(10, 2)", dialect="duckdb")

    assert isinstance(data_type, polyglot_sql.DataType)
    assert data_type.sql("duckdb") == "DECIMAL(10, 2)"


def test_parse_one_into_data_type_matches_sqlglot_compatibility_path():
    data_type = polyglot_sql.parse_one(
        "VARCHAR(255)", dialect="duckdb", into=polyglot_sql.DataType
    )

    assert isinstance(data_type, polyglot_sql.DataType)
    assert data_type.sql("duckdb") == "TEXT(255)"


def test_parse_one_into_only_supports_data_type():
    with pytest.raises(NotImplementedError):
        polyglot_sql.parse_one("a", into=polyglot_sql.Column)


def test_parse_data_type_rejects_trailing_sql():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.parse_data_type("DECIMAL(10, 2) SELECT 1", dialect="duckdb")


def test_parse_invalid_sql_raises_parse_error():
    with pytest.raises(polyglot_sql.ParseError):
        polyglot_sql.parse("SELECT FROM", dialect="postgres")


def test_parse_calls_execute_concurrently_with_stable_results_and_errors():
    sql = " UNION ALL ".join(
        f"SELECT {value} AS id FROM events_{value}" for value in range(80)
    )

    def parse_valid(_):
        return polyglot_sql.parse_one(sql, dialect="postgres").sql("postgres")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(parse_valid, range(32)))

    assert len(set(results)) == 1

    def parse_invalid(_):
        with pytest.raises(polyglot_sql.ParseError):
            polyglot_sql.parse("SELECT FROM", dialect="postgres")

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(parse_invalid, range(32)))
