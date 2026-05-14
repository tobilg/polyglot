import polyglot_sql


def test_qualify_tables_flattens_union_derived_tables():
    ast = polyglot_sql.parse_one(
        "SELECT * FROM (SELECT * FROM tab_1) UNION ALL SELECT * FROM (SELECT * FROM tab_1)"
    )

    qualified = polyglot_sql.qualify_tables(ast)
    generated = polyglot_sql.generate(qualified)[0]

    assert isinstance(qualified, polyglot_sql.Expression)
    assert (
        generated
        == "SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _0 UNION ALL SELECT * FROM (SELECT * FROM tab_1 AS tab_1) AS _1"
    )


def test_rename_tables_can_alias_renamed_tables():
    ast = polyglot_sql.parse_one("SELECT a FROM old_table")

    renamed = polyglot_sql.rename_tables(
        ast,
        {"old_table": "new_table"},
        alias_renamed_tables=True,
    )
    generated = polyglot_sql.generate(renamed)[0]

    assert isinstance(renamed, polyglot_sql.Expression)
    assert generated == "SELECT a FROM new_table AS new_table"


def test_transform_functions_preserve_list_inputs():
    ast = polyglot_sql.parse("SELECT a FROM old_table")

    renamed = polyglot_sql.rename_tables(
        ast,
        {"old_table": "new_table"},
        alias_renamed_tables=True,
    )

    assert isinstance(renamed, list)
    assert isinstance(renamed[0], polyglot_sql.Expression)
    assert polyglot_sql.generate(renamed)[0] == "SELECT a FROM new_table AS new_table"
