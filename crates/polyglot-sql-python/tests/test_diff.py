import pytest

import polyglot_sql


def test_diff_different_selects():
    edits = polyglot_sql.diff("SELECT a FROM t", "SELECT b FROM t", dialect="postgres")
    assert isinstance(edits, list)
    assert len(edits) > 0
    assert "edit_type" in edits[0]
    assert "expression" in edits[0]


def test_diff_identical_sql_empty():
    edits = polyglot_sql.diff("SELECT a FROM t", "SELECT a FROM t", dialect="postgres")
    assert edits == []


def test_diff_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.diff("SELECT 1", "SELECT 2", dialect="not_a_dialect")
