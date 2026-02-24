import pytest

import polyglot_sql


def test_validate_valid_sql():
    result = polyglot_sql.validate("SELECT 1", dialect="postgres")
    assert result.valid is True
    assert result.errors == []
    assert bool(result) is True


def test_validate_invalid_sql():
    result = polyglot_sql.validate("SELECT FROM", dialect="postgres")
    assert result.valid is False
    assert len(result.errors) > 0
    assert isinstance(result.errors[0].message, str)
    assert isinstance(result.errors[0].line, int)
    assert isinstance(result.errors[0].col, int)


def test_validate_bool_false_for_invalid():
    result = polyglot_sql.validate("SELECT FROM", dialect="postgres")
    assert bool(result) is False


def test_validate_repr_is_readable():
    result = polyglot_sql.validate("SELECT 1", dialect="postgres")
    text = repr(result)
    assert "ValidationResult" in text
    assert "valid=" in text


def test_validate_unknown_dialect_raises_value_error():
    with pytest.raises(ValueError):
        polyglot_sql.validate("SELECT 1", dialect="not_a_dialect")
