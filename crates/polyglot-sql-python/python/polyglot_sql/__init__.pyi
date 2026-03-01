from typing import Any


class PolyglotError(Exception): ...
class ParseError(PolyglotError): ...
class GenerateError(PolyglotError): ...
class TranspileError(PolyglotError): ...
class ValidationError(PolyglotError): ...


class ValidationErrorInfo:
    @property
    def message(self) -> str: ...

    @property
    def line(self) -> int: ...

    @property
    def col(self) -> int: ...

    @property
    def code(self) -> str: ...

    @property
    def severity(self) -> str: ...


class ValidationResult:
    @property
    def valid(self) -> bool: ...

    @property
    def errors(self) -> list[ValidationErrorInfo]: ...

    def __bool__(self) -> bool: ...


def transpile(
    sql: str,
    read: str = "generic",
    write: str = "generic",
    *,
    pretty: bool = False,
) -> list[str]: ...


def parse(sql: str, dialect: str = "generic") -> list[dict[str, Any]]: ...


def parse_one(sql: str, dialect: str = "generic") -> dict[str, Any]: ...


def generate(
    ast: dict[str, Any] | list[dict[str, Any]],
    dialect: str = "generic",
    *,
    pretty: bool = False,
) -> list[str]: ...


def format_sql(
    sql: str,
    dialect: str = "generic",
    *,
    max_input_bytes: int | None = None,
    max_tokens: int | None = None,
    max_ast_nodes: int | None = None,
    max_set_op_chain: int | None = None,
) -> str: ...


def validate(sql: str, dialect: str = "generic") -> ValidationResult: ...


def optimize(sql: str, dialect: str = "generic") -> str: ...


def lineage(column: str, sql: str, dialect: str = "generic") -> dict[str, Any]: ...

def lineage_with_schema(
    column: str,
    sql: str,
    schema: dict[str, Any],
    dialect: str = "generic",
) -> dict[str, Any]: ...


def source_tables(column: str, sql: str, dialect: str = "generic") -> list[str]: ...


def diff(sql1: str, sql2: str, dialect: str = "generic") -> list[dict[str, Any]]: ...


def dialects() -> list[str]: ...


def format(
    sql: str,
    dialect: str = "generic",
    *,
    max_input_bytes: int | None = None,
    max_tokens: int | None = None,
    max_ast_nodes: int | None = None,
    max_set_op_chain: int | None = None,
) -> str: ...


__version__: str
