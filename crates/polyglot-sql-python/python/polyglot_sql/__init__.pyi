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


class Expression:
    @property
    def kind(self) -> str: ...

    @property
    def tree_depth(self) -> int: ...

    def sql(self, dialect: str | None = None, *, pretty: bool = False) -> str: ...

    def to_dict(self) -> dict[str, Any]: ...

    def arg(self, name: str) -> Any: ...

    def children(self) -> list[Expression]: ...

    def walk(self, order: str = "dfs") -> list[Expression]: ...

    def find(self, kind: str) -> Expression | None: ...

    def find_all(self, kind: str) -> list[Expression]: ...


def transpile(
    sql: str,
    read: str | None = None,
    write: str | None = None,
    *,
    identity: bool = True,
    error_level: str | None = None,
    pretty: bool = False,
) -> list[str]: ...


def parse(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    error_level: str | None = None,
) -> list[dict[str, Any]]: ...


def parse_expr(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    error_level: str | None = None,
) -> list[Expression]: ...


def parse_one(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    into: Any | None = None,
    error_level: str | None = None,
) -> dict[str, Any]: ...


def parse_one_expr(
    sql: str,
    read: str | None = None,
    dialect: str | None = None,
    *,
    into: Any | None = None,
    error_level: str | None = None,
) -> Expression: ...


def generate(
    ast: Expression | dict[str, Any] | list[Expression] | list[dict[str, Any]],
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


def optimize(sql: str, dialect: str | None = None, *, read: str | None = None) -> str: ...


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
