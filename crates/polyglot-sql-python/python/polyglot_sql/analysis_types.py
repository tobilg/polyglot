"""Typed dictionary contracts; runtime APIs continue accepting ordinary dictionaries."""
from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, TypedDict

if TYPE_CHECKING:
    from polyglot_sql import ComplexityGuardOptions

ReferenceConfidence = Literal["resolved", "ambiguous", "unknown"]
ProjectionNullability = Literal["non_null", "nullable", "unknown"]
TransformKind = Literal["direct", "cast", "aggregation", "constant", "expression", "star"]
ColumnUseContext = Literal["join", "filter", "group", "having", "qualify", "window_partition", "window_order", "window_frame", "order", "aggregate_order", "set_operation_filter"]
FunctionNameCase = Literal["insensitive", "sensitive"]
_SourceKind = Literal["root", "table", "derived_table", "cte", "virtual", "unknown"]

class _ColumnReferenceOptional(TypedDict, total=False):
    schema: Optional[str]

class SchemaColumnReference(_ColumnReferenceOptional):
    table: str
    column: str

class SchemaTableReference(_ColumnReferenceOptional):
    table: str
    columns: list[str]

class _ForeignKeyOptional(TypedDict, total=False):
    name: Optional[str]

class SchemaForeignKey(_ForeignKeyOptional):
    columns: list[str]
    references: SchemaTableReference

class _ColumnOptional(TypedDict, total=False):
    type: str
    nullable: Optional[bool]
    primaryKey: bool
    unique: bool
    references: Optional[SchemaColumnReference]

class SchemaColumn(_ColumnOptional):
    name: str

class _TableOptional(TypedDict, total=False):
    schema: Optional[str]
    aliases: list[str]
    primaryKey: list[str]
    uniqueKeys: list[list[str]]
    foreignKeys: list[SchemaForeignKey]

class SchemaTable(_TableOptional):
    name: str
    columns: list[SchemaColumn]

class _SchemaOptional(TypedDict, total=False):
    strict: Optional[bool]

class ValidationSchema(_SchemaOptional):
    tables: list[SchemaTable]

class AnalyzeQueryOptions(TypedDict, total=False):
    dialect: str
    schema: Optional[ValidationSchema]
    complexityGuard: Optional[ComplexityGuardOptions]

class QuerySourceSpan(TypedDict):
    start: int
    end: int

class _ColumnReferenceFactOptional(TypedDict, total=False):
    pathTransformKind: TransformKind
    pathFunctions: list[str]
    pathWindow: bool

class ColumnReferenceFact(_ColumnReferenceFactOptional):
    sourceName: Optional[str]
    sourceAlias: Optional[str]
    sourceKind: _SourceKind
    table: Optional[str]
    column: str
    unqualified: bool
    confidence: ReferenceConfidence

class _SpanOptional(TypedDict, total=False):
    span: QuerySourceSpan

class ColumnUseReferenceFact(ColumnReferenceFact, _SpanOptional):
    pass

class ColumnUseFact(_SpanOptional):
    context: ColumnUseContext
    scopePath: str
    expressionPath: str
    expressionSql: str
    references: list[ColumnUseReferenceFact]

class TransformFunctionFact(TypedDict):
    name: str
    literalArgs: list[str]
    columnArgs: list[ColumnReferenceFact]

class _ProjectionOptional(TypedDict, total=False):
    transformFunction: TransformFunctionFact

class ProjectionFact(_ProjectionOptional):
    index: int
    name: Optional[str]
    isStar: bool
    starTable: Optional[str]
    transformKind: TransformKind
    castType: Optional[str]
    typeHint: Optional[str]
    nullability: ProjectionNullability
    upstream: list[ColumnReferenceFact]

class CteFact(TypedDict):
    name: str
    columns: list[str]
    bodySql: str
    outputColumns: list[str]

class RelationFact(TypedDict):
    name: str
    alias: Optional[str]
    kind: _SourceKind
    columns: list[str]
    catalog: Optional[str]
    schema: Optional[str]
    table: Optional[str]

class StarProjectionFact(TypedDict):
    index: int
    table: Optional[str]
    expandedColumns: list[str]

class SetOperationBranchFact(TypedDict):
    index: int
    role: Literal["value", "filter"]
    projections: list[ProjectionFact]

class SetOperationFact(TypedDict):
    kind: str
    all: bool
    distinct: bool
    outputColumns: list[str]
    branches: list[SetOperationBranchFact]

class QueryAnalysis(TypedDict):
    shape: Literal["select", "set_operation"]
    ctes: list[str]
    cteFacts: list[CteFact]
    projections: list[ProjectionFact]
    relations: list[RelationFact]
    baseTables: list[RelationFact]
    starProjections: list[StarProjectionFact]
    setOperations: list[SetOperationFact]
    columnUses: list[ColumnUseFact]

class _SignatureOptional(TypedDict, total=False):
    maxArity: Optional[int]

class FunctionSignature(_SignatureOptional):
    minArity: int

class _CatalogOptional(TypedDict, total=False):
    nameCase: FunctionNameCase

class _FunctionCatalogEntryOptional(TypedDict, total=False):
    nameCase: Optional[FunctionNameCase]

class FunctionCatalogEntry(_FunctionCatalogEntryOptional):
    name: str
    signatures: list[FunctionSignature]

class FunctionCatalogSpec(_CatalogOptional):
    functions: list[FunctionCatalogEntry]
