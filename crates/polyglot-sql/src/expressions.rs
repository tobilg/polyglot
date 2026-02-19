//! SQL Expression AST (Abstract Syntax Tree).
//!
//! This module defines all the AST node types used to represent parsed SQL
//! statements and expressions. The design follows Python sqlglot's expression
//! hierarchy, ported to a Rust enum-based AST.
//!
//! # Architecture
//!
//! The central type is [`Expression`], a large tagged enum with one variant per
//! SQL construct. Inner structs carry the fields for each variant. Most
//! heap-allocated variants are wrapped in `Box` to keep the enum size small.
//!
//! # Variant Groups
//!
//! | Group | Examples | Purpose |
//! |---|---|---|
//! | **Queries** | `Select`, `Union`, `Intersect`, `Except`, `Subquery` | Top-level query structures |
//! | **DML** | `Insert`, `Update`, `Delete`, `Merge`, `Copy` | Data manipulation |
//! | **DDL** | `CreateTable`, `AlterTable`, `DropView`, `CreateIndex` | Schema definition |
//! | **Clauses** | `From`, `Join`, `Where`, `GroupBy`, `OrderBy`, `With` | Query clauses |
//! | **Operators** | `And`, `Or`, `Add`, `Eq`, `Like`, `Not` | Binary and unary operations |
//! | **Functions** | `Function`, `AggregateFunction`, `WindowFunction`, `Count`, `Sum` | Scalar, aggregate, and window functions |
//! | **Literals** | `Literal`, `Boolean`, `Null`, `Interval` | Constant values |
//! | **Types** | `DataType`, `Cast`, `TryCast`, `SafeCast` | Data types and casts |
//! | **Identifiers** | `Identifier`, `Column`, `Table`, `Star` | Name references |
//!
//! # SQL Generation
//!
//! Every `Expression` can be rendered back to SQL via [`Expression::sql()`]
//! (generic dialect) or [`Expression::sql_for()`] (specific dialect). The
//! actual generation logic lives in the `generator` module.

use serde::{Deserialize, Serialize};
use std::fmt;
#[cfg(feature = "bindings")]
use ts_rs::TS;

/// Helper function for serde default value
fn default_true() -> bool {
    true
}

fn is_true(v: &bool) -> bool {
    *v
}

/// Represent any SQL expression or statement as a single, recursive AST node.
///
/// `Expression` is the root type of the polyglot AST. Every parsed SQL
/// construct -- from a simple integer literal to a multi-CTE query with
/// window functions -- is represented as a variant of this enum.
///
/// Variants are organized into logical groups (see the module-level docs).
/// Most non-trivial variants box their payload so that `size_of::<Expression>()`
/// stays small (currently two words: tag + pointer).
///
/// # Constructing Expressions
///
/// Use the convenience constructors on `impl Expression` for common cases:
///
/// ```rust,ignore
/// use polyglot_sql::expressions::Expression;
///
/// let col  = Expression::column("id");
/// let lit  = Expression::number(42);
/// let star = Expression::star();
/// ```
///
/// # Generating SQL
///
/// ```rust,ignore
/// let expr = Expression::column("name");
/// assert_eq!(expr.sql(), "name");
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum Expression {
    // Literals
    Literal(Literal),
    Boolean(BooleanLiteral),
    Null(Null),

    // Identifiers
    Identifier(Identifier),
    Column(Column),
    Table(TableRef),
    Star(Star),
    /// Snowflake braced wildcard syntax: {*}, {tbl.*}, {* EXCLUDE (...)}, {* ILIKE '...'}
    BracedWildcard(Box<Expression>),

    // Queries
    Select(Box<Select>),
    Union(Box<Union>),
    Intersect(Box<Intersect>),
    Except(Box<Except>),
    Subquery(Box<Subquery>),
    PipeOperator(Box<PipeOperator>),
    Pivot(Box<Pivot>),
    PivotAlias(Box<PivotAlias>),
    Unpivot(Box<Unpivot>),
    Values(Box<Values>),
    PreWhere(Box<PreWhere>),
    Stream(Box<Stream>),
    UsingData(Box<UsingData>),
    XmlNamespace(Box<XmlNamespace>),

    // DML
    Insert(Box<Insert>),
    Update(Box<Update>),
    Delete(Box<Delete>),
    Copy(Box<CopyStmt>),
    Put(Box<PutStmt>),
    StageReference(Box<StageReference>),

    // Expressions
    Alias(Box<Alias>),
    Cast(Box<Cast>),
    Collation(Box<CollationExpr>),
    Case(Box<Case>),

    // Binary operations
    And(Box<BinaryOp>),
    Or(Box<BinaryOp>),
    Add(Box<BinaryOp>),
    Sub(Box<BinaryOp>),
    Mul(Box<BinaryOp>),
    Div(Box<BinaryOp>),
    Mod(Box<BinaryOp>),
    Eq(Box<BinaryOp>),
    Neq(Box<BinaryOp>),
    Lt(Box<BinaryOp>),
    Lte(Box<BinaryOp>),
    Gt(Box<BinaryOp>),
    Gte(Box<BinaryOp>),
    Like(Box<LikeOp>),
    ILike(Box<LikeOp>),
    /// SQLite MATCH operator (FTS)
    Match(Box<BinaryOp>),
    BitwiseAnd(Box<BinaryOp>),
    BitwiseOr(Box<BinaryOp>),
    BitwiseXor(Box<BinaryOp>),
    Concat(Box<BinaryOp>),
    Adjacent(Box<BinaryOp>),  // PostgreSQL range adjacency operator (-|-)
    TsMatch(Box<BinaryOp>),   // PostgreSQL text search match operator (@@)
    PropertyEQ(Box<BinaryOp>), // := assignment operator (MySQL @var := val, DuckDB named args)

    // PostgreSQL array/JSONB operators
    ArrayContainsAll(Box<BinaryOp>),       // @> operator (array contains all)
    ArrayContainedBy(Box<BinaryOp>),       // <@ operator (array contained by)
    ArrayOverlaps(Box<BinaryOp>),          // && operator (array overlaps)
    JSONBContainsAllTopKeys(Box<BinaryOp>), // ?& operator (JSONB contains all keys)
    JSONBContainsAnyTopKeys(Box<BinaryOp>), // ?| operator (JSONB contains any key)
    JSONBDeleteAtPath(Box<BinaryOp>),      // #- operator (JSONB delete at path)
    ExtendsLeft(Box<BinaryOp>),            // &< operator (PostgreSQL range extends left)
    ExtendsRight(Box<BinaryOp>),           // &> operator (PostgreSQL range extends right)

    // Unary operations
    Not(Box<UnaryOp>),
    Neg(Box<UnaryOp>),
    BitwiseNot(Box<UnaryOp>),

    // Predicates
    In(Box<In>),
    Between(Box<Between>),
    IsNull(Box<IsNull>),
    IsTrue(Box<IsTrueFalse>),
    IsFalse(Box<IsTrueFalse>),
    IsJson(Box<IsJson>),
    Is(Box<BinaryOp>),  // General IS expression (e.g., a IS ?)
    Exists(Box<Exists>),
    /// MySQL MEMBER OF operator: expr MEMBER OF(json_array)
    MemberOf(Box<BinaryOp>),

    // Functions
    Function(Box<Function>),
    AggregateFunction(Box<AggregateFunction>),
    WindowFunction(Box<WindowFunction>),

    // Clauses
    From(Box<From>),
    Join(Box<Join>),
    JoinedTable(Box<JoinedTable>),
    Where(Box<Where>),
    GroupBy(Box<GroupBy>),
    Having(Box<Having>),
    OrderBy(Box<OrderBy>),
    Limit(Box<Limit>),
    Offset(Box<Offset>),
    Qualify(Box<Qualify>),
    With(Box<With>),
    Cte(Box<Cte>),
    DistributeBy(Box<DistributeBy>),
    ClusterBy(Box<ClusterBy>),
    SortBy(Box<SortBy>),
    LateralView(Box<LateralView>),
    Hint(Box<Hint>),
    Pseudocolumn(Pseudocolumn),

    // Oracle hierarchical queries (CONNECT BY)
    Connect(Box<Connect>),
    Prior(Box<Prior>),
    ConnectByRoot(Box<ConnectByRoot>),

    // Pattern matching (MATCH_RECOGNIZE)
    MatchRecognize(Box<MatchRecognize>),

    // Order expressions
    Ordered(Box<Ordered>),

    // Window specifications
    Window(Box<WindowSpec>),
    Over(Box<Over>),
    WithinGroup(Box<WithinGroup>),

    // Data types
    DataType(DataType),

    // Arrays and structs
    Array(Box<Array>),
    Struct(Box<Struct>),
    Tuple(Box<Tuple>),

    // Interval
    Interval(Box<Interval>),

    // String functions
    ConcatWs(Box<ConcatWs>),
    Substring(Box<SubstringFunc>),
    Upper(Box<UnaryFunc>),
    Lower(Box<UnaryFunc>),
    Length(Box<UnaryFunc>),
    Trim(Box<TrimFunc>),
    LTrim(Box<UnaryFunc>),
    RTrim(Box<UnaryFunc>),
    Replace(Box<ReplaceFunc>),
    Reverse(Box<UnaryFunc>),
    Left(Box<LeftRightFunc>),
    Right(Box<LeftRightFunc>),
    Repeat(Box<RepeatFunc>),
    Lpad(Box<PadFunc>),
    Rpad(Box<PadFunc>),
    Split(Box<SplitFunc>),
    RegexpLike(Box<RegexpFunc>),
    RegexpReplace(Box<RegexpReplaceFunc>),
    RegexpExtract(Box<RegexpExtractFunc>),
    Overlay(Box<OverlayFunc>),

    // Math functions
    Abs(Box<UnaryFunc>),
    Round(Box<RoundFunc>),
    Floor(Box<FloorFunc>),
    Ceil(Box<CeilFunc>),
    Power(Box<BinaryFunc>),
    Sqrt(Box<UnaryFunc>),
    Cbrt(Box<UnaryFunc>),
    Ln(Box<UnaryFunc>),
    Log(Box<LogFunc>),
    Exp(Box<UnaryFunc>),
    Sign(Box<UnaryFunc>),
    Greatest(Box<VarArgFunc>),
    Least(Box<VarArgFunc>),

    // Date/time functions
    CurrentDate(CurrentDate),
    CurrentTime(CurrentTime),
    CurrentTimestamp(CurrentTimestamp),
    CurrentTimestampLTZ(CurrentTimestampLTZ),
    AtTimeZone(Box<AtTimeZone>),
    DateAdd(Box<DateAddFunc>),
    DateSub(Box<DateAddFunc>),
    DateDiff(Box<DateDiffFunc>),
    DateTrunc(Box<DateTruncFunc>),
    Extract(Box<ExtractFunc>),
    ToDate(Box<ToDateFunc>),
    ToTimestamp(Box<ToTimestampFunc>),
    Date(Box<UnaryFunc>),
    Time(Box<UnaryFunc>),
    DateFromUnixDate(Box<UnaryFunc>),
    UnixDate(Box<UnaryFunc>),
    UnixSeconds(Box<UnaryFunc>),
    UnixMillis(Box<UnaryFunc>),
    UnixMicros(Box<UnaryFunc>),
    UnixToTimeStr(Box<BinaryFunc>),
    TimeStrToDate(Box<UnaryFunc>),
    DateToDi(Box<UnaryFunc>),
    DiToDate(Box<UnaryFunc>),
    TsOrDiToDi(Box<UnaryFunc>),
    TsOrDsToDatetime(Box<UnaryFunc>),
    TsOrDsToTimestamp(Box<UnaryFunc>),
    YearOfWeek(Box<UnaryFunc>),
    YearOfWeekIso(Box<UnaryFunc>),

    // Control flow functions
    Coalesce(Box<VarArgFunc>),
    NullIf(Box<BinaryFunc>),
    IfFunc(Box<IfFunc>),
    IfNull(Box<BinaryFunc>),
    Nvl(Box<BinaryFunc>),
    Nvl2(Box<Nvl2Func>),

    // Type conversion
    TryCast(Box<Cast>),
    SafeCast(Box<Cast>),

    // Typed aggregate functions
    Count(Box<CountFunc>),
    Sum(Box<AggFunc>),
    Avg(Box<AggFunc>),
    Min(Box<AggFunc>),
    Max(Box<AggFunc>),
    GroupConcat(Box<GroupConcatFunc>),
    StringAgg(Box<StringAggFunc>),
    ListAgg(Box<ListAggFunc>),
    ArrayAgg(Box<AggFunc>),
    CountIf(Box<AggFunc>),
    SumIf(Box<SumIfFunc>),
    Stddev(Box<AggFunc>),
    StddevPop(Box<AggFunc>),
    StddevSamp(Box<AggFunc>),
    Variance(Box<AggFunc>),
    VarPop(Box<AggFunc>),
    VarSamp(Box<AggFunc>),
    Median(Box<AggFunc>),
    Mode(Box<AggFunc>),
    First(Box<AggFunc>),
    Last(Box<AggFunc>),
    AnyValue(Box<AggFunc>),
    ApproxDistinct(Box<AggFunc>),
    ApproxCountDistinct(Box<AggFunc>),
    ApproxPercentile(Box<ApproxPercentileFunc>),
    Percentile(Box<PercentileFunc>),
    LogicalAnd(Box<AggFunc>),
    LogicalOr(Box<AggFunc>),
    Skewness(Box<AggFunc>),
    BitwiseCount(Box<UnaryFunc>),
    ArrayConcatAgg(Box<AggFunc>),
    ArrayUniqueAgg(Box<AggFunc>),
    BoolXorAgg(Box<AggFunc>),

    // Typed window functions
    RowNumber(RowNumber),
    Rank(Rank),
    DenseRank(DenseRank),
    NTile(Box<NTileFunc>),
    Lead(Box<LeadLagFunc>),
    Lag(Box<LeadLagFunc>),
    FirstValue(Box<ValueFunc>),
    LastValue(Box<ValueFunc>),
    NthValue(Box<NthValueFunc>),
    PercentRank(PercentRank),
    CumeDist(CumeDist),
    PercentileCont(Box<PercentileFunc>),
    PercentileDisc(Box<PercentileFunc>),

    // Additional string functions
    Contains(Box<BinaryFunc>),
    StartsWith(Box<BinaryFunc>),
    EndsWith(Box<BinaryFunc>),
    Position(Box<PositionFunc>),
    Initcap(Box<UnaryFunc>),
    Ascii(Box<UnaryFunc>),
    Chr(Box<UnaryFunc>),
    /// MySQL CHAR function with multiple args and optional USING charset
    CharFunc(Box<CharFunc>),
    Soundex(Box<UnaryFunc>),
    Levenshtein(Box<BinaryFunc>),
    ByteLength(Box<UnaryFunc>),
    Hex(Box<UnaryFunc>),
    LowerHex(Box<UnaryFunc>),
    Unicode(Box<UnaryFunc>),

    // Additional math functions
    ModFunc(Box<BinaryFunc>),
    Random(Random),
    Rand(Box<Rand>),
    TruncFunc(Box<TruncateFunc>),
    Pi(Pi),
    Radians(Box<UnaryFunc>),
    Degrees(Box<UnaryFunc>),
    Sin(Box<UnaryFunc>),
    Cos(Box<UnaryFunc>),
    Tan(Box<UnaryFunc>),
    Asin(Box<UnaryFunc>),
    Acos(Box<UnaryFunc>),
    Atan(Box<UnaryFunc>),
    Atan2(Box<BinaryFunc>),
    IsNan(Box<UnaryFunc>),
    IsInf(Box<UnaryFunc>),
    IntDiv(Box<BinaryFunc>),

    // Control flow
    Decode(Box<DecodeFunc>),

    // Additional date/time functions
    DateFormat(Box<DateFormatFunc>),
    FormatDate(Box<DateFormatFunc>),
    Year(Box<UnaryFunc>),
    Month(Box<UnaryFunc>),
    Day(Box<UnaryFunc>),
    Hour(Box<UnaryFunc>),
    Minute(Box<UnaryFunc>),
    Second(Box<UnaryFunc>),
    DayOfWeek(Box<UnaryFunc>),
    DayOfWeekIso(Box<UnaryFunc>),
    DayOfMonth(Box<UnaryFunc>),
    DayOfYear(Box<UnaryFunc>),
    WeekOfYear(Box<UnaryFunc>),
    Quarter(Box<UnaryFunc>),
    AddMonths(Box<BinaryFunc>),
    MonthsBetween(Box<BinaryFunc>),
    LastDay(Box<LastDayFunc>),
    NextDay(Box<BinaryFunc>),
    Epoch(Box<UnaryFunc>),
    EpochMs(Box<UnaryFunc>),
    FromUnixtime(Box<FromUnixtimeFunc>),
    UnixTimestamp(Box<UnixTimestampFunc>),
    MakeDate(Box<MakeDateFunc>),
    MakeTimestamp(Box<MakeTimestampFunc>),
    TimestampTrunc(Box<DateTruncFunc>),
    TimeStrToUnix(Box<UnaryFunc>),

    // Session/User functions
    SessionUser(SessionUser),

    // Hash/Crypto functions
    SHA(Box<UnaryFunc>),
    SHA1Digest(Box<UnaryFunc>),

    // Time conversion functions
    TimeToUnix(Box<UnaryFunc>),

    // Array functions
    ArrayFunc(Box<ArrayConstructor>),
    ArrayLength(Box<UnaryFunc>),
    ArraySize(Box<UnaryFunc>),
    Cardinality(Box<UnaryFunc>),
    ArrayContains(Box<BinaryFunc>),
    ArrayPosition(Box<BinaryFunc>),
    ArrayAppend(Box<BinaryFunc>),
    ArrayPrepend(Box<BinaryFunc>),
    ArrayConcat(Box<VarArgFunc>),
    ArraySort(Box<ArraySortFunc>),
    ArrayReverse(Box<UnaryFunc>),
    ArrayDistinct(Box<UnaryFunc>),
    ArrayJoin(Box<ArrayJoinFunc>),
    ArrayToString(Box<ArrayJoinFunc>),
    Unnest(Box<UnnestFunc>),
    Explode(Box<UnaryFunc>),
    ExplodeOuter(Box<UnaryFunc>),
    ArrayFilter(Box<ArrayFilterFunc>),
    ArrayTransform(Box<ArrayTransformFunc>),
    ArrayFlatten(Box<UnaryFunc>),
    ArrayCompact(Box<UnaryFunc>),
    ArrayIntersect(Box<VarArgFunc>),
    ArrayUnion(Box<BinaryFunc>),
    ArrayExcept(Box<BinaryFunc>),
    ArrayRemove(Box<BinaryFunc>),
    ArrayZip(Box<VarArgFunc>),
    Sequence(Box<SequenceFunc>),
    Generate(Box<SequenceFunc>),
    ExplodingGenerateSeries(Box<SequenceFunc>),
    ToArray(Box<UnaryFunc>),
    StarMap(Box<BinaryFunc>),

    // Struct functions
    StructFunc(Box<StructConstructor>),
    StructExtract(Box<StructExtractFunc>),
    NamedStruct(Box<NamedStructFunc>),

    // Map functions
    MapFunc(Box<MapConstructor>),
    MapFromEntries(Box<UnaryFunc>),
    MapFromArrays(Box<BinaryFunc>),
    MapKeys(Box<UnaryFunc>),
    MapValues(Box<UnaryFunc>),
    MapContainsKey(Box<BinaryFunc>),
    MapConcat(Box<VarArgFunc>),
    ElementAt(Box<BinaryFunc>),
    TransformKeys(Box<TransformFunc>),
    TransformValues(Box<TransformFunc>),

    // JSON functions
    JsonExtract(Box<JsonExtractFunc>),
    JsonExtractScalar(Box<JsonExtractFunc>),
    JsonExtractPath(Box<JsonPathFunc>),
    JsonArray(Box<VarArgFunc>),
    JsonObject(Box<JsonObjectFunc>),
    JsonQuery(Box<JsonExtractFunc>),
    JsonValue(Box<JsonExtractFunc>),
    JsonArrayLength(Box<UnaryFunc>),
    JsonKeys(Box<UnaryFunc>),
    JsonType(Box<UnaryFunc>),
    ParseJson(Box<UnaryFunc>),
    ToJson(Box<UnaryFunc>),
    JsonSet(Box<JsonModifyFunc>),
    JsonInsert(Box<JsonModifyFunc>),
    JsonRemove(Box<JsonPathFunc>),
    JsonMergePatch(Box<BinaryFunc>),
    JsonArrayAgg(Box<JsonArrayAggFunc>),
    JsonObjectAgg(Box<JsonObjectAggFunc>),

    // Type casting/conversion
    Convert(Box<ConvertFunc>),
    Typeof(Box<UnaryFunc>),

    // Additional expressions
    Lambda(Box<LambdaExpr>),
    Parameter(Box<Parameter>),
    Placeholder(Placeholder),
    NamedArgument(Box<NamedArgument>),
    /// TABLE ref or MODEL ref used as a function argument (BigQuery)
    /// e.g., GAP_FILL(TABLE device_data, ...) or ML.PREDICT(MODEL mydataset.mymodel, ...)
    TableArgument(Box<TableArgument>),
    SqlComment(Box<SqlComment>),

    // Additional predicates
    NullSafeEq(Box<BinaryOp>),
    NullSafeNeq(Box<BinaryOp>),
    Glob(Box<BinaryOp>),
    SimilarTo(Box<SimilarToExpr>),
    Any(Box<QuantifiedExpr>),
    All(Box<QuantifiedExpr>),
    Overlaps(Box<OverlapsExpr>),

    // Bitwise operations
    BitwiseLeftShift(Box<BinaryOp>),
    BitwiseRightShift(Box<BinaryOp>),
    BitwiseAndAgg(Box<AggFunc>),
    BitwiseOrAgg(Box<AggFunc>),
    BitwiseXorAgg(Box<AggFunc>),

    // Array/struct/map access
    Subscript(Box<Subscript>),
    Dot(Box<DotAccess>),
    MethodCall(Box<MethodCall>),
    ArraySlice(Box<ArraySlice>),

    // DDL statements
    CreateTable(Box<CreateTable>),
    DropTable(Box<DropTable>),
    AlterTable(Box<AlterTable>),
    CreateIndex(Box<CreateIndex>),
    DropIndex(Box<DropIndex>),
    CreateView(Box<CreateView>),
    DropView(Box<DropView>),
    AlterView(Box<AlterView>),
    AlterIndex(Box<AlterIndex>),
    Truncate(Box<Truncate>),
    Use(Box<Use>),
    Cache(Box<Cache>),
    Uncache(Box<Uncache>),
    LoadData(Box<LoadData>),
    Pragma(Box<Pragma>),
    Grant(Box<Grant>),
    Revoke(Box<Revoke>),
    Comment(Box<Comment>),
    SetStatement(Box<SetStatement>),
    // Phase 4: Additional DDL statements
    CreateSchema(Box<CreateSchema>),
    DropSchema(Box<DropSchema>),
    DropNamespace(Box<DropNamespace>),
    CreateDatabase(Box<CreateDatabase>),
    DropDatabase(Box<DropDatabase>),
    CreateFunction(Box<CreateFunction>),
    DropFunction(Box<DropFunction>),
    CreateProcedure(Box<CreateProcedure>),
    DropProcedure(Box<DropProcedure>),
    CreateSequence(Box<CreateSequence>),
    DropSequence(Box<DropSequence>),
    AlterSequence(Box<AlterSequence>),
    CreateTrigger(Box<CreateTrigger>),
    DropTrigger(Box<DropTrigger>),
    CreateType(Box<CreateType>),
    DropType(Box<DropType>),
    Describe(Box<Describe>),
    Show(Box<Show>),

    // Transaction and other commands
    Command(Box<Command>),
    Kill(Box<Kill>),
    /// EXEC/EXECUTE statement (TSQL stored procedure call)
    Execute(Box<ExecuteStatement>),

    // Placeholder for unparsed/raw SQL
    Raw(Raw),

    // Paren for grouping
    Paren(Box<Paren>),

    // Expression with trailing comments (for round-trip preservation)
    Annotated(Box<Annotated>),

    // === BATCH GENERATED EXPRESSION TYPES ===
    // Generated from Python sqlglot expressions.py
    Refresh(Box<Refresh>),
    LockingStatement(Box<LockingStatement>),
    SequenceProperties(Box<SequenceProperties>),
    TruncateTable(Box<TruncateTable>),
    Clone(Box<Clone>),
    Attach(Box<Attach>),
    Detach(Box<Detach>),
    Install(Box<Install>),
    Summarize(Box<Summarize>),
    Declare(Box<Declare>),
    DeclareItem(Box<DeclareItem>),
    Set(Box<Set>),
    Heredoc(Box<Heredoc>),
    SetItem(Box<SetItem>),
    QueryBand(Box<QueryBand>),
    UserDefinedFunction(Box<UserDefinedFunction>),
    RecursiveWithSearch(Box<RecursiveWithSearch>),
    ProjectionDef(Box<ProjectionDef>),
    TableAlias(Box<TableAlias>),
    ByteString(Box<ByteString>),
    HexStringExpr(Box<HexStringExpr>),
    UnicodeString(Box<UnicodeString>),
    ColumnPosition(Box<ColumnPosition>),
    ColumnDef(Box<ColumnDef>),
    AlterColumn(Box<AlterColumn>),
    AlterSortKey(Box<AlterSortKey>),
    AlterSet(Box<AlterSet>),
    RenameColumn(Box<RenameColumn>),
    Comprehension(Box<Comprehension>),
    MergeTreeTTLAction(Box<MergeTreeTTLAction>),
    MergeTreeTTL(Box<MergeTreeTTL>),
    IndexConstraintOption(Box<IndexConstraintOption>),
    ColumnConstraint(Box<ColumnConstraint>),
    PeriodForSystemTimeConstraint(Box<PeriodForSystemTimeConstraint>),
    CaseSpecificColumnConstraint(Box<CaseSpecificColumnConstraint>),
    CharacterSetColumnConstraint(Box<CharacterSetColumnConstraint>),
    CheckColumnConstraint(Box<CheckColumnConstraint>),
    CompressColumnConstraint(Box<CompressColumnConstraint>),
    DateFormatColumnConstraint(Box<DateFormatColumnConstraint>),
    EphemeralColumnConstraint(Box<EphemeralColumnConstraint>),
    WithOperator(Box<WithOperator>),
    GeneratedAsIdentityColumnConstraint(Box<GeneratedAsIdentityColumnConstraint>),
    AutoIncrementColumnConstraint(AutoIncrementColumnConstraint),
    CommentColumnConstraint(CommentColumnConstraint),
    GeneratedAsRowColumnConstraint(Box<GeneratedAsRowColumnConstraint>),
    IndexColumnConstraint(Box<IndexColumnConstraint>),
    MaskingPolicyColumnConstraint(Box<MaskingPolicyColumnConstraint>),
    NotNullColumnConstraint(Box<NotNullColumnConstraint>),
    PrimaryKeyColumnConstraint(Box<PrimaryKeyColumnConstraint>),
    UniqueColumnConstraint(Box<UniqueColumnConstraint>),
    WatermarkColumnConstraint(Box<WatermarkColumnConstraint>),
    ComputedColumnConstraint(Box<ComputedColumnConstraint>),
    InOutColumnConstraint(Box<InOutColumnConstraint>),
    DefaultColumnConstraint(Box<DefaultColumnConstraint>),
    PathColumnConstraint(Box<PathColumnConstraint>),
    Constraint(Box<Constraint>),
    Export(Box<Export>),
    Filter(Box<Filter>),
    Changes(Box<Changes>),
    CopyParameter(Box<CopyParameter>),
    Credentials(Box<Credentials>),
    Directory(Box<Directory>),
    ForeignKey(Box<ForeignKey>),
    ColumnPrefix(Box<ColumnPrefix>),
    PrimaryKey(Box<PrimaryKey>),
    IntoClause(Box<IntoClause>),
    JoinHint(Box<JoinHint>),
    Opclass(Box<Opclass>),
    Index(Box<Index>),
    IndexParameters(Box<IndexParameters>),
    ConditionalInsert(Box<ConditionalInsert>),
    MultitableInserts(Box<MultitableInserts>),
    OnConflict(Box<OnConflict>),
    OnCondition(Box<OnCondition>),
    Returning(Box<Returning>),
    Introducer(Box<Introducer>),
    PartitionRange(Box<PartitionRange>),
    Fetch(Box<Fetch>),
    Group(Box<Group>),
    Cube(Box<Cube>),
    Rollup(Box<Rollup>),
    GroupingSets(Box<GroupingSets>),
    LimitOptions(Box<LimitOptions>),
    Lateral(Box<Lateral>),
    TableFromRows(Box<TableFromRows>),
    RowsFrom(Box<RowsFrom>),
    MatchRecognizeMeasure(Box<MatchRecognizeMeasure>),
    WithFill(Box<WithFill>),
    Property(Box<Property>),
    GrantPrivilege(Box<GrantPrivilege>),
    GrantPrincipal(Box<GrantPrincipal>),
    AllowedValuesProperty(Box<AllowedValuesProperty>),
    AlgorithmProperty(Box<AlgorithmProperty>),
    AutoIncrementProperty(Box<AutoIncrementProperty>),
    AutoRefreshProperty(Box<AutoRefreshProperty>),
    BackupProperty(Box<BackupProperty>),
    BuildProperty(Box<BuildProperty>),
    BlockCompressionProperty(Box<BlockCompressionProperty>),
    CharacterSetProperty(Box<CharacterSetProperty>),
    ChecksumProperty(Box<ChecksumProperty>),
    CollateProperty(Box<CollateProperty>),
    DataBlocksizeProperty(Box<DataBlocksizeProperty>),
    DataDeletionProperty(Box<DataDeletionProperty>),
    DefinerProperty(Box<DefinerProperty>),
    DistKeyProperty(Box<DistKeyProperty>),
    DistributedByProperty(Box<DistributedByProperty>),
    DistStyleProperty(Box<DistStyleProperty>),
    DuplicateKeyProperty(Box<DuplicateKeyProperty>),
    EngineProperty(Box<EngineProperty>),
    ToTableProperty(Box<ToTableProperty>),
    ExecuteAsProperty(Box<ExecuteAsProperty>),
    ExternalProperty(Box<ExternalProperty>),
    FallbackProperty(Box<FallbackProperty>),
    FileFormatProperty(Box<FileFormatProperty>),
    CredentialsProperty(Box<CredentialsProperty>),
    FreespaceProperty(Box<FreespaceProperty>),
    InheritsProperty(Box<InheritsProperty>),
    InputModelProperty(Box<InputModelProperty>),
    OutputModelProperty(Box<OutputModelProperty>),
    IsolatedLoadingProperty(Box<IsolatedLoadingProperty>),
    JournalProperty(Box<JournalProperty>),
    LanguageProperty(Box<LanguageProperty>),
    EnviromentProperty(Box<EnviromentProperty>),
    ClusteredByProperty(Box<ClusteredByProperty>),
    DictProperty(Box<DictProperty>),
    DictRange(Box<DictRange>),
    OnCluster(Box<OnCluster>),
    LikeProperty(Box<LikeProperty>),
    LocationProperty(Box<LocationProperty>),
    LockProperty(Box<LockProperty>),
    LockingProperty(Box<LockingProperty>),
    LogProperty(Box<LogProperty>),
    MaterializedProperty(Box<MaterializedProperty>),
    MergeBlockRatioProperty(Box<MergeBlockRatioProperty>),
    OnProperty(Box<OnProperty>),
    OnCommitProperty(Box<OnCommitProperty>),
    PartitionedByProperty(Box<PartitionedByProperty>),
    PartitionedByBucket(Box<PartitionedByBucket>),
    PartitionByTruncate(Box<PartitionByTruncate>),
    PartitionByRangeProperty(Box<PartitionByRangeProperty>),
    PartitionByRangePropertyDynamic(Box<PartitionByRangePropertyDynamic>),
    PartitionByListProperty(Box<PartitionByListProperty>),
    PartitionList(Box<PartitionList>),
    Partition(Box<Partition>),
    RefreshTriggerProperty(Box<RefreshTriggerProperty>),
    UniqueKeyProperty(Box<UniqueKeyProperty>),
    RollupProperty(Box<RollupProperty>),
    PartitionBoundSpec(Box<PartitionBoundSpec>),
    PartitionedOfProperty(Box<PartitionedOfProperty>),
    RemoteWithConnectionModelProperty(Box<RemoteWithConnectionModelProperty>),
    ReturnsProperty(Box<ReturnsProperty>),
    RowFormatProperty(Box<RowFormatProperty>),
    RowFormatDelimitedProperty(Box<RowFormatDelimitedProperty>),
    RowFormatSerdeProperty(Box<RowFormatSerdeProperty>),
    QueryTransform(Box<QueryTransform>),
    SampleProperty(Box<SampleProperty>),
    SecurityProperty(Box<SecurityProperty>),
    SchemaCommentProperty(Box<SchemaCommentProperty>),
    SemanticView(Box<SemanticView>),
    SerdeProperties(Box<SerdeProperties>),
    SetProperty(Box<SetProperty>),
    SharingProperty(Box<SharingProperty>),
    SetConfigProperty(Box<SetConfigProperty>),
    SettingsProperty(Box<SettingsProperty>),
    SortKeyProperty(Box<SortKeyProperty>),
    SqlReadWriteProperty(Box<SqlReadWriteProperty>),
    SqlSecurityProperty(Box<SqlSecurityProperty>),
    StabilityProperty(Box<StabilityProperty>),
    StorageHandlerProperty(Box<StorageHandlerProperty>),
    TemporaryProperty(Box<TemporaryProperty>),
    Tags(Box<Tags>),
    TransformModelProperty(Box<TransformModelProperty>),
    TransientProperty(Box<TransientProperty>),
    UsingTemplateProperty(Box<UsingTemplateProperty>),
    ViewAttributeProperty(Box<ViewAttributeProperty>),
    VolatileProperty(Box<VolatileProperty>),
    WithDataProperty(Box<WithDataProperty>),
    WithJournalTableProperty(Box<WithJournalTableProperty>),
    WithSchemaBindingProperty(Box<WithSchemaBindingProperty>),
    WithSystemVersioningProperty(Box<WithSystemVersioningProperty>),
    WithProcedureOptions(Box<WithProcedureOptions>),
    EncodeProperty(Box<EncodeProperty>),
    IncludeProperty(Box<IncludeProperty>),
    Properties(Box<Properties>),
    InputOutputFormat(Box<InputOutputFormat>),
    Reference(Box<Reference>),
    QueryOption(Box<QueryOption>),
    WithTableHint(Box<WithTableHint>),
    IndexTableHint(Box<IndexTableHint>),
    HistoricalData(Box<HistoricalData>),
    Get(Box<Get>),
    SetOperation(Box<SetOperation>),
    Var(Box<Var>),
    Version(Box<Version>),
    Schema(Box<Schema>),
    Lock(Box<Lock>),
    TableSample(Box<TableSample>),
    Tag(Box<Tag>),
    UnpivotColumns(Box<UnpivotColumns>),
    WindowSpec(Box<WindowSpec>),
    SessionParameter(Box<SessionParameter>),
    PseudoType(Box<PseudoType>),
    ObjectIdentifier(Box<ObjectIdentifier>),
    Transaction(Box<Transaction>),
    Commit(Box<Commit>),
    Rollback(Box<Rollback>),
    AlterSession(Box<AlterSession>),
    Analyze(Box<Analyze>),
    AnalyzeStatistics(Box<AnalyzeStatistics>),
    AnalyzeHistogram(Box<AnalyzeHistogram>),
    AnalyzeSample(Box<AnalyzeSample>),
    AnalyzeListChainedRows(Box<AnalyzeListChainedRows>),
    AnalyzeDelete(Box<AnalyzeDelete>),
    AnalyzeWith(Box<AnalyzeWith>),
    AnalyzeValidate(Box<AnalyzeValidate>),
    AddPartition(Box<AddPartition>),
    AttachOption(Box<AttachOption>),
    DropPartition(Box<DropPartition>),
    ReplacePartition(Box<ReplacePartition>),
    DPipe(Box<DPipe>),
    Operator(Box<Operator>),
    PivotAny(Box<PivotAny>),
    Aliases(Box<Aliases>),
    AtIndex(Box<AtIndex>),
    FromTimeZone(Box<FromTimeZone>),
    FormatPhrase(Box<FormatPhrase>),
    ForIn(Box<ForIn>),
    TimeUnit(Box<TimeUnit>),
    IntervalOp(Box<IntervalOp>),
    IntervalSpan(Box<IntervalSpan>),
    HavingMax(Box<HavingMax>),
    CosineDistance(Box<CosineDistance>),
    DotProduct(Box<DotProduct>),
    EuclideanDistance(Box<EuclideanDistance>),
    ManhattanDistance(Box<ManhattanDistance>),
    JarowinklerSimilarity(Box<JarowinklerSimilarity>),
    Booland(Box<Booland>),
    Boolor(Box<Boolor>),
    ParameterizedAgg(Box<ParameterizedAgg>),
    ArgMax(Box<ArgMax>),
    ArgMin(Box<ArgMin>),
    ApproxTopK(Box<ApproxTopK>),
    ApproxTopKAccumulate(Box<ApproxTopKAccumulate>),
    ApproxTopKCombine(Box<ApproxTopKCombine>),
    ApproxTopKEstimate(Box<ApproxTopKEstimate>),
    ApproxTopSum(Box<ApproxTopSum>),
    ApproxQuantiles(Box<ApproxQuantiles>),
    Minhash(Box<Minhash>),
    FarmFingerprint(Box<FarmFingerprint>),
    Float64(Box<Float64>),
    Transform(Box<Transform>),
    Translate(Box<Translate>),
    Grouping(Box<Grouping>),
    GroupingId(Box<GroupingId>),
    Anonymous(Box<Anonymous>),
    AnonymousAggFunc(Box<AnonymousAggFunc>),
    CombinedAggFunc(Box<CombinedAggFunc>),
    CombinedParameterizedAgg(Box<CombinedParameterizedAgg>),
    HashAgg(Box<HashAgg>),
    Hll(Box<Hll>),
    Apply(Box<Apply>),
    ToBoolean(Box<ToBoolean>),
    List(Box<List>),
    ToMap(Box<ToMap>),
    Pad(Box<Pad>),
    ToChar(Box<ToChar>),
    ToNumber(Box<ToNumber>),
    ToDouble(Box<ToDouble>),
    Int64(Box<UnaryFunc>),
    StringFunc(Box<StringFunc>),
    ToDecfloat(Box<ToDecfloat>),
    TryToDecfloat(Box<TryToDecfloat>),
    ToFile(Box<ToFile>),
    Columns(Box<Columns>),
    ConvertToCharset(Box<ConvertToCharset>),
    ConvertTimezone(Box<ConvertTimezone>),
    GenerateSeries(Box<GenerateSeries>),
    AIAgg(Box<AIAgg>),
    AIClassify(Box<AIClassify>),
    ArrayAll(Box<ArrayAll>),
    ArrayAny(Box<ArrayAny>),
    ArrayConstructCompact(Box<ArrayConstructCompact>),
    StPoint(Box<StPoint>),
    StDistance(Box<StDistance>),
    StringToArray(Box<StringToArray>),
    ArraySum(Box<ArraySum>),
    ObjectAgg(Box<ObjectAgg>),
    CastToStrType(Box<CastToStrType>),
    CheckJson(Box<CheckJson>),
    CheckXml(Box<CheckXml>),
    TranslateCharacters(Box<TranslateCharacters>),
    CurrentSchemas(Box<CurrentSchemas>),
    CurrentDatetime(Box<CurrentDatetime>),
    Localtime(Box<Localtime>),
    Localtimestamp(Box<Localtimestamp>),
    Systimestamp(Box<Systimestamp>),
    CurrentSchema(Box<CurrentSchema>),
    CurrentUser(Box<CurrentUser>),
    UtcTime(Box<UtcTime>),
    UtcTimestamp(Box<UtcTimestamp>),
    Timestamp(Box<TimestampFunc>),
    DateBin(Box<DateBin>),
    Datetime(Box<Datetime>),
    DatetimeAdd(Box<DatetimeAdd>),
    DatetimeSub(Box<DatetimeSub>),
    DatetimeDiff(Box<DatetimeDiff>),
    DatetimeTrunc(Box<DatetimeTrunc>),
    Dayname(Box<Dayname>),
    MakeInterval(Box<MakeInterval>),
    PreviousDay(Box<PreviousDay>),
    Elt(Box<Elt>),
    TimestampAdd(Box<TimestampAdd>),
    TimestampSub(Box<TimestampSub>),
    TimestampDiff(Box<TimestampDiff>),
    TimeSlice(Box<TimeSlice>),
    TimeAdd(Box<TimeAdd>),
    TimeSub(Box<TimeSub>),
    TimeDiff(Box<TimeDiff>),
    TimeTrunc(Box<TimeTrunc>),
    DateFromParts(Box<DateFromParts>),
    TimeFromParts(Box<TimeFromParts>),
    DecodeCase(Box<DecodeCase>),
    Decrypt(Box<Decrypt>),
    DecryptRaw(Box<DecryptRaw>),
    Encode(Box<Encode>),
    Encrypt(Box<Encrypt>),
    EncryptRaw(Box<EncryptRaw>),
    EqualNull(Box<EqualNull>),
    ToBinary(Box<ToBinary>),
    Base64DecodeBinary(Box<Base64DecodeBinary>),
    Base64DecodeString(Box<Base64DecodeString>),
    Base64Encode(Box<Base64Encode>),
    TryBase64DecodeBinary(Box<TryBase64DecodeBinary>),
    TryBase64DecodeString(Box<TryBase64DecodeString>),
    GapFill(Box<GapFill>),
    GenerateDateArray(Box<GenerateDateArray>),
    GenerateTimestampArray(Box<GenerateTimestampArray>),
    GetExtract(Box<GetExtract>),
    Getbit(Box<Getbit>),
    OverflowTruncateBehavior(Box<OverflowTruncateBehavior>),
    HexEncode(Box<HexEncode>),
    Compress(Box<Compress>),
    DecompressBinary(Box<DecompressBinary>),
    DecompressString(Box<DecompressString>),
    Xor(Box<Xor>),
    Nullif(Box<Nullif>),
    JSON(Box<JSON>),
    JSONPath(Box<JSONPath>),
    JSONPathFilter(Box<JSONPathFilter>),
    JSONPathKey(Box<JSONPathKey>),
    JSONPathRecursive(Box<JSONPathRecursive>),
    JSONPathScript(Box<JSONPathScript>),
    JSONPathSlice(Box<JSONPathSlice>),
    JSONPathSelector(Box<JSONPathSelector>),
    JSONPathSubscript(Box<JSONPathSubscript>),
    JSONPathUnion(Box<JSONPathUnion>),
    Format(Box<Format>),
    JSONKeys(Box<JSONKeys>),
    JSONKeyValue(Box<JSONKeyValue>),
    JSONKeysAtDepth(Box<JSONKeysAtDepth>),
    JSONObject(Box<JSONObject>),
    JSONObjectAgg(Box<JSONObjectAgg>),
    JSONBObjectAgg(Box<JSONBObjectAgg>),
    JSONArray(Box<JSONArray>),
    JSONArrayAgg(Box<JSONArrayAgg>),
    JSONExists(Box<JSONExists>),
    JSONColumnDef(Box<JSONColumnDef>),
    JSONSchema(Box<JSONSchema>),
    JSONSet(Box<JSONSet>),
    JSONStripNulls(Box<JSONStripNulls>),
    JSONValue(Box<JSONValue>),
    JSONValueArray(Box<JSONValueArray>),
    JSONRemove(Box<JSONRemove>),
    JSONTable(Box<JSONTable>),
    JSONType(Box<JSONType>),
    ObjectInsert(Box<ObjectInsert>),
    OpenJSONColumnDef(Box<OpenJSONColumnDef>),
    OpenJSON(Box<OpenJSON>),
    JSONBExists(Box<JSONBExists>),
    JSONBContains(Box<BinaryFunc>),
    JSONBExtract(Box<BinaryFunc>),
    JSONCast(Box<JSONCast>),
    JSONExtract(Box<JSONExtract>),
    JSONExtractQuote(Box<JSONExtractQuote>),
    JSONExtractArray(Box<JSONExtractArray>),
    JSONExtractScalar(Box<JSONExtractScalar>),
    JSONBExtractScalar(Box<JSONBExtractScalar>),
    JSONFormat(Box<JSONFormat>),
    JSONBool(Box<UnaryFunc>),
    JSONPathRoot(JSONPathRoot),
    JSONArrayAppend(Box<JSONArrayAppend>),
    JSONArrayContains(Box<JSONArrayContains>),
    JSONArrayInsert(Box<JSONArrayInsert>),
    ParseJSON(Box<ParseJSON>),
    ParseUrl(Box<ParseUrl>),
    ParseIp(Box<ParseIp>),
    ParseTime(Box<ParseTime>),
    ParseDatetime(Box<ParseDatetime>),
    Map(Box<Map>),
    MapCat(Box<MapCat>),
    MapDelete(Box<MapDelete>),
    MapInsert(Box<MapInsert>),
    MapPick(Box<MapPick>),
    ScopeResolution(Box<ScopeResolution>),
    Slice(Box<Slice>),
    VarMap(Box<VarMap>),
    MatchAgainst(Box<MatchAgainst>),
    MD5Digest(Box<MD5Digest>),
    MD5NumberLower64(Box<UnaryFunc>),
    MD5NumberUpper64(Box<UnaryFunc>),
    Monthname(Box<Monthname>),
    Ntile(Box<Ntile>),
    Normalize(Box<Normalize>),
    Normal(Box<Normal>),
    Predict(Box<Predict>),
    MLTranslate(Box<MLTranslate>),
    FeaturesAtTime(Box<FeaturesAtTime>),
    GenerateEmbedding(Box<GenerateEmbedding>),
    MLForecast(Box<MLForecast>),
    ModelAttribute(Box<ModelAttribute>),
    VectorSearch(Box<VectorSearch>),
    Quantile(Box<Quantile>),
    ApproxQuantile(Box<ApproxQuantile>),
    ApproxPercentileEstimate(Box<ApproxPercentileEstimate>),
    Randn(Box<Randn>),
    Randstr(Box<Randstr>),
    RangeN(Box<RangeN>),
    RangeBucket(Box<RangeBucket>),
    ReadCSV(Box<ReadCSV>),
    ReadParquet(Box<ReadParquet>),
    Reduce(Box<Reduce>),
    RegexpExtractAll(Box<RegexpExtractAll>),
    RegexpILike(Box<RegexpILike>),
    RegexpFullMatch(Box<RegexpFullMatch>),
    RegexpInstr(Box<RegexpInstr>),
    RegexpSplit(Box<RegexpSplit>),
    RegexpCount(Box<RegexpCount>),
    RegrValx(Box<RegrValx>),
    RegrValy(Box<RegrValy>),
    RegrAvgy(Box<RegrAvgy>),
    RegrAvgx(Box<RegrAvgx>),
    RegrCount(Box<RegrCount>),
    RegrIntercept(Box<RegrIntercept>),
    RegrR2(Box<RegrR2>),
    RegrSxx(Box<RegrSxx>),
    RegrSxy(Box<RegrSxy>),
    RegrSyy(Box<RegrSyy>),
    RegrSlope(Box<RegrSlope>),
    SafeAdd(Box<SafeAdd>),
    SafeDivide(Box<SafeDivide>),
    SafeMultiply(Box<SafeMultiply>),
    SafeSubtract(Box<SafeSubtract>),
    SHA2(Box<SHA2>),
    SHA2Digest(Box<SHA2Digest>),
    SortArray(Box<SortArray>),
    SplitPart(Box<SplitPart>),
    SubstringIndex(Box<SubstringIndex>),
    StandardHash(Box<StandardHash>),
    StrPosition(Box<StrPosition>),
    Search(Box<Search>),
    SearchIp(Box<SearchIp>),
    StrToDate(Box<StrToDate>),
    DateStrToDate(Box<UnaryFunc>),
    DateToDateStr(Box<UnaryFunc>),
    StrToTime(Box<StrToTime>),
    StrToUnix(Box<StrToUnix>),
    StrToMap(Box<StrToMap>),
    NumberToStr(Box<NumberToStr>),
    FromBase(Box<FromBase>),
    Stuff(Box<Stuff>),
    TimeToStr(Box<TimeToStr>),
    TimeStrToTime(Box<TimeStrToTime>),
    TsOrDsAdd(Box<TsOrDsAdd>),
    TsOrDsDiff(Box<TsOrDsDiff>),
    TsOrDsToDate(Box<TsOrDsToDate>),
    TsOrDsToTime(Box<TsOrDsToTime>),
    Unhex(Box<Unhex>),
    Uniform(Box<Uniform>),
    UnixToStr(Box<UnixToStr>),
    UnixToTime(Box<UnixToTime>),
    Uuid(Box<Uuid>),
    TimestampFromParts(Box<TimestampFromParts>),
    TimestampTzFromParts(Box<TimestampTzFromParts>),
    Corr(Box<Corr>),
    WidthBucket(Box<WidthBucket>),
    CovarSamp(Box<CovarSamp>),
    CovarPop(Box<CovarPop>),
    Week(Box<Week>),
    XMLElement(Box<XMLElement>),
    XMLGet(Box<XMLGet>),
    XMLTable(Box<XMLTable>),
    XMLKeyValueOption(Box<XMLKeyValueOption>),
    Zipf(Box<Zipf>),
    Merge(Box<Merge>),
    When(Box<When>),
    Whens(Box<Whens>),
    NextValueFor(Box<NextValueFor>),
    /// RETURN statement (DuckDB stored procedures)
    ReturnStmt(Box<Expression>),
}

impl Expression {
    /// Returns `true` if this expression is a valid top-level SQL statement.
    ///
    /// Bare expressions like identifiers, literals, and function calls are not
    /// valid statements. This is used by `validate()` to reject inputs like
    /// `SELECT scooby dooby doo` which the parser splits into `SELECT scooby AS dooby`
    /// plus the bare identifier `doo`.
    pub fn is_statement(&self) -> bool {
        match self {
            // Queries
            Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Subquery(_)
            | Expression::Values(_)
            | Expression::PipeOperator(_)

            // DML
            | Expression::Insert(_)
            | Expression::Update(_)
            | Expression::Delete(_)
            | Expression::Copy(_)
            | Expression::Put(_)
            | Expression::Merge(_)

            // DDL
            | Expression::CreateTable(_)
            | Expression::DropTable(_)
            | Expression::AlterTable(_)
            | Expression::CreateIndex(_)
            | Expression::DropIndex(_)
            | Expression::CreateView(_)
            | Expression::DropView(_)
            | Expression::AlterView(_)
            | Expression::AlterIndex(_)
            | Expression::Truncate(_)
            | Expression::TruncateTable(_)
            | Expression::CreateSchema(_)
            | Expression::DropSchema(_)
            | Expression::DropNamespace(_)
            | Expression::CreateDatabase(_)
            | Expression::DropDatabase(_)
            | Expression::CreateFunction(_)
            | Expression::DropFunction(_)
            | Expression::CreateProcedure(_)
            | Expression::DropProcedure(_)
            | Expression::CreateSequence(_)
            | Expression::DropSequence(_)
            | Expression::AlterSequence(_)
            | Expression::CreateTrigger(_)
            | Expression::DropTrigger(_)
            | Expression::CreateType(_)
            | Expression::DropType(_)
            | Expression::Comment(_)

            // Session/Transaction/Control
            | Expression::Use(_)
            | Expression::Set(_)
            | Expression::SetStatement(_)
            | Expression::Transaction(_)
            | Expression::Commit(_)
            | Expression::Rollback(_)
            | Expression::Grant(_)
            | Expression::Revoke(_)
            | Expression::Cache(_)
            | Expression::Uncache(_)
            | Expression::LoadData(_)
            | Expression::Pragma(_)
            | Expression::Describe(_)
            | Expression::Show(_)
            | Expression::Kill(_)
            | Expression::Execute(_)
            | Expression::Declare(_)
            | Expression::Refresh(_)
            | Expression::AlterSession(_)
            | Expression::LockingStatement(_)

            // Analyze
            | Expression::Analyze(_)
            | Expression::AnalyzeStatistics(_)
            | Expression::AnalyzeHistogram(_)
            | Expression::AnalyzeSample(_)
            | Expression::AnalyzeListChainedRows(_)
            | Expression::AnalyzeDelete(_)

            // Attach/Detach/Install/Summarize
            | Expression::Attach(_)
            | Expression::Detach(_)
            | Expression::Install(_)
            | Expression::Summarize(_)

            // Pivot at statement level
            | Expression::Pivot(_)
            | Expression::Unpivot(_)

            // Command (raw/unparsed statements)
            | Expression::Command(_)
            | Expression::Raw(_)

            // Return statement
            | Expression::ReturnStmt(_) => true,

            // Annotated wraps another expression with comments — check inner
            Expression::Annotated(a) => a.this.is_statement(),

            // Alias at top level can wrap a statement (e.g., parenthesized subquery with alias)
            Expression::Alias(a) => a.this.is_statement(),

            // Everything else (identifiers, literals, operators, functions, etc.)
            _ => false,
        }
    }

    /// Create a literal number expression from an integer.
    pub fn number(n: i64) -> Self {
        Expression::Literal(Literal::Number(n.to_string()))
    }

    /// Create a single-quoted literal string expression.
    pub fn string(s: impl Into<String>) -> Self {
        Expression::Literal(Literal::String(s.into()))
    }

    /// Create a literal number expression from a float.
    pub fn float(f: f64) -> Self {
        Expression::Literal(Literal::Number(f.to_string()))
    }

    /// Create an unqualified column reference (e.g. `name`).
    pub fn column(name: impl Into<String>) -> Self {
        Expression::Column(Column {
            name: Identifier::new(name),
            table: None,
            join_mark: false,
            trailing_comments: Vec::new(),
        })
    }

    /// Create a qualified column reference (`table.column`).
    pub fn qualified_column(table: impl Into<String>, column: impl Into<String>) -> Self {
        Expression::Column(Column {
            name: Identifier::new(column),
            table: Some(Identifier::new(table)),
            join_mark: false,
            trailing_comments: Vec::new(),
        })
    }

    /// Create a bare identifier expression (not a column reference).
    pub fn identifier(name: impl Into<String>) -> Self {
        Expression::Identifier(Identifier::new(name))
    }

    /// Create a NULL expression
    pub fn null() -> Self {
        Expression::Null(Null)
    }

    /// Create a TRUE expression
    pub fn true_() -> Self {
        Expression::Boolean(BooleanLiteral { value: true })
    }

    /// Create a FALSE expression
    pub fn false_() -> Self {
        Expression::Boolean(BooleanLiteral { value: false })
    }

    /// Create a wildcard star (`*`) expression with no EXCEPT/REPLACE/RENAME modifiers.
    pub fn star() -> Self {
        Expression::Star(Star {
            table: None,
            except: None,
            replace: None,
            rename: None,
            trailing_comments: Vec::new(),
        })
    }

    /// Wrap this expression in an `AS` alias (e.g. `expr AS name`).
    pub fn alias(self, name: impl Into<String>) -> Self {
        Expression::Alias(Box::new(Alias::new(self, Identifier::new(name))))
    }

    /// Check if this is a SELECT expression
    pub fn is_select(&self) -> bool {
        matches!(self, Expression::Select(_))
    }

    /// Try to get as a Select
    pub fn as_select(&self) -> Option<&Select> {
        match self {
            Expression::Select(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as a mutable Select
    pub fn as_select_mut(&mut self) -> Option<&mut Select> {
        match self {
            Expression::Select(s) => Some(s),
            _ => None,
        }
    }

    /// Generate a SQL string for this expression using the generic (dialect-agnostic) generator.
    ///
    /// Returns an empty string if generation fails. For dialect-specific output,
    /// use [`sql_for()`](Self::sql_for) instead.
    pub fn sql(&self) -> String {
        crate::generator::Generator::sql(self).unwrap_or_default()
    }

    /// Generate a SQL string for this expression targeting a specific dialect.
    ///
    /// Dialect-specific rules (identifier quoting, function names, type mappings,
    /// syntax variations) are applied automatically.  Returns an empty string if
    /// generation fails.
    pub fn sql_for(&self, dialect: crate::dialects::DialectType) -> String {
        crate::generate(self, dialect).unwrap_or_default()
    }

}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Basic display - full SQL generation is in generator module
        match self {
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::Identifier(id) => write!(f, "{}", id),
            Expression::Column(col) => write!(f, "{}", col),
            Expression::Star(_) => write!(f, "*"),
            Expression::Null(_) => write!(f, "NULL"),
            Expression::Boolean(b) => write!(f, "{}", if b.value { "TRUE" } else { "FALSE" }),
            Expression::Select(_) => write!(f, "SELECT ..."),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// Represent a SQL literal value.
///
/// Numeric values are stored as their original text representation (not parsed
/// to `i64`/`f64`) so that precision, trailing zeros, and hex notation are
/// preserved across round-trips.
///
/// Dialect-specific literal forms (triple-quoted strings, dollar-quoted
/// strings, raw strings, etc.) each have a dedicated variant so that the
/// generator can emit them with the correct syntax.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "literal_type", content = "value", rename_all = "snake_case")]
pub enum Literal {
    /// Single-quoted string literal: `'hello'`
    String(String),
    /// Numeric literal, stored as the original text: `42`, `3.14`, `1e10`
    Number(String),
    /// Hex string literal: `X'FF'`
    HexString(String),
    /// Hex number: 0xA, 0xFF (BigQuery, SQLite style) - represents an integer in hex notation
    HexNumber(String),
    BitString(String),
    /// Byte string: b"..." (BigQuery style)
    ByteString(String),
    /// National string: N'abc'
    NationalString(String),
    /// DATE literal: DATE '2024-01-15'
    Date(String),
    /// TIME literal: TIME '10:30:00'
    Time(String),
    /// TIMESTAMP literal: TIMESTAMP '2024-01-15 10:30:00'
    Timestamp(String),
    /// DATETIME literal: DATETIME '2024-01-15 10:30:00' (BigQuery)
    Datetime(String),
    /// Triple-quoted string: """...""" or '''...'''
    /// Contains (content, quote_char) where quote_char is '"' or '\''
    TripleQuotedString(String, char),
    /// Escape string: E'...' (PostgreSQL)
    EscapeString(String),
    /// Dollar-quoted string: $$...$$  (PostgreSQL)
    DollarString(String),
    /// Raw string: r"..." or r'...' (BigQuery, Spark, Databricks)
    /// In raw strings, backslashes are literal and not escape characters.
    /// When converting to a regular string, backslashes must be doubled.
    RawString(String),
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Literal::String(s) => write!(f, "'{}'", s),
            Literal::Number(n) => write!(f, "{}", n),
            Literal::HexString(h) => write!(f, "X'{}'", h),
            Literal::HexNumber(h) => write!(f, "0x{}", h),
            Literal::BitString(b) => write!(f, "B'{}'", b),
            Literal::ByteString(b) => write!(f, "b'{}'", b),
            Literal::NationalString(s) => write!(f, "N'{}'", s),
            Literal::Date(d) => write!(f, "DATE '{}'", d),
            Literal::Time(t) => write!(f, "TIME '{}'", t),
            Literal::Timestamp(ts) => write!(f, "TIMESTAMP '{}'", ts),
            Literal::Datetime(dt) => write!(f, "DATETIME '{}'", dt),
            Literal::TripleQuotedString(s, q) => {
                write!(f, "{0}{0}{0}{1}{0}{0}{0}", q, s)
            }
            Literal::EscapeString(s) => write!(f, "E'{}'", s),
            Literal::DollarString(s) => write!(f, "$${}$$", s),
            Literal::RawString(s) => write!(f, "r'{}'", s),
        }
    }
}

/// Boolean literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BooleanLiteral {
    pub value: bool,
}

/// NULL literal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Null;

/// Represent a SQL identifier (table name, column name, alias, keyword-as-name, etc.).
///
/// The `quoted` flag indicates whether the identifier was originally delimited
/// (double-quoted, backtick-quoted, or bracket-quoted depending on the
/// dialect). The generator uses this flag to decide whether to emit quoting
/// characters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Identifier {
    /// The raw text of the identifier, without any quoting characters.
    pub name: String,
    /// Whether the identifier was quoted in the source SQL.
    pub quoted: bool,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl Identifier {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: false,
            trailing_comments: Vec::new(),
        }
    }

    pub fn quoted(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            quoted: true,
            trailing_comments: Vec::new(),
        }
    }

    pub fn empty() -> Self {
        Self {
            name: String::new(),
            quoted: false,
            trailing_comments: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.name.is_empty()
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.quoted {
            write!(f, "\"{}\"", self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Represent a column reference, optionally qualified by a table name.
///
/// Renders as `name` when unqualified, or `table.name` when qualified.
/// Use [`Expression::column()`] or [`Expression::qualified_column()`] for
/// convenient construction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Column {
    /// The column name.
    pub name: Identifier,
    /// Optional table qualifier (e.g. `t` in `t.col`).
    pub table: Option<Identifier>,
    /// Oracle-style join marker (+) for outer joins
    #[serde(default)]
    pub join_mark: bool,
    /// Trailing comments that appeared after this column reference
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(table) = &self.table {
            write!(f, "{}.{}", table, self.name)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

/// Represent a table reference with optional schema and catalog qualifiers.
///
/// Renders as `name`, `schema.name`, or `catalog.schema.name` depending on
/// which qualifiers are present. Supports aliases, column alias lists,
/// time-travel clauses (Snowflake, BigQuery), table hints (TSQL), and
/// several other dialect-specific extensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TableRef {
    /// The unqualified table name.
    pub name: Identifier,
    /// Optional schema qualifier (e.g. `public` in `public.users`).
    pub schema: Option<Identifier>,
    /// Optional catalog qualifier (e.g. `mydb` in `mydb.public.users`).
    pub catalog: Option<Identifier>,
    /// Optional table alias (e.g. `t` in `FROM users AS t`).
    pub alias: Option<Identifier>,
    /// Whether AS keyword was explicitly used for the alias
    #[serde(default)]
    pub alias_explicit_as: bool,
    /// Column aliases for table alias: AS t(c1, c2)
    #[serde(default)]
    pub column_aliases: Vec<Identifier>,
    /// Trailing comments that appeared after this table reference
    #[serde(default)]
    pub trailing_comments: Vec<String>,
    /// Snowflake time travel: BEFORE (STATEMENT => ...) or AT (TIMESTAMP => ...)
    #[serde(default)]
    pub when: Option<Box<HistoricalData>>,
    /// PostgreSQL ONLY modifier: prevents scanning child tables in inheritance hierarchy
    #[serde(default)]
    pub only: bool,
    /// ClickHouse FINAL modifier: forces final aggregation for MergeTree tables
    #[serde(default)]
    pub final_: bool,
    /// TABLESAMPLE clause attached to this table reference (DuckDB, BigQuery)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_sample: Option<Box<Sample>>,
    /// TSQL table hints: WITH (TABLOCK, INDEX(myindex), ...)
    #[serde(default)]
    pub hints: Vec<Expression>,
    /// TSQL: FOR SYSTEM_TIME temporal clause
    /// Contains the full clause text, e.g., "FOR SYSTEM_TIME BETWEEN c AND d"
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_time: Option<String>,
    /// MySQL: PARTITION(p0, p1, ...) hint for reading from specific partitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<Identifier>,
    /// Snowflake IDENTIFIER() function: dynamic table name from string/variable
    /// When set, this is used instead of the name field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identifier_func: Option<Box<Expression>>,
    /// Snowflake CHANGES clause: CHANGES (INFORMATION => ...) AT (...) END (...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes: Option<Box<Changes>>,
    /// Time travel version clause: FOR VERSION AS OF / FOR TIMESTAMP AS OF (Presto/Trino, BigQuery, Databricks)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Box<Version>>,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            schema: None,
            catalog: None,
            alias: None,
            alias_explicit_as: false,
            column_aliases: Vec::new(),
            trailing_comments: Vec::new(),
            when: None,
            only: false,
            final_: false,
            table_sample: None,
            hints: Vec::new(),
            system_time: None,
            partitions: Vec::new(),
            identifier_func: None,
            changes: None,
            version: None,
        }
    }

    /// Create with a schema qualifier.
    pub fn new_with_schema(name: impl Into<String>, schema: impl Into<String>) -> Self {
        let mut t = Self::new(name);
        t.schema = Some(Identifier::new(schema));
        t
    }

    /// Create with catalog and schema qualifiers.
    pub fn new_with_catalog(
        name: impl Into<String>,
        schema: impl Into<String>,
        catalog: impl Into<String>,
    ) -> Self {
        let mut t = Self::new(name);
        t.schema = Some(Identifier::new(schema));
        t.catalog = Some(Identifier::new(catalog));
        t
    }

    /// Create from an Identifier, preserving the quoted flag
    pub fn from_identifier(name: Identifier) -> Self {
        Self {
            name,
            schema: None,
            catalog: None,
            alias: None,
            alias_explicit_as: false,
            column_aliases: Vec::new(),
            trailing_comments: Vec::new(),
            when: None,
            only: false,
            final_: false,
            table_sample: None,
            hints: Vec::new(),
            system_time: None,
            partitions: Vec::new(),
            identifier_func: None,
            changes: None,
            version: None,
        }
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(Identifier::new(alias));
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(Identifier::new(schema));
        self
    }
}

/// Represent a wildcard star expression (`*`, `table.*`).
///
/// Supports the EXCEPT/EXCLUDE, REPLACE, and RENAME modifiers found in
/// DuckDB, BigQuery, and Snowflake (e.g. `SELECT * EXCEPT (id) FROM t`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Star {
    /// Optional table qualifier (e.g. `t` in `t.*`).
    pub table: Option<Identifier>,
    /// EXCLUDE / EXCEPT columns (DuckDB, BigQuery, Snowflake)
    pub except: Option<Vec<Identifier>>,
    /// REPLACE expressions (BigQuery, Snowflake)
    pub replace: Option<Vec<Alias>>,
    /// RENAME columns (Snowflake)
    pub rename: Option<Vec<(Identifier, Identifier)>>,
    /// Trailing comments that appeared after the star
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trailing_comments: Vec<String>,
}

/// Represent a complete SELECT statement.
///
/// This is the most feature-rich AST node, covering the full surface area of
/// SELECT syntax across 30+ SQL dialects. Fields that are `Option` or empty
/// `Vec` are omitted from the generated SQL when absent.
///
/// # Key Fields
///
/// - `expressions` -- the select-list (columns, `*`, computed expressions).
/// - `from` -- the FROM clause. `None` for `SELECT 1` style queries.
/// - `joins` -- zero or more JOIN clauses, each with a [`JoinKind`].
/// - `where_clause` -- the WHERE predicate.
/// - `group_by` -- GROUP BY, including ROLLUP/CUBE/GROUPING SETS.
/// - `having` -- HAVING predicate.
/// - `order_by` -- ORDER BY with ASC/DESC and NULLS FIRST/LAST.
/// - `limit` / `offset` / `fetch` -- result set limiting.
/// - `with` -- Common Table Expressions (CTEs).
/// - `distinct` / `distinct_on` -- DISTINCT and PostgreSQL DISTINCT ON.
/// - `windows` -- named window definitions (WINDOW w AS ...).
///
/// Dialect-specific extensions are supported via fields like `prewhere`
/// (ClickHouse), `qualify` (Snowflake/BigQuery/DuckDB), `connect` (Oracle
/// CONNECT BY), `for_xml` (TSQL), and `settings` (ClickHouse).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Select {
    /// The select-list: columns, expressions, aliases, and wildcards.
    pub expressions: Vec<Expression>,
    /// The FROM clause, containing one or more table sources.
    pub from: Option<From>,
    /// JOIN clauses applied after the FROM source.
    pub joins: Vec<Join>,
    pub lateral_views: Vec<LateralView>,
    /// ClickHouse PREWHERE clause
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prewhere: Option<Expression>,
    pub where_clause: Option<Where>,
    pub group_by: Option<GroupBy>,
    pub having: Option<Having>,
    pub qualify: Option<Qualify>,
    pub order_by: Option<OrderBy>,
    pub distribute_by: Option<DistributeBy>,
    pub cluster_by: Option<ClusterBy>,
    pub sort_by: Option<SortBy>,
    pub limit: Option<Limit>,
    pub offset: Option<Offset>,
    /// ClickHouse LIMIT BY clause expressions
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_by: Option<Vec<Expression>>,
    pub fetch: Option<Fetch>,
    pub distinct: bool,
    pub distinct_on: Option<Vec<Expression>>,
    pub top: Option<Top>,
    pub with: Option<With>,
    pub sample: Option<Sample>,
    /// ClickHouse SETTINGS clause (e.g., SETTINGS max_threads = 4)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settings: Option<Vec<Expression>>,
    /// ClickHouse FORMAT clause (e.g., FORMAT PrettyCompact)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<Expression>,
    pub windows: Option<Vec<NamedWindow>>,
    pub hint: Option<Hint>,
    /// Oracle CONNECT BY clause for hierarchical queries
    pub connect: Option<Connect>,
    /// SELECT ... INTO table_name for creating tables
    pub into: Option<SelectInto>,
    /// FOR UPDATE/SHARE locking clauses
    #[serde(default)]
    pub locks: Vec<Lock>,
    /// T-SQL FOR XML clause options (PATH, RAW, AUTO, EXPLICIT, BINARY BASE64, ELEMENTS XSINIL, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub for_xml: Vec<Expression>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// Comments that appear after SELECT keyword (before expressions)
    /// e.g., SELECT /*hint*/ col -> post_select_comments: ["/*hint*/"]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub post_select_comments: Vec<String>,
    /// BigQuery SELECT AS STRUCT / SELECT AS VALUE kind
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// MySQL operation modifiers (HIGH_PRIORITY, STRAIGHT_JOIN, SQL_CALC_FOUND_ROWS, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operation_modifiers: Vec<String>,
    /// Whether QUALIFY appears after WINDOW (DuckDB) vs before (Snowflake/BigQuery default)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub qualify_after_window: bool,
    /// TSQL OPTION clause (e.g., OPTION(LABEL = 'foo'))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub option: Option<String>,
}

impl Select {
    pub fn new() -> Self {
        Self {
            expressions: Vec::new(),
            from: None,
            joins: Vec::new(),
            lateral_views: Vec::new(),
            prewhere: None,
            where_clause: None,
            group_by: None,
            having: None,
            qualify: None,
            order_by: None,
            distribute_by: None,
            cluster_by: None,
            sort_by: None,
            limit: None,
            offset: None,
            limit_by: None,
            fetch: None,
            distinct: false,
            distinct_on: None,
            top: None,
            with: None,
            sample: None,
            settings: None,
            format: None,
            windows: None,
            hint: None,
            connect: None,
            into: None,
            locks: Vec::new(),
            for_xml: Vec::new(),
            leading_comments: Vec::new(),
            post_select_comments: Vec::new(),
            kind: None,
            operation_modifiers: Vec::new(),
            qualify_after_window: false,
            option: None,
        }
    }

    /// Add a column to select
    pub fn column(mut self, expr: Expression) -> Self {
        self.expressions.push(expr);
        self
    }

    /// Set the FROM clause
    pub fn from(mut self, table: Expression) -> Self {
        self.from = Some(From {
            expressions: vec![table],
        });
        self
    }

    /// Add a WHERE clause
    pub fn where_(mut self, condition: Expression) -> Self {
        self.where_clause = Some(Where { this: condition });
        self
    }

    /// Set DISTINCT
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add a JOIN
    pub fn join(mut self, join: Join) -> Self {
        self.joins.push(join);
        self
    }

    /// Set ORDER BY
    pub fn order_by(mut self, expressions: Vec<Ordered>) -> Self {
        self.order_by = Some(OrderBy { expressions, siblings: false, comments: Vec::new() });
        self
    }

    /// Set LIMIT
    pub fn limit(mut self, n: Expression) -> Self {
        self.limit = Some(Limit { this: n, percent: false , comments: Vec::new() });
        self
    }

    /// Set OFFSET
    pub fn offset(mut self, n: Expression) -> Self {
        self.offset = Some(Offset { this: n, rows: None });
        self
    }
}

impl Default for Select {
    fn default() -> Self {
        Self::new()
    }
}

/// Represent a UNION set operation between two query expressions.
///
/// When `all` is true, duplicate rows are preserved (UNION ALL).
/// ORDER BY, LIMIT, and OFFSET can be applied to the combined result.
/// Supports DuckDB's BY NAME modifier and BigQuery's CORRESPONDING modifier.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Union {
    /// The left-hand query operand.
    pub left: Expression,
    /// The right-hand query operand.
    pub right: Expression,
    /// Whether UNION ALL (true) or UNION (false, which deduplicates).
    pub all: bool,
    /// Whether DISTINCT was explicitly specified
    #[serde(default)]
    pub distinct: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire UNION result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire UNION result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire UNION result
    pub offset: Option<Box<Expression>>,
    /// DISTRIBUTE BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribute_by: Option<DistributeBy>,
    /// SORT BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortBy>,
    /// CLUSTER BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_by: Option<ClusterBy>,
    /// DuckDB BY NAME modifier
    #[serde(default)]
    pub by_name: bool,
    /// BigQuery: Set operation side (LEFT, RIGHT, FULL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// BigQuery: Set operation kind (INNER)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// BigQuery: CORRESPONDING modifier
    #[serde(default)]
    pub corresponding: bool,
    /// BigQuery: STRICT modifier (before CORRESPONDING)
    #[serde(default)]
    pub strict: bool,
    /// BigQuery: BY (columns) after CORRESPONDING
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_columns: Vec<Expression>,
}

/// Represent an INTERSECT set operation between two query expressions.
///
/// Returns only rows that appear in both operands. When `all` is true,
/// duplicates are preserved according to their multiplicity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Intersect {
    /// The left-hand query operand.
    pub left: Expression,
    /// The right-hand query operand.
    pub right: Expression,
    /// Whether INTERSECT ALL (true) or INTERSECT (false, which deduplicates).
    pub all: bool,
    /// Whether DISTINCT was explicitly specified
    #[serde(default)]
    pub distinct: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire INTERSECT result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire INTERSECT result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire INTERSECT result
    pub offset: Option<Box<Expression>>,
    /// DISTRIBUTE BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribute_by: Option<DistributeBy>,
    /// SORT BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortBy>,
    /// CLUSTER BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_by: Option<ClusterBy>,
    /// DuckDB BY NAME modifier
    #[serde(default)]
    pub by_name: bool,
    /// BigQuery: Set operation side (LEFT, RIGHT, FULL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// BigQuery: Set operation kind (INNER)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// BigQuery: CORRESPONDING modifier
    #[serde(default)]
    pub corresponding: bool,
    /// BigQuery: STRICT modifier (before CORRESPONDING)
    #[serde(default)]
    pub strict: bool,
    /// BigQuery: BY (columns) after CORRESPONDING
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_columns: Vec<Expression>,
}

/// Represent an EXCEPT (MINUS) set operation between two query expressions.
///
/// Returns rows from the left operand that do not appear in the right operand.
/// When `all` is true, duplicates are subtracted according to their multiplicity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Except {
    /// The left-hand query operand.
    pub left: Expression,
    /// The right-hand query operand (rows to subtract).
    pub right: Expression,
    /// Whether EXCEPT ALL (true) or EXCEPT (false, which deduplicates).
    pub all: bool,
    /// Whether DISTINCT was explicitly specified
    #[serde(default)]
    pub distinct: bool,
    /// Optional WITH clause
    pub with: Option<With>,
    /// ORDER BY applied to entire EXCEPT result
    pub order_by: Option<OrderBy>,
    /// LIMIT applied to entire EXCEPT result
    pub limit: Option<Box<Expression>>,
    /// OFFSET applied to entire EXCEPT result
    pub offset: Option<Box<Expression>>,
    /// DISTRIBUTE BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribute_by: Option<DistributeBy>,
    /// SORT BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortBy>,
    /// CLUSTER BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_by: Option<ClusterBy>,
    /// DuckDB BY NAME modifier
    #[serde(default)]
    pub by_name: bool,
    /// BigQuery: Set operation side (LEFT, RIGHT, FULL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,
    /// BigQuery: Set operation kind (INNER)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// BigQuery: CORRESPONDING modifier
    #[serde(default)]
    pub corresponding: bool,
    /// BigQuery: STRICT modifier (before CORRESPONDING)
    #[serde(default)]
    pub strict: bool,
    /// BigQuery: BY (columns) after CORRESPONDING
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_columns: Vec<Expression>,
}

/// INTO clause for SELECT INTO statements
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SelectInto {
    /// Target table or variable (used when single target)
    pub this: Expression,
    /// Whether TEMPORARY keyword was used
    #[serde(default)]
    pub temporary: bool,
    /// Whether UNLOGGED keyword was used (PostgreSQL)
    #[serde(default)]
    pub unlogged: bool,
    /// Whether BULK COLLECT INTO was used (Oracle PL/SQL)
    #[serde(default)]
    pub bulk_collect: bool,
    /// Multiple target variables (Oracle PL/SQL: BULK COLLECT INTO v1, v2)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expressions: Vec<Expression>,
}

/// Represent a parenthesized subquery expression.
///
/// A subquery wraps an inner query (typically a SELECT, UNION, etc.) in
/// parentheses and optionally applies an alias, column aliases, ORDER BY,
/// LIMIT, and OFFSET. The `modifiers_inside` flag controls whether the
/// modifiers are rendered inside or outside the parentheses.
///
/// Subqueries appear in many SQL contexts: FROM clauses, WHERE IN/EXISTS,
/// scalar subqueries in select-lists, and derived tables.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Subquery {
    /// The inner query expression.
    pub this: Expression,
    /// Optional alias for the derived table.
    pub alias: Option<Identifier>,
    /// Optional column aliases: AS t(c1, c2)
    pub column_aliases: Vec<Identifier>,
    /// ORDER BY clause (for parenthesized queries)
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<Limit>,
    /// OFFSET clause
    pub offset: Option<Offset>,
    /// DISTRIBUTE BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distribute_by: Option<DistributeBy>,
    /// SORT BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_by: Option<SortBy>,
    /// CLUSTER BY clause (Hive/Spark)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_by: Option<ClusterBy>,
    /// Whether this is a LATERAL subquery (can reference earlier tables in FROM)
    #[serde(default)]
    pub lateral: bool,
    /// Whether modifiers (ORDER BY, LIMIT, OFFSET) should be generated inside the parentheses
    /// true: (SELECT 1 LIMIT 1)  - modifiers inside
    /// false: (SELECT 1) LIMIT 1 - modifiers outside
    #[serde(default)]
    pub modifiers_inside: bool,
    /// Trailing comments after the closing paren
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

/// Pipe operator expression: query |> transform
///
/// Used in DataFusion and BigQuery pipe syntax:
///   FROM t |> WHERE x > 1 |> SELECT x, y |> ORDER BY x |> LIMIT 10
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PipeOperator {
    /// The input query/expression (left side of |>)
    pub this: Expression,
    /// The piped operation (right side of |>)
    pub expression: Expression,
}

/// VALUES table constructor: VALUES (1, 'a'), (2, 'b')
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Values {
    /// The rows of values
    pub expressions: Vec<Tuple>,
    /// Optional alias for the table
    pub alias: Option<Identifier>,
    /// Optional column aliases: AS t(c1, c2)
    pub column_aliases: Vec<Identifier>,
}

/// PIVOT operation - supports both standard and DuckDB simplified syntax
///
/// Standard syntax (in FROM clause):
///   table PIVOT(agg_func [AS alias], ... FOR column IN (value [AS alias], ...))
///   table UNPIVOT(value_col FOR name_col IN (col1, col2, ...))
///
/// DuckDB simplified syntax (statement-level):
///   PIVOT table ON columns [IN (...)] USING agg_func [AS alias], ... [GROUP BY ...]
///   UNPIVOT table ON columns INTO NAME name_col VALUE val_col
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Pivot {
    /// Source table/expression
    pub this: Expression,
    /// For standard PIVOT: the aggregation function(s) (first is primary)
    /// For DuckDB simplified: unused (use `using` instead)
    #[serde(default)]
    pub expressions: Vec<Expression>,
    /// For standard PIVOT: the FOR...IN clause(s) as In expressions
    #[serde(default)]
    pub fields: Vec<Expression>,
    /// For standard: unused. For DuckDB simplified: the USING aggregation functions
    #[serde(default)]
    pub using: Vec<Expression>,
    /// GROUP BY clause (used in both standard inside-parens and DuckDB simplified)
    #[serde(default)]
    pub group: Option<Box<Expression>>,
    /// Whether this is an UNPIVOT (vs PIVOT)
    #[serde(default)]
    pub unpivot: bool,
    /// For DuckDB UNPIVOT: INTO NAME col VALUE col
    #[serde(default)]
    pub into: Option<Box<Expression>>,
    /// Optional alias
    #[serde(default)]
    pub alias: Option<Identifier>,
    /// Include/exclude nulls (for UNPIVOT)
    #[serde(default)]
    pub include_nulls: Option<bool>,
    /// Default on null value (Snowflake)
    #[serde(default)]
    pub default_on_null: Option<Box<Expression>>,
    /// WITH clause (CTEs)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with: Option<With>,
}

/// UNPIVOT operation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Unpivot {
    pub this: Expression,
    pub value_column: Identifier,
    pub name_column: Identifier,
    pub columns: Vec<Expression>,
    pub alias: Option<Identifier>,
    /// Whether the value_column was parenthesized in the original SQL
    #[serde(default)]
    pub value_column_parenthesized: bool,
    /// INCLUDE NULLS (true), EXCLUDE NULLS (false), or not specified (None)
    #[serde(default)]
    pub include_nulls: Option<bool>,
    /// Additional value columns when parenthesized (e.g., (first_half_sales, second_half_sales))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_value_columns: Vec<Identifier>,
}

/// PIVOT alias for aliasing pivot expressions
/// The alias can be an identifier or an expression (for Oracle/BigQuery string concatenation aliases)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PivotAlias {
    pub this: Expression,
    pub alias: Expression,
}

/// PREWHERE clause (ClickHouse) - early filtering before WHERE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PreWhere {
    pub this: Expression,
}

/// STREAM definition (Snowflake) - for change data capture
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Stream {
    pub this: Expression,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on: Option<Expression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub show_initial_rows: Option<bool>,
}

/// USING DATA clause for data import statements
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UsingData {
    pub this: Expression,
}

/// XML Namespace declaration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct XmlNamespace {
    pub this: Expression,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<Identifier>,
}

/// ROW FORMAT clause for Hive/Spark
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowFormat {
    pub delimited: bool,
    pub fields_terminated_by: Option<String>,
    pub collection_items_terminated_by: Option<String>,
    pub map_keys_terminated_by: Option<String>,
    pub lines_terminated_by: Option<String>,
    pub null_defined_as: Option<String>,
}

/// Directory insert for INSERT OVERWRITE DIRECTORY (Hive/Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DirectoryInsert {
    pub local: bool,
    pub path: String,
    pub row_format: Option<RowFormat>,
    /// STORED AS clause (e.g., TEXTFILE, ORC, PARQUET)
    #[serde(default)]
    pub stored_as: Option<String>,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Insert {
    pub table: TableRef,
    pub columns: Vec<Identifier>,
    pub values: Vec<Vec<Expression>>,
    pub query: Option<Expression>,
    /// INSERT OVERWRITE for Hive/Spark
    pub overwrite: bool,
    /// PARTITION clause for Hive/Spark
    pub partition: Vec<(Identifier, Option<Expression>)>,
    /// INSERT OVERWRITE DIRECTORY for Hive/Spark
    #[serde(default)]
    pub directory: Option<DirectoryInsert>,
    /// RETURNING clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub returning: Vec<Expression>,
    /// OUTPUT clause (TSQL)
    #[serde(default)]
    pub output: Option<OutputClause>,
    /// ON CONFLICT clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub on_conflict: Option<Box<Expression>>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// IF EXISTS clause (Hive)
    #[serde(default)]
    pub if_exists: bool,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
    /// INSERT IGNORE (MySQL) - ignore duplicate key errors
    #[serde(default)]
    pub ignore: bool,
    /// Source alias for VALUES clause (MySQL): VALUES (1, 2) AS new_data
    #[serde(default)]
    pub source_alias: Option<Identifier>,
    /// Table alias (PostgreSQL): INSERT INTO table AS t(...)
    #[serde(default)]
    pub alias: Option<Identifier>,
    /// Whether the alias uses explicit AS keyword
    #[serde(default)]
    pub alias_explicit_as: bool,
    /// DEFAULT VALUES (PostgreSQL): INSERT INTO t DEFAULT VALUES
    #[serde(default)]
    pub default_values: bool,
    /// BY NAME modifier (DuckDB): INSERT INTO x BY NAME SELECT ...
    #[serde(default)]
    pub by_name: bool,
    /// SQLite conflict action: INSERT OR ABORT|FAIL|IGNORE|REPLACE|ROLLBACK INTO ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict_action: Option<String>,
    /// MySQL/SQLite REPLACE INTO statement (treat like INSERT)
    #[serde(default)]
    pub is_replace: bool,
    /// Oracle-style hint: INSERT /*+ APPEND */ INTO ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<Hint>,
    /// REPLACE WHERE clause (Databricks): INSERT INTO a REPLACE WHERE cond VALUES ...
    #[serde(default)]
    pub replace_where: Option<Box<Expression>>,
    /// Source table (Hive/Spark): INSERT OVERWRITE TABLE target TABLE source
    #[serde(default)]
    pub source: Option<Box<Expression>>,
    /// ClickHouse: INSERT INTO FUNCTION func_name(...) - the function call
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_target: Option<Box<Expression>>,
    /// ClickHouse: PARTITION BY expr
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<Box<Expression>>,
    /// ClickHouse: SETTINGS key = val, ...
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settings: Vec<Expression>,
}

/// OUTPUT clause (TSQL) - used in INSERT, UPDATE, DELETE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OutputClause {
    /// Columns/expressions to output
    pub columns: Vec<Expression>,
    /// Optional INTO target table or table variable
    #[serde(default)]
    pub into_table: Option<Expression>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Update {
    pub table: TableRef,
    /// Additional tables for multi-table UPDATE (MySQL syntax)
    #[serde(default)]
    pub extra_tables: Vec<TableRef>,
    /// JOINs attached to the table list (MySQL multi-table syntax)
    #[serde(default)]
    pub table_joins: Vec<Join>,
    pub set: Vec<(Identifier, Expression)>,
    pub from_clause: Option<From>,
    /// JOINs after FROM clause (PostgreSQL, Snowflake, SQL Server syntax)
    #[serde(default)]
    pub from_joins: Vec<Join>,
    pub where_clause: Option<Where>,
    /// RETURNING clause (PostgreSQL, SQLite)
    #[serde(default)]
    pub returning: Vec<Expression>,
    /// OUTPUT clause (TSQL)
    #[serde(default)]
    pub output: Option<OutputClause>,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// LIMIT clause (MySQL)
    #[serde(default)]
    pub limit: Option<Expression>,
    /// ORDER BY clause (MySQL)
    #[serde(default)]
    pub order_by: Option<OrderBy>,
    /// Whether FROM clause appears before SET (Snowflake syntax)
    #[serde(default)]
    pub from_before_set: bool,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Delete {
    pub table: TableRef,
    /// ClickHouse: ON CLUSTER clause for distributed DDL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cluster: Option<OnCluster>,
    /// Optional alias for the table
    pub alias: Option<Identifier>,
    /// Whether the alias was declared with explicit AS keyword
    #[serde(default)]
    pub alias_explicit_as: bool,
    /// PostgreSQL/DuckDB USING clause - additional tables to join
    pub using: Vec<TableRef>,
    pub where_clause: Option<Where>,
    /// OUTPUT clause (TSQL)
    #[serde(default)]
    pub output: Option<OutputClause>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// WITH clause (CTEs)
    #[serde(default)]
    pub with: Option<With>,
    /// LIMIT clause (MySQL)
    #[serde(default)]
    pub limit: Option<Expression>,
    /// ORDER BY clause (MySQL)
    #[serde(default)]
    pub order_by: Option<OrderBy>,
    /// RETURNING clause (PostgreSQL)
    #[serde(default)]
    pub returning: Vec<Expression>,
    /// MySQL multi-table DELETE: DELETE t1, t2 FROM ... or DELETE FROM t1, t2 USING ...
    /// These are the target tables to delete from
    #[serde(default)]
    pub tables: Vec<TableRef>,
    /// True if tables were after FROM keyword (DELETE FROM t1, t2 USING syntax)
    /// False if tables were before FROM keyword (DELETE t1, t2 FROM syntax)
    #[serde(default)]
    pub tables_from_using: bool,
    /// JOINs in MySQL multi-table DELETE: DELETE t1 FROM t1 LEFT JOIN t2 ...
    #[serde(default)]
    pub joins: Vec<Join>,
    /// FORCE INDEX hint (MySQL): DELETE FROM t FORCE INDEX (idx)
    #[serde(default)]
    pub force_index: Option<String>,
    /// BigQuery-style DELETE without FROM keyword: DELETE table WHERE ...
    #[serde(default)]
    pub no_from: bool,
}

/// COPY statement (Snowflake, PostgreSQL, DuckDB, TSQL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CopyStmt {
    /// Target table or query
    pub this: Expression,
    /// True for FROM (loading into table), false for TO (exporting)
    pub kind: bool,
    /// Source/destination file(s) or stage
    pub files: Vec<Expression>,
    /// Copy parameters
    #[serde(default)]
    pub params: Vec<CopyParameter>,
    /// Credentials for external access
    #[serde(default)]
    pub credentials: Option<Box<Credentials>>,
    /// Whether the INTO keyword was used (COPY INTO vs COPY)
    #[serde(default)]
    pub is_into: bool,
    /// Whether parameters are wrapped in WITH (...) syntax
    #[serde(default)]
    pub with_wrapped: bool,
}

/// COPY parameter (e.g., FILE_FORMAT = CSV or FORMAT PARQUET)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CopyParameter {
    pub name: String,
    pub value: Option<Expression>,
    pub values: Vec<Expression>,
    /// Whether the parameter used = sign (TSQL: KEY = VALUE vs DuckDB: KEY VALUE)
    #[serde(default)]
    pub eq: bool,
}

/// Credentials for external access (S3, Azure, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Credentials {
    pub credentials: Vec<(String, String)>,
    pub encryption: Option<String>,
    pub storage: Option<String>,
}

/// PUT statement (Snowflake)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PutStmt {
    /// Source file path
    pub source: String,
    /// Whether source was quoted in the original SQL
    #[serde(default)]
    pub source_quoted: bool,
    /// Target stage
    pub target: Expression,
    /// PUT parameters
    #[serde(default)]
    pub params: Vec<CopyParameter>,
}

/// Stage reference (Snowflake) - @stage_name or @namespace.stage/path
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StageReference {
    /// Stage name including @ prefix (e.g., "@mystage", "@namespace.mystage")
    pub name: String,
    /// Optional path within the stage (e.g., "/path/to/file.csv")
    #[serde(default)]
    pub path: Option<String>,
    /// Optional FILE_FORMAT parameter
    #[serde(default)]
    pub file_format: Option<Expression>,
    /// Optional PATTERN parameter
    #[serde(default)]
    pub pattern: Option<String>,
    /// Whether the stage reference was originally quoted (e.g., '@mystage')
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub quoted: bool,
}

/// Historical data / Time travel (Snowflake) - BEFORE (STATEMENT => ...) or AT (TIMESTAMP => ...)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct HistoricalData {
    /// The time travel kind: "BEFORE", "AT", or "END" (as an Identifier expression)
    pub this: Box<Expression>,
    /// The time travel type: "STATEMENT", "TIMESTAMP", "OFFSET", "STREAM", or "VERSION"
    pub kind: String,
    /// The expression value (e.g., the statement ID or timestamp)
    pub expression: Box<Expression>,
}

/// Represent an aliased expression (`expr AS name`).
///
/// Used for column aliases in select-lists, table aliases on subqueries,
/// and column alias lists on table-valued expressions (e.g. `AS t(c1, c2)`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Alias {
    /// The expression being aliased.
    pub this: Expression,
    /// The alias name (required for simple aliases, optional when only column aliases provided)
    pub alias: Identifier,
    /// Optional column aliases for table-valued functions: AS t(col1, col2) or AS (col1, col2)
    #[serde(default)]
    pub column_aliases: Vec<Identifier>,
    /// Comments that appeared between the expression and AS keyword
    #[serde(default)]
    pub pre_alias_comments: Vec<String>,
    /// Trailing comments that appeared after the alias
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl Alias {
    /// Create a simple alias
    pub fn new(this: Expression, alias: Identifier) -> Self {
        Self {
            this,
            alias,
            column_aliases: Vec::new(),
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }

    /// Create an alias with column aliases only (no table alias name)
    pub fn with_columns(this: Expression, column_aliases: Vec<Identifier>) -> Self {
        Self {
            this,
            alias: Identifier::empty(),
            column_aliases,
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }
}

/// Represent a type cast expression.
///
/// Covers both the standard `CAST(expr AS type)` syntax and the PostgreSQL
/// shorthand `expr::type`. Also used as the payload for `TryCast` and
/// `SafeCast` variants. Supports optional FORMAT (BigQuery) and DEFAULT ON
/// CONVERSION ERROR (Oracle) clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Cast {
    /// The expression being cast.
    pub this: Expression,
    /// The target data type.
    pub to: DataType,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
    /// Whether PostgreSQL `::` syntax was used (true) vs CAST() function (false)
    #[serde(default)]
    pub double_colon_syntax: bool,
    /// FORMAT clause for BigQuery: CAST(x AS STRING FORMAT 'format_string')
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub format: Option<Box<Expression>>,
    /// DEFAULT value ON CONVERSION ERROR (Oracle): CAST(x AS type DEFAULT val ON CONVERSION ERROR)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub default: Option<Box<Expression>>,
}

///// COLLATE expression: expr COLLATE 'collation_name' or expr COLLATE collation_name
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CollationExpr {
    pub this: Expression,
    pub collation: String,
    /// True if the collation was single-quoted in the original SQL (string literal)
    #[serde(default)]
    pub quoted: bool,
    /// True if the collation was double-quoted in the original SQL (identifier)
    #[serde(default)]
    pub double_quoted: bool,
}

/// Represent a CASE expression (both simple and searched forms).
///
/// When `operand` is `Some`, this is a simple CASE (`CASE x WHEN 1 THEN ...`).
/// When `operand` is `None`, this is a searched CASE (`CASE WHEN x > 0 THEN ...`).
/// Each entry in `whens` is a `(condition, result)` pair.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Case {
    /// The operand for simple CASE, or `None` for searched CASE.
    pub operand: Option<Expression>,
    /// Pairs of (WHEN condition, THEN result).
    pub whens: Vec<(Expression, Expression)>,
    /// Optional ELSE result.
    pub else_: Option<Expression>,
    /// Comments from the CASE keyword (emitted after END)
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// Represent a binary operation (two operands separated by an operator).
///
/// This is the shared payload struct for all binary operator variants in the
/// [`Expression`] enum: arithmetic (`Add`, `Sub`, `Mul`, `Div`, `Mod`),
/// comparison (`Eq`, `Neq`, `Lt`, `Gt`, etc.), logical (`And`, `Or`),
/// bitwise, and dialect-specific operators. Comment fields enable round-trip
/// preservation of inline comments around operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BinaryOp {
    pub left: Expression,
    pub right: Expression,
    /// Comments after the left operand (before the operator)
    #[serde(default)]
    pub left_comments: Vec<String>,
    /// Comments after the operator (before the right operand)
    #[serde(default)]
    pub operator_comments: Vec<String>,
    /// Comments after the right operand
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl BinaryOp {
    pub fn new(left: Expression, right: Expression) -> Self {
        Self {
            left,
            right,
            left_comments: Vec::new(),
            operator_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }
}

/// LIKE/ILIKE operation with optional ESCAPE clause and quantifier (ANY/ALL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LikeOp {
    pub left: Expression,
    pub right: Expression,
    /// ESCAPE character/expression
    #[serde(default)]
    pub escape: Option<Expression>,
    /// Quantifier: ANY, ALL, or SOME
    #[serde(default)]
    pub quantifier: Option<String>,
}

impl LikeOp {
    pub fn new(left: Expression, right: Expression) -> Self {
        Self {
            left,
            right,
            escape: None,
            quantifier: None,
        }
    }

    pub fn with_escape(left: Expression, right: Expression, escape: Expression) -> Self {
        Self {
            left,
            right,
            escape: Some(escape),
            quantifier: None,
        }
    }
}

/// Represent a unary operation (single operand with a prefix operator).
///
/// Shared payload for `Not`, `Neg`, and `BitwiseNot` variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnaryOp {
    /// The operand expression.
    pub this: Expression,
}

impl UnaryOp {
    pub fn new(this: Expression) -> Self {
        Self { this }
    }
}

/// Represent an IN predicate (`x IN (1, 2, 3)` or `x IN (SELECT ...)`).
///
/// Either `expressions` (a value list) or `query` (a subquery) is populated,
/// but not both. When `not` is true, the predicate is `NOT IN`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct In {
    /// The expression being tested.
    pub this: Expression,
    /// The value list (mutually exclusive with `query`).
    pub expressions: Vec<Expression>,
    /// A subquery (mutually exclusive with `expressions`).
    pub query: Option<Expression>,
    /// Whether this is NOT IN.
    pub not: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub global: bool,
    /// BigQuery: IN UNNEST(expr)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unnest: Option<Box<Expression>>,
    /// Whether the right side is a bare field reference (no parentheses).
    /// Matches Python sqlglot's `field` attribute on `In` expression.
    /// e.g., `a IN subquery1` vs `a IN (subquery1)`
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_field: bool,
}

/// Represent a BETWEEN predicate (`x BETWEEN low AND high`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Between {
    /// The expression being tested.
    pub this: Expression,
    /// The lower bound.
    pub low: Expression,
    /// The upper bound.
    pub high: Expression,
    /// Whether this is NOT BETWEEN.
    pub not: bool,
    /// SYMMETRIC/ASYMMETRIC qualifier: None = regular, Some(true) = SYMMETRIC, Some(false) = ASYMMETRIC
    #[serde(default)]
    pub symmetric: Option<bool>,
}

/// IS NULL predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IsNull {
    pub this: Expression,
    pub not: bool,
    /// Whether this was the postfix form (ISNULL/NOTNULL) vs standard (IS NULL/IS NOT NULL)
    #[serde(default)]
    pub postfix_form: bool,
}

/// IS TRUE / IS FALSE predicate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IsTrueFalse {
    pub this: Expression,
    pub not: bool,
}

/// IS JSON predicate (SQL standard)
/// Checks if a value is valid JSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IsJson {
    pub this: Expression,
    /// JSON type: VALUE, SCALAR, OBJECT, or ARRAY (None = just IS JSON)
    pub json_type: Option<String>,
    /// Key uniqueness constraint
    pub unique_keys: Option<JsonUniqueKeys>,
    /// Whether IS NOT JSON
    pub negated: bool,
}

/// JSON unique keys constraint variants
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum JsonUniqueKeys {
    /// WITH UNIQUE KEYS
    With,
    /// WITHOUT UNIQUE KEYS
    Without,
    /// UNIQUE KEYS (shorthand for WITH UNIQUE KEYS)
    Shorthand,
}

/// Represent an EXISTS predicate (`EXISTS (SELECT ...)` or `NOT EXISTS (...)`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Exists {
    /// The subquery expression.
    pub this: Expression,
    /// Whether this is NOT EXISTS.
    pub not: bool,
}

/// Represent a scalar function call (e.g. `UPPER(name)`, `COALESCE(a, b)`).
///
/// This is the generic function node. Well-known aggregates, window functions,
/// and built-in functions each have their own dedicated `Expression` variants
/// (e.g. `Count`, `Sum`, `WindowFunction`). Functions that the parser does
/// not recognize as built-ins are represented with this struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Function {
    /// The function name, as originally written (may be schema-qualified).
    pub name: String,
    /// Positional arguments to the function.
    pub args: Vec<Expression>,
    /// Whether DISTINCT was specified inside the call (e.g. `COUNT(DISTINCT x)`).
    pub distinct: bool,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
    /// Whether this function uses bracket syntax (e.g., MAP[keys, values])
    #[serde(default)]
    pub use_bracket_syntax: bool,
    /// Whether this function was called without parentheses (e.g., CURRENT_TIMESTAMP vs CURRENT_TIMESTAMP())
    #[serde(default)]
    pub no_parens: bool,
    /// Whether the function name was quoted (e.g., `p.d.UdF` in BigQuery)
    #[serde(default)]
    pub quoted: bool,
}

impl Default for Function {
    fn default() -> Self {
        Self {
            name: String::new(),
            args: Vec::new(),
            distinct: false,
            trailing_comments: Vec::new(),
            use_bracket_syntax: false,
            no_parens: false,
            quoted: false,
        }
    }
}

impl Function {
    pub fn new(name: impl Into<String>, args: Vec<Expression>) -> Self {
        Self {
            name: name.into(),
            args,
            distinct: false,
            trailing_comments: Vec::new(),
            use_bracket_syntax: false,
            no_parens: false,
            quoted: false,
        }
    }
}

/// Represent a named aggregate function call with optional FILTER, ORDER BY, and LIMIT.
///
/// This struct is used for aggregate function calls that are not covered by
/// one of the dedicated typed variants (e.g. `Count`, `Sum`). It supports
/// SQL:2003 FILTER (WHERE ...) clauses, ordered-set aggregates, and
/// IGNORE NULLS / RESPECT NULLS modifiers.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AggregateFunction {
    /// The aggregate function name (e.g. "JSON_AGG", "XMLAGG").
    pub name: String,
    /// Positional arguments.
    pub args: Vec<Expression>,
    /// Whether DISTINCT was specified.
    pub distinct: bool,
    /// Optional FILTER (WHERE ...) clause applied to the aggregate.
    pub filter: Option<Expression>,
    /// ORDER BY inside aggregate (e.g., JSON_AGG(x ORDER BY y))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<Ordered>,
    /// LIMIT inside aggregate (e.g., ARRAY_CONCAT_AGG(x LIMIT 2))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Box<Expression>>,
    /// IGNORE NULLS / RESPECT NULLS
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_nulls: Option<bool>,
}

/// Represent a window function call with its OVER clause.
///
/// The inner `this` expression is typically a window-specific expression
/// (e.g. `RowNumber`, `Rank`, `Lead`) or an aggregate used as a window
/// function.  The `over` field carries the PARTITION BY, ORDER BY, and
/// frame specification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WindowFunction {
    /// The function expression (e.g. ROW_NUMBER(), SUM(amount)).
    pub this: Expression,
    /// The OVER clause defining the window partitioning, ordering, and frame.
    pub over: Over,
    /// Oracle KEEP clause: KEEP (DENSE_RANK FIRST|LAST ORDER BY ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep: Option<Keep>,
}

/// Oracle KEEP clause for aggregate functions
/// Syntax: aggregate_function KEEP (DENSE_RANK FIRST|LAST ORDER BY column [ASC|DESC])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Keep {
    /// true = FIRST, false = LAST
    pub first: bool,
    /// ORDER BY clause inside KEEP
    pub order_by: Vec<Ordered>,
}

/// WITHIN GROUP clause (for ordered-set aggregate functions)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithinGroup {
    /// The aggregate function (LISTAGG, PERCENTILE_CONT, etc.)
    pub this: Expression,
    /// The ORDER BY clause within the group
    pub order_by: Vec<Ordered>,
}

/// Represent the FROM clause of a SELECT statement.
///
/// Contains one or more table sources (tables, subqueries, table-valued
/// functions, etc.). Multiple entries represent comma-separated implicit joins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct From {
    /// The table source expressions.
    pub expressions: Vec<Expression>,
}

/// Represent a JOIN clause between two table sources.
///
/// The join condition can be specified via `on` (ON predicate) or `using`
/// (USING column list), but not both. The `kind` field determines the join
/// type (INNER, LEFT, CROSS, etc.).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Join {
    /// The right-hand table expression being joined.
    pub this: Expression,
    /// The ON condition (mutually exclusive with `using`).
    pub on: Option<Expression>,
    /// The USING column list (mutually exclusive with `on`).
    pub using: Vec<Identifier>,
    /// The join type (INNER, LEFT, RIGHT, FULL, CROSS, etc.).
    pub kind: JoinKind,
    /// Whether INNER keyword was explicitly used (INNER JOIN vs JOIN)
    pub use_inner_keyword: bool,
    /// Whether OUTER keyword was explicitly used (LEFT OUTER JOIN vs LEFT JOIN)
    pub use_outer_keyword: bool,
    /// Whether the ON/USING condition was deferred (assigned right-to-left for chained JOINs)
    pub deferred_condition: bool,
    /// TSQL join hint: LOOP, HASH, MERGE (e.g., INNER LOOP JOIN)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_hint: Option<String>,
    /// Snowflake ASOF JOIN match condition (MATCH_CONDITION clause)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_condition: Option<Expression>,
    /// PIVOT/UNPIVOT operations that follow this join (Oracle/TSQL syntax)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pivots: Vec<Expression>,
    /// Comments collected from between join-kind keywords (e.g., INNER /* comment */ JOIN)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
    /// Nesting group identifier for nested join pretty-printing.
    /// Joins in the same group were parsed together; group boundaries come from
    /// deferred condition resolution phases.
    #[serde(default)]
    pub nesting_group: usize,
}

/// Enumerate all supported SQL join types.
///
/// Covers the standard join types (INNER, LEFT, RIGHT, FULL, CROSS, NATURAL)
/// as well as dialect-specific variants: SEMI/ANTI joins, LATERAL joins,
/// CROSS/OUTER APPLY (TSQL), ASOF joins (DuckDB/Snowflake), ARRAY joins
/// (ClickHouse), STRAIGHT_JOIN (MySQL), and implicit comma-joins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Outer,  // Standalone OUTER JOIN (without LEFT/RIGHT/FULL)
    Cross,
    Natural,
    NaturalLeft,
    NaturalRight,
    NaturalFull,
    Semi,
    Anti,
    // Directional SEMI/ANTI joins
    LeftSemi,
    LeftAnti,
    RightSemi,
    RightAnti,
    // SQL Server specific
    CrossApply,
    OuterApply,
    // Time-series specific
    AsOf,
    AsOfLeft,
    AsOfRight,
    // Lateral join
    Lateral,
    LeftLateral,
    // MySQL specific
    Straight,
    // Implicit join (comma-separated tables: FROM a, b)
    Implicit,
    // ClickHouse ARRAY JOIN
    Array,
    LeftArray,
}

impl Default for JoinKind {
    fn default() -> Self {
        JoinKind::Inner
    }
}

/// Parenthesized table expression with joins
/// Represents: (tbl1 CROSS JOIN tbl2) or ((SELECT 1) CROSS JOIN (SELECT 2))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JoinedTable {
    /// The left-hand side table expression
    pub left: Expression,
    /// The joins applied to the left table
    pub joins: Vec<Join>,
    /// LATERAL VIEW clauses (Hive/Spark)
    pub lateral_views: Vec<LateralView>,
    /// Optional alias for the joined table expression
    pub alias: Option<Identifier>,
}

/// Represent a WHERE clause containing a boolean filter predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Where {
    /// The filter predicate expression.
    pub this: Expression,
}

/// Represent a GROUP BY clause with optional ALL/DISTINCT and WITH TOTALS modifiers.
///
/// The `expressions` list may contain plain columns, ordinal positions,
/// ROLLUP/CUBE/GROUPING SETS expressions, or the special empty-set `()`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GroupBy {
    /// The grouping expressions.
    pub expressions: Vec<Expression>,
    /// GROUP BY modifier: Some(true) = ALL, Some(false) = DISTINCT, None = no modifier
    #[serde(default)]
    pub all: Option<bool>,
    /// ClickHouse: WITH TOTALS modifier
    #[serde(default)]
    pub totals: bool,
    /// Leading comments that appeared before the GROUP BY keyword
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// Represent a HAVING clause containing a predicate over aggregate results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Having {
    /// The filter predicate, typically involving aggregate functions.
    pub this: Expression,
    /// Leading comments that appeared before the HAVING keyword
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// Represent an ORDER BY clause containing one or more sort specifications.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OrderBy {
    /// The sort specifications, each with direction and null ordering.
    pub expressions: Vec<Ordered>,
    /// Whether this is ORDER SIBLINGS BY (Oracle hierarchical queries)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub siblings: bool,
    /// Leading comments that appeared before the ORDER BY keyword
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// Represent an expression with sort direction and null ordering.
///
/// Used inside ORDER BY clauses, window frame ORDER BY, and index definitions.
/// When `desc` is false the sort is ascending. The `nulls_first` field
/// controls the NULLS FIRST / NULLS LAST modifier; `None` means unspecified
/// (database default).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Ordered {
    /// The expression to sort by.
    pub this: Expression,
    /// Whether the sort direction is descending (true) or ascending (false).
    pub desc: bool,
    /// `Some(true)` = NULLS FIRST, `Some(false)` = NULLS LAST, `None` = unspecified.
    pub nulls_first: Option<bool>,
    /// Whether ASC was explicitly written (not just implied)
    #[serde(default)]
    pub explicit_asc: bool,
    /// ClickHouse WITH FILL clause
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_fill: Option<Box<WithFill>>,
}

impl Ordered {
    pub fn asc(expr: Expression) -> Self {
        Self {
            this: expr,
            desc: false,
            nulls_first: None,
            explicit_asc: false,
            with_fill: None,
        }
    }

    pub fn desc(expr: Expression) -> Self {
        Self {
            this: expr,
            desc: true,
            nulls_first: None,
            explicit_asc: false,
            with_fill: None,
        }
    }
}

/// DISTRIBUTE BY clause (Hive/Spark)
/// Controls how rows are distributed across reducers
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct DistributeBy {
    pub expressions: Vec<Expression>,
}

/// CLUSTER BY clause (Hive/Spark)
/// Combines DISTRIBUTE BY and SORT BY on the same columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct ClusterBy {
    pub expressions: Vec<Ordered>,
}

/// SORT BY clause (Hive/Spark)
/// Sorts data within each reducer (local sort, not global)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct SortBy {
    pub expressions: Vec<Ordered>,
}

/// LATERAL VIEW clause (Hive/Spark)
/// Used for unnesting arrays/maps with EXPLODE, POSEXPLODE, etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct LateralView {
    /// The table-generating function (EXPLODE, POSEXPLODE, etc.)
    pub this: Expression,
    /// Table alias for the generated table
    pub table_alias: Option<Identifier>,
    /// Column aliases for the generated columns
    pub column_aliases: Vec<Identifier>,
    /// OUTER keyword - preserve nulls when input is empty/null
    pub outer: bool,
}

/// Query hint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct Hint {
    pub expressions: Vec<HintExpression>,
}

/// Individual hint expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum HintExpression {
    /// Function-style hint: USE_HASH(table)
    Function { name: String, args: Vec<Expression> },
    /// Simple identifier hint: PARALLEL
    Identifier(String),
    /// Raw hint text (unparsed)
    Raw(String),
}

/// Pseudocolumn type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum PseudocolumnType {
    Rownum,      // Oracle ROWNUM
    Rowid,       // Oracle ROWID
    Level,       // Oracle LEVEL (for CONNECT BY)
    Sysdate,     // Oracle SYSDATE
    ObjectId,    // Oracle OBJECT_ID
    ObjectValue, // Oracle OBJECT_VALUE
}

impl PseudocolumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            PseudocolumnType::Rownum => "ROWNUM",
            PseudocolumnType::Rowid => "ROWID",
            PseudocolumnType::Level => "LEVEL",
            PseudocolumnType::Sysdate => "SYSDATE",
            PseudocolumnType::ObjectId => "OBJECT_ID",
            PseudocolumnType::ObjectValue => "OBJECT_VALUE",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ROWNUM" => Some(PseudocolumnType::Rownum),
            "ROWID" => Some(PseudocolumnType::Rowid),
            "LEVEL" => Some(PseudocolumnType::Level),
            "SYSDATE" => Some(PseudocolumnType::Sysdate),
            "OBJECT_ID" => Some(PseudocolumnType::ObjectId),
            "OBJECT_VALUE" => Some(PseudocolumnType::ObjectValue),
            _ => None,
        }
    }
}

/// Pseudocolumn expression (Oracle ROWNUM, ROWID, LEVEL, etc.)
/// These are special identifiers that should not be quoted
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct Pseudocolumn {
    pub kind: PseudocolumnType,
}

impl Pseudocolumn {
    pub fn rownum() -> Self {
        Self { kind: PseudocolumnType::Rownum }
    }

    pub fn rowid() -> Self {
        Self { kind: PseudocolumnType::Rowid }
    }

    pub fn level() -> Self {
        Self { kind: PseudocolumnType::Level }
    }
}

/// Oracle CONNECT BY clause for hierarchical queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct Connect {
    /// START WITH condition (optional, can come before or after CONNECT BY)
    pub start: Option<Expression>,
    /// CONNECT BY condition (required, contains PRIOR references)
    pub connect: Expression,
    /// NOCYCLE keyword to prevent infinite loops
    pub nocycle: bool,
}

/// Oracle PRIOR expression - references parent row's value in CONNECT BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct Prior {
    pub this: Expression,
}

/// Oracle CONNECT_BY_ROOT function - returns root row's column value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct ConnectByRoot {
    pub this: Expression,
}

/// MATCH_RECOGNIZE clause for row pattern matching (Oracle/Snowflake/Presto/Trino)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct MatchRecognize {
    /// Source table/expression
    pub this: Option<Box<Expression>>,
    /// PARTITION BY expressions
    pub partition_by: Option<Vec<Expression>>,
    /// ORDER BY expressions
    pub order_by: Option<Vec<Ordered>>,
    /// MEASURES definitions
    pub measures: Option<Vec<MatchRecognizeMeasure>>,
    /// Row semantics (ONE ROW PER MATCH, ALL ROWS PER MATCH, etc.)
    pub rows: Option<MatchRecognizeRows>,
    /// AFTER MATCH SKIP behavior
    pub after: Option<MatchRecognizeAfter>,
    /// PATTERN definition (stored as raw string for complex regex patterns)
    pub pattern: Option<String>,
    /// DEFINE clauses (pattern variable definitions)
    pub define: Option<Vec<(Identifier, Expression)>>,
    /// Optional alias for the result
    pub alias: Option<Identifier>,
    /// Whether AS keyword was explicitly present before alias
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub alias_explicit_as: bool,
}

/// MEASURES expression with optional RUNNING/FINAL semantics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub struct MatchRecognizeMeasure {
    /// The measure expression
    pub this: Expression,
    /// RUNNING or FINAL semantics (Snowflake-specific)
    pub window_frame: Option<MatchRecognizeSemantics>,
}

/// Semantics for MEASURES in MATCH_RECOGNIZE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum MatchRecognizeSemantics {
    Running,
    Final,
}

/// Row output semantics for MATCH_RECOGNIZE
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum MatchRecognizeRows {
    OneRowPerMatch,
    AllRowsPerMatch,
    AllRowsPerMatchShowEmptyMatches,
    AllRowsPerMatchOmitEmptyMatches,
    AllRowsPerMatchWithUnmatchedRows,
}

/// AFTER MATCH SKIP behavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum MatchRecognizeAfter {
    PastLastRow,
    ToNextRow,
    ToFirst(Identifier),
    ToLast(Identifier),
}

/// Represent a LIMIT clause that restricts the number of returned rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Limit {
    /// The limit count expression.
    pub this: Expression,
    /// Whether PERCENT modifier is present (DuckDB: LIMIT 10 PERCENT)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub percent: bool,
    /// Comments from before the LIMIT keyword (emitted after the limit value)
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// OFFSET clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Offset {
    pub this: Expression,
    /// Whether ROW/ROWS keyword was used (SQL standard syntax)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub rows: Option<bool>,
}

/// TOP clause (SQL Server)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Top {
    pub this: Expression,
    pub percent: bool,
    pub with_ties: bool,
    /// Whether the expression was parenthesized: TOP (10) vs TOP 10
    #[serde(default)]
    pub parenthesized: bool,
}

/// FETCH FIRST/NEXT clause (SQL standard)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Fetch {
    /// FIRST or NEXT
    pub direction: String,
    /// Count expression (optional)
    pub count: Option<Expression>,
    /// PERCENT modifier
    pub percent: bool,
    /// ROWS or ROW keyword present
    pub rows: bool,
    /// WITH TIES modifier
    pub with_ties: bool,
}

/// Represent a QUALIFY clause for filtering on window function results.
///
/// Supported by Snowflake, BigQuery, DuckDB, and Databricks. The predicate
/// typically references a window function (e.g.
/// `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Qualify {
    /// The filter predicate over window function results.
    pub this: Expression,
}

/// SAMPLE / TABLESAMPLE clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Sample {
    pub method: SampleMethod,
    pub size: Expression,
    pub seed: Option<Expression>,
    /// ClickHouse OFFSET expression after SAMPLE size
    #[serde(default)]
    pub offset: Option<Expression>,
    /// Whether the unit comes after the size (e.g., "100 ROWS" vs "ROW 100")
    pub unit_after_size: bool,
    /// Whether the keyword was SAMPLE (true) or TABLESAMPLE (false)
    #[serde(default)]
    pub use_sample_keyword: bool,
    /// Whether the method was explicitly specified (BERNOULLI, SYSTEM, etc.)
    #[serde(default)]
    pub explicit_method: bool,
    /// Whether the method keyword appeared before the size (TABLESAMPLE BERNOULLI (10))
    #[serde(default)]
    pub method_before_size: bool,
    /// Whether SEED keyword was used (true) or REPEATABLE (false)
    #[serde(default)]
    pub use_seed_keyword: bool,
    /// BUCKET numerator for Hive bucket sampling (BUCKET 1 OUT OF 5)
    pub bucket_numerator: Option<Box<Expression>>,
    /// BUCKET denominator (the 5 in BUCKET 1 OUT OF 5)
    pub bucket_denominator: Option<Box<Expression>>,
    /// BUCKET field for ON clause (BUCKET 1 OUT OF 5 ON x)
    pub bucket_field: Option<Box<Expression>>,
    /// Whether this is a DuckDB USING SAMPLE clause (vs SAMPLE/TABLESAMPLE)
    #[serde(default)]
    pub is_using_sample: bool,
    /// Whether the unit was explicitly PERCENT (vs ROWS)
    #[serde(default)]
    pub is_percent: bool,
    /// Whether to suppress method output (for cross-dialect transpilation)
    #[serde(default)]
    pub suppress_method_output: bool,
}

/// Sample method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum SampleMethod {
    Bernoulli,
    System,
    Block,
    Row,
    Percent,
    /// Hive bucket sampling
    Bucket,
    /// DuckDB reservoir sampling
    Reservoir,
}

/// Named window definition (WINDOW w AS (...))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NamedWindow {
    pub name: Identifier,
    pub spec: Over,
}

/// Represent a WITH clause containing one or more Common Table Expressions (CTEs).
///
/// When `recursive` is true, the clause is `WITH RECURSIVE`, enabling CTEs
/// that reference themselves. Each CTE is defined in the `ctes` vector and
/// can be referenced by name in subsequent CTEs and in the main query body.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct With {
    /// The list of CTE definitions, in order.
    pub ctes: Vec<Cte>,
    /// Whether the WITH RECURSIVE keyword was used.
    pub recursive: bool,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// SEARCH/CYCLE clause for recursive CTEs (PostgreSQL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<Box<Expression>>,
}

/// Represent a single Common Table Expression definition.
///
/// A CTE has a name (`alias`), an optional column list, and a body query.
/// The `materialized` field maps to PostgreSQL's `MATERIALIZED` /
/// `NOT MATERIALIZED` hints. ClickHouse supports an inverted syntax where
/// the expression comes before the alias (`alias_first`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Cte {
    /// The CTE name.
    pub alias: Identifier,
    /// The CTE body (typically a SELECT, UNION, etc.).
    pub this: Expression,
    /// Optional column alias list: `cte_name(c1, c2) AS (...)`.
    pub columns: Vec<Identifier>,
    /// `Some(true)` = MATERIALIZED, `Some(false)` = NOT MATERIALIZED, `None` = unspecified.
    pub materialized: Option<bool>,
    /// USING KEY (columns) for DuckDB recursive CTEs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_expressions: Vec<Identifier>,
    /// ClickHouse supports expression-first WITH items: WITH <expr> AS <alias>
    #[serde(default)]
    pub alias_first: bool,
    /// Comments associated with this CTE (placed after alias name, before AS)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// Window specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Ordered>,
    pub frame: Option<WindowFrame>,
}

/// OVER clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Over {
    /// Named window reference (e.g., OVER w or OVER (w ORDER BY x))
    pub window_name: Option<Identifier>,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<Ordered>,
    pub frame: Option<WindowFrame>,
    pub alias: Option<Identifier>,
}

/// Window frame
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WindowFrame {
    pub kind: WindowFrameKind,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
    pub exclude: Option<WindowFrameExclude>,
    /// Original text of the frame kind keyword (preserves input case, e.g. "range")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind_text: Option<String>,
    /// Original text of the start bound side keyword (e.g. "preceding")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_side_text: Option<String>,
    /// Original text of the end bound side keyword
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_side_text: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum WindowFrameKind {
    Rows,
    Range,
    Groups,
}

/// EXCLUDE clause for window frames
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum WindowFrameExclude {
    CurrentRow,
    Group,
    Ties,
    NoOthers,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(Box<Expression>),
    Following(Box<Expression>),
    /// Bare PRECEDING without value (inverted syntax: just "PRECEDING")
    BarePreceding,
    /// Bare FOLLOWING without value (inverted syntax: just "FOLLOWING")
    BareFollowing,
    /// Bare numeric bound without PRECEDING/FOLLOWING (e.g., RANGE BETWEEN 1 AND 3)
    Value(Box<Expression>),
}

/// Struct field with optional OPTIONS clause (BigQuery) and COMMENT (Spark/Databricks)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StructField {
    pub name: String,
    pub data_type: DataType,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<Expression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl StructField {
    /// Create a new struct field without options
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type, options: Vec::new(), comment: None }
    }

    /// Create a new struct field with options
    pub fn with_options(name: String, data_type: DataType, options: Vec<Expression>) -> Self {
        Self { name, data_type, options, comment: None }
    }

    /// Create a new struct field with options and comment
    pub fn with_options_and_comment(name: String, data_type: DataType, options: Vec<Expression>, comment: Option<String>) -> Self {
        Self { name, data_type, options, comment }
    }
}

/// Enumerate all SQL data types recognized by the parser.
///
/// Covers standard SQL types (BOOLEAN, INT, VARCHAR, TIMESTAMP, etc.) as well
/// as dialect-specific types (JSONB, VECTOR, OBJECT, etc.). Parametric types
/// like ARRAY, MAP, and STRUCT are represented with nested [`DataType`] fields.
///
/// This enum is used in CAST expressions, column definitions, function return
/// types, and anywhere a data type specification appears in SQL.
///
/// Types that do not match any known variant fall through to `Custom { name }`,
/// preserving the original type name for round-trip fidelity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "data_type", rename_all = "snake_case")]
pub enum DataType {
    // Numeric
    Boolean,
    TinyInt { length: Option<u32> },
    SmallInt { length: Option<u32> },
    /// Int type with optional length. `integer_spelling` indicates whether the original
    /// type was spelled as `INTEGER` (true) vs `INT` (false), used for certain dialects
    /// like Databricks that preserve the original spelling in specific contexts (e.g., ?:: syntax).
    Int { length: Option<u32>, #[serde(default, skip_serializing_if = "std::ops::Not::not")] integer_spelling: bool },
    BigInt { length: Option<u32> },
    /// Float type with optional precision and scale. `real_spelling` indicates whether the original
    /// type was spelled as `REAL` (true) vs `FLOAT` (false), used for dialects like Redshift that
    /// preserve the original spelling.
    Float { precision: Option<u32>, scale: Option<u32>, #[serde(default, skip_serializing_if = "std::ops::Not::not")] real_spelling: bool },
    Double { precision: Option<u32>, scale: Option<u32> },
    Decimal { precision: Option<u32>, scale: Option<u32> },

    // String
    Char { length: Option<u32> },
    /// VarChar type with optional length. `parenthesized_length` indicates whether the length
    /// was wrapped in extra parentheses (Hive: `VARCHAR((50))` inside STRUCT definitions).
    VarChar { length: Option<u32>, #[serde(default, skip_serializing_if = "std::ops::Not::not")] parenthesized_length: bool },
    /// String type with optional max length (BigQuery STRING(n))
    String { length: Option<u32> },
    Text,
    /// TEXT with optional length: TEXT(n) - used by MySQL, SQLite, DuckDB, etc.
    #[serde(alias = "TextWithLength")]
    TextWithLength { length: u32 },

    // Binary
    Binary { length: Option<u32> },
    VarBinary { length: Option<u32> },
    Blob,

    // Bit
    Bit { length: Option<u32> },
    VarBit { length: Option<u32> },

    // Date/Time
    Date,
    Time { precision: Option<u32>, #[serde(default)] timezone: bool },
    Timestamp { precision: Option<u32>, timezone: bool },
    Interval {
        unit: Option<String>,
        /// For range intervals like INTERVAL DAY TO HOUR
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to: Option<String>,
    },

    // JSON
    Json,
    JsonB,

    // UUID
    Uuid,

    // Array
    Array {
        element_type: Box<DataType>,
        /// Optional dimension size for PostgreSQL (e.g., [3] in INT[3])
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dimension: Option<u32>,
    },

    /// List type (Materialize): INT LIST, TEXT LIST LIST
    /// Uses postfix LIST syntax instead of ARRAY<T>
    List {
        element_type: Box<DataType>,
    },

    // Struct/Map
    // nested: true means parenthesized syntax STRUCT(name TYPE, ...) (DuckDB/Presto/ROW)
    // nested: false means angle-bracket syntax STRUCT<name TYPE, ...> (BigQuery)
    Struct { fields: Vec<StructField>, nested: bool },
    Map { key_type: Box<DataType>, value_type: Box<DataType> },

    // Enum type (DuckDB): ENUM('RED', 'GREEN', 'BLUE')
    Enum { values: Vec<String>, #[serde(default, skip_serializing_if = "Vec::is_empty")] assignments: Vec<Option<String>> },

    // Set type (MySQL): SET('a', 'b', 'c')
    Set { values: Vec<String> },

    // Union type (DuckDB): UNION(num INT, str TEXT)
    Union { fields: Vec<(String, DataType)> },

    // Vector (Snowflake / SingleStore)
    Vector { #[serde(default)] element_type: Option<Box<DataType>>, dimension: Option<u32> },

    // Object (Snowflake structured type)
    // fields: Vec of (field_name, field_type, not_null)
    Object { fields: Vec<(String, DataType, bool)>, modifier: Option<String> },

    // Nullable wrapper (ClickHouse): Nullable(String), Nullable(Int32)
    Nullable { inner: Box<DataType> },

    // Custom/User-defined
    Custom { name: String },

    // Spatial types
    Geometry {
        subtype: Option<String>,
        srid: Option<u32>,
    },
    Geography {
        subtype: Option<String>,
        srid: Option<u32>,
    },

    // Character Set (for CONVERT USING in MySQL)
    // Renders as CHAR CHARACTER SET {name} in cast target
    CharacterSet { name: String },

    // Unknown
    Unknown,
}

/// Array expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(rename = "SqlArray"))]
pub struct Array {
    pub expressions: Vec<Expression>,
}

/// Struct expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Struct {
    pub fields: Vec<(Option<String>, Expression)>,
}

/// Tuple expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Tuple {
    pub expressions: Vec<Expression>,
}

/// Interval expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Interval {
    /// The value expression (e.g., '1', 5, column_ref)
    pub this: Option<Expression>,
    /// The unit specification (optional - can be None, a simple unit, a span, or an expression)
    pub unit: Option<IntervalUnitSpec>,
}

/// Specification for interval unit - can be a simple unit, a span (HOUR TO SECOND), or an expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IntervalUnitSpec {
    /// Simple interval unit (YEAR, MONTH, DAY, etc.)
    Simple {
        unit: IntervalUnit,
        /// Whether to use plural form (e.g., DAYS vs DAY)
        use_plural: bool,
    },
    /// Interval span (e.g., HOUR TO SECOND)
    Span(IntervalSpan),
    /// Expression-based interval span for Oracle (e.g., DAY(9) TO SECOND(3))
    /// The start and end can be expressions like function calls with precision
    ExprSpan(IntervalSpanExpr),
    /// Expression as unit (e.g., CURRENT_DATE, CAST(GETDATE() AS DATE))
    Expr(Box<Expression>),
}

/// Interval span for ranges like HOUR TO SECOND
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IntervalSpan {
    /// Start unit (e.g., HOUR)
    pub this: IntervalUnit,
    /// End unit (e.g., SECOND)
    pub expression: IntervalUnit,
}

/// Expression-based interval span for Oracle (e.g., DAY(9) TO SECOND(3))
/// Unlike IntervalSpan, this uses expressions to represent units with optional precision
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IntervalSpanExpr {
    /// Start unit expression (e.g., Var("DAY") or Anonymous("DAY", [9]))
    pub this: Box<Expression>,
    /// End unit expression (e.g., Var("SECOND") or Anonymous("SECOND", [3]))
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum IntervalUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

/// SQL Command (COMMIT, ROLLBACK, BEGIN, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Command {
    /// The command text (e.g., "ROLLBACK", "COMMIT", "BEGIN")
    pub this: String,
}

/// EXEC/EXECUTE statement (TSQL stored procedure call)
/// Syntax: EXEC [schema.]procedure_name [@param=value, ...]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExecuteStatement {
    /// The procedure name (can be qualified: schema.proc_name)
    pub this: Expression,
    /// Named parameters: @param=value pairs
    #[serde(default)]
    pub parameters: Vec<ExecuteParameter>,
}

/// Named parameter in EXEC statement: @name=value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExecuteParameter {
    /// Parameter name (including @)
    pub name: String,
    /// Parameter value
    pub value: Expression,
}

/// KILL statement (MySQL/MariaDB)
/// KILL [CONNECTION | QUERY] <id>
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Kill {
    /// The target (process ID or connection ID)
    pub this: Expression,
    /// Optional kind: "CONNECTION" or "QUERY"
    pub kind: Option<String>,
}

/// Raw/unparsed SQL
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Raw {
    pub sql: String,
}

// ============================================================================
// Function expression types
// ============================================================================

/// Generic unary function (takes a single argument)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnaryFunc {
    pub this: Expression,
    /// Original function name for round-trip preservation (e.g., CHAR_LENGTH vs LENGTH)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

impl UnaryFunc {
    /// Create a new UnaryFunc with no original_name
    pub fn new(this: Expression) -> Self {
        Self { this, original_name: None }
    }

    /// Create a new UnaryFunc with an original name for round-trip preservation
    pub fn with_name(this: Expression, name: String) -> Self {
        Self { this, original_name: Some(name) }
    }
}

/// CHAR/CHR function with multiple args and optional USING charset
/// e.g., CHAR(77, 77.3, '77.3' USING utf8mb4)
/// e.g., CHR(187 USING NCHAR_CS) -- Oracle
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CharFunc {
    pub args: Vec<Expression>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub charset: Option<String>,
    /// Original function name (CHAR or CHR), defaults to CHAR
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
}

/// Generic binary function (takes two arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BinaryFunc {
    pub this: Expression,
    pub expression: Expression,
    /// Original function name for round-trip preservation (e.g., NVL vs IFNULL)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// Variable argument function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct VarArgFunc {
    pub expressions: Vec<Expression>,
    /// Original function name for round-trip preservation (e.g., COALESCE vs IFNULL)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// CONCAT_WS function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConcatWs {
    pub separator: Expression,
    pub expressions: Vec<Expression>,
}

/// SUBSTRING function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SubstringFunc {
    pub this: Expression,
    pub start: Expression,
    pub length: Option<Expression>,
    /// Whether SQL standard FROM/FOR syntax was used (true) vs comma-separated (false)
    #[serde(default)]
    pub from_for_syntax: bool,
}

/// OVERLAY function - OVERLAY(string PLACING replacement FROM position [FOR length])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OverlayFunc {
    pub this: Expression,
    pub replacement: Expression,
    pub from: Expression,
    pub length: Option<Expression>,
}

/// TRIM function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TrimFunc {
    pub this: Expression,
    pub characters: Option<Expression>,
    pub position: TrimPosition,
    /// Whether SQL standard syntax was used (TRIM(BOTH chars FROM str)) vs function syntax (TRIM(str))
    #[serde(default)]
    pub sql_standard_syntax: bool,
    /// Whether the position was explicitly specified (BOTH/LEADING/TRAILING) vs defaulted
    #[serde(default)]
    pub position_explicit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TrimPosition {
    Both,
    Leading,
    Trailing,
}

/// REPLACE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ReplaceFunc {
    pub this: Expression,
    pub old: Expression,
    pub new: Expression,
}

/// LEFT/RIGHT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LeftRightFunc {
    pub this: Expression,
    pub length: Expression,
}

/// REPEAT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RepeatFunc {
    pub this: Expression,
    pub times: Expression,
}

/// LPAD/RPAD function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PadFunc {
    pub this: Expression,
    pub length: Expression,
    pub fill: Option<Expression>,
}

/// SPLIT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SplitFunc {
    pub this: Expression,
    pub delimiter: Expression,
}

/// REGEXP_LIKE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub flags: Option<Expression>,
}

/// REGEXP_REPLACE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpReplaceFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub replacement: Expression,
    pub flags: Option<Expression>,
}

/// REGEXP_EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpExtractFunc {
    pub this: Expression,
    pub pattern: Expression,
    pub group: Option<Expression>,
}

/// ROUND function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RoundFunc {
    pub this: Expression,
    pub decimals: Option<Expression>,
}

/// FLOOR function with optional scale and time unit (Druid: FLOOR(time TO unit))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FloorFunc {
    pub this: Expression,
    pub scale: Option<Expression>,
    /// Time unit for Druid-style FLOOR(time TO unit) syntax
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to: Option<Expression>,
}

/// CEIL function with optional decimals and time unit (Druid: CEIL(time TO unit))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CeilFunc {
    pub this: Expression,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub decimals: Option<Expression>,
    /// Time unit for Druid-style CEIL(time TO unit) syntax
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub to: Option<Expression>,
}

/// LOG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LogFunc {
    pub this: Expression,
    pub base: Option<Expression>,
}

/// CURRENT_DATE (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentDate;

/// CURRENT_TIME
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentTime {
    pub precision: Option<u32>,
}

/// CURRENT_TIMESTAMP
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentTimestamp {
    pub precision: Option<u32>,
    /// If true, generate SYSDATE instead of CURRENT_TIMESTAMP (Oracle-specific)
    #[serde(default)]
    pub sysdate: bool,
}

/// CURRENT_TIMESTAMP_LTZ - Snowflake local timezone timestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentTimestampLTZ {
    pub precision: Option<u32>,
}

/// AT TIME ZONE expression for timezone conversion
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AtTimeZone {
    /// The expression to convert
    pub this: Expression,
    /// The target timezone
    pub zone: Expression,
}

/// DATE_ADD / DATE_SUB function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateAddFunc {
    pub this: Expression,
    pub interval: Expression,
    pub unit: IntervalUnit,
}

/// DATEDIFF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateDiffFunc {
    pub this: Expression,
    pub expression: Expression,
    pub unit: Option<IntervalUnit>,
}

/// DATE_TRUNC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateTruncFunc {
    pub this: Expression,
    pub unit: DateTimeField,
}

/// EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExtractFunc {
    pub this: Expression,
    pub field: DateTimeField,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    DayOfWeek,
    DayOfYear,
    Week,
    /// Week with a modifier like WEEK(monday), WEEK(sunday)
    WeekWithModifier(String),
    Quarter,
    Epoch,
    Timezone,
    TimezoneHour,
    TimezoneMinute,
    Date,
    Time,
    /// Custom datetime field for dialect-specific or arbitrary fields
    Custom(String),
}

/// TO_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToDateFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// TO_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToTimestampFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// IF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IfFunc {
    pub condition: Expression,
    pub true_value: Expression,
    pub false_value: Option<Expression>,
    /// Original function name (IF, IFF, IIF) for round-trip preservation
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub original_name: Option<String>,
}

/// NVL2 function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Nvl2Func {
    pub this: Expression,
    pub true_value: Expression,
    pub false_value: Expression,
}

// ============================================================================
// Typed Aggregate Function types
// ============================================================================

/// Generic aggregate function base type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AggFunc {
    pub this: Expression,
    pub distinct: bool,
    pub filter: Option<Expression>,
    pub order_by: Vec<Ordered>,
    /// Original function name (case-preserving) when parsed from SQL
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
    /// IGNORE NULLS (true) or RESPECT NULLS (false), None if not specified
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub ignore_nulls: Option<bool>,
    /// HAVING MAX/MIN expr inside aggregate (BigQuery syntax)
    /// e.g., ANY_VALUE(fruit HAVING MAX sold) - (expression, is_max: true for MAX, false for MIN)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub having_max: Option<(Box<Expression>, bool)>,
    /// LIMIT inside aggregate (e.g., ARRAY_AGG(x ORDER BY y LIMIT 2))
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<Box<Expression>>,
}

/// COUNT function with optional star
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CountFunc {
    pub this: Option<Expression>,
    pub star: bool,
    pub distinct: bool,
    pub filter: Option<Expression>,
    /// IGNORE NULLS (true) or RESPECT NULLS (false)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_nulls: Option<bool>,
    /// Original function name for case preservation (e.g., "count" or "COUNT")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_name: Option<String>,
}

/// GROUP_CONCAT function (MySQL style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GroupConcatFunc {
    pub this: Expression,
    pub separator: Option<Expression>,
    pub order_by: Option<Vec<Ordered>>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// STRING_AGG function (PostgreSQL/Standard SQL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StringAggFunc {
    pub this: Expression,
    #[serde(default)]
    pub separator: Option<Expression>,
    #[serde(default)]
    pub order_by: Option<Vec<Ordered>>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub filter: Option<Expression>,
    /// BigQuery LIMIT inside STRING_AGG
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<Box<Expression>>,
}

/// LISTAGG function (Oracle style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ListAggFunc {
    pub this: Expression,
    pub separator: Option<Expression>,
    pub on_overflow: Option<ListAggOverflow>,
    pub order_by: Option<Vec<Ordered>>,
    pub distinct: bool,
    pub filter: Option<Expression>,
}

/// LISTAGG ON OVERFLOW behavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ListAggOverflow {
    Error,
    Truncate {
        filler: Option<Expression>,
        with_count: bool,
    },
}

/// SUM_IF / COUNT_IF function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SumIfFunc {
    pub this: Expression,
    pub condition: Expression,
    pub filter: Option<Expression>,
}

/// APPROX_PERCENTILE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxPercentileFunc {
    pub this: Expression,
    pub percentile: Expression,
    pub accuracy: Option<Expression>,
    pub filter: Option<Expression>,
}

/// PERCENTILE_CONT / PERCENTILE_DISC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PercentileFunc {
    pub this: Expression,
    pub percentile: Expression,
    pub order_by: Option<Vec<Ordered>>,
    pub filter: Option<Expression>,
}

// ============================================================================
// Typed Window Function types
// ============================================================================

/// ROW_NUMBER function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowNumber;

/// RANK function (DuckDB allows ORDER BY inside, Oracle allows hypothetical args with WITHIN GROUP)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Rank {
    /// DuckDB: RANK(ORDER BY col) - order by inside function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<Ordered>>,
    /// Oracle hypothetical rank: RANK(val1, val2, ...) WITHIN GROUP (ORDER BY ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expression>,
}

/// DENSE_RANK function (Oracle allows hypothetical args with WITHIN GROUP)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DenseRank {
    /// Oracle hypothetical rank: DENSE_RANK(val1, val2, ...) WITHIN GROUP (ORDER BY ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expression>,
}

/// NTILE function (DuckDB allows ORDER BY inside)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NTileFunc {
    /// num_buckets is optional to support Databricks NTILE() without arguments
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_buckets: Option<Expression>,
    /// DuckDB: NTILE(n ORDER BY col) - order by inside function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<Ordered>>,
}

/// LEAD / LAG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LeadLagFunc {
    pub this: Expression,
    pub offset: Option<Expression>,
    pub default: Option<Expression>,
    pub ignore_nulls: bool,
}

/// FIRST_VALUE / LAST_VALUE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ValueFunc {
    pub this: Expression,
    /// None = not specified, Some(true) = IGNORE NULLS, Some(false) = RESPECT NULLS
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_nulls: Option<bool>,
}

/// NTH_VALUE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NthValueFunc {
    pub this: Expression,
    pub offset: Expression,
    /// None = not specified, Some(true) = IGNORE NULLS, Some(false) = RESPECT NULLS
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore_nulls: Option<bool>,
}

/// PERCENT_RANK function (DuckDB allows ORDER BY inside, Oracle allows hypothetical args with WITHIN GROUP)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PercentRank {
    /// DuckDB: PERCENT_RANK(ORDER BY col) - order by inside function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<Ordered>>,
    /// Oracle hypothetical rank: PERCENT_RANK(val1, val2, ...) WITHIN GROUP (ORDER BY ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expression>,
}

/// CUME_DIST function (DuckDB allows ORDER BY inside, Oracle allows hypothetical args with WITHIN GROUP)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CumeDist {
    /// DuckDB: CUME_DIST(ORDER BY col) - order by inside function
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_by: Option<Vec<Ordered>>,
    /// Oracle hypothetical rank: CUME_DIST(val1, val2, ...) WITHIN GROUP (ORDER BY ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expression>,
}

// ============================================================================
// Additional String Function types
// ============================================================================

/// POSITION/INSTR function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PositionFunc {
    pub substring: Expression,
    pub string: Expression,
    pub start: Option<Expression>,
}

// ============================================================================
// Additional Math Function types
// ============================================================================

/// RANDOM function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Random;

/// RAND function (optional seed, or Teradata RANDOM(lower, upper))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Rand {
    pub seed: Option<Box<Expression>>,
    /// Teradata RANDOM lower bound
    #[serde(default)]
    pub lower: Option<Box<Expression>>,
    /// Teradata RANDOM upper bound
    #[serde(default)]
    pub upper: Option<Box<Expression>>,
}

/// TRUNCATE / TRUNC function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TruncateFunc {
    pub this: Expression,
    pub decimals: Option<Expression>,
}

/// PI function (no arguments)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Pi;

// ============================================================================
// Control Flow Function types
// ============================================================================

/// DECODE function (Oracle style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DecodeFunc {
    pub this: Expression,
    pub search_results: Vec<(Expression, Expression)>,
    pub default: Option<Expression>,
}

// ============================================================================
// Additional Date/Time Function types
// ============================================================================

/// DATE_FORMAT / FORMAT_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateFormatFunc {
    pub this: Expression,
    pub format: Expression,
}

/// FROM_UNIXTIME function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FromUnixtimeFunc {
    pub this: Expression,
    pub format: Option<Expression>,
}

/// UNIX_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnixTimestampFunc {
    pub this: Option<Expression>,
    pub format: Option<Expression>,
}

/// MAKE_DATE function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MakeDateFunc {
    pub year: Expression,
    pub month: Expression,
    pub day: Expression,
}

/// MAKE_TIMESTAMP function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MakeTimestampFunc {
    pub year: Expression,
    pub month: Expression,
    pub day: Expression,
    pub hour: Expression,
    pub minute: Expression,
    pub second: Expression,
    pub timezone: Option<Expression>,
}

/// LAST_DAY function with optional date part (for BigQuery granularity like WEEK(SUNDAY))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LastDayFunc {
    pub this: Expression,
    /// Optional date part for granularity (e.g., MONTH, YEAR, WEEK(SUNDAY))
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unit: Option<DateTimeField>,
}

// ============================================================================
// Array Function types
// ============================================================================

/// ARRAY constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayConstructor {
    pub expressions: Vec<Expression>,
    pub bracket_notation: bool,
    /// True if LIST keyword was used instead of ARRAY (DuckDB)
    pub use_list_keyword: bool,
}

/// ARRAY_SORT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArraySortFunc {
    pub this: Expression,
    pub comparator: Option<Expression>,
    pub desc: bool,
    pub nulls_first: Option<bool>,
}

/// ARRAY_JOIN / ARRAY_TO_STRING function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayJoinFunc {
    pub this: Expression,
    pub separator: Expression,
    pub null_replacement: Option<Expression>,
}

/// UNNEST function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnnestFunc {
    pub this: Expression,
    /// Additional arguments for multi-argument UNNEST (e.g., UNNEST(arr1, arr2))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expressions: Vec<Expression>,
    pub with_ordinality: bool,
    pub alias: Option<Identifier>,
    /// BigQuery: offset alias for WITH OFFSET AS <name>
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset_alias: Option<Identifier>,
}

/// ARRAY_FILTER function (with lambda)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayFilterFunc {
    pub this: Expression,
    pub filter: Expression,
}

/// ARRAY_TRANSFORM / TRANSFORM function (with lambda)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayTransformFunc {
    pub this: Expression,
    pub transform: Expression,
}

/// SEQUENCE / GENERATE_SERIES function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SequenceFunc {
    pub start: Expression,
    pub stop: Expression,
    pub step: Option<Expression>,
}

// ============================================================================
// Struct Function types
// ============================================================================

/// STRUCT constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StructConstructor {
    pub fields: Vec<(Option<Identifier>, Expression)>,
}

/// STRUCT_EXTRACT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StructExtractFunc {
    pub this: Expression,
    pub field: Identifier,
}

/// NAMED_STRUCT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NamedStructFunc {
    pub pairs: Vec<(Expression, Expression)>,
}

// ============================================================================
// Map Function types
// ============================================================================

/// MAP constructor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MapConstructor {
    pub keys: Vec<Expression>,
    pub values: Vec<Expression>,
    /// Whether curly brace syntax was used (`{'a': 1}`) vs MAP function (`MAP(...)`)
    #[serde(default)]
    pub curly_brace_syntax: bool,
    /// Whether MAP keyword was present (`MAP {'a': 1}`) vs bare curly braces (`{'a': 1}`)
    #[serde(default)]
    pub with_map_keyword: bool,
}

/// TRANSFORM_KEYS / TRANSFORM_VALUES function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TransformFunc {
    pub this: Expression,
    pub transform: Expression,
}

// ============================================================================
// JSON Function types
// ============================================================================

/// JSON_EXTRACT / JSON_EXTRACT_SCALAR function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonExtractFunc {
    pub this: Expression,
    pub path: Expression,
    pub returning: Option<DataType>,
    /// True if parsed from -> or ->> operator syntax
    #[serde(default)]
    pub arrow_syntax: bool,
    /// True if parsed from #>> operator syntax (PostgreSQL JSONB path text extraction)
    #[serde(default)]
    pub hash_arrow_syntax: bool,
    /// Wrapper option: WITH/WITHOUT [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER
    #[serde(default)]
    pub wrapper_option: Option<String>,
    /// Quotes handling: KEEP QUOTES or OMIT QUOTES
    #[serde(default)]
    pub quotes_option: Option<String>,
    /// ON SCALAR STRING flag
    #[serde(default)]
    pub on_scalar_string: bool,
    /// Error handling: NULL ON ERROR, ERROR ON ERROR, etc.
    #[serde(default)]
    pub on_error: Option<String>,
}

/// JSON path extraction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonPathFunc {
    pub this: Expression,
    pub paths: Vec<Expression>,
}

/// JSON_OBJECT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonObjectFunc {
    pub pairs: Vec<(Expression, Expression)>,
    pub null_handling: Option<JsonNullHandling>,
    #[serde(default)]
    pub with_unique_keys: bool,
    #[serde(default)]
    pub returning_type: Option<DataType>,
    #[serde(default)]
    pub format_json: bool,
    #[serde(default)]
    pub encoding: Option<String>,
    /// For JSON_OBJECT(*) syntax
    #[serde(default)]
    pub star: bool,
}

/// JSON null handling options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum JsonNullHandling {
    NullOnNull,
    AbsentOnNull,
}

/// JSON_SET / JSON_INSERT function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonModifyFunc {
    pub this: Expression,
    pub path_values: Vec<(Expression, Expression)>,
}

/// JSON_ARRAYAGG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonArrayAggFunc {
    pub this: Expression,
    pub order_by: Option<Vec<Ordered>>,
    pub null_handling: Option<JsonNullHandling>,
    pub filter: Option<Expression>,
}

/// JSON_OBJECTAGG function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JsonObjectAggFunc {
    pub key: Expression,
    pub value: Expression,
    pub null_handling: Option<JsonNullHandling>,
    pub filter: Option<Expression>,
}

// ============================================================================
// Type Casting Function types
// ============================================================================

/// CONVERT function (SQL Server style)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConvertFunc {
    pub this: Expression,
    pub to: DataType,
    pub style: Option<Expression>,
}

// ============================================================================
// Additional Expression types
// ============================================================================

/// Lambda expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LambdaExpr {
    pub parameters: Vec<Identifier>,
    pub body: Expression,
    /// True if using DuckDB's LAMBDA x : expr syntax (vs x -> expr)
    #[serde(default)]
    pub colon: bool,
    /// Optional type annotations for parameters (Snowflake: a int -> a + 1)
    /// Maps parameter index to data type
    #[serde(default)]
    pub parameter_types: Vec<Option<DataType>>,
}

/// Parameter (parameterized queries)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Parameter {
    pub name: Option<String>,
    pub index: Option<u32>,
    pub style: ParameterStyle,
    /// Whether the name was quoted (e.g., @"x" vs @x)
    #[serde(default)]
    pub quoted: bool,
    /// Whether the name was string-quoted with single quotes (e.g., @'foo')
    #[serde(default)]
    pub string_quoted: bool,
    /// Optional secondary expression for ${kind:name} syntax (Hive hiveconf variables)
    #[serde(default)]
    pub expression: Option<String>,
}

/// Parameter placeholder styles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ParameterStyle {
    Question,        // ?
    Dollar,          // $1, $2
    DollarBrace,     // ${name} (Databricks, Hive template variables)
    Brace,           // {name} (Spark/Databricks widget/template variables)
    Colon,           // :name
    At,              // @name
    DoubleAt,        // @@name (system variables in MySQL/SQL Server)
    DoubleDollar,    // $$name
    Percent,         // %s, %(name)s (PostgreSQL psycopg2 style)
}

/// Placeholder expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Placeholder {
    pub index: Option<u32>,
}

/// Named argument in function call: name => value or name := value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NamedArgument {
    pub name: Identifier,
    pub value: Expression,
    /// The separator used: `=>`, `:=`, or `=`
    pub separator: NamedArgSeparator,
}

/// Separator style for named arguments
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum NamedArgSeparator {
    /// `=>` (standard SQL, Snowflake, BigQuery)
    DArrow,
    /// `:=` (Oracle, MySQL)
    ColonEq,
    /// `=` (simple equals, some dialects)
    Eq,
}

/// TABLE ref or MODEL ref used as a function argument (BigQuery)
/// e.g., GAP_FILL(TABLE device_data, ...) or ML.PREDICT(MODEL mydataset.mymodel, ...)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TableArgument {
    /// The keyword prefix: "TABLE" or "MODEL"
    pub prefix: String,
    /// The table/model reference expression
    pub this: Expression,
}

/// SQL Comment preservation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SqlComment {
    pub text: String,
    pub is_block: bool,
}

// ============================================================================
// Additional Predicate types
// ============================================================================

/// SIMILAR TO expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SimilarToExpr {
    pub this: Expression,
    pub pattern: Expression,
    pub escape: Option<Expression>,
    pub not: bool,
}

/// ANY / ALL quantified expression
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct QuantifiedExpr {
    pub this: Expression,
    pub subquery: Expression,
    pub op: Option<QuantifiedOp>,
}

/// Comparison operator for quantified expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum QuantifiedOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

/// OVERLAPS expression
/// Supports two forms:
/// 1. Simple binary: a OVERLAPS b (this, expression are set)
/// 2. Full ANSI: (a, b) OVERLAPS (c, d) (left_start, left_end, right_start, right_end are set)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OverlapsExpr {
    /// Left operand for simple binary form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub this: Option<Expression>,
    /// Right operand for simple binary form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<Expression>,
    /// Left range start for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_start: Option<Expression>,
    /// Left range end for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left_end: Option<Expression>,
    /// Right range start for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_start: Option<Expression>,
    /// Right range end for full ANSI form
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right_end: Option<Expression>,
}

// ============================================================================
// Array/Struct/Map access
// ============================================================================

/// Subscript access (array[index] or map[key])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Subscript {
    pub this: Expression,
    pub index: Expression,
}

/// Dot access (struct.field)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DotAccess {
    pub this: Expression,
    pub field: Identifier,
}

/// Method call (expr.method(args))
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MethodCall {
    pub this: Expression,
    pub method: Identifier,
    pub args: Vec<Expression>,
}

/// Array slice (array[start:end])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArraySlice {
    pub this: Expression,
    pub start: Option<Expression>,
    pub end: Option<Expression>,
}

// ============================================================================
// DDL (Data Definition Language) Statements
// ============================================================================

/// ON COMMIT behavior for temporary tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum OnCommit {
    /// ON COMMIT PRESERVE ROWS
    PreserveRows,
    /// ON COMMIT DELETE ROWS
    DeleteRows,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateTable {
    pub name: TableRef,
    /// ClickHouse: ON CLUSTER clause for distributed DDL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cluster: Option<OnCluster>,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub or_replace: bool,
    /// Table modifier: DYNAMIC, ICEBERG, EXTERNAL, HYBRID (Snowflake)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_modifier: Option<String>,
    pub as_select: Option<Expression>,
    /// Whether the AS SELECT was wrapped in parentheses
    #[serde(default)]
    pub as_select_parenthesized: bool,
    /// ON COMMIT behavior for temporary tables
    #[serde(default)]
    pub on_commit: Option<OnCommit>,
    /// Clone source table (e.g., CREATE TABLE t CLONE source_table)
    #[serde(default)]
    pub clone_source: Option<TableRef>,
    /// Time travel AT/BEFORE clause for CLONE (e.g., AT(TIMESTAMP => '...'))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clone_at_clause: Option<Expression>,
    /// Whether this is a COPY operation (BigQuery) vs CLONE (Snowflake/Databricks)
    #[serde(default)]
    pub is_copy: bool,
    /// Whether this is a SHALLOW CLONE (Databricks/Delta Lake)
    #[serde(default)]
    pub shallow_clone: bool,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
    /// WITH properties (e.g., WITH (FORMAT='parquet'))
    #[serde(default)]
    pub with_properties: Vec<(String, String)>,
    /// Teradata: table options after name before columns (comma-separated)
    #[serde(default)]
    pub teradata_post_name_options: Vec<String>,
    /// Teradata: WITH DATA (true) or WITH NO DATA (false) after AS SELECT
    #[serde(default)]
    pub with_data: Option<bool>,
    /// Teradata: AND STATISTICS (true) or AND NO STATISTICS (false)
    #[serde(default)]
    pub with_statistics: Option<bool>,
    /// Teradata: Index specifications (NO PRIMARY INDEX, UNIQUE PRIMARY INDEX, etc.)
    #[serde(default)]
    pub teradata_indexes: Vec<TeradataIndex>,
    /// WITH clause (CTEs) - for CREATE TABLE ... AS WITH ... SELECT ...
    #[serde(default)]
    pub with_cte: Option<With>,
    /// Table properties like DEFAULT COLLATE (BigQuery)
    #[serde(default)]
    pub properties: Vec<Expression>,
    /// PostgreSQL PARTITION OF property (e.g., CREATE TABLE t PARTITION OF parent ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_of: Option<Expression>,
    /// TSQL: WITH(SYSTEM_VERSIONING=ON(...)) after column definitions
    #[serde(default)]
    pub post_table_properties: Vec<Expression>,
    /// MySQL table options after column definitions (ENGINE=val, AUTO_INCREMENT=val, etc.)
    #[serde(default)]
    pub mysql_table_options: Vec<(String, String)>,
    /// PostgreSQL INHERITS clause: INHERITS (parent1, parent2, ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inherits: Vec<TableRef>,
    /// TSQL ON filegroup or ON filegroup (partition_column) clause
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_property: Option<OnProperty>,
    /// Snowflake: COPY GRANTS clause to copy privileges from replaced table
    #[serde(default)]
    pub copy_grants: bool,
    /// Snowflake: USING TEMPLATE expression for schema inference
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub using_template: Option<Box<Expression>>,
    /// StarRocks: ROLLUP (r1(col1, col2), r2(col1))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollup: Option<RollupProperty>,
}

/// Teradata index specification for CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TeradataIndex {
    /// Index kind: NoPrimary, Primary, PrimaryAmp, Unique, UniquePrimary
    pub kind: TeradataIndexKind,
    /// Optional index name
    pub name: Option<String>,
    /// Optional column list
    pub columns: Vec<String>,
}

/// Kind of Teradata index
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TeradataIndexKind {
    /// NO PRIMARY INDEX
    NoPrimary,
    /// PRIMARY INDEX
    Primary,
    /// PRIMARY AMP INDEX
    PrimaryAmp,
    /// UNIQUE INDEX
    Unique,
    /// UNIQUE PRIMARY INDEX
    UniquePrimary,
    /// INDEX (secondary, non-primary)
    Secondary,
}

impl CreateTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            on_cluster: None,
            columns: Vec::new(),
            constraints: Vec::new(),
            if_not_exists: false,
            temporary: false,
            or_replace: false,
            table_modifier: None,
            as_select: None,
            as_select_parenthesized: false,
            on_commit: None,
            clone_source: None,
            clone_at_clause: None,
            shallow_clone: false, is_copy: false,
            leading_comments: Vec::new(),
            with_properties: Vec::new(),
            teradata_post_name_options: Vec::new(),
            with_data: None,
            with_statistics: None,
            teradata_indexes: Vec::new(),
            with_cte: None,
            properties: Vec::new(),
            partition_of: None,
            post_table_properties: Vec::new(),
            mysql_table_options: Vec::new(),
            inherits: Vec::new(),
            on_property: None,
            copy_grants: false,
            using_template: None,
            rollup: None,
        }
    }
}

/// Sort order for PRIMARY KEY ASC/DESC
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Type of column constraint for tracking order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ConstraintType {
    NotNull,
    Null,
    PrimaryKey,
    Unique,
    Default,
    AutoIncrement,
    Collate,
    Comment,
    References,
    Check,
    GeneratedAsIdentity,
    /// Snowflake: TAG (key='value', ...)
    Tags,
    /// Computed/generated column
    ComputedColumn,
    /// TSQL temporal: GENERATED ALWAYS AS ROW START|END
    GeneratedAsRow,
    /// MySQL: ON UPDATE expression
    OnUpdate,
    /// PATH constraint for XMLTABLE/JSON_TABLE columns
    Path,
    /// Redshift: ENCODE encoding_type
    Encode,
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ColumnDef {
    pub name: Identifier,
    pub data_type: DataType,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub primary_key: bool,
    /// Sort order for PRIMARY KEY (ASC/DESC)
    #[serde(default)]
    pub primary_key_order: Option<SortOrder>,
    pub unique: bool,
    /// PostgreSQL 15+: UNIQUE NULLS NOT DISTINCT
    #[serde(default)]
    pub unique_nulls_not_distinct: bool,
    pub auto_increment: bool,
    pub comment: Option<String>,
    pub constraints: Vec<ColumnConstraint>,
    /// Track original order of constraints for accurate regeneration
    #[serde(default)]
    pub constraint_order: Vec<ConstraintType>,
    /// Teradata: FORMAT 'pattern'
    #[serde(default)]
    pub format: Option<String>,
    /// Teradata: TITLE 'title'
    #[serde(default)]
    pub title: Option<String>,
    /// Teradata: INLINE LENGTH n
    #[serde(default)]
    pub inline_length: Option<u64>,
    /// Teradata: COMPRESS or COMPRESS (values) or COMPRESS 'value'
    #[serde(default)]
    pub compress: Option<Vec<Expression>>,
    /// Teradata: CHARACTER SET name
    #[serde(default)]
    pub character_set: Option<String>,
    /// Teradata: UPPERCASE
    #[serde(default)]
    pub uppercase: bool,
    /// Teradata: CASESPECIFIC / NOT CASESPECIFIC (None = not specified, Some(true) = CASESPECIFIC, Some(false) = NOT CASESPECIFIC)
    #[serde(default)]
    pub casespecific: Option<bool>,
    /// Snowflake: AUTOINCREMENT START value
    #[serde(default)]
    pub auto_increment_start: Option<Box<Expression>>,
    /// Snowflake: AUTOINCREMENT INCREMENT value
    #[serde(default)]
    pub auto_increment_increment: Option<Box<Expression>>,
    /// Snowflake: AUTOINCREMENT ORDER/NOORDER (true = ORDER, false = NOORDER, None = not specified)
    #[serde(default)]
    pub auto_increment_order: Option<bool>,
    /// MySQL: UNSIGNED modifier
    #[serde(default)]
    pub unsigned: bool,
    /// MySQL: ZEROFILL modifier
    #[serde(default)]
    pub zerofill: bool,
    /// MySQL: ON UPDATE expression (e.g., ON UPDATE CURRENT_TIMESTAMP)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_update: Option<Expression>,
    /// Named constraint for UNIQUE (e.g., CONSTRAINT must_be_different UNIQUE)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_constraint_name: Option<String>,
    /// Named constraint for NOT NULL (e.g., CONSTRAINT present NOT NULL)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_null_constraint_name: Option<String>,
    /// Named constraint for PRIMARY KEY (e.g., CONSTRAINT pk_name PRIMARY KEY)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key_constraint_name: Option<String>,
    /// Named constraint for CHECK (e.g., CONSTRAINT chk_name CHECK(...))
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_constraint_name: Option<String>,
    /// BigQuery: OPTIONS (key=value, ...) on column
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<Expression>,
    /// SQLite: Column definition without explicit type
    #[serde(default)]
    pub no_type: bool,
    /// Redshift: ENCODE encoding_type (e.g., ZSTD, DELTA, LZO, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    /// ClickHouse: CODEC(LZ4HC(9), ZSTD, DELTA)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// ClickHouse: EPHEMERAL [expr] modifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<Option<Box<Expression>>>,
    /// ClickHouse: MATERIALIZED expr modifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized_expr: Option<Box<Expression>>,
    /// ClickHouse: ALIAS expr modifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias_expr: Option<Box<Expression>>,
    /// ClickHouse: TTL expr modifier on columns
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_expr: Option<Box<Expression>>,
    /// TSQL: NOT FOR REPLICATION
    #[serde(default)]
    pub not_for_replication: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: Identifier::new(name),
            data_type,
            nullable: None,
            default: None,
            primary_key: false,
            primary_key_order: None,
            unique: false,
            unique_nulls_not_distinct: false,
            auto_increment: false,
            comment: None,
            constraints: Vec::new(),
            constraint_order: Vec::new(),
            format: None,
            title: None,
            inline_length: None,
            compress: None,
            character_set: None,
            uppercase: false,
            casespecific: None,
            auto_increment_start: None,
            auto_increment_increment: None,
            auto_increment_order: None,
            unsigned: false,
            zerofill: false,
            on_update: None,
            unique_constraint_name: None,
            not_null_constraint_name: None,
            primary_key_constraint_name: None,
            check_constraint_name: None,
            options: Vec::new(),
            no_type: false,
            encoding: None,
            codec: None,
            ephemeral: None,
            materialized_expr: None,
            alias_expr: None,
            ttl_expr: None,
            not_for_replication: false,
        }
    }
}

/// Column-level constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ColumnConstraint {
    NotNull,
    Null,
    Unique,
    PrimaryKey,
    Default(Expression),
    Check(Expression),
    References(ForeignKeyRef),
    GeneratedAsIdentity(GeneratedAsIdentity),
    Collate(Identifier),
    Comment(String),
    /// Snowflake: TAG (key='value', ...)
    Tags(Tags),
    /// Computed/generated column: GENERATED ALWAYS AS (expr) STORED|VIRTUAL (MySQL/PostgreSQL)
    /// or AS (expr) PERSISTED [NOT NULL] (TSQL)
    ComputedColumn(ComputedColumn),
    /// TSQL temporal: GENERATED ALWAYS AS ROW START|END [HIDDEN]
    GeneratedAsRow(GeneratedAsRow),
    /// PATH constraint for XMLTABLE/JSON_TABLE columns: PATH 'xpath'
    Path(Expression),
}

/// Computed/generated column constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ComputedColumn {
    /// The expression that computes the column value
    pub expression: Box<Expression>,
    /// PERSISTED (TSQL) or STORED (MySQL/PostgreSQL) = true; VIRTUAL = false; None = not specified
    #[serde(default)]
    pub persisted: bool,
    /// NOT NULL (TSQL computed columns)
    #[serde(default)]
    pub not_null: bool,
    /// The persistence keyword used: "STORED", "VIRTUAL", or "PERSISTED"
    /// When None, defaults to dialect-appropriate output
    #[serde(default)]
    pub persistence_kind: Option<String>,
    /// Optional data type for SingleStore: AS (expr) PERSISTED TYPE NOT NULL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<DataType>,
}

/// TSQL temporal column constraint: GENERATED ALWAYS AS ROW START|END [HIDDEN]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GeneratedAsRow {
    /// true = ROW START, false = ROW END
    pub start: bool,
    /// HIDDEN modifier
    #[serde(default)]
    pub hidden: bool,
}

/// Generated identity column constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GeneratedAsIdentity {
    /// True for ALWAYS, False for BY DEFAULT
    pub always: bool,
    /// ON NULL (only valid with BY DEFAULT)
    pub on_null: bool,
    /// START WITH value
    pub start: Option<Box<Expression>>,
    /// INCREMENT BY value
    pub increment: Option<Box<Expression>>,
    /// MINVALUE
    pub minvalue: Option<Box<Expression>>,
    /// MAXVALUE
    pub maxvalue: Option<Box<Expression>>,
    /// CYCLE option - Some(true) = CYCLE, Some(false) = NO CYCLE, None = not specified
    pub cycle: Option<bool>,
}

/// Constraint modifiers (shared between table-level constraints)
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConstraintModifiers {
    /// ENFORCED / NOT ENFORCED
    pub enforced: Option<bool>,
    /// DEFERRABLE / NOT DEFERRABLE
    pub deferrable: Option<bool>,
    /// INITIALLY DEFERRED / INITIALLY IMMEDIATE
    pub initially_deferred: Option<bool>,
    /// NORELY (Oracle)
    pub norely: bool,
    /// RELY (Oracle)
    pub rely: bool,
    /// USING index type (MySQL): BTREE or HASH
    #[serde(default)]
    pub using: Option<String>,
    /// True if USING appeared before columns (MySQL: INDEX USING BTREE (col) vs INDEX (col) USING BTREE)
    #[serde(default)]
    pub using_before_columns: bool,
    /// MySQL index COMMENT 'text'
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// MySQL index VISIBLE/INVISIBLE
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible: Option<bool>,
    /// MySQL ENGINE_ATTRIBUTE = 'value'
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_attribute: Option<String>,
    /// MySQL WITH PARSER name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_parser: Option<String>,
    /// PostgreSQL NOT VALID (constraint is not validated against existing data)
    #[serde(default)]
    pub not_valid: bool,
    /// TSQL CLUSTERED/NONCLUSTERED modifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clustered: Option<String>,
    /// SQLite ON CONFLICT clause: ROLLBACK, ABORT, FAIL, IGNORE, or REPLACE
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<String>,
    /// TSQL WITH options (e.g., PAD_INDEX=ON, STATISTICS_NORECOMPUTE=OFF)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with_options: Vec<(String, String)>,
    /// TSQL ON filegroup (e.g., ON [INDEX], ON [PRIMARY])
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_filegroup: Option<Identifier>,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        /// INCLUDE (columns) - non-key columns included in the index (PostgreSQL)
        #[serde(default)]
        include_columns: Vec<Identifier>,
        #[serde(default)]
        modifiers: ConstraintModifiers,
        /// Whether the CONSTRAINT keyword was used (vs MySQL's `PRIMARY KEY name (cols)` syntax)
        #[serde(default)]
        has_constraint_keyword: bool,
    },
    Unique {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        /// Whether columns are parenthesized (false for UNIQUE idx_name without parens)
        #[serde(default)]
        columns_parenthesized: bool,
        #[serde(default)]
        modifiers: ConstraintModifiers,
        /// Whether the CONSTRAINT keyword was used (vs MySQL's `UNIQUE name (cols)` syntax)
        #[serde(default)]
        has_constraint_keyword: bool,
        /// PostgreSQL 15+: NULLS NOT DISTINCT
        #[serde(default)]
        nulls_not_distinct: bool,
    },
    ForeignKey {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        #[serde(default)]
        references: Option<ForeignKeyRef>,
        /// ON DELETE action when REFERENCES is absent
        #[serde(default)]
        on_delete: Option<ReferentialAction>,
        /// ON UPDATE action when REFERENCES is absent
        #[serde(default)]
        on_update: Option<ReferentialAction>,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    Check {
        name: Option<Identifier>,
        expression: Expression,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    /// INDEX / KEY constraint (MySQL)
    Index {
        name: Option<Identifier>,
        columns: Vec<Identifier>,
        /// Index kind: UNIQUE, FULLTEXT, SPATIAL, etc.
        #[serde(default)]
        kind: Option<String>,
        #[serde(default)]
        modifiers: ConstraintModifiers,
        /// True if KEY keyword was used instead of INDEX
        #[serde(default)]
        use_key_keyword: bool,
        /// ClickHouse: indexed expression (instead of columns)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expression: Option<Box<Expression>>,
        /// ClickHouse: TYPE type_func(args)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        index_type: Option<Box<Expression>>,
        /// ClickHouse: GRANULARITY n
        #[serde(default, skip_serializing_if = "Option::is_none")]
        granularity: Option<Box<Expression>>,
    },
    /// ClickHouse PROJECTION definition
    Projection {
        name: Identifier,
        expression: Expression,
    },
    /// PostgreSQL LIKE clause: LIKE source_table [INCLUDING|EXCLUDING options]
    Like {
        source: TableRef,
        /// Options as (INCLUDING|EXCLUDING, property) pairs
        options: Vec<(LikeOptionAction, String)>,
    },
    /// TSQL PERIOD FOR SYSTEM_TIME (start_col, end_col)
    PeriodForSystemTime {
        start_col: Identifier,
        end_col: Identifier,
    },
    /// PostgreSQL EXCLUDE constraint
    /// EXCLUDE [USING method] (element WITH operator, ...) [INCLUDE (cols)] [WHERE (expr)] [WITH (params)]
    Exclude {
        name: Option<Identifier>,
        /// Index access method (gist, btree, etc.)
        #[serde(default)]
        using: Option<String>,
        /// Elements: (expression, operator) pairs
        elements: Vec<ExcludeElement>,
        /// INCLUDE columns
        #[serde(default)]
        include_columns: Vec<Identifier>,
        /// WHERE predicate
        #[serde(default)]
        where_clause: Option<Box<Expression>>,
        /// WITH (storage_parameters)
        #[serde(default)]
        with_params: Vec<(String, String)>,
        /// USING INDEX TABLESPACE tablespace_name
        #[serde(default)]
        using_index_tablespace: Option<String>,
        #[serde(default)]
        modifiers: ConstraintModifiers,
    },
    /// Snowflake TAG clause: TAG (key='value', key2='value2')
    Tags(Tags),
    /// PostgreSQL table-level INITIALLY DEFERRED/INITIALLY IMMEDIATE
    /// This is a standalone clause at the end of the CREATE TABLE that sets the default
    /// for all deferrable constraints in the table
    InitiallyDeferred {
        /// true = INITIALLY DEFERRED, false = INITIALLY IMMEDIATE
        deferred: bool,
    },
}

/// Element in an EXCLUDE constraint: expression WITH operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExcludeElement {
    /// The column expression (may include operator class, ordering, nulls)
    pub expression: String,
    /// The operator (e.g., &&, =)
    pub operator: String,
}

/// Action for LIKE clause options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum LikeOptionAction {
    Including,
    Excluding,
}

/// MATCH type for foreign keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum MatchType {
    Full,
    Partial,
    Simple,
}

/// Foreign key reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ForeignKeyRef {
    pub table: TableRef,
    pub columns: Vec<Identifier>,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
    /// True if ON UPDATE appears before ON DELETE in the original SQL
    #[serde(default)]
    pub on_update_first: bool,
    /// MATCH clause (FULL, PARTIAL, SIMPLE)
    #[serde(default)]
    pub match_type: Option<MatchType>,
    /// True if MATCH appears after ON DELETE/ON UPDATE clauses
    #[serde(default)]
    pub match_after_actions: bool,
    /// CONSTRAINT name (e.g., CONSTRAINT fk_name REFERENCES ...)
    #[serde(default)]
    pub constraint_name: Option<String>,
    /// DEFERRABLE / NOT DEFERRABLE
    #[serde(default)]
    pub deferrable: Option<bool>,
    /// Snowflake: FOREIGN KEY REFERENCES (includes FOREIGN KEY keywords before REFERENCES)
    #[serde(default)]
    pub has_foreign_key_keywords: bool,
}

/// Referential action for foreign keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropTable {
    pub names: Vec<TableRef>,
    pub if_exists: bool,
    pub cascade: bool,
    /// Oracle: CASCADE CONSTRAINTS
    #[serde(default)]
    pub cascade_constraints: bool,
    /// Oracle: PURGE
    #[serde(default)]
    pub purge: bool,
    /// Comments that appear before the DROP keyword (e.g., leading line comments)
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

impl DropTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            names: vec![TableRef::new(name)],
            if_exists: false,
            cascade: false,
            cascade_constraints: false,
            purge: false,
            leading_comments: Vec::new(),
        }
    }
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterTable {
    pub name: TableRef,
    pub actions: Vec<AlterTableAction>,
    /// IF EXISTS clause
    #[serde(default)]
    pub if_exists: bool,
    /// MySQL: ALGORITHM=INPLACE|COPY|DEFAULT|INSTANT
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// MySQL: LOCK=NONE|SHARED|DEFAULT|EXCLUSIVE
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
    /// TSQL: WITH CHECK / WITH NOCHECK modifier before ADD CONSTRAINT
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_check: Option<String>,
    /// Hive: PARTITION clause before actions (e.g., ALTER TABLE x PARTITION(y=z) ADD COLUMN ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<Vec<(Identifier, Expression)>>,
    /// ClickHouse: ON CLUSTER clause for distributed DDL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cluster: Option<OnCluster>,
}

impl AlterTable {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            actions: Vec::new(),
            if_exists: false,
            algorithm: None,
            lock: None,
            with_check: None,
            partition: None,
            on_cluster: None,
        }
    }
}

/// Column position for ADD COLUMN (MySQL/MariaDB)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ColumnPosition {
    First,
    After(Identifier),
}

/// Actions for ALTER TABLE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum AlterTableAction {
    AddColumn {
        column: ColumnDef,
        if_not_exists: bool,
        position: Option<ColumnPosition>,
    },
    DropColumn {
        name: Identifier,
        if_exists: bool,
        cascade: bool,
    },
    RenameColumn {
        old_name: Identifier,
        new_name: Identifier,
        if_exists: bool,
    },
    AlterColumn {
        name: Identifier,
        action: AlterColumnAction,
        /// Whether this was parsed from MODIFY COLUMN syntax (MySQL)
        #[serde(default)]
        use_modify_keyword: bool,
    },
    RenameTable(TableRef),
    AddConstraint(TableConstraint),
    DropConstraint {
        name: Identifier,
        if_exists: bool,
    },
    /// DROP FOREIGN KEY action (Oracle/MySQL): ALTER TABLE t DROP FOREIGN KEY fk_name
    DropForeignKey {
        name: Identifier,
    },
    /// DROP PARTITION action (Hive/BigQuery)
    DropPartition {
        /// List of partitions to drop (each partition is a list of key=value pairs)
        partitions: Vec<Vec<(Identifier, Expression)>>,
        if_exists: bool,
    },
    /// ADD PARTITION action (Hive/Spark)
    AddPartition {
        /// The partition expression
        partition: Expression,
        if_not_exists: bool,
        location: Option<Expression>,
    },
    /// DELETE action (BigQuery): ALTER TABLE t DELETE WHERE condition
    Delete {
        where_clause: Expression,
    },
    /// SWAP WITH action (Snowflake): ALTER TABLE a SWAP WITH b
    SwapWith(TableRef),
    /// SET property action (Snowflake): ALTER TABLE t SET property=value
    SetProperty {
        properties: Vec<(String, Expression)>,
    },
    /// UNSET property action (Snowflake): ALTER TABLE t UNSET property
    UnsetProperty {
        properties: Vec<String>,
    },
    /// CLUSTER BY action (Snowflake): ALTER TABLE t CLUSTER BY (col1, col2)
    ClusterBy {
        expressions: Vec<Expression>,
    },
    /// SET TAG action (Snowflake): ALTER TABLE t SET TAG key='value'
    SetTag {
        expressions: Vec<(String, Expression)>,
    },
    /// UNSET TAG action (Snowflake): ALTER TABLE t UNSET TAG key1, key2
    UnsetTag {
        names: Vec<String>,
    },
    /// SET with parenthesized options (TSQL): ALTER TABLE t SET (SYSTEM_VERSIONING=ON, ...)
    SetOptions {
        expressions: Vec<Expression>,
    },
    /// ALTER INDEX action (MySQL): ALTER TABLE t ALTER INDEX i VISIBLE/INVISIBLE
    AlterIndex {
        name: Identifier,
        visible: bool,
    },
    /// PostgreSQL: ALTER TABLE t SET LOGGED/UNLOGGED/WITHOUT CLUSTER/WITHOUT OIDS/ACCESS METHOD/TABLESPACE
    SetAttribute {
        attribute: String,
    },
    /// Snowflake: ALTER TABLE t SET STAGE_FILE_FORMAT = (options)
    SetStageFileFormat {
        options: Option<Expression>,
    },
    /// Snowflake: ALTER TABLE t SET STAGE_COPY_OPTIONS = (options)
    SetStageCopyOptions {
        options: Option<Expression>,
    },
    /// Hive/Spark: ADD COLUMNS (col1 TYPE, col2 TYPE) [CASCADE]
    AddColumns {
        columns: Vec<ColumnDef>,
        cascade: bool,
    },
    /// Spark/Databricks: DROP COLUMNS (col1, col2, ...)
    DropColumns {
        names: Vec<Identifier>,
    },
    /// Hive/MySQL/SingleStore: CHANGE [COLUMN] old_name new_name [data_type] [COMMENT 'comment']
    /// In SingleStore, data_type can be omitted for simple column renames
    ChangeColumn {
        old_name: Identifier,
        new_name: Identifier,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data_type: Option<DataType>,
        comment: Option<String>,
        #[serde(default)]
        cascade: bool,
    },
    /// Redshift: ALTER TABLE t ALTER SORTKEY AUTO|NONE|(col1, col2)
    /// Also: ALTER TABLE t ALTER COMPOUND SORTKEY (col1, col2)
    AlterSortKey {
        /// AUTO or NONE keyword
        this: Option<String>,
        /// Column list for (col1, col2) syntax
        expressions: Vec<Expression>,
        /// Whether COMPOUND keyword was present
        compound: bool,
    },
    /// Redshift: ALTER TABLE t ALTER DISTSTYLE ALL|EVEN|AUTO|KEY
    /// Also: ALTER TABLE t ALTER DISTSTYLE KEY DISTKEY col
    /// Also: ALTER TABLE t ALTER DISTKEY col (shorthand for DISTSTYLE KEY DISTKEY col)
    AlterDistStyle {
        /// Distribution style: ALL, EVEN, AUTO, or KEY
        style: String,
        /// DISTKEY column (only when style is KEY)
        distkey: Option<Identifier>,
    },
    /// Redshift: ALTER TABLE t SET TABLE PROPERTIES ('a' = '5', 'b' = 'c')
    SetTableProperties {
        properties: Vec<(Expression, Expression)>,
    },
    /// Redshift: ALTER TABLE t SET LOCATION 's3://bucket/folder/'
    SetLocation {
        location: String,
    },
    /// Redshift: ALTER TABLE t SET FILE FORMAT AVRO
    SetFileFormat {
        format: String,
    },
    /// ClickHouse: ALTER TABLE t REPLACE PARTITION expr FROM source_table
    ReplacePartition {
        partition: Expression,
        source: Option<Box<Expression>>,
    },
}

/// Actions for ALTER COLUMN
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum AlterColumnAction {
    SetDataType {
        data_type: DataType,
        /// USING expression for type conversion (PostgreSQL)
        using: Option<Expression>,
        /// COLLATE clause (TSQL: ALTER COLUMN col TYPE COLLATE collation_name)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        collate: Option<String>,
    },
    SetDefault(Expression),
    DropDefault,
    SetNotNull,
    DropNotNull,
    /// Set column comment
    Comment(String),
    /// MySQL: SET VISIBLE
    SetVisible,
    /// MySQL: SET INVISIBLE
    SetInvisible,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateIndex {
    pub name: Identifier,
    pub table: TableRef,
    pub columns: Vec<IndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
    pub using: Option<String>,
    /// TSQL CLUSTERED/NONCLUSTERED modifier
    #[serde(default)]
    pub clustered: Option<String>,
    /// PostgreSQL CONCURRENTLY modifier
    #[serde(default)]
    pub concurrently: bool,
    /// PostgreSQL WHERE clause for partial indexes
    #[serde(default)]
    pub where_clause: Option<Box<Expression>>,
    /// PostgreSQL INCLUDE columns
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include_columns: Vec<Identifier>,
    /// TSQL WITH options (e.g., allow_page_locks=on)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with_options: Vec<(String, String)>,
    /// TSQL ON filegroup or partition scheme (e.g., ON PRIMARY, ON X([y]))
    #[serde(default)]
    pub on_filegroup: Option<String>,
}

impl CreateIndex {
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: TableRef::new(table),
            columns: Vec::new(),
            unique: false,
            if_not_exists: false,
            using: None,
            clustered: None,
            concurrently: false,
            where_clause: None,
            include_columns: Vec::new(),
            with_options: Vec::new(),
            on_filegroup: None,
        }
    }
}

/// Index column specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IndexColumn {
    pub column: Identifier,
    pub desc: bool,
    /// Explicit ASC keyword was present
    #[serde(default)]
    pub asc: bool,
    pub nulls_first: Option<bool>,
    /// PostgreSQL operator class (e.g., varchar_pattern_ops, public.gin_trgm_ops)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opclass: Option<String>,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropIndex {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub if_exists: bool,
    /// PostgreSQL CONCURRENTLY modifier
    #[serde(default)]
    pub concurrently: bool,
}

impl DropIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            if_exists: false,
            concurrently: false,
        }
    }
}

/// View column definition with optional COMMENT and OPTIONS (BigQuery)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ViewColumn {
    pub name: Identifier,
    pub comment: Option<String>,
    /// BigQuery: OPTIONS (key=value, ...) on column
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<Expression>,
}

impl ViewColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            comment: None,
            options: Vec::new(),
        }
    }

    pub fn with_comment(name: impl Into<String>, comment: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            comment: Some(comment.into()),
            options: Vec::new(),
        }
    }
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateView {
    pub name: TableRef,
    pub columns: Vec<ViewColumn>,
    pub query: Expression,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub materialized: bool,
    pub temporary: bool,
    /// Snowflake: SECURE VIEW
    #[serde(default)]
    pub secure: bool,
    /// MySQL: ALGORITHM=UNDEFINED/MERGE/TEMPTABLE
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// MySQL: DEFINER=user@host
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definer: Option<String>,
    /// MySQL: SQL SECURITY DEFINER/INVOKER; Presto: SECURITY DEFINER/INVOKER
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<FunctionSecurity>,
    /// True for MySQL-style "SQL SECURITY", false for Presto-style "SECURITY"
    #[serde(default = "default_true")]
    pub security_sql_style: bool,
    /// Whether the query was parenthesized: AS (SELECT ...)
    #[serde(default)]
    pub query_parenthesized: bool,
    /// Teradata: LOCKING mode (ROW, TABLE, DATABASE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_mode: Option<String>,
    /// Teradata: LOCKING access type (ACCESS, READ, WRITE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locking_access: Option<String>,
    /// Snowflake: COPY GRANTS
    #[serde(default)]
    pub copy_grants: bool,
    /// Snowflake: COMMENT = 'text'
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub comment: Option<String>,
    /// Snowflake: TAG (name='value', ...)
    #[serde(default)]
    pub tags: Vec<(String, String)>,
    /// BigQuery: OPTIONS (key=value, ...)
    #[serde(default)]
    pub options: Vec<Expression>,
    /// Doris: BUILD IMMEDIATE/DEFERRED for materialized views
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub build: Option<String>,
    /// Doris: REFRESH property for materialized views
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub refresh: Option<Box<RefreshTriggerProperty>>,
    /// Doris: Schema with typed column definitions for materialized views
    /// This is used instead of `columns` when the view has typed column definitions
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub schema: Option<Box<Schema>>,
    /// Doris: KEY (columns) for materialized views
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub unique_key: Option<Box<UniqueKeyProperty>>,
    /// Redshift: WITH NO SCHEMA BINDING
    #[serde(default)]
    pub no_schema_binding: bool,
    /// Redshift: AUTO REFRESH YES|NO for materialized views
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub auto_refresh: Option<bool>,
    /// ClickHouse: ON CLUSTER clause
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cluster: Option<OnCluster>,
    /// ClickHouse: TO destination_table
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_table: Option<TableRef>,
    /// ClickHouse: Table properties (ENGINE, ORDER BY, SAMPLE, SETTINGS, TTL, etc.) for materialized views
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_properties: Vec<Expression>,
}

impl CreateView {
    pub fn new(name: impl Into<String>, query: Expression) -> Self {
        Self {
            name: TableRef::new(name),
            columns: Vec::new(),
            query,
            or_replace: false,
            if_not_exists: false,
            materialized: false,
            temporary: false,
            secure: false,
            algorithm: None,
            definer: None,
            security: None,
            security_sql_style: true,
            query_parenthesized: false,
            locking_mode: None,
            locking_access: None,
            copy_grants: false,
            comment: None,
            tags: Vec::new(),
            options: Vec::new(),
            build: None,
            refresh: None,
            schema: None,
            unique_key: None,
            no_schema_binding: false,
            auto_refresh: None,
            on_cluster: None,
            to_table: None,
            table_properties: Vec::new(),
        }
    }
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropView {
    pub name: TableRef,
    pub if_exists: bool,
    pub materialized: bool,
}

impl DropView {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            materialized: false,
        }
    }
}

/// TRUNCATE TABLE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Truncate {
    /// Target of TRUNCATE (TABLE vs DATABASE)
    #[serde(default)]
    pub target: TruncateTarget,
    /// IF EXISTS clause
    #[serde(default)]
    pub if_exists: bool,
    pub table: TableRef,
    /// ClickHouse: ON CLUSTER clause for distributed DDL
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_cluster: Option<OnCluster>,
    pub cascade: bool,
    /// Additional tables for multi-table TRUNCATE
    #[serde(default)]
    pub extra_tables: Vec<TruncateTableEntry>,
    /// RESTART IDENTITY or CONTINUE IDENTITY
    #[serde(default)]
    pub identity: Option<TruncateIdentity>,
    /// RESTRICT option (alternative to CASCADE)
    #[serde(default)]
    pub restrict: bool,
    /// Hive PARTITION clause: PARTITION(key=value, ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<Box<Expression>>,
}

/// A table entry in a TRUNCATE statement, with optional ONLY modifier and * suffix
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TruncateTableEntry {
    pub table: TableRef,
    /// Whether the table has a * suffix (inherit children)
    #[serde(default)]
    pub star: bool,
}

/// TRUNCATE target type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TruncateTarget {
    Table,
    Database,
}

impl Default for TruncateTarget {
    fn default() -> Self {
        TruncateTarget::Table
    }
}

/// TRUNCATE identity option
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TruncateIdentity {
    Restart,
    Continue,
}

impl Truncate {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            target: TruncateTarget::Table,
            if_exists: false,
            table: TableRef::new(table),
            on_cluster: None,
            cascade: false,
            extra_tables: Vec::new(),
            identity: None,
            restrict: false,
            partition: None,
        }
    }
}

/// USE statement (USE database, USE ROLE, USE WAREHOUSE, USE CATALOG, USE SCHEMA)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Use {
    /// The kind of object (DATABASE, SCHEMA, ROLE, WAREHOUSE, CATALOG, or None for default)
    pub kind: Option<UseKind>,
    /// The name of the object
    pub this: Identifier,
}

/// Kind of USE statement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum UseKind {
    Database,
    Schema,
    Role,
    Warehouse,
    Catalog,
    /// Snowflake: USE SECONDARY ROLES ALL|NONE
    SecondaryRoles,
}

/// SET variable statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SetStatement {
    /// The items being set
    pub items: Vec<SetItem>,
}

/// A single SET item (variable assignment)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SetItem {
    /// The variable name
    pub name: Expression,
    /// The value to set
    pub value: Expression,
    /// Kind: None for plain SET, Some("GLOBAL") for SET GLOBAL, etc.
    pub kind: Option<String>,
    /// Whether the SET item was parsed without an = sign (TSQL: SET KEY VALUE)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_equals: bool,
}

/// CACHE TABLE statement (Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Cache {
    /// The table to cache
    pub table: Identifier,
    /// LAZY keyword - defer caching until first use
    pub lazy: bool,
    /// Optional OPTIONS clause (key-value pairs)
    pub options: Vec<(Expression, Expression)>,
    /// Optional AS clause with query
    pub query: Option<Expression>,
}

/// UNCACHE TABLE statement (Spark)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Uncache {
    /// The table to uncache
    pub table: Identifier,
    /// IF EXISTS clause
    pub if_exists: bool,
}

/// LOAD DATA statement (Hive)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LoadData {
    /// LOCAL keyword - load from local filesystem
    pub local: bool,
    /// The path to load data from (INPATH value)
    pub inpath: String,
    /// Whether to overwrite existing data
    pub overwrite: bool,
    /// The target table
    pub table: Expression,
    /// Optional PARTITION clause with key-value pairs
    pub partition: Vec<(Identifier, Expression)>,
    /// Optional INPUTFORMAT clause
    pub input_format: Option<String>,
    /// Optional SERDE clause
    pub serde: Option<String>,
}

/// PRAGMA statement (SQLite)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Pragma {
    /// Optional schema prefix (e.g., "schema" in "schema.pragma_name")
    pub schema: Option<Identifier>,
    /// The pragma name
    pub name: Identifier,
    /// Optional value for assignment (PRAGMA name = value)
    pub value: Option<Expression>,
    /// Optional arguments for function-style pragmas (PRAGMA name(arg))
    pub args: Vec<Expression>,
}

/// A privilege with optional column list for GRANT/REVOKE
/// Examples: SELECT, UPDATE(col1, col2), ALL(col1, col2, col3)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Privilege {
    /// The privilege name (e.g., SELECT, INSERT, UPDATE, ALL)
    pub name: String,
    /// Optional column list for column-level privileges (e.g., UPDATE(col1, col2))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

/// Principal in GRANT/REVOKE (user, role, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GrantPrincipal {
    /// The name of the principal
    pub name: Identifier,
    /// Whether prefixed with ROLE keyword
    pub is_role: bool,
    /// Whether prefixed with GROUP keyword (Redshift)
    #[serde(default)]
    pub is_group: bool,
}

/// GRANT statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Grant {
    /// Privileges to grant (e.g., SELECT, INSERT, UPDATE(col1, col2))
    pub privileges: Vec<Privilege>,
    /// Object kind (TABLE, SCHEMA, FUNCTION, etc.)
    pub kind: Option<String>,
    /// The object to grant on
    pub securable: Identifier,
    /// Function parameter types (for FUNCTION kind)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub function_params: Vec<String>,
    /// The grantees
    pub principals: Vec<GrantPrincipal>,
    /// WITH GRANT OPTION
    pub grant_option: bool,
    /// TSQL: AS principal (the grantor role)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_principal: Option<Identifier>,
}

/// REVOKE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Revoke {
    /// Privileges to revoke (e.g., SELECT, INSERT, UPDATE(col1, col2))
    pub privileges: Vec<Privilege>,
    /// Object kind (TABLE, SCHEMA, FUNCTION, etc.)
    pub kind: Option<String>,
    /// The object to revoke from
    pub securable: Identifier,
    /// Function parameter types (for FUNCTION kind)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub function_params: Vec<String>,
    /// The grantees
    pub principals: Vec<GrantPrincipal>,
    /// GRANT OPTION FOR
    pub grant_option: bool,
    /// CASCADE
    pub cascade: bool,
    /// RESTRICT
    #[serde(default)]
    pub restrict: bool,
}

/// COMMENT ON statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Comment {
    /// The object being commented on
    pub this: Expression,
    /// The object kind (COLUMN, TABLE, DATABASE, etc.)
    pub kind: String,
    /// The comment text expression
    pub expression: Expression,
    /// IF EXISTS clause
    pub exists: bool,
    /// MATERIALIZED keyword
    pub materialized: bool,
}

// ============================================================================
// Phase 4: Additional DDL Statements
// ============================================================================

/// ALTER VIEW statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterView {
    pub name: TableRef,
    pub actions: Vec<AlterViewAction>,
    /// MySQL: ALGORITHM = MERGE|TEMPTABLE|UNDEFINED
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// MySQL: DEFINER = 'user'@'host'
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definer: Option<String>,
    /// MySQL: SQL SECURITY = DEFINER|INVOKER
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_security: Option<String>,
    /// TSQL: WITH option (SCHEMABINDING, ENCRYPTION, VIEW_METADATA)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_option: Option<String>,
    /// Hive: Column aliases with optional comments: (c1 COMMENT 'text', c2)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ViewColumn>,
}

/// Actions for ALTER VIEW
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum AlterViewAction {
    /// Rename the view
    Rename(TableRef),
    /// Change owner
    OwnerTo(Identifier),
    /// Set schema
    SetSchema(Identifier),
    /// Set authorization (Trino/Presto)
    SetAuthorization(String),
    /// Alter column
    AlterColumn {
        name: Identifier,
        action: AlterColumnAction,
    },
    /// Redefine view as query (SELECT, UNION, etc.)
    AsSelect(Box<Expression>),
    /// Hive: SET TBLPROPERTIES ('key'='value', ...)
    SetTblproperties(Vec<(String, String)>),
    /// Hive: UNSET TBLPROPERTIES ('key1', 'key2', ...)
    UnsetTblproperties(Vec<String>),
}

impl AlterView {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            actions: Vec::new(),
            algorithm: None,
            definer: None,
            sql_security: None,
            with_option: None,
            columns: Vec::new(),
        }
    }
}

/// ALTER INDEX statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterIndex {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub actions: Vec<AlterIndexAction>,
}

/// Actions for ALTER INDEX
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum AlterIndexAction {
    /// Rename the index
    Rename(Identifier),
    /// Set tablespace
    SetTablespace(Identifier),
    /// Set visibility (MySQL)
    Visible(bool),
}

impl AlterIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            actions: Vec::new(),
        }
    }
}

/// CREATE SCHEMA statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateSchema {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub authorization: Option<Identifier>,
    #[serde(default)]
    pub clone_from: Option<Identifier>,
    /// AT/BEFORE clause for time travel (Snowflake)
    #[serde(default)]
    pub at_clause: Option<Expression>,
    /// Schema properties like DEFAULT COLLATE
    #[serde(default)]
    pub properties: Vec<Expression>,
    /// Leading comments before the statement
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub leading_comments: Vec<String>,
}

impl CreateSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_not_exists: false,
            authorization: None,
            clone_from: None,
            at_clause: None,
            properties: Vec::new(),
            leading_comments: Vec::new(),
        }
    }
}

/// DROP SCHEMA statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropSchema {
    pub name: Identifier,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// DROP NAMESPACE statement (Spark/Databricks - alias for DROP SCHEMA)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropNamespace {
    pub name: Identifier,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropNamespace {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE DATABASE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateDatabase {
    pub name: Identifier,
    pub if_not_exists: bool,
    pub options: Vec<DatabaseOption>,
    /// Snowflake CLONE source
    #[serde(default)]
    pub clone_from: Option<Identifier>,
    /// AT/BEFORE clause for time travel (Snowflake)
    #[serde(default)]
    pub at_clause: Option<Expression>,
}

/// Database option
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum DatabaseOption {
    CharacterSet(String),
    Collate(String),
    Owner(Identifier),
    Template(Identifier),
    Encoding(String),
    Location(String),
}

impl CreateDatabase {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_not_exists: false,
            options: Vec::new(),
            clone_from: None,
            at_clause: None,
        }
    }
}

/// DROP DATABASE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropDatabase {
    pub name: Identifier,
    pub if_exists: bool,
}

impl DropDatabase {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            if_exists: false,
        }
    }
}

/// CREATE FUNCTION statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateFunction {
    pub name: TableRef,
    pub parameters: Vec<FunctionParameter>,
    pub return_type: Option<DataType>,
    pub body: Option<FunctionBody>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub language: Option<String>,
    pub deterministic: Option<bool>,
    pub returns_null_on_null_input: Option<bool>,
    pub security: Option<FunctionSecurity>,
    /// Whether parentheses were present in the original syntax
    #[serde(default = "default_true")]
    pub has_parens: bool,
    /// SQL data access characteristic (CONTAINS SQL, READS SQL DATA, etc.)
    #[serde(default)]
    pub sql_data_access: Option<SqlDataAccess>,
    /// TSQL: RETURNS @var TABLE (col_defs) - stores the variable name and column definitions as raw string
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns_table_body: Option<String>,
    /// True if LANGUAGE clause appears before RETURNS clause
    #[serde(default)]
    pub language_first: bool,
    /// PostgreSQL SET options: SET key = value, SET key FROM CURRENT
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set_options: Vec<FunctionSetOption>,
    /// True if STRICT was used instead of RETURNS NULL ON NULL INPUT
    #[serde(default)]
    pub strict: bool,
    /// BigQuery: OPTIONS (key=value, ...)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<Expression>,
    /// BigQuery: True if this is a TABLE FUNCTION (CREATE TABLE FUNCTION)
    #[serde(default)]
    pub is_table_function: bool,
    /// Original order of function properties (SET, AS, LANGUAGE, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub property_order: Vec<FunctionPropertyKind>,
    /// Databricks: ENVIRONMENT (dependencies = '...', environment_version = '...')
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment: Vec<Expression>,
}

/// A SET option in CREATE FUNCTION (PostgreSQL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FunctionSetOption {
    pub name: String,
    pub value: FunctionSetValue,
}

/// The value of a SET option
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum FunctionSetValue {
    /// SET key = value (use_to = false) or SET key TO value (use_to = true)
    Value { value: String, use_to: bool },
    /// SET key FROM CURRENT
    FromCurrent,
}

/// SQL data access characteristics for functions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum SqlDataAccess {
    /// NO SQL
    NoSql,
    /// CONTAINS SQL
    ContainsSql,
    /// READS SQL DATA
    ReadsSqlData,
    /// MODIFIES SQL DATA
    ModifiesSqlData,
}

/// Types of properties in CREATE FUNCTION for tracking their original order
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum FunctionPropertyKind {
    /// SET option
    Set,
    /// AS body
    As,
    /// LANGUAGE clause
    Language,
    /// IMMUTABLE/VOLATILE/STABLE (determinism)
    Determinism,
    /// CALLED ON NULL INPUT / RETURNS NULL ON NULL INPUT / STRICT
    NullInput,
    /// SECURITY DEFINER/INVOKER
    Security,
    /// SQL data access (CONTAINS SQL, READS SQL DATA, etc.)
    SqlDataAccess,
    /// OPTIONS clause (BigQuery)
    Options,
    /// ENVIRONMENT clause (Databricks)
    Environment,
}

/// Function parameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FunctionParameter {
    pub name: Option<Identifier>,
    pub data_type: DataType,
    pub mode: Option<ParameterMode>,
    pub default: Option<Expression>,
}

/// Parameter mode (IN, OUT, INOUT)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum ParameterMode {
    In,
    Out,
    InOut,
}

/// Function body
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum FunctionBody {
    /// AS $$ ... $$ (dollar-quoted)
    Block(String),
    /// AS 'string' (single-quoted string literal body)
    StringLiteral(String),
    /// AS 'expression'
    Expression(Expression),
    /// EXTERNAL NAME 'library'
    External(String),
    /// RETURN expression
    Return(Expression),
    /// BEGIN ... END block with parsed statements
    Statements(Vec<Expression>),
    /// AS $$...$$ or $tag$...$tag$ (dollar-quoted with optional tag)
    /// Stores (content, optional_tag)
    DollarQuoted {
        content: String,
        tag: Option<String>,
    },
}

/// Function security (DEFINER, INVOKER, or NONE)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum FunctionSecurity {
    Definer,
    Invoker,
    /// StarRocks/MySQL: SECURITY NONE
    None,
}

impl CreateFunction {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: Vec::new(),
            return_type: None,
            body: None,
            or_replace: false,
            if_not_exists: false,
            temporary: false,
            language: None,
            deterministic: None,
            returns_null_on_null_input: None,
            security: None,
            has_parens: true,
            sql_data_access: None,
            returns_table_body: None,
            language_first: false,
            set_options: Vec::new(),
            strict: false,
            options: Vec::new(),
            is_table_function: false,
            property_order: Vec::new(),
            environment: Vec::new(),
        }
    }
}

/// DROP FUNCTION statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropFunction {
    pub name: TableRef,
    pub parameters: Option<Vec<DataType>>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropFunction {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE PROCEDURE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateProcedure {
    pub name: TableRef,
    pub parameters: Vec<FunctionParameter>,
    pub body: Option<FunctionBody>,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub language: Option<String>,
    pub security: Option<FunctionSecurity>,
    /// Return type (Snowflake: RETURNS OBJECT, RETURNS VARCHAR, etc.)
    #[serde(default)]
    pub return_type: Option<DataType>,
    /// Execution context (EXECUTE AS CALLER, EXECUTE AS OWNER)
    #[serde(default)]
    pub execute_as: Option<String>,
    /// TSQL WITH options (ENCRYPTION, RECOMPILE, SCHEMABINDING, etc.)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub with_options: Vec<String>,
    /// Whether the parameter list had parentheses (false for TSQL procedures without parens)
    #[serde(default = "default_true", skip_serializing_if = "is_true")]
    pub has_parens: bool,
    /// Whether the short form PROC was used (instead of PROCEDURE)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_proc_keyword: bool,
}

impl CreateProcedure {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: Vec::new(),
            body: None,
            or_replace: false,
            if_not_exists: false,
            language: None,
            security: None,
            return_type: None,
            execute_as: None,
            with_options: Vec::new(),
            has_parens: true,
            use_proc_keyword: false,
        }
    }
}

/// DROP PROCEDURE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropProcedure {
    pub name: TableRef,
    pub parameters: Option<Vec<DataType>>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropProcedure {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            parameters: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// Sequence property tag for ordering
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum SeqPropKind {
    Start,
    Increment,
    Minvalue,
    Maxvalue,
    Cache,
    NoCache,
    Cycle,
    NoCycle,
    OwnedBy,
    Order,
    NoOrder,
    Comment,
    /// SHARING=<value> (Oracle)
    Sharing,
    /// KEEP (Oracle)
    Keep,
    /// NOKEEP (Oracle)
    NoKeep,
    /// SCALE [EXTEND|NOEXTEND] (Oracle)
    Scale,
    /// NOSCALE (Oracle)
    NoScale,
    /// SHARD [EXTEND|NOEXTEND] (Oracle)
    Shard,
    /// NOSHARD (Oracle)
    NoShard,
    /// SESSION (Oracle)
    Session,
    /// GLOBAL (Oracle)
    Global,
    /// NOCACHE (single word, Oracle)
    NoCacheWord,
    /// NOCYCLE (single word, Oracle)
    NoCycleWord,
    /// NOMINVALUE (single word, Oracle)
    NoMinvalueWord,
    /// NOMAXVALUE (single word, Oracle)
    NoMaxvalueWord,
}

/// CREATE SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateSequence {
    pub name: TableRef,
    pub if_not_exists: bool,
    pub temporary: bool,
    #[serde(default)]
    pub or_replace: bool,
    /// AS <type> clause (e.g., AS SMALLINT, AS BIGINT)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_type: Option<DataType>,
    pub increment: Option<i64>,
    pub minvalue: Option<SequenceBound>,
    pub maxvalue: Option<SequenceBound>,
    pub start: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
    pub owned_by: Option<TableRef>,
    /// Whether OWNED BY NONE was specified
    #[serde(default)]
    pub owned_by_none: bool,
    /// Snowflake: ORDER or NOORDER (true = ORDER, false = NOORDER, None = not specified)
    #[serde(default)]
    pub order: Option<bool>,
    /// Snowflake: COMMENT = 'value'
    #[serde(default)]
    pub comment: Option<String>,
    /// SHARING=<value> (Oracle)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sharing: Option<String>,
    /// SCALE modifier: Some("EXTEND"), Some("NOEXTEND"), Some("") for plain SCALE
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale_modifier: Option<String>,
    /// SHARD modifier: Some("EXTEND"), Some("NOEXTEND"), Some("") for plain SHARD
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_modifier: Option<String>,
    /// Tracks the order in which properties appeared in the source
    #[serde(default)]
    pub property_order: Vec<SeqPropKind>,
}

/// Sequence bound (value or NO MINVALUE/NO MAXVALUE)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum SequenceBound {
    Value(i64),
    None,
}

impl CreateSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_not_exists: false,
            temporary: false,
            or_replace: false,
            as_type: None,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            cache: None,
            cycle: false,
            owned_by: None,
            owned_by_none: false,
            order: None,
            comment: None,
            sharing: None,
            scale_modifier: None,
            shard_modifier: None,
            property_order: Vec::new(),
        }
    }
}

/// DROP SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropSequence {
    pub name: TableRef,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// ALTER SEQUENCE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterSequence {
    pub name: TableRef,
    pub if_exists: bool,
    pub increment: Option<i64>,
    pub minvalue: Option<SequenceBound>,
    pub maxvalue: Option<SequenceBound>,
    pub start: Option<i64>,
    pub restart: Option<Option<i64>>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<Option<TableRef>>,
}

impl AlterSequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            increment: None,
            minvalue: None,
            maxvalue: None,
            start: None,
            restart: None,
            cache: None,
            cycle: None,
            owned_by: None,
        }
    }
}

/// CREATE TRIGGER statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateTrigger {
    pub name: Identifier,
    pub table: TableRef,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each: TriggerForEach,
    pub when: Option<Expression>,
    pub body: TriggerBody,
    pub or_replace: bool,
    pub constraint: bool,
    pub deferrable: Option<bool>,
    pub initially_deferred: Option<bool>,
    pub referencing: Option<TriggerReferencing>,
}

/// Trigger timing (BEFORE, AFTER, INSTEAD OF)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event (INSERT, UPDATE, DELETE, TRUNCATE)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TriggerEvent {
    Insert,
    Update(Option<Vec<Identifier>>),
    Delete,
    Truncate,
}

/// Trigger FOR EACH clause
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TriggerForEach {
    Row,
    Statement,
}

/// Trigger body
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TriggerBody {
    /// EXECUTE FUNCTION/PROCEDURE name(args)
    Execute {
        function: TableRef,
        args: Vec<Expression>,
    },
    /// BEGIN ... END block
    Block(String),
}

/// Trigger REFERENCING clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TriggerReferencing {
    pub old_table: Option<Identifier>,
    pub new_table: Option<Identifier>,
    pub old_row: Option<Identifier>,
    pub new_row: Option<Identifier>,
}

impl CreateTrigger {
    pub fn new(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: TableRef::new(table),
            timing: TriggerTiming::Before,
            events: Vec::new(),
            for_each: TriggerForEach::Row,
            when: None,
            body: TriggerBody::Execute {
                function: TableRef::new(""),
                args: Vec::new(),
            },
            or_replace: false,
            constraint: false,
            deferrable: None,
            initially_deferred: None,
            referencing: None,
        }
    }
}

/// DROP TRIGGER statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropTrigger {
    pub name: Identifier,
    pub table: Option<TableRef>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropTrigger {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: Identifier::new(name),
            table: None,
            if_exists: false,
            cascade: false,
        }
    }
}

/// CREATE TYPE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CreateType {
    pub name: TableRef,
    pub definition: TypeDefinition,
    pub if_not_exists: bool,
}

/// Type definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub enum TypeDefinition {
    /// ENUM type: CREATE TYPE name AS ENUM ('val1', 'val2', ...)
    Enum(Vec<String>),
    /// Composite type: CREATE TYPE name AS (field1 type1, field2 type2, ...)
    Composite(Vec<TypeAttribute>),
    /// Range type: CREATE TYPE name AS RANGE (SUBTYPE = type, ...)
    Range {
        subtype: DataType,
        subtype_diff: Option<String>,
        canonical: Option<String>,
    },
    /// Base type (for advanced usage)
    Base {
        input: String,
        output: String,
        internallength: Option<i32>,
    },
    /// Domain type
    Domain {
        base_type: DataType,
        default: Option<Expression>,
        constraints: Vec<DomainConstraint>,
    },
}

/// Type attribute for composite types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TypeAttribute {
    pub name: Identifier,
    pub data_type: DataType,
    pub collate: Option<Identifier>,
}

/// Domain constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DomainConstraint {
    pub name: Option<Identifier>,
    pub check: Expression,
}

impl CreateType {
    pub fn new_enum(name: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            name: TableRef::new(name),
            definition: TypeDefinition::Enum(values),
            if_not_exists: false,
        }
    }

    pub fn new_composite(name: impl Into<String>, attributes: Vec<TypeAttribute>) -> Self {
        Self {
            name: TableRef::new(name),
            definition: TypeDefinition::Composite(attributes),
            if_not_exists: false,
        }
    }
}

/// DROP TYPE statement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropType {
    pub name: TableRef,
    pub if_exists: bool,
    pub cascade: bool,
}

impl DropType {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: TableRef::new(name),
            if_exists: false,
            cascade: false,
        }
    }
}

/// DESCRIBE statement - shows table structure or query plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Describe {
    /// The target to describe (table name or query)
    pub target: Expression,
    /// EXTENDED format
    pub extended: bool,
    /// FORMATTED format
    pub formatted: bool,
    /// Object kind (e.g., "SEMANTIC VIEW", "TABLE", etc.)
    #[serde(default)]
    pub kind: Option<String>,
    /// Properties like type=stage
    #[serde(default)]
    pub properties: Vec<(String, String)>,
    /// Style keyword (e.g., "ANALYZE", "HISTORY")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<String>,
    /// Partition specification for DESCRIBE PARTITION
    #[serde(default)]
    pub partition: Option<Box<Expression>>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

impl Describe {
    pub fn new(target: Expression) -> Self {
        Self {
            target,
            extended: false,
            formatted: false,
            kind: None,
            properties: Vec::new(),
            style: None,
            partition: None,
            leading_comments: Vec::new(),
        }
    }
}

/// SHOW statement - displays database objects
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Show {
    /// The thing to show (DATABASES, TABLES, SCHEMAS, etc.)
    pub this: String,
    /// Whether TERSE was specified
    #[serde(default)]
    pub terse: bool,
    /// Whether HISTORY was specified
    #[serde(default)]
    pub history: bool,
    /// LIKE pattern
    pub like: Option<Expression>,
    /// IN scope kind (ACCOUNT, DATABASE, SCHEMA, TABLE)
    pub scope_kind: Option<String>,
    /// IN scope object
    pub scope: Option<Expression>,
    /// STARTS WITH pattern
    pub starts_with: Option<Expression>,
    /// LIMIT clause
    pub limit: Option<Box<Limit>>,
    /// FROM clause (for specific object)
    pub from: Option<Expression>,
    /// WHERE clause (MySQL: SHOW STATUS WHERE ...)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub where_clause: Option<Expression>,
    /// FOR target (MySQL: SHOW GRANTS FOR user, SHOW PROFILE ... FOR QUERY n)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub for_target: Option<Expression>,
    /// Second FROM clause (MySQL: SHOW COLUMNS FROM tbl FROM db)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<Expression>,
    /// Target identifier (MySQL: engine name in SHOW ENGINE, table in SHOW COLUMNS FROM)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<Expression>,
    /// MUTEX flag for SHOW ENGINE (true=MUTEX, false=STATUS, None=neither)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutex: Option<bool>,
    /// WITH PRIVILEGES clause (Snowflake: SHOW ... WITH PRIVILEGES USAGE, MODIFY)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub privileges: Vec<String>,
}

impl Show {
    pub fn new(this: impl Into<String>) -> Self {
        Self {
            this: this.into(),
            terse: false,
            history: false,
            like: None,
            scope_kind: None,
            scope: None,
            starts_with: None,
            limit: None,
            from: None,
            where_clause: None,
            for_target: None,
            db: None,
            target: None,
            mutex: None,
            privileges: Vec::new(),
        }
    }
}

/// Represent an explicit parenthesized expression for grouping precedence.
///
/// Preserves user-written parentheses so that `(a + b) * c` round-trips
/// correctly instead of being flattened to `a + b * c`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Paren {
    /// The inner expression wrapped by parentheses.
    pub this: Expression,
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

/// Expression annotated with trailing comments (for round-trip preservation)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Annotated {
    pub this: Expression,
    pub trailing_comments: Vec<String>,
}


// === BATCH GENERATED STRUCT DEFINITIONS ===
// Generated from Python sqlglot expressions.py

/// Refresh
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Refresh {
    pub this: Box<Expression>,
    pub kind: String,
}

/// LockingStatement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LockingStatement {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SequenceProperties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SequenceProperties {
    #[serde(default)]
    pub increment: Option<Box<Expression>>,
    #[serde(default)]
    pub minvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub maxvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub cache: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub owned: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// TruncateTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TruncateTable {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub is_database: Option<Box<Expression>>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub only: Option<Box<Expression>>,
    #[serde(default)]
    pub cluster: Option<Box<Expression>>,
    #[serde(default)]
    pub identity: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub partition: Option<Box<Expression>>,
}

/// Clone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Clone {
    pub this: Box<Expression>,
    #[serde(default)]
    pub shallow: Option<Box<Expression>>,
    #[serde(default)]
    pub copy: Option<Box<Expression>>,
}

/// Attach
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Attach {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Detach
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Detach {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
}

/// Install
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Install {
    pub this: Box<Expression>,
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub force: Option<Box<Expression>>,
}

/// Summarize
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Summarize {
    pub this: Box<Expression>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
}

/// Declare
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Declare {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// DeclareItem
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DeclareItem {
    pub this: Box<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub has_as: bool,
    /// BigQuery: additional variable names in multi-variable DECLARE (DECLARE X, Y, Z INT64)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_names: Vec<Expression>,
}

/// Set
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Set {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub unset: Option<Box<Expression>>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
}

/// Heredoc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Heredoc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
}

/// QueryBand
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct QueryBand {
    pub this: Box<Expression>,
    #[serde(default)]
    pub scope: Option<Box<Expression>>,
    #[serde(default)]
    pub update: Option<Box<Expression>>,
}

/// UserDefinedFunction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UserDefinedFunction {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub wrapped: Option<Box<Expression>>,
}

/// RecursiveWithSearch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RecursiveWithSearch {
    pub kind: String,
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
}

/// ProjectionDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ProjectionDef {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// TableAlias
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TableAlias {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
}

/// ByteString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ByteString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub is_bytes: Option<Box<Expression>>,
}

/// HexStringExpr - Hex string expression (not literal)
/// BigQuery: converts to FROM_HEX(this)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct HexStringExpr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub is_integer: Option<bool>,
}

/// UnicodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnicodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub escape: Option<Box<Expression>>,
}

/// AlterColumn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterColumn {
    pub this: Box<Expression>,
    #[serde(default)]
    pub dtype: Option<Box<Expression>>,
    #[serde(default)]
    pub collate: Option<Box<Expression>>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub drop: Option<Box<Expression>>,
    #[serde(default)]
    pub comment: Option<Box<Expression>>,
    #[serde(default)]
    pub allow_null: Option<Box<Expression>>,
    #[serde(default)]
    pub visible: Option<Box<Expression>>,
    #[serde(default)]
    pub rename_to: Option<Box<Expression>>,
}

/// AlterSortKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterSortKey {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub compound: Option<Box<Expression>>,
}

/// AlterSet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterSet {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub tablespace: Option<Box<Expression>>,
    #[serde(default)]
    pub access_method: Option<Box<Expression>>,
    #[serde(default)]
    pub file_format: Option<Box<Expression>>,
    #[serde(default)]
    pub copy_options: Option<Box<Expression>>,
    #[serde(default)]
    pub tag: Option<Box<Expression>>,
    #[serde(default)]
    pub location: Option<Box<Expression>>,
    #[serde(default)]
    pub serde: Option<Box<Expression>>,
}

/// RenameColumn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RenameColumn {
    pub this: Box<Expression>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
    #[serde(default)]
    pub exists: bool,
}

/// Comprehension
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Comprehension {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub iterator: Option<Box<Expression>>,
    #[serde(default)]
    pub condition: Option<Box<Expression>>,
}

/// MergeTreeTTLAction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MergeTreeTTLAction {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
    #[serde(default)]
    pub recompress: Option<Box<Expression>>,
    #[serde(default)]
    pub to_disk: Option<Box<Expression>>,
    #[serde(default)]
    pub to_volume: Option<Box<Expression>>,
}

/// MergeTreeTTL
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MergeTreeTTL {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
    #[serde(default)]
    pub aggregates: Option<Box<Expression>>,
}

/// IndexConstraintOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IndexConstraintOption {
    #[serde(default)]
    pub key_block_size: Option<Box<Expression>>,
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub parser: Option<Box<Expression>>,
    #[serde(default)]
    pub comment: Option<Box<Expression>>,
    #[serde(default)]
    pub visible: Option<Box<Expression>>,
    #[serde(default)]
    pub engine_attr: Option<Box<Expression>>,
    #[serde(default)]
    pub secondary_engine_attr: Option<Box<Expression>>,
}

/// PeriodForSystemTimeConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PeriodForSystemTimeConstraint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CaseSpecificColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CaseSpecificColumnConstraint {
    #[serde(default)]
    pub not_: Option<Box<Expression>>,
}

/// CharacterSetColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CharacterSetColumnConstraint {
    pub this: Box<Expression>,
}

/// CheckColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CheckColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub enforced: Option<Box<Expression>>,
}

/// CompressColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CompressColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// DateFormatColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateFormatColumnConstraint {
    pub this: Box<Expression>,
}

/// EphemeralColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EphemeralColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// WithOperator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithOperator {
    pub this: Box<Expression>,
    pub op: String,
}

/// GeneratedAsIdentityColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GeneratedAsIdentityColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub on_null: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub increment: Option<Box<Expression>>,
    #[serde(default)]
    pub minvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub maxvalue: Option<Box<Expression>>,
    #[serde(default)]
    pub cycle: Option<Box<Expression>>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}

/// AutoIncrementColumnConstraint - MySQL/TSQL auto-increment marker
/// TSQL: outputs "IDENTITY"
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AutoIncrementColumnConstraint;

/// CommentColumnConstraint - Column comment marker
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CommentColumnConstraint;

/// GeneratedAsRowColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GeneratedAsRowColumnConstraint {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub hidden: Option<Box<Expression>>,
}

/// IndexColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IndexColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub index_type: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub granularity: Option<Box<Expression>>,
}

/// MaskingPolicyColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MaskingPolicyColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// NotNullColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NotNullColumnConstraint {
    #[serde(default)]
    pub allow_null: Option<Box<Expression>>,
}

/// DefaultColumnConstraint - DEFAULT value for a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DefaultColumnConstraint {
    pub this: Box<Expression>,
}

/// PrimaryKeyColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PrimaryKeyColumnConstraint {
    #[serde(default)]
    pub desc: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// UniqueColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UniqueColumnConstraint {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub index_type: Option<Box<Expression>>,
    #[serde(default)]
    pub on_conflict: Option<Box<Expression>>,
    #[serde(default)]
    pub nulls: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// WatermarkColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WatermarkColumnConstraint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ComputedColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ComputedColumnConstraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub persisted: Option<Box<Expression>>,
    #[serde(default)]
    pub not_null: Option<Box<Expression>>,
    #[serde(default)]
    pub data_type: Option<Box<Expression>>,
}

/// InOutColumnConstraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct InOutColumnConstraint {
    #[serde(default)]
    pub input_: Option<Box<Expression>>,
    #[serde(default)]
    pub output: Option<Box<Expression>>,
}

/// PathColumnConstraint - PATH 'xpath' for XMLTABLE/JSON_TABLE columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PathColumnConstraint {
    pub this: Box<Expression>,
}

/// Constraint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Constraint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Export
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Export {
    pub this: Box<Expression>,
    #[serde(default)]
    pub connection: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// Filter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Filter {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Changes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Changes {
    #[serde(default)]
    pub information: Option<Box<Expression>>,
    #[serde(default)]
    pub at_before: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
}

/// Directory
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Directory {
    pub this: Box<Expression>,
    #[serde(default)]
    pub local: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format: Option<Box<Expression>>,
}

/// ForeignKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ForeignKey {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub reference: Option<Box<Expression>>,
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
    #[serde(default)]
    pub update: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// ColumnPrefix
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ColumnPrefix {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PrimaryKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PrimaryKey {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub include: Option<Box<Expression>>,
}

/// Into
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IntoClause {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub temporary: bool,
    #[serde(default)]
    pub unlogged: Option<Box<Expression>>,
    #[serde(default)]
    pub bulk_collect: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JoinHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JoinHint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Opclass
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Opclass {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Index
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Index {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
    #[serde(default)]
    pub unique: bool,
    #[serde(default)]
    pub primary: Option<Box<Expression>>,
    #[serde(default)]
    pub amp: Option<Box<Expression>>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// IndexParameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IndexParameters {
    #[serde(default)]
    pub using: Option<Box<Expression>>,
    #[serde(default)]
    pub include: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
    #[serde(default)]
    pub with_storage: Option<Box<Expression>>,
    #[serde(default)]
    pub partition_by: Option<Box<Expression>>,
    #[serde(default)]
    pub tablespace: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
}

/// ConditionalInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConditionalInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub else_: Option<Box<Expression>>,
}

/// MultitableInserts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MultitableInserts {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    pub kind: String,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
    /// Leading comments before the statement
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

/// OnConflict
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OnConflict {
    #[serde(default)]
    pub duplicate: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub action: Option<Box<Expression>>,
    #[serde(default)]
    pub conflict_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub index_predicate: Option<Box<Expression>>,
    #[serde(default)]
    pub constraint: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
}

/// OnCondition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OnCondition {
    #[serde(default)]
    pub error: Option<Box<Expression>>,
    #[serde(default)]
    pub empty: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// Returning
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Returning {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub into: Option<Box<Expression>>,
}

/// Introducer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Introducer {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionRange
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionRange {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Group
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Group {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub grouping_sets: Option<Box<Expression>>,
    #[serde(default)]
    pub cube: Option<Box<Expression>>,
    #[serde(default)]
    pub rollup: Option<Box<Expression>>,
    #[serde(default)]
    pub totals: Option<Box<Expression>>,
    /// GROUP BY modifier: Some(true) = ALL, Some(false) = DISTINCT, None = no modifier
    #[serde(default)]
    pub all: Option<bool>,
}

/// Cube
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Cube {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Rollup
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Rollup {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// GroupingSets
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GroupingSets {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// LimitOptions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LimitOptions {
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
    #[serde(default)]
    pub rows: Option<Box<Expression>>,
    #[serde(default)]
    pub with_ties: Option<Box<Expression>>,
}

/// Lateral
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Lateral {
    pub this: Box<Expression>,
    #[serde(default)]
    pub view: Option<Box<Expression>>,
    #[serde(default)]
    pub outer: Option<Box<Expression>>,
    #[serde(default)]
    pub alias: Option<String>,
    /// Whether the alias was originally quoted (backtick/double-quote)
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub alias_quoted: bool,
    #[serde(default)]
    pub cross_apply: Option<Box<Expression>>,
    #[serde(default)]
    pub ordinality: Option<Box<Expression>>,
    /// Column aliases for the lateral expression (e.g., LATERAL func() AS alias(col1, col2))
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_aliases: Vec<String>,
}

/// TableFromRows
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TableFromRows {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub joins: Vec<Expression>,
    #[serde(default)]
    pub pivots: Option<Box<Expression>>,
    #[serde(default)]
    pub sample: Option<Box<Expression>>,
}

/// RowsFrom - PostgreSQL ROWS FROM (func1(args) AS alias1(...), func2(args) AS alias2(...)) syntax
/// Used for set-returning functions with typed column definitions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowsFrom {
    /// List of function expressions, each potentially with an alias and typed columns
    pub expressions: Vec<Expression>,
    /// WITH ORDINALITY modifier
    #[serde(default)]
    pub ordinality: bool,
    /// Optional outer alias: ROWS FROM (...) AS alias(col1 type1, col2 type2)
    #[serde(default)]
    pub alias: Option<Box<Expression>>,
}

/// WithFill
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithFill {
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
    #[serde(default)]
    pub interpolate: Option<Box<Expression>>,
}

/// Property
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Property {
    pub this: Box<Expression>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
}

/// GrantPrivilege
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GrantPrivilege {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AllowedValuesProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AllowedValuesProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AlgorithmProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlgorithmProperty {
    pub this: Box<Expression>,
}

/// AutoIncrementProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AutoIncrementProperty {
    pub this: Box<Expression>,
}

/// AutoRefreshProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AutoRefreshProperty {
    pub this: Box<Expression>,
}

/// BackupProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BackupProperty {
    pub this: Box<Expression>,
}

/// BuildProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BuildProperty {
    pub this: Box<Expression>,
}

/// BlockCompressionProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct BlockCompressionProperty {
    #[serde(default)]
    pub autotemp: Option<Box<Expression>>,
    #[serde(default)]
    pub always: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub manual: Option<Box<Expression>>,
    #[serde(default)]
    pub never: Option<Box<Expression>>,
}

/// CharacterSetProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CharacterSetProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// ChecksumProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ChecksumProperty {
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// CollateProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CollateProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// DataBlocksizeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DataBlocksizeProperty {
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub units: Option<Box<Expression>>,
    #[serde(default)]
    pub minimum: Option<Box<Expression>>,
    #[serde(default)]
    pub maximum: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
}

/// DataDeletionProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DataDeletionProperty {
    pub on: Box<Expression>,
    #[serde(default)]
    pub filter_column: Option<Box<Expression>>,
    #[serde(default)]
    pub retention_period: Option<Box<Expression>>,
}

/// DefinerProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DefinerProperty {
    pub this: Box<Expression>,
}

/// DistKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DistKeyProperty {
    pub this: Box<Expression>,
}

/// DistributedByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DistributedByProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    pub kind: String,
    #[serde(default)]
    pub buckets: Option<Box<Expression>>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}

/// DistStyleProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DistStyleProperty {
    pub this: Box<Expression>,
}

/// DuplicateKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DuplicateKeyProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// EngineProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EngineProperty {
    pub this: Box<Expression>,
}

/// ToTableProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToTableProperty {
    pub this: Box<Expression>,
}

/// ExecuteAsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExecuteAsProperty {
    pub this: Box<Expression>,
}

/// ExternalProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExternalProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// FallbackProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FallbackProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub protection: Option<Box<Expression>>,
}

/// FileFormatProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FileFormatProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub hive_format: Option<Box<Expression>>,
}

/// CredentialsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CredentialsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// FreespaceProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FreespaceProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
}

/// InheritsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct InheritsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// InputModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct InputModelProperty {
    pub this: Box<Expression>,
}

/// OutputModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OutputModelProperty {
    pub this: Box<Expression>,
}

/// IsolatedLoadingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IsolatedLoadingProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub concurrent: Option<Box<Expression>>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
}

/// JournalProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JournalProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub dual: Option<Box<Expression>>,
    #[serde(default)]
    pub before: Option<Box<Expression>>,
    #[serde(default)]
    pub local: Option<Box<Expression>>,
    #[serde(default)]
    pub after: Option<Box<Expression>>,
}

/// LanguageProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LanguageProperty {
    pub this: Box<Expression>,
}

/// EnviromentProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EnviromentProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ClusteredByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ClusteredByProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub sorted_by: Option<Box<Expression>>,
    #[serde(default)]
    pub buckets: Option<Box<Expression>>,
}

/// DictProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DictProperty {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub settings: Option<Box<Expression>>,
}

/// DictRange
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DictRange {
    pub this: Box<Expression>,
    #[serde(default)]
    pub min: Option<Box<Expression>>,
    #[serde(default)]
    pub max: Option<Box<Expression>>,
}

/// OnCluster
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OnCluster {
    pub this: Box<Expression>,
}

/// LikeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LikeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// LocationProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LocationProperty {
    pub this: Box<Expression>,
}

/// LockProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LockProperty {
    pub this: Box<Expression>,
}

/// LockingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LockingProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    pub kind: String,
    #[serde(default)]
    pub for_or_in: Option<Box<Expression>>,
    #[serde(default)]
    pub lock_type: Option<Box<Expression>>,
    #[serde(default)]
    pub override_: Option<Box<Expression>>,
}

/// LogProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct LogProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
}

/// MaterializedProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MaterializedProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// MergeBlockRatioProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MergeBlockRatioProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub default: Option<Box<Expression>>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
}

/// OnProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OnProperty {
    pub this: Box<Expression>,
}

/// OnCommitProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OnCommitProperty {
    #[serde(default)]
    pub delete: Option<Box<Expression>>,
}

/// PartitionedByProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionedByProperty {
    pub this: Box<Expression>,
}

/// PartitionedByBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionedByBucket {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionByTruncate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionByTruncate {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// PartitionByRangeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionByRangeProperty {
    #[serde(default)]
    pub partition_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub create_expressions: Option<Box<Expression>>,
}

/// PartitionByRangePropertyDynamic
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionByRangePropertyDynamic {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub every: Option<Box<Expression>>,
}

/// PartitionByListProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionByListProperty {
    #[serde(default)]
    pub partition_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub create_expressions: Option<Box<Expression>>,
}

/// PartitionList
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionList {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Partition - represents PARTITION/SUBPARTITION clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Partition {
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub subpartition: bool,
}

/// RefreshTriggerProperty - Doris REFRESH clause for materialized views
/// e.g., REFRESH COMPLETE ON MANUAL, REFRESH AUTO ON SCHEDULE EVERY 5 MINUTE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RefreshTriggerProperty {
    /// Method: COMPLETE or AUTO
    pub method: String,
    /// Trigger kind: MANUAL, COMMIT, or SCHEDULE
    #[serde(default)]
    pub kind: Option<String>,
    /// For SCHEDULE: EVERY n (the number)
    #[serde(default)]
    pub every: Option<Box<Expression>>,
    /// For SCHEDULE: the time unit (MINUTE, HOUR, DAY, etc.)
    #[serde(default)]
    pub unit: Option<String>,
    /// For SCHEDULE: STARTS 'datetime'
    #[serde(default)]
    pub starts: Option<Box<Expression>>,
}

/// UniqueKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UniqueKeyProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// RollupProperty - StarRocks ROLLUP (index_name(col1, col2), ...)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RollupProperty {
    pub expressions: Vec<RollupIndex>,
}

/// RollupIndex - A single rollup index: name(col1, col2)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RollupIndex {
    pub name: Identifier,
    pub expressions: Vec<Identifier>,
}

/// PartitionBoundSpec
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionBoundSpec {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub from_expressions: Option<Box<Expression>>,
    #[serde(default)]
    pub to_expressions: Option<Box<Expression>>,
}

/// PartitionedOfProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PartitionedOfProperty {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RemoteWithConnectionModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RemoteWithConnectionModelProperty {
    pub this: Box<Expression>,
}

/// ReturnsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ReturnsProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub is_table: Option<Box<Expression>>,
    #[serde(default)]
    pub table: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// RowFormatProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowFormatProperty {
    pub this: Box<Expression>,
}

/// RowFormatDelimitedProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowFormatDelimitedProperty {
    #[serde(default)]
    pub fields: Option<Box<Expression>>,
    #[serde(default)]
    pub escaped: Option<Box<Expression>>,
    #[serde(default)]
    pub collection_items: Option<Box<Expression>>,
    #[serde(default)]
    pub map_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub lines: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
    #[serde(default)]
    pub serde: Option<Box<Expression>>,
}

/// RowFormatSerdeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RowFormatSerdeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub serde_properties: Option<Box<Expression>>,
}

/// QueryTransform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct QueryTransform {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub command_script: Option<Box<Expression>>,
    #[serde(default)]
    pub schema: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format_before: Option<Box<Expression>>,
    #[serde(default)]
    pub record_writer: Option<Box<Expression>>,
    #[serde(default)]
    pub row_format_after: Option<Box<Expression>>,
    #[serde(default)]
    pub record_reader: Option<Box<Expression>>,
}

/// SampleProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SampleProperty {
    pub this: Box<Expression>,
}

/// SecurityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SecurityProperty {
    pub this: Box<Expression>,
}

/// SchemaCommentProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SchemaCommentProperty {
    pub this: Box<Expression>,
}

/// SemanticView
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SemanticView {
    pub this: Box<Expression>,
    #[serde(default)]
    pub metrics: Option<Box<Expression>>,
    #[serde(default)]
    pub dimensions: Option<Box<Expression>>,
    #[serde(default)]
    pub facts: Option<Box<Expression>>,
    #[serde(default)]
    pub where_: Option<Box<Expression>>,
}

/// SerdeProperties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SerdeProperties {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
}

/// SetProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SetProperty {
    #[serde(default)]
    pub multi: Option<Box<Expression>>,
}

/// SharingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SharingProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// SetConfigProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SetConfigProperty {
    pub this: Box<Expression>,
}

/// SettingsProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SettingsProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// SortKeyProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SortKeyProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub compound: Option<Box<Expression>>,
}

/// SqlReadWriteProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SqlReadWriteProperty {
    pub this: Box<Expression>,
}

/// SqlSecurityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SqlSecurityProperty {
    pub this: Box<Expression>,
}

/// StabilityProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StabilityProperty {
    pub this: Box<Expression>,
}

/// StorageHandlerProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StorageHandlerProperty {
    pub this: Box<Expression>,
}

/// TemporaryProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TemporaryProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Tags
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Tags {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TransformModelProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TransformModelProperty {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TransientProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TransientProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// UsingTemplateProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UsingTemplateProperty {
    pub this: Box<Expression>,
}

/// ViewAttributeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ViewAttributeProperty {
    pub this: Box<Expression>,
}

/// VolatileProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct VolatileProperty {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// WithDataProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithDataProperty {
    #[serde(default)]
    pub no: Option<Box<Expression>>,
    #[serde(default)]
    pub statistics: Option<Box<Expression>>,
}

/// WithJournalTableProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithJournalTableProperty {
    pub this: Box<Expression>,
}

/// WithSchemaBindingProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithSchemaBindingProperty {
    pub this: Box<Expression>,
}

/// WithSystemVersioningProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithSystemVersioningProperty {
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub data_consistency: Option<Box<Expression>>,
    #[serde(default)]
    pub retention_period: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
}

/// WithProcedureOptions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithProcedureOptions {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// EncodeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EncodeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub properties: Vec<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
}

/// IncludeProperty
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IncludeProperty {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub column_def: Option<Box<Expression>>,
}

/// Properties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Properties {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// InputOutputFormat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct InputOutputFormat {
    #[serde(default)]
    pub input_format: Option<Box<Expression>>,
    #[serde(default)]
    pub output_format: Option<Box<Expression>>,
}

/// Reference
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Reference {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// QueryOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct QueryOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// WithTableHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WithTableHint {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// IndexTableHint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IndexTableHint {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
}

/// Get
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Get {
    pub this: Box<Expression>,
    #[serde(default)]
    pub target: Option<Box<Expression>>,
    #[serde(default)]
    pub properties: Vec<Expression>,
}

/// SetOperation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SetOperation {
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub by_name: Option<Box<Expression>>,
    #[serde(default)]
    pub side: Option<Box<Expression>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
}

/// Var - Simple variable reference (for SQL variables, keywords as values)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Var {
    pub this: String,
}

/// Version
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Version {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Schema {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Lock
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Lock {
    #[serde(default)]
    pub update: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub wait: Option<Box<Expression>>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
}

/// TableSample - wraps an expression with a TABLESAMPLE clause
/// Used when TABLESAMPLE follows a non-Table expression (subquery, function, etc.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TableSample {
    /// The expression being sampled (subquery, function, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub this: Option<Box<Expression>>,
    /// The sample specification
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample: Option<Box<Sample>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub bucket_numerator: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_denominator: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_field: Option<Box<Expression>>,
    #[serde(default)]
    pub percent: Option<Box<Expression>>,
    #[serde(default)]
    pub rows: Option<Box<Expression>>,
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub seed: Option<Box<Expression>>,
}

/// Tags are used for generating arbitrary sql like SELECT <span>x</span>.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Tag {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub prefix: Option<Box<Expression>>,
    #[serde(default)]
    pub postfix: Option<Box<Expression>>,
}

/// UnpivotColumns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnpivotColumns {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// SessionParameter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SessionParameter {
    pub this: Box<Expression>,
    #[serde(default)]
    pub kind: Option<String>,
}

/// PseudoType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PseudoType {
    pub this: Box<Expression>,
}

/// ObjectIdentifier
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ObjectIdentifier {
    pub this: Box<Expression>,
}

/// Transaction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Transaction {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub modes: Option<Box<Expression>>,
    #[serde(default)]
    pub mark: Option<Box<Expression>>,
}

/// Commit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Commit {
    #[serde(default)]
    pub chain: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub durability: Option<Box<Expression>>,
}

/// Rollback
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Rollback {
    #[serde(default)]
    pub savepoint: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// AlterSession
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AlterSession {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub unset: Option<Box<Expression>>,
}

/// Analyze
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Analyze {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
    #[serde(default)]
    pub partition: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub properties: Vec<Expression>,
    /// Column list for ANALYZE tbl(col1, col2) syntax (PostgreSQL)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

/// AnalyzeStatistics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeStatistics {
    pub kind: String,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnalyzeHistogram
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeHistogram {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub update_options: Option<Box<Expression>>,
}

/// AnalyzeSample
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeSample {
    pub kind: String,
    #[serde(default)]
    pub sample: Option<Box<Expression>>,
}

/// AnalyzeListChainedRows
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeListChainedRows {
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// AnalyzeDelete
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeDelete {
    #[serde(default)]
    pub kind: Option<String>,
}

/// AnalyzeWith
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeWith {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnalyzeValidate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnalyzeValidate {
    pub kind: String,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// AddPartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AddPartition {
    pub this: Box<Expression>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub location: Option<Box<Expression>>,
}

/// AttachOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AttachOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// DropPartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DropPartition {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub exists: bool,
}

/// ReplacePartition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ReplacePartition {
    pub expression: Box<Expression>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
}

/// DPipe
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DPipe {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Operator {
    pub this: Box<Expression>,
    #[serde(default)]
    pub operator: Option<Box<Expression>>,
    pub expression: Box<Expression>,
    /// Comments between OPERATOR() and the RHS expression
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub comments: Vec<String>,
}

/// PivotAny
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PivotAny {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Aliases
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Aliases {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AtIndex
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AtIndex {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// FromTimeZone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FromTimeZone {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Format override for a column in Teradata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FormatPhrase {
    pub this: Box<Expression>,
    pub format: String,
}

/// ForIn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ForIn {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Automatically converts unit arg into a var.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeUnit {
    #[serde(default)]
    pub unit: Option<String>,
}

/// IntervalOp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct IntervalOp {
    #[serde(default)]
    pub unit: Option<String>,
    pub expression: Box<Expression>,
}

/// HavingMax
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct HavingMax {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub max: Option<Box<Expression>>,
}

/// CosineDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CosineDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// DotProduct
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DotProduct {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// EuclideanDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EuclideanDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ManhattanDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ManhattanDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JarowinklerSimilarity
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JarowinklerSimilarity {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Booland
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Booland {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Boolor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Boolor {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ParameterizedAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParameterizedAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// ArgMax
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArgMax {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ArgMin
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArgMin {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ApproxTopK
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxTopK {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub counters: Option<Box<Expression>>,
}

/// ApproxTopKAccumulate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxTopKAccumulate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopKCombine
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxTopKCombine {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopKEstimate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxTopKEstimate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ApproxTopSum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxTopSum {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// ApproxQuantiles
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxQuantiles {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Minhash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Minhash {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// FarmFingerprint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FarmFingerprint {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Float64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Float64 {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Transform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Transform {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Translate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Translate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub from_: Option<Box<Expression>>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
}

/// Grouping
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Grouping {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// GroupingId
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GroupingId {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Anonymous
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Anonymous {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// AnonymousAggFunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AnonymousAggFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// CombinedAggFunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CombinedAggFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// CombinedParameterizedAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CombinedParameterizedAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub params: Vec<Expression>,
}

/// HashAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct HashAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Hll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Hll {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Apply
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Apply {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ToBoolean
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToBoolean {
    pub this: Box<Expression>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// List
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct List {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ToMap - Materialize-style map constructor
/// Can hold either:
/// - A SELECT subquery (MAP(SELECT 'a', 1))
/// - A struct with key=>value entries (MAP['a' => 1, 'b' => 2])
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToMap {
    /// Either a Select subquery or a Struct containing PropertyEQ entries
    pub this: Box<Expression>,
}

/// Pad
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Pad {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub fill_pattern: Option<Box<Expression>>,
    #[serde(default)]
    pub is_left: Option<Box<Expression>>,
}

/// ToChar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToChar {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub nlsparam: Option<Box<Expression>>,
    #[serde(default)]
    pub is_numeric: Option<Box<Expression>>,
}

/// StringFunc - String type conversion function (BigQuery STRING)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StringFunc {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// ToNumber
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToNumber {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<Box<Expression>>,
    #[serde(default)]
    pub nlsparam: Option<Box<Expression>>,
    #[serde(default)]
    pub precision: Option<Box<Expression>>,
    #[serde(default)]
    pub scale: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
    #[serde(default)]
    pub safe_name: Option<Box<Expression>>,
}

/// ToDouble
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToDouble {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// ToDecfloat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToDecfloat {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// TryToDecfloat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TryToDecfloat {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// ToFile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToFile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Columns {
    pub this: Box<Expression>,
    #[serde(default)]
    pub unpack: Option<Box<Expression>>,
}

/// ConvertToCharset
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConvertToCharset {
    pub this: Box<Expression>,
    #[serde(default)]
    pub dest: Option<Box<Expression>>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
}

/// ConvertTimezone
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ConvertTimezone {
    #[serde(default)]
    pub source_tz: Option<Box<Expression>>,
    #[serde(default)]
    pub target_tz: Option<Box<Expression>>,
    #[serde(default)]
    pub timestamp: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// GenerateSeries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GenerateSeries {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
    #[serde(default)]
    pub is_end_exclusive: Option<Box<Expression>>,
}

/// AIAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AIAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// AIClassify
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct AIClassify {
    pub this: Box<Expression>,
    #[serde(default)]
    pub categories: Option<Box<Expression>>,
    #[serde(default)]
    pub config: Option<Box<Expression>>,
}

/// ArrayAll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayAll {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ArrayAny
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayAny {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ArrayConstructCompact
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArrayConstructCompact {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// StPoint
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StPoint {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// StDistance
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StDistance {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub use_spheroid: Option<Box<Expression>>,
}

/// StringToArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StringToArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub null: Option<Box<Expression>>,
}

/// ArraySum
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ArraySum {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ObjectAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CastToStrType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CastToStrType {
    pub this: Box<Expression>,
    #[serde(default)]
    pub to: Option<Box<Expression>>,
}

/// CheckJson
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CheckJson {
    pub this: Box<Expression>,
}

/// CheckXml
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CheckXml {
    pub this: Box<Expression>,
    #[serde(default)]
    pub disable_auto_convert: Option<Box<Expression>>,
}

/// TranslateCharacters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TranslateCharacters {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub with_error: Option<Box<Expression>>,
}

/// CurrentSchemas
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentSchemas {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentDatetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentDatetime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Localtime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Localtime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Localtimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Localtimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Systimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Systimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentSchema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentSchema {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// CurrentUser
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CurrentUser {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// SessionUser - MySQL/PostgreSQL SESSION_USER function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SessionUser;

/// JSONPathRoot - Represents $ in JSON path expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathRoot;

/// UtcTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UtcTime {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// UtcTimestamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UtcTimestamp {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// TimestampFunc - TIMESTAMP constructor function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampFunc {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub with_tz: Option<bool>,
    #[serde(default)]
    pub safe: Option<bool>,
}

/// DateBin
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateBin {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub origin: Option<Box<Expression>>,
}

/// Datetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Datetime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// DatetimeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DatetimeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DatetimeSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DatetimeDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// DatetimeTrunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DatetimeTrunc {
    pub this: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Dayname
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Dayname {
    pub this: Box<Expression>,
    #[serde(default)]
    pub abbreviated: Option<Box<Expression>>,
}

/// MakeInterval
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MakeInterval {
    #[serde(default)]
    pub year: Option<Box<Expression>>,
    #[serde(default)]
    pub month: Option<Box<Expression>>,
    #[serde(default)]
    pub week: Option<Box<Expression>>,
    #[serde(default)]
    pub day: Option<Box<Expression>>,
    #[serde(default)]
    pub hour: Option<Box<Expression>>,
    #[serde(default)]
    pub minute: Option<Box<Expression>>,
    #[serde(default)]
    pub second: Option<Box<Expression>>,
}

/// PreviousDay
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct PreviousDay {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Elt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Elt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// TimestampAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimestampSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimestampDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeSlice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeSlice {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub kind: Option<String>,
}

/// TimeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeSub
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeSub {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TimeTrunc
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeTrunc {
    pub this: Box<Expression>,
    pub unit: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// DateFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DateFromParts {
    #[serde(default)]
    pub year: Option<Box<Expression>>,
    #[serde(default)]
    pub month: Option<Box<Expression>>,
    #[serde(default)]
    pub day: Option<Box<Expression>>,
    #[serde(default)]
    pub allow_overflow: Option<Box<Expression>>,
}

/// TimeFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeFromParts {
    #[serde(default)]
    pub hour: Option<Box<Expression>>,
    #[serde(default)]
    pub min: Option<Box<Expression>>,
    #[serde(default)]
    pub sec: Option<Box<Expression>>,
    #[serde(default)]
    pub nano: Option<Box<Expression>>,
    #[serde(default)]
    pub fractions: Option<Box<Expression>>,
    #[serde(default)]
    pub precision: Option<i64>,
}

/// DecodeCase
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DecodeCase {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Decrypt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Decrypt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub passphrase: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// DecryptRaw
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DecryptRaw {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub iv: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
    #[serde(default)]
    pub aead: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Encode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Encode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub charset: Option<Box<Expression>>,
}

/// Encrypt
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Encrypt {
    pub this: Box<Expression>,
    #[serde(default)]
    pub passphrase: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
}

/// EncryptRaw
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EncryptRaw {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub iv: Option<Box<Expression>>,
    #[serde(default)]
    pub aad: Option<Box<Expression>>,
    #[serde(default)]
    pub encryption_method: Option<Box<Expression>>,
}

/// EqualNull
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct EqualNull {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ToBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ToBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Base64DecodeBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Base64DecodeBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// Base64DecodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Base64DecodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// Base64Encode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Base64Encode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub max_line_length: Option<Box<Expression>>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// TryBase64DecodeBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TryBase64DecodeBinary {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// TryBase64DecodeString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TryBase64DecodeString {
    pub this: Box<Expression>,
    #[serde(default)]
    pub alphabet: Option<Box<Expression>>,
}

/// GapFill
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GapFill {
    pub this: Box<Expression>,
    #[serde(default)]
    pub ts_column: Option<Box<Expression>>,
    #[serde(default)]
    pub bucket_width: Option<Box<Expression>>,
    #[serde(default)]
    pub partitioning_columns: Option<Box<Expression>>,
    #[serde(default)]
    pub value_columns: Option<Box<Expression>>,
    #[serde(default)]
    pub origin: Option<Box<Expression>>,
    #[serde(default)]
    pub ignore_nulls: Option<Box<Expression>>,
}

/// GenerateDateArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GenerateDateArray {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// GenerateTimestampArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GenerateTimestampArray {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// GetExtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GetExtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Getbit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Getbit {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub zero_is_msb: Option<Box<Expression>>,
}

/// OverflowTruncateBehavior
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OverflowTruncateBehavior {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub with_count: Option<Box<Expression>>,
}

/// HexEncode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct HexEncode {
    pub this: Box<Expression>,
    #[serde(default)]
    pub case: Option<Box<Expression>>,
}

/// Compress
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Compress {
    pub this: Box<Expression>,
    #[serde(default)]
    pub method: Option<String>,
}

/// DecompressBinary
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DecompressBinary {
    pub this: Box<Expression>,
    pub method: String,
}

/// DecompressString
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct DecompressString {
    pub this: Box<Expression>,
    pub method: String,
}

/// Xor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Xor {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Nullif
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Nullif {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSON {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    #[serde(default)]
    pub unique: bool,
}

/// JSONPath
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPath {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub escape: Option<Box<Expression>>,
}

/// JSONPathFilter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathFilter {
    pub this: Box<Expression>,
}

/// JSONPathKey
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathKey {
    pub this: Box<Expression>,
}

/// JSONPathRecursive
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathRecursive {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// JSONPathScript
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathScript {
    pub this: Box<Expression>,
}

/// JSONPathSlice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathSlice {
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub end: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// JSONPathSelector
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathSelector {
    pub this: Box<Expression>,
}

/// JSONPathSubscript
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathSubscript {
    pub this: Box<Expression>,
}

/// JSONPathUnion
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONPathUnion {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Format
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Format {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONKeys
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONKeys {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONKeyValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONKeyValue {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSONKeysAtDepth
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONKeysAtDepth {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
}

/// JSONObject
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONObject {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub unique_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub encoding: Option<Box<Expression>>,
}

/// JSONObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONObjectAgg {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub unique_keys: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub encoding: Option<Box<Expression>>,
}

/// JSONBObjectAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONBObjectAgg {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// JSONArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONArray {
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub strict: Option<Box<Expression>>,
}

/// JSONArrayAgg
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONArrayAgg {
    pub this: Box<Expression>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
    #[serde(default)]
    pub null_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
    #[serde(default)]
    pub strict: Option<Box<Expression>>,
}

/// JSONExists
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONExists {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub passing: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
    #[serde(default)]
    pub from_dcolonqmark: Option<Box<Expression>>,
}

/// JSONColumnDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONColumnDef {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub nested_schema: Option<Box<Expression>>,
    #[serde(default)]
    pub ordinality: Option<Box<Expression>>,
}

/// JSONSchema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONSchema {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONSet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONSet {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONStripNulls
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONStripNulls {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub include_arrays: Option<Box<Expression>>,
    #[serde(default)]
    pub remove_empty: Option<Box<Expression>>,
}

/// JSONValue
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONValue {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub returning: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
}

/// JSONValueArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONValueArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// JSONRemove
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONRemove {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONTable {
    pub this: Box<Expression>,
    #[serde(default)]
    pub schema: Option<Box<Expression>>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub error_handling: Option<Box<Expression>>,
    #[serde(default)]
    pub empty_handling: Option<Box<Expression>>,
}

/// JSONType
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONType {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// ObjectInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ObjectInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
    #[serde(default)]
    pub update_flag: Option<Box<Expression>>,
}

/// OpenJSONColumnDef
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OpenJSONColumnDef {
    pub this: Box<Expression>,
    pub kind: String,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub as_json: Option<Box<Expression>>,
    /// The parsed data type for proper generation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<DataType>,
}

/// OpenJSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct OpenJSON {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONBExists
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONBExists {
    pub this: Box<Expression>,
    #[serde(default)]
    pub path: Option<Box<Expression>>,
}

/// JSONCast
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONCast {
    pub this: Box<Expression>,
    pub to: DataType,
}

/// JSONExtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONExtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub only_json_types: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub variant_extract: Option<Box<Expression>>,
    #[serde(default)]
    pub json_query: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub quote: Option<Box<Expression>>,
    #[serde(default)]
    pub on_condition: Option<Box<Expression>>,
    #[serde(default)]
    pub requires_json: Option<Box<Expression>>,
}

/// JSONExtractQuote
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONExtractQuote {
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub scalar: Option<Box<Expression>>,
}

/// JSONExtractArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONExtractArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// JSONExtractScalar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONExtractScalar {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub only_json_types: Option<Box<Expression>>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
    #[serde(default)]
    pub scalar_only: Option<Box<Expression>>,
}

/// JSONBExtractScalar
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONBExtractScalar {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
}

/// JSONFormat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONFormat {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
    #[serde(default)]
    pub is_json: Option<Box<Expression>>,
    #[serde(default)]
    pub to_json: Option<Box<Expression>>,
}

/// JSONArrayAppend
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONArrayAppend {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// JSONArrayContains
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONArrayContains {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_type: Option<Box<Expression>>,
}

/// JSONArrayInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct JSONArrayInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ParseJSON
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParseJSON {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// ParseUrl
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParseUrl {
    pub this: Box<Expression>,
    #[serde(default)]
    pub part_to_extract: Option<Box<Expression>>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub permissive: Option<Box<Expression>>,
}

/// ParseIp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParseIp {
    pub this: Box<Expression>,
    #[serde(default)]
    pub type_: Option<Box<Expression>>,
    #[serde(default)]
    pub permissive: Option<Box<Expression>>,
}

/// ParseTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParseTime {
    pub this: Box<Expression>,
    pub format: String,
}

/// ParseDatetime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ParseDatetime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Map
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Map {
    #[serde(default)]
    pub keys: Vec<Expression>,
    #[serde(default)]
    pub values: Vec<Expression>,
}

/// MapCat
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MapCat {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// MapDelete
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MapDelete {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// MapInsert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MapInsert {
    pub this: Box<Expression>,
    #[serde(default)]
    pub key: Option<Box<Expression>>,
    #[serde(default)]
    pub value: Option<Box<Expression>>,
    #[serde(default)]
    pub update_flag: Option<Box<Expression>>,
}

/// MapPick
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MapPick {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ScopeResolution
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ScopeResolution {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    pub expression: Box<Expression>,
}

/// Slice
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Slice {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub step: Option<Box<Expression>>,
}

/// VarMap
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct VarMap {
    #[serde(default)]
    pub keys: Vec<Expression>,
    #[serde(default)]
    pub values: Vec<Expression>,
}

/// MatchAgainst
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MatchAgainst {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub modifier: Option<Box<Expression>>,
}

/// MD5Digest
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MD5Digest {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Monthname
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Monthname {
    pub this: Box<Expression>,
    #[serde(default)]
    pub abbreviated: Option<Box<Expression>>,
}

/// Ntile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Ntile {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Normalize
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Normalize {
    pub this: Box<Expression>,
    #[serde(default)]
    pub form: Option<Box<Expression>>,
    #[serde(default)]
    pub is_casefold: Option<Box<Expression>>,
}

/// Normal
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Normal {
    pub this: Box<Expression>,
    #[serde(default)]
    pub stddev: Option<Box<Expression>>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
}

/// Predict
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Predict {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// MLTranslate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MLTranslate {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// FeaturesAtTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FeaturesAtTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub time: Option<Box<Expression>>,
    #[serde(default)]
    pub num_rows: Option<Box<Expression>>,
    #[serde(default)]
    pub ignore_feature_nulls: Option<Box<Expression>>,
}

/// GenerateEmbedding
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct GenerateEmbedding {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
    #[serde(default)]
    pub is_text: Option<Box<Expression>>,
}

/// MLForecast
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct MLForecast {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
    #[serde(default)]
    pub params_struct: Option<Box<Expression>>,
}

/// ModelAttribute
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ModelAttribute {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// VectorSearch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct VectorSearch {
    pub this: Box<Expression>,
    #[serde(default)]
    pub column_to_search: Option<Box<Expression>>,
    #[serde(default)]
    pub query_table: Option<Box<Expression>>,
    #[serde(default)]
    pub query_column_to_search: Option<Box<Expression>>,
    #[serde(default)]
    pub top_k: Option<Box<Expression>>,
    #[serde(default)]
    pub distance_type: Option<Box<Expression>>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// Quantile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Quantile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub quantile: Option<Box<Expression>>,
}

/// ApproxQuantile
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxQuantile {
    pub this: Box<Expression>,
    #[serde(default)]
    pub quantile: Option<Box<Expression>>,
    #[serde(default)]
    pub accuracy: Option<Box<Expression>>,
    #[serde(default)]
    pub weight: Option<Box<Expression>>,
    #[serde(default)]
    pub error_tolerance: Option<Box<Expression>>,
}

/// ApproxPercentileEstimate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ApproxPercentileEstimate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub percentile: Option<Box<Expression>>,
}

/// Randn
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Randn {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
}

/// Randstr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Randstr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub generator: Option<Box<Expression>>,
}

/// RangeN
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RangeN {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub each: Option<Box<Expression>>,
}

/// RangeBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RangeBucket {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// ReadCSV
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ReadCSV {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// ReadParquet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ReadParquet {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// Reduce
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Reduce {
    pub this: Box<Expression>,
    #[serde(default)]
    pub initial: Option<Box<Expression>>,
    #[serde(default)]
    pub merge: Option<Box<Expression>>,
    #[serde(default)]
    pub finish: Option<Box<Expression>>,
}

/// RegexpExtractAll
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpExtractAll {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
}

/// RegexpILike
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpILike {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub flag: Option<Box<Expression>>,
}

/// RegexpFullMatch
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpFullMatch {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub options: Vec<Expression>,
}

/// RegexpInstr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpInstr {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
    #[serde(default)]
    pub option: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
    #[serde(default)]
    pub group: Option<Box<Expression>>,
}

/// RegexpSplit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpSplit {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub limit: Option<Box<Expression>>,
}

/// RegexpCount
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegexpCount {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub parameters: Option<Box<Expression>>,
}

/// RegrValx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrValx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrValy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrValy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrAvgy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrAvgy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrAvgx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrAvgx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrCount
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrCount {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrIntercept
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrIntercept {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrR2
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrR2 {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSxx
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrSxx {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSxy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrSxy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSyy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrSyy {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// RegrSlope
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct RegrSlope {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SafeAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeDivide
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SafeDivide {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeMultiply
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SafeMultiply {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SafeSubtract
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SafeSubtract {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// SHA2
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SHA2 {
    pub this: Box<Expression>,
    #[serde(default)]
    pub length: Option<i64>,
}

/// SHA2Digest
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SHA2Digest {
    pub this: Box<Expression>,
    #[serde(default)]
    pub length: Option<i64>,
}

/// SortArray
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SortArray {
    pub this: Box<Expression>,
    #[serde(default)]
    pub asc: Option<Box<Expression>>,
    #[serde(default)]
    pub nulls_first: Option<Box<Expression>>,
}

/// SplitPart
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SplitPart {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delimiter: Option<Box<Expression>>,
    #[serde(default)]
    pub part_index: Option<Box<Expression>>,
}

/// SUBSTRING_INDEX(str, delim, count)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SubstringIndex {
    pub this: Box<Expression>,
    #[serde(default)]
    pub delimiter: Option<Box<Expression>>,
    #[serde(default)]
    pub count: Option<Box<Expression>>,
}

/// StandardHash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StandardHash {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// StrPosition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StrPosition {
    pub this: Box<Expression>,
    #[serde(default)]
    pub substr: Option<Box<Expression>>,
    #[serde(default)]
    pub position: Option<Box<Expression>>,
    #[serde(default)]
    pub occurrence: Option<Box<Expression>>,
}

/// Search
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Search {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub json_scope: Option<Box<Expression>>,
    #[serde(default)]
    pub analyzer: Option<Box<Expression>>,
    #[serde(default)]
    pub analyzer_options: Option<Box<Expression>>,
    #[serde(default)]
    pub search_mode: Option<Box<Expression>>,
}

/// SearchIp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct SearchIp {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// StrToDate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StrToDate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// StrToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StrToTime {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
    #[serde(default)]
    pub target_type: Option<Box<Expression>>,
}

/// StrToUnix
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StrToUnix {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub format: Option<String>,
}

/// StrToMap
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct StrToMap {
    pub this: Box<Expression>,
    #[serde(default)]
    pub pair_delim: Option<Box<Expression>>,
    #[serde(default)]
    pub key_value_delim: Option<Box<Expression>>,
    #[serde(default)]
    pub duplicate_resolution_callback: Option<Box<Expression>>,
}

/// NumberToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NumberToStr {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub culture: Option<Box<Expression>>,
}

/// FromBase
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct FromBase {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Stuff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Stuff {
    pub this: Box<Expression>,
    #[serde(default)]
    pub start: Option<Box<Expression>>,
    #[serde(default)]
    pub length: Option<i64>,
    pub expression: Box<Expression>,
}

/// TimeToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeToStr {
    pub this: Box<Expression>,
    pub format: String,
    #[serde(default)]
    pub culture: Option<Box<Expression>>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// TimeStrToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimeStrToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// TsOrDsAdd
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TsOrDsAdd {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub return_type: Option<Box<Expression>>,
}

/// TsOrDsDiff
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TsOrDsDiff {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// TsOrDsToDate
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TsOrDsToDate {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// TsOrDsToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TsOrDsToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub safe: Option<Box<Expression>>,
}

/// Unhex
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Unhex {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Uniform
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Uniform {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
    #[serde(default)]
    pub seed: Option<Box<Expression>>,
}

/// UnixToStr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnixToStr {
    pub this: Box<Expression>,
    #[serde(default)]
    pub format: Option<String>,
}

/// UnixToTime
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct UnixToTime {
    pub this: Box<Expression>,
    #[serde(default)]
    pub scale: Option<i64>,
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub hours: Option<Box<Expression>>,
    #[serde(default)]
    pub minutes: Option<Box<Expression>>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub target_type: Option<Box<Expression>>,
}

/// Uuid
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Uuid {
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub is_string: Option<Box<Expression>>,
}

/// TimestampFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampFromParts {
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
    #[serde(default)]
    pub milli: Option<Box<Expression>>,
    #[serde(default)]
    pub this: Option<Box<Expression>>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// TimestampTzFromParts
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct TimestampTzFromParts {
    #[serde(default)]
    pub zone: Option<Box<Expression>>,
}

/// Corr
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Corr {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub null_on_zero_variance: Option<Box<Expression>>,
}

/// WidthBucket
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct WidthBucket {
    pub this: Box<Expression>,
    #[serde(default)]
    pub min_value: Option<Box<Expression>>,
    #[serde(default)]
    pub max_value: Option<Box<Expression>>,
    #[serde(default)]
    pub num_buckets: Option<Box<Expression>>,
    #[serde(default)]
    pub threshold: Option<Box<Expression>>,
}

/// CovarSamp
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CovarSamp {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// CovarPop
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct CovarPop {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
}

/// Week
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Week {
    pub this: Box<Expression>,
    #[serde(default)]
    pub mode: Option<Box<Expression>>,
}

/// XMLElement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct XMLElement {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expressions: Vec<Expression>,
    #[serde(default)]
    pub evalname: Option<Box<Expression>>,
}

/// XMLGet
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct XMLGet {
    pub this: Box<Expression>,
    pub expression: Box<Expression>,
    #[serde(default)]
    pub instance: Option<Box<Expression>>,
}

/// XMLTable
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct XMLTable {
    pub this: Box<Expression>,
    #[serde(default)]
    pub namespaces: Option<Box<Expression>>,
    #[serde(default)]
    pub passing: Option<Box<Expression>>,
    #[serde(default)]
    pub columns: Vec<Expression>,
    #[serde(default)]
    pub by_ref: Option<Box<Expression>>,
}

/// XMLKeyValueOption
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct XMLKeyValueOption {
    pub this: Box<Expression>,
    #[serde(default)]
    pub expression: Option<Box<Expression>>,
}

/// Zipf
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Zipf {
    pub this: Box<Expression>,
    #[serde(default)]
    pub elementcount: Option<Box<Expression>>,
    #[serde(default)]
    pub gen: Option<Box<Expression>>,
}

/// Merge
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Merge {
    pub this: Box<Expression>,
    pub using: Box<Expression>,
    #[serde(default)]
    pub on: Option<Box<Expression>>,
    #[serde(default)]
    pub using_cond: Option<Box<Expression>>,
    #[serde(default)]
    pub whens: Option<Box<Expression>>,
    #[serde(default)]
    pub with_: Option<Box<Expression>>,
    #[serde(default)]
    pub returning: Option<Box<Expression>>,
}

/// When
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct When {
    #[serde(default)]
    pub matched: Option<Box<Expression>>,
    #[serde(default)]
    pub source: Option<Box<Expression>>,
    #[serde(default)]
    pub condition: Option<Box<Expression>>,
    pub then: Box<Expression>,
}

/// Wraps around one or more WHEN [NOT] MATCHED [...] clauses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Whens {
    #[serde(default)]
    pub expressions: Vec<Expression>,
}

/// NextValueFor
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct NextValueFor {
    pub this: Box<Expression>,
    #[serde(default)]
    pub order: Option<Box<Expression>>,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "bindings")]
    fn export_typescript_types() {
        // This test exports TypeScript types to the generated directory
        // Run with: cargo test -p polyglot-sql --features bindings export_typescript_types
        Expression::export_all(&ts_rs::Config::default()).expect("Failed to export Expression types");
    }

    #[test]
    fn test_simple_select_builder() {
        let select = Select::new()
            .column(Expression::star())
            .from(Expression::Table(TableRef::new("users")));

        assert_eq!(select.expressions.len(), 1);
        assert!(select.from.is_some());
    }

    #[test]
    fn test_expression_alias() {
        let expr = Expression::column("id").alias("user_id");

        match expr {
            Expression::Alias(a) => {
                assert_eq!(a.alias.name, "user_id");
            }
            _ => panic!("Expected Alias"),
        }
    }

    #[test]
    fn test_literal_creation() {
        let num = Expression::number(42);
        let str = Expression::string("hello");

        match num {
            Expression::Literal(Literal::Number(n)) => assert_eq!(n, "42"),
            _ => panic!("Expected Number"),
        }

        match str {
            Expression::Literal(Literal::String(s)) => assert_eq!(s, "hello"),
            _ => panic!("Expected String"),
        }
    }

    #[test]
    fn test_expression_sql() {
        let expr = crate::parse_one("SELECT 1 + 2", crate::DialectType::Generic).unwrap();
        assert_eq!(expr.sql(), "SELECT 1 + 2");
    }

    #[test]
    fn test_expression_sql_for() {
        let expr = crate::parse_one("SELECT IF(x > 0, 1, 0)", crate::DialectType::Generic).unwrap();
        let sql = expr.sql_for(crate::DialectType::Generic);
        // Generic mode normalizes IF() to CASE WHEN
        assert!(sql.contains("CASE WHEN"), "Expected CASE WHEN in: {}", sql);
    }
}
