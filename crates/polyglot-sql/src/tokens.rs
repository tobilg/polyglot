//! Token types and tokenization for SQL parsing
//!
//! This module defines all SQL token types and the tokenizer that converts
//! SQL strings into token streams.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Parse a DollarString token text into (tag, content).
/// If the text contains '\x00', the part before is the tag and after is content.
/// Otherwise, the whole text is the content with no tag.
pub fn parse_dollar_string_token(text: &str) -> (Option<String>, String) {
    if let Some(pos) = text.find('\x00') {
        let tag = &text[..pos];
        let content = &text[pos + 1..];
        (Some(tag.to_string()), content.to_string())
    } else {
        (None, text.to_string())
    }
}

/// Represents a position in the source SQL
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Span {
    /// Starting byte offset
    pub start: usize,
    /// Ending byte offset (exclusive)
    pub end: usize,
    /// Line number (1-based)
    pub line: usize,
    /// Column number (1-based)
    pub column: usize,
}

impl Span {
    pub fn new(start: usize, end: usize, line: usize, column: usize) -> Self {
        Self { start, end, line, column }
    }
}

/// A token in the SQL token stream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Token {
    /// The type of token
    pub token_type: TokenType,
    /// The raw text of the token
    pub text: String,
    /// Position information
    pub span: Span,
    /// Leading comments (comments that appeared before this token)
    #[serde(default)]
    pub comments: Vec<String>,
    /// Trailing comments (comments that appeared after this token, before the next one)
    #[serde(default)]
    pub trailing_comments: Vec<String>,
}

impl Token {
    /// Create a new token
    pub fn new(token_type: TokenType, text: impl Into<String>, span: Span) -> Self {
        Self {
            token_type,
            text: text.into(),
            span,
            comments: Vec::new(),
            trailing_comments: Vec::new(),
        }
    }

    /// Create a NUMBER token
    pub fn number(n: i64) -> Self {
        Self::new(TokenType::Number, n.to_string(), Span::default())
    }

    /// Create a STRING token
    pub fn string(s: impl Into<String>) -> Self {
        Self::new(TokenType::String, s, Span::default())
    }

    /// Create an IDENTIFIER token
    pub fn identifier(s: impl Into<String>) -> Self {
        Self::new(TokenType::Identifier, s, Span::default())
    }

    /// Create a VAR token
    pub fn var(s: impl Into<String>) -> Self {
        Self::new(TokenType::Var, s, Span::default())
    }

    /// Add a comment to this token
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comments.push(comment.into());
        self
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({})", self.token_type, self.text)
    }
}

/// All possible token types in SQL
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(u16)]
pub enum TokenType {
    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Dash,
    Plus,
    Colon,
    DotColon,
    DColon,
    DColonDollar,
    DColonPercent,
    DColonQMark,
    DQMark,
    Semicolon,
    Star,
    Backslash,
    Slash,
    Lt,
    Lte,
    Gt,
    Gte,
    Not,
    Eq,
    Neq,
    NullsafeEq,
    ColonEq,
    ColonGt,
    NColonGt,
    And,
    Or,
    Amp,
    DPipe,
    PipeGt,
    Pipe,
    PipeSlash,
    DPipeSlash,
    Caret,
    CaretAt,
    LtLt,  // <<
    GtGt,  // >>
    Tilde,
    Arrow,
    DArrow,
    FArrow,
    Hash,
    HashArrow,
    DHashArrow,
    LrArrow,
    DAt,
    AtAt,
    LtAt,
    AtGt,
    Dollar,
    Parameter,
    Session,
    SessionParameter,
    SessionUser,
    DAmp,
    AmpLt,
    AmpGt,
    Adjacent,
    Xor,
    DStar,
    QMarkAmp,
    QMarkPipe,
    HashDash,
    Exclamation,

    UriStart,
    BlockStart,
    BlockEnd,
    Space,
    Break,

    // Comments (emitted as tokens for round-trip fidelity)
    BlockComment,  // /* ... */
    LineComment,   // -- ...

    // Literals
    String,
    DollarString,  // $$...$$
    TripleDoubleQuotedString,  // """..."""
    TripleSingleQuotedString,  // '''...'''
    Number,
    Identifier,
    QuotedIdentifier,
    Database,
    Column,
    ColumnDef,
    Schema,
    Table,
    Warehouse,
    Stage,
    Streamlit,
    Var,
    BitString,
    HexString,
    /// Hex number: 0xA, 0xFF (BigQuery, SQLite style) - represents an integer in hex notation
    HexNumber,
    ByteString,
    NationalString,
    EscapeString,  // PostgreSQL E'...' escape string
    RawString,
    HeredocString,
    HeredocStringAlternative,
    UnicodeString,

    // Data Types
    Bit,
    Boolean,
    TinyInt,
    UTinyInt,
    SmallInt,
    USmallInt,
    MediumInt,
    UMediumInt,
    Int,
    UInt,
    BigInt,
    UBigInt,
    BigNum,
    Int128,
    UInt128,
    Int256,
    UInt256,
    Float,
    Double,
    UDouble,
    Decimal,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    DecFloat,
    UDecimal,
    BigDecimal,
    Char,
    NChar,
    VarChar,
    NVarChar,
    BpChar,
    Text,
    MediumText,
    LongText,
    Blob,
    MediumBlob,
    LongBlob,
    TinyBlob,
    TinyText,
    Name,
    Binary,
    VarBinary,
    Json,
    JsonB,
    Time,
    TimeTz,
    TimeNs,
    Timestamp,
    TimestampTz,
    TimestampLtz,
    TimestampNtz,
    TimestampS,
    TimestampMs,
    TimestampNs,
    DateTime,
    DateTime2,
    DateTime64,
    SmallDateTime,
    Date,
    Date32,
    Int4Range,
    Int4MultiRange,
    Int8Range,
    Int8MultiRange,
    NumRange,
    NumMultiRange,
    TsRange,
    TsMultiRange,
    TsTzRange,
    TsTzMultiRange,
    DateRange,
    DateMultiRange,
    Uuid,
    Geography,
    GeographyPoint,
    Nullable,
    Geometry,
    Point,
    Ring,
    LineString,
    LocalTime,
    LocalTimestamp,
    SysTimestamp,
    MultiLineString,
    Polygon,
    MultiPolygon,
    HllSketch,
    HStore,
    Super,
    Serial,
    SmallSerial,
    BigSerial,
    Xml,
    Year,
    UserDefined,
    Money,
    SmallMoney,
    RowVersion,
    Image,
    Variant,
    Object,
    Inet,
    IpAddress,
    IpPrefix,
    Ipv4,
    Ipv6,
    Enum,
    Enum8,
    Enum16,
    FixedString,
    LowCardinality,
    Nested,
    AggregateFunction,
    SimpleAggregateFunction,
    TDigest,
    Unknown,
    Vector,
    Dynamic,
    Void,

    // Keywords
    Add,
    Alias,
    Alter,
    All,
    Anti,
    Any,
    Apply,
    Array,
    Asc,
    AsOf,
    Attach,
    AutoIncrement,
    Begin,
    Between,
    BulkCollectInto,
    Cache,
    Cascade,
    Case,
    CharacterSet,
    Cluster,
    ClusterBy,
    Collate,
    Command,
    Comment,
    Commit,
    Preserve,
    Connect,
    ConnectBy,
    Constraint,
    Copy,
    Create,
    Cross,
    Cube,
    CurrentDate,
    CurrentDateTime,
    CurrentSchema,
    CurrentTime,
    CurrentTimestamp,
    CurrentUser,
    CurrentRole,
    CurrentCatalog,
    Declare,
    Default,
    Delete,
    Desc,
    Describe,
    Detach,
    Dictionary,
    Distinct,
    Distribute,
    DistributeBy,
    Div,
    Drop,
    Else,
    End,
    Escape,
    Except,
    Execute,
    Exists,
    False,
    Fetch,
    File,
    FileFormat,
    Filter,
    Final,
    First,
    For,
    Force,
    ForeignKey,
    Format,
    From,
    Full,
    Function,
    Get,
    Glob,
    Global,
    Grant,
    GroupBy,
    GroupingSets,
    Having,
    Hint,
    Ignore,
    ILike,
    In,
    Index,
    IndexedBy,
    Inner,
    Input,
    Insert,
    Install,
    Intersect,
    Interval,
    Into,
    Inpath,
    InputFormat,
    Introducer,
    IRLike,
    Is,
    IsNull,
    Join,
    JoinMarker,
    Keep,
    Key,
    Kill,
    Lambda,
    Language,
    Lateral,
    Left,
    Like,
    NotLike,     // !~~ operator (PostgreSQL)
    NotILike,    // !~~* operator (PostgreSQL)
    NotRLike,    // !~ operator (PostgreSQL)
    NotIRLike,   // !~* operator (PostgreSQL)
    Limit,
    List,
    Load,
    Local,
    Lock,
    Map,
    Match,
    MatchCondition,
    MatchRecognize,
    MemberOf,
    Materialized,
    Merge,
    Mod,
    Model,
    Natural,
    Next,
    NoAction,
    Nothing,
    NotNull,
    Null,
    ObjectIdentifier,
    Offset,
    On,
    Only,
    Operator,
    OrderBy,
    OrderSiblingsBy,
    Ordered,
    Ordinality,
    Out,
    Outer,
    Output,
    Over,
    Overlaps,
    Overwrite,
    Partition,
    PartitionBy,
    Percent,
    Pivot,
    Placeholder,
    Positional,
    Pragma,
    Prewhere,
    PrimaryKey,
    Procedure,
    Properties,
    PseudoType,
    Put,
    Qualify,
    Quote,
    QDColon,
    Range,
    Recursive,
    Refresh,
    Rename,
    Replace,
    Returning,
    Revoke,
    References,
    Restrict,
    Right,
    RLike,
    Rollback,
    Rollup,
    Row,
    Rows,
    Select,
    Semi,
    Savepoint,
    Separator,
    Sequence,
    Serde,
    SerdeProperties,
    Set,
    Settings,
    Show,
    Siblings,
    SimilarTo,
    Some,
    Sort,
    SortBy,
    SoundsLike,
    StartWith,
    StorageIntegration,
    StraightJoin,
    Struct,
    Summarize,
    TableSample,
    Sample,
    Bernoulli,
    System,
    Block,
    Seed,
    Repeatable,
    Tag,
    Temporary,
    Transaction,
    To,
    Top,
    Then,
    True,
    Truncate,
    Uncache,
    Union,
    Unnest,
    Unpivot,
    Update,
    Use,
    Using,
    Values,
    View,
    SemanticView,
    Volatile,
    When,
    Where,
    Window,
    With,
    Ties,
    Exclude,
    No,
    Others,
    Unique,
    UtcDate,
    UtcTime,
    UtcTimestamp,
    VersionSnapshot,
    TimestampSnapshot,
    Option,
    Sink,
    Source,
    Analyze,
    Namespace,
    Export,
    As,
    By,
    Nulls,
    Respect,
    Last,
    If,
    Cast,
    TryCast,
    SafeCast,
    Count,
    Extract,
    Substring,
    Trim,
    Leading,
    Trailing,
    Both,
    Position,
    Overlaying,
    Placing,
    Treat,
    Within,
    Group,
    Order,

    // Window function keywords
    Unbounded,
    Preceding,
    Following,
    Current,
    Groups,

    // DDL-specific keywords (Phase 4)
    Trigger,
    Type,
    Domain,
    Returns,
    Body,
    Increment,
    Minvalue,
    Maxvalue,
    Start,
    Cycle,
    NoCycle,
    Prior,
    Generated,
    Identity,
    Always,
    // MATCH_RECOGNIZE tokens
    Measures,
    Pattern,
    Define,
    Running,
    Owned,
    After,
    Before,
    Instead,
    Each,
    Statement,
    Referencing,
    Old,
    New,
    Of,
    Check,
    Authorization,
    Restart,

    // Special
    Eof,
}

impl TokenType {
    /// Check if this token type is a keyword that can be used as an identifier in certain contexts
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            TokenType::Select
                | TokenType::From
                | TokenType::Where
                | TokenType::And
                | TokenType::Or
                | TokenType::Not
                | TokenType::In
                | TokenType::Is
                | TokenType::Null
                | TokenType::True
                | TokenType::False
                | TokenType::As
                | TokenType::On
                | TokenType::Join
                | TokenType::Left
                | TokenType::Right
                | TokenType::Inner
                | TokenType::Outer
                | TokenType::Full
                | TokenType::Cross
                | TokenType::Semi
                | TokenType::Anti
                | TokenType::Union
                | TokenType::Except
                | TokenType::Intersect
                | TokenType::GroupBy
                | TokenType::OrderBy
                | TokenType::Having
                | TokenType::Limit
                | TokenType::Offset
                | TokenType::Case
                | TokenType::When
                | TokenType::Then
                | TokenType::Else
                | TokenType::End
                | TokenType::Create
                | TokenType::Drop
                | TokenType::Alter
                | TokenType::Insert
                | TokenType::Update
                | TokenType::Delete
                | TokenType::Into
                | TokenType::Values
                | TokenType::Set
                | TokenType::With
                | TokenType::Distinct
                | TokenType::All
                | TokenType::Exists
                | TokenType::Between
                | TokenType::Like
                | TokenType::ILike
                // Additional keywords that can be used as identifiers
                | TokenType::Filter
                | TokenType::Date
                | TokenType::Timestamp
                | TokenType::TimestampTz
                | TokenType::Interval
                | TokenType::Time
                | TokenType::Table
                | TokenType::Index
                | TokenType::Column
                | TokenType::Database
                | TokenType::Schema
                | TokenType::View
                | TokenType::Function
                | TokenType::Procedure
                | TokenType::Trigger
                | TokenType::Sequence
                | TokenType::Over
                | TokenType::Partition
                | TokenType::Window
                | TokenType::Rows
                | TokenType::Range
                | TokenType::First
                | TokenType::Last
                | TokenType::Preceding
                | TokenType::Following
                | TokenType::Current
                | TokenType::Row
                | TokenType::Unbounded
                | TokenType::Array
                | TokenType::Struct
                | TokenType::Map
                | TokenType::PrimaryKey
                | TokenType::Key
                | TokenType::ForeignKey
                | TokenType::References
                | TokenType::Unique
                | TokenType::Check
                | TokenType::Default
                | TokenType::Constraint
                | TokenType::Comment
                | TokenType::Rollup
                | TokenType::Cube
                | TokenType::Grant
                | TokenType::Revoke
                | TokenType::Type
                | TokenType::Use
                | TokenType::Cache
                | TokenType::Uncache
                | TokenType::Load
                | TokenType::Any
                | TokenType::Some
                | TokenType::Asc
                | TokenType::Desc
                | TokenType::Nulls
                | TokenType::Lateral
                | TokenType::Natural
                | TokenType::Escape
                | TokenType::Glob
                | TokenType::Match
                | TokenType::Recursive
                | TokenType::Replace
                | TokenType::Returns
                | TokenType::If
                | TokenType::Pivot
                | TokenType::Unpivot
                | TokenType::Json
                | TokenType::Blob
                | TokenType::Text
                | TokenType::Int
                | TokenType::BigInt
                | TokenType::SmallInt
                | TokenType::TinyInt
                | TokenType::Int128
                | TokenType::UInt128
                | TokenType::Int256
                | TokenType::UInt256
                | TokenType::UInt
                | TokenType::UBigInt
                | TokenType::Float
                | TokenType::Double
                | TokenType::Decimal
                | TokenType::Boolean
                | TokenType::VarChar
                | TokenType::Char
                | TokenType::Binary
                | TokenType::VarBinary
                | TokenType::No
                | TokenType::DateTime
                | TokenType::Truncate
                | TokenType::Execute
                | TokenType::Merge
                | TokenType::Top
                | TokenType::Begin
                | TokenType::Generated
                | TokenType::Identity
                | TokenType::Always
                | TokenType::Extract
                // Keywords that can be identifiers in certain contexts
                | TokenType::AsOf
                | TokenType::Prior
                | TokenType::After
                | TokenType::Restrict
                | TokenType::Cascade
                | TokenType::Local
                | TokenType::Rename
                | TokenType::Enum
                | TokenType::Within
                | TokenType::Format
                | TokenType::Final
                | TokenType::FileFormat
                | TokenType::Input
                | TokenType::InputFormat
                | TokenType::Copy
                | TokenType::Put
                | TokenType::Get
                | TokenType::Show
                | TokenType::Serde
                | TokenType::Sample
                | TokenType::Sort
                | TokenType::Collate
                | TokenType::Ties
                | TokenType::IsNull
                | TokenType::NotNull
                | TokenType::Exclude
                | TokenType::Temporary
                | TokenType::Add
                | TokenType::Ordinality
                | TokenType::Overlaps
                | TokenType::Block
                | TokenType::Pattern
                | TokenType::Group
                | TokenType::Cluster
                | TokenType::Repeatable
                | TokenType::Groups
                | TokenType::Commit
                | TokenType::Warehouse
                | TokenType::System
                | TokenType::By
                | TokenType::To
                | TokenType::Fetch
                | TokenType::For
                | TokenType::Only
                | TokenType::Next
                | TokenType::Lock
                | TokenType::Refresh
                | TokenType::Settings
                | TokenType::Operator
                | TokenType::Overwrite
                | TokenType::StraightJoin
                | TokenType::Start
                // Additional keywords registered in tokenizer but previously missing from is_keyword()
                | TokenType::Ignore
                | TokenType::Domain
                | TokenType::Apply
                | TokenType::Respect
                | TokenType::Materialized
                | TokenType::Prewhere
                | TokenType::Old
                | TokenType::New
                | TokenType::Cast
                | TokenType::TryCast
                | TokenType::SafeCast
                | TokenType::Transaction
                | TokenType::Describe
                | TokenType::Kill
                | TokenType::Lambda
                | TokenType::Declare
                | TokenType::Keep
                | TokenType::Output
                | TokenType::Percent
                | TokenType::Qualify
                | TokenType::Returning
                | TokenType::Language
                | TokenType::Preserve
                | TokenType::Savepoint
                | TokenType::Rollback
                | TokenType::Body
                | TokenType::Increment
                | TokenType::Minvalue
                | TokenType::Maxvalue
                | TokenType::Cycle
                | TokenType::NoCycle
                | TokenType::Seed
                | TokenType::Namespace
                | TokenType::Authorization
                | TokenType::Order
                | TokenType::Restart
                | TokenType::Before
                | TokenType::Instead
                | TokenType::Each
                | TokenType::Statement
                | TokenType::Referencing
                | TokenType::Of
                | TokenType::Separator
                | TokenType::Others
                | TokenType::Placing
                | TokenType::Owned
                | TokenType::Running
                | TokenType::Define
                | TokenType::Measures
                | TokenType::MatchRecognize
                | TokenType::AutoIncrement
                | TokenType::Connect
                | TokenType::Distribute
                | TokenType::Bernoulli
                | TokenType::TableSample
                | TokenType::Inpath
                | TokenType::Pragma
                | TokenType::Siblings
                | TokenType::SerdeProperties
        )
    }

    /// Check if this token type is a comparison operator
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            TokenType::Eq
                | TokenType::Neq
                | TokenType::Lt
                | TokenType::Lte
                | TokenType::Gt
                | TokenType::Gte
                | TokenType::NullsafeEq
        )
    }

    /// Check if this token type is an arithmetic operator
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            TokenType::Plus
                | TokenType::Dash
                | TokenType::Star
                | TokenType::Slash
                | TokenType::Percent
                | TokenType::Mod
                | TokenType::Div
        )
    }
}

impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Tokenizer configuration for a dialect
#[derive(Debug, Clone)]
pub struct TokenizerConfig {
    /// Keywords mapping (uppercase keyword -> token type)
    pub keywords: std::collections::HashMap<String, TokenType>,
    /// Single character tokens
    pub single_tokens: std::collections::HashMap<char, TokenType>,
    /// Quote characters (start -> end)
    pub quotes: std::collections::HashMap<String, String>,
    /// Identifier quote characters (start -> end)
    pub identifiers: std::collections::HashMap<char, char>,
    /// Comment definitions (start -> optional end)
    pub comments: std::collections::HashMap<String, Option<String>>,
    /// String escape characters
    pub string_escapes: Vec<char>,
    /// Whether to support nested comments
    pub nested_comments: bool,
    /// Valid escape follow characters (for MySQL-style escaping).
    /// When a backslash is followed by a character NOT in this list,
    /// the backslash is discarded. When empty, all backslash escapes
    /// preserve the backslash for unrecognized sequences.
    pub escape_follow_chars: Vec<char>,
    /// Whether b'...' is a byte string (true for BigQuery) or bit string (false for standard SQL).
    /// Default is false (bit string).
    pub b_prefix_is_byte_string: bool,
    /// Numeric literal suffixes (uppercase suffix -> type name), e.g. {"L": "BIGINT", "S": "SMALLINT"}
    /// Used by Hive/Spark to parse 1L as CAST(1 AS BIGINT)
    pub numeric_literals: std::collections::HashMap<String, String>,
    /// Whether unquoted identifiers can start with a digit (e.g., `1a`, `1_a`).
    /// When true, a number followed by letters/underscore is treated as an identifier.
    /// Used by Hive, Spark, MySQL, ClickHouse.
    pub identifiers_can_start_with_digit: bool,
    /// Whether 0x/0X prefix should be treated as hex literals.
    /// When true, `0XCC` is tokenized instead of Number("0") + Identifier("XCC").
    /// Used by BigQuery, SQLite, Teradata.
    pub hex_number_strings: bool,
    /// Whether hex string literals from 0x prefix represent integer values.
    /// When true (BigQuery), 0xA is tokenized as HexNumber (integer in hex notation).
    /// When false (SQLite, Teradata), 0xCC is tokenized as HexString (binary/blob value).
    pub hex_string_is_integer_type: bool,
    /// Whether string escape sequences (like \') are allowed in raw strings.
    /// When true (BigQuery default), \' inside r'...' escapes the quote.
    /// When false (Spark/Databricks), backslashes in raw strings are always literal.
    /// Python sqlglot: STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS (default True)
    pub string_escapes_allowed_in_raw_strings: bool,
    /// Whether # starts a single-line comment (ClickHouse, MySQL)
    pub hash_comments: bool,
}

impl Default for TokenizerConfig {
    fn default() -> Self {
        let mut keywords = std::collections::HashMap::new();
        // Add basic SQL keywords
        keywords.insert("SELECT".to_string(), TokenType::Select);
        keywords.insert("FROM".to_string(), TokenType::From);
        keywords.insert("WHERE".to_string(), TokenType::Where);
        keywords.insert("AND".to_string(), TokenType::And);
        keywords.insert("OR".to_string(), TokenType::Or);
        keywords.insert("NOT".to_string(), TokenType::Not);
        keywords.insert("AS".to_string(), TokenType::As);
        keywords.insert("ON".to_string(), TokenType::On);
        keywords.insert("JOIN".to_string(), TokenType::Join);
        keywords.insert("LEFT".to_string(), TokenType::Left);
        keywords.insert("RIGHT".to_string(), TokenType::Right);
        keywords.insert("INNER".to_string(), TokenType::Inner);
        keywords.insert("OUTER".to_string(), TokenType::Outer);
        keywords.insert("OUTPUT".to_string(), TokenType::Output);
        keywords.insert("FULL".to_string(), TokenType::Full);
        keywords.insert("CROSS".to_string(), TokenType::Cross);
        keywords.insert("SEMI".to_string(), TokenType::Semi);
        keywords.insert("ANTI".to_string(), TokenType::Anti);
        keywords.insert("STRAIGHT_JOIN".to_string(), TokenType::StraightJoin);
        keywords.insert("UNION".to_string(), TokenType::Union);
        keywords.insert("EXCEPT".to_string(), TokenType::Except);
        keywords.insert("MINUS".to_string(), TokenType::Except); // Oracle/Redshift alias for EXCEPT
        keywords.insert("INTERSECT".to_string(), TokenType::Intersect);
        keywords.insert("GROUP".to_string(), TokenType::Group);
        keywords.insert("CUBE".to_string(), TokenType::Cube);
        keywords.insert("ROLLUP".to_string(), TokenType::Rollup);
        keywords.insert("WITHIN".to_string(), TokenType::Within);
        keywords.insert("ORDER".to_string(), TokenType::Order);
        keywords.insert("BY".to_string(), TokenType::By);
        keywords.insert("HAVING".to_string(), TokenType::Having);
        keywords.insert("LIMIT".to_string(), TokenType::Limit);
        keywords.insert("OFFSET".to_string(), TokenType::Offset);
        keywords.insert("ORDINALITY".to_string(), TokenType::Ordinality);
        keywords.insert("FETCH".to_string(), TokenType::Fetch);
        keywords.insert("FIRST".to_string(), TokenType::First);
        keywords.insert("NEXT".to_string(), TokenType::Next);
        keywords.insert("ONLY".to_string(), TokenType::Only);
        keywords.insert("KEEP".to_string(), TokenType::Keep);
        keywords.insert("IGNORE".to_string(), TokenType::Ignore);
        keywords.insert("INPUT".to_string(), TokenType::Input);
        keywords.insert("CASE".to_string(), TokenType::Case);
        keywords.insert("WHEN".to_string(), TokenType::When);
        keywords.insert("THEN".to_string(), TokenType::Then);
        keywords.insert("ELSE".to_string(), TokenType::Else);
        keywords.insert("END".to_string(), TokenType::End);
        keywords.insert("ENDIF".to_string(), TokenType::End); // Exasol alias for END
        keywords.insert("NULL".to_string(), TokenType::Null);
        keywords.insert("TRUE".to_string(), TokenType::True);
        keywords.insert("FALSE".to_string(), TokenType::False);
        keywords.insert("IS".to_string(), TokenType::Is);
        keywords.insert("IN".to_string(), TokenType::In);
        keywords.insert("BETWEEN".to_string(), TokenType::Between);
        keywords.insert("OVERLAPS".to_string(), TokenType::Overlaps);
        keywords.insert("LIKE".to_string(), TokenType::Like);
        keywords.insert("ILIKE".to_string(), TokenType::ILike);
        keywords.insert("RLIKE".to_string(), TokenType::RLike);
        keywords.insert("REGEXP".to_string(), TokenType::RLike);
        keywords.insert("ESCAPE".to_string(), TokenType::Escape);
        keywords.insert("EXISTS".to_string(), TokenType::Exists);
        keywords.insert("DISTINCT".to_string(), TokenType::Distinct);
        keywords.insert("ALL".to_string(), TokenType::All);
        keywords.insert("WITH".to_string(), TokenType::With);
        keywords.insert("CREATE".to_string(), TokenType::Create);
        keywords.insert("DROP".to_string(), TokenType::Drop);
        keywords.insert("ALTER".to_string(), TokenType::Alter);
        keywords.insert("TRUNCATE".to_string(), TokenType::Truncate);
        keywords.insert("TABLE".to_string(), TokenType::Table);
        keywords.insert("VIEW".to_string(), TokenType::View);
        keywords.insert("INDEX".to_string(), TokenType::Index);
        keywords.insert("COLUMN".to_string(), TokenType::Column);
        keywords.insert("CONSTRAINT".to_string(), TokenType::Constraint);
        keywords.insert("ADD".to_string(), TokenType::Add);
        keywords.insert("CASCADE".to_string(), TokenType::Cascade);
        keywords.insert("RESTRICT".to_string(), TokenType::Restrict);
        keywords.insert("RENAME".to_string(), TokenType::Rename);
        keywords.insert("TEMPORARY".to_string(), TokenType::Temporary);
        keywords.insert("TEMP".to_string(), TokenType::Temporary);
        keywords.insert("UNIQUE".to_string(), TokenType::Unique);
        keywords.insert("PRIMARY".to_string(), TokenType::PrimaryKey);
        keywords.insert("FOREIGN".to_string(), TokenType::ForeignKey);
        keywords.insert("KEY".to_string(), TokenType::Key);
        keywords.insert("KILL".to_string(), TokenType::Kill);
        keywords.insert("REFERENCES".to_string(), TokenType::References);
        keywords.insert("DEFAULT".to_string(), TokenType::Default);
        keywords.insert("DECLARE".to_string(), TokenType::Declare);
        keywords.insert("AUTO_INCREMENT".to_string(), TokenType::AutoIncrement);
        keywords.insert("AUTOINCREMENT".to_string(), TokenType::AutoIncrement);  // Snowflake style
        keywords.insert("MATERIALIZED".to_string(), TokenType::Materialized);
        keywords.insert("REPLACE".to_string(), TokenType::Replace);
        keywords.insert("TO".to_string(), TokenType::To);
        keywords.insert("INSERT".to_string(), TokenType::Insert);
        keywords.insert("OVERWRITE".to_string(), TokenType::Overwrite);
        keywords.insert("UPDATE".to_string(), TokenType::Update);
        keywords.insert("USE".to_string(), TokenType::Use);
        keywords.insert("WAREHOUSE".to_string(), TokenType::Warehouse);
        keywords.insert("GLOB".to_string(), TokenType::Glob);
        keywords.insert("DELETE".to_string(), TokenType::Delete);
        keywords.insert("MERGE".to_string(), TokenType::Merge);
        keywords.insert("CACHE".to_string(), TokenType::Cache);
        keywords.insert("UNCACHE".to_string(), TokenType::Uncache);
        keywords.insert("REFRESH".to_string(), TokenType::Refresh);
        keywords.insert("GRANT".to_string(), TokenType::Grant);
        keywords.insert("REVOKE".to_string(), TokenType::Revoke);
        keywords.insert("COMMENT".to_string(), TokenType::Comment);
        keywords.insert("COLLATE".to_string(), TokenType::Collate);
        keywords.insert("INTO".to_string(), TokenType::Into);
        keywords.insert("VALUES".to_string(), TokenType::Values);
        keywords.insert("SET".to_string(), TokenType::Set);
        keywords.insert("SETTINGS".to_string(), TokenType::Settings);
        keywords.insert("SEPARATOR".to_string(), TokenType::Separator);
        keywords.insert("ASC".to_string(), TokenType::Asc);
        keywords.insert("DESC".to_string(), TokenType::Desc);
        keywords.insert("NULLS".to_string(), TokenType::Nulls);
        keywords.insert("RESPECT".to_string(), TokenType::Respect);
        keywords.insert("FIRST".to_string(), TokenType::First);
        keywords.insert("LAST".to_string(), TokenType::Last);
        keywords.insert("IF".to_string(), TokenType::If);
        keywords.insert("CAST".to_string(), TokenType::Cast);
        keywords.insert("TRY_CAST".to_string(), TokenType::TryCast);
        keywords.insert("SAFE_CAST".to_string(), TokenType::SafeCast);
        keywords.insert("OVER".to_string(), TokenType::Over);
        keywords.insert("PARTITION".to_string(), TokenType::Partition);
        keywords.insert("PLACING".to_string(), TokenType::Placing);
        keywords.insert("WINDOW".to_string(), TokenType::Window);
        keywords.insert("ROWS".to_string(), TokenType::Rows);
        keywords.insert("RANGE".to_string(), TokenType::Range);
        keywords.insert("FILTER".to_string(), TokenType::Filter);
        keywords.insert("NATURAL".to_string(), TokenType::Natural);
        keywords.insert("USING".to_string(), TokenType::Using);
        keywords.insert("UNBOUNDED".to_string(), TokenType::Unbounded);
        keywords.insert("PRECEDING".to_string(), TokenType::Preceding);
        keywords.insert("FOLLOWING".to_string(), TokenType::Following);
        keywords.insert("CURRENT".to_string(), TokenType::Current);
        keywords.insert("ROW".to_string(), TokenType::Row);
        keywords.insert("GROUPS".to_string(), TokenType::Groups);
        keywords.insert("RECURSIVE".to_string(), TokenType::Recursive);
        // TRIM function position keywords
        keywords.insert("BOTH".to_string(), TokenType::Both);
        keywords.insert("LEADING".to_string(), TokenType::Leading);
        keywords.insert("TRAILING".to_string(), TokenType::Trailing);
        keywords.insert("INTERVAL".to_string(), TokenType::Interval);
        // Phase 3: Additional keywords
        keywords.insert("TOP".to_string(), TokenType::Top);
        keywords.insert("QUALIFY".to_string(), TokenType::Qualify);
        keywords.insert("SAMPLE".to_string(), TokenType::Sample);
        keywords.insert("TABLESAMPLE".to_string(), TokenType::TableSample);
        keywords.insert("BERNOULLI".to_string(), TokenType::Bernoulli);
        keywords.insert("SYSTEM".to_string(), TokenType::System);
        keywords.insert("BLOCK".to_string(), TokenType::Block);
        keywords.insert("SEED".to_string(), TokenType::Seed);
        keywords.insert("REPEATABLE".to_string(), TokenType::Repeatable);
        keywords.insert("TIES".to_string(), TokenType::Ties);
        keywords.insert("LATERAL".to_string(), TokenType::Lateral);
        keywords.insert("LAMBDA".to_string(), TokenType::Lambda);
        keywords.insert("APPLY".to_string(), TokenType::Apply);
        // Oracle CONNECT BY keywords
        keywords.insert("CONNECT".to_string(), TokenType::Connect);
        // Hive/Spark specific keywords
        keywords.insert("CLUSTER".to_string(), TokenType::Cluster);
        keywords.insert("DISTRIBUTE".to_string(), TokenType::Distribute);
        keywords.insert("SORT".to_string(), TokenType::Sort);
        keywords.insert("PIVOT".to_string(), TokenType::Pivot);
        keywords.insert("PREWHERE".to_string(), TokenType::Prewhere);
        keywords.insert("UNPIVOT".to_string(), TokenType::Unpivot);
        keywords.insert("FOR".to_string(), TokenType::For);
        keywords.insert("ANY".to_string(), TokenType::Any);
        keywords.insert("SOME".to_string(), TokenType::Some);
        keywords.insert("ASOF".to_string(), TokenType::AsOf);
        keywords.insert("PERCENT".to_string(), TokenType::Percent);
        keywords.insert("EXCLUDE".to_string(), TokenType::Exclude);
        keywords.insert("NO".to_string(), TokenType::No);
        keywords.insert("OTHERS".to_string(), TokenType::Others);
        // PostgreSQL OPERATOR() syntax for schema-qualified operators
        keywords.insert("OPERATOR".to_string(), TokenType::Operator);
        // Phase 4: DDL keywords
        keywords.insert("SCHEMA".to_string(), TokenType::Schema);
        keywords.insert("NAMESPACE".to_string(), TokenType::Namespace);
        keywords.insert("DATABASE".to_string(), TokenType::Database);
        keywords.insert("FUNCTION".to_string(), TokenType::Function);
        keywords.insert("PROCEDURE".to_string(), TokenType::Procedure);
        keywords.insert("PROC".to_string(), TokenType::Procedure);
        keywords.insert("SEQUENCE".to_string(), TokenType::Sequence);
        keywords.insert("TRIGGER".to_string(), TokenType::Trigger);
        keywords.insert("TYPE".to_string(), TokenType::Type);
        keywords.insert("DOMAIN".to_string(), TokenType::Domain);
        keywords.insert("RETURNS".to_string(), TokenType::Returns);
        keywords.insert("RETURNING".to_string(), TokenType::Returning);
        keywords.insert("LANGUAGE".to_string(), TokenType::Language);
        keywords.insert("ROLLBACK".to_string(), TokenType::Rollback);
        keywords.insert("COMMIT".to_string(), TokenType::Commit);
        keywords.insert("BEGIN".to_string(), TokenType::Begin);
        keywords.insert("DESCRIBE".to_string(), TokenType::Describe);
        keywords.insert("PRESERVE".to_string(), TokenType::Preserve);
        keywords.insert("TRANSACTION".to_string(), TokenType::Transaction);
        keywords.insert("SAVEPOINT".to_string(), TokenType::Savepoint);
        keywords.insert("BODY".to_string(), TokenType::Body);
        keywords.insert("INCREMENT".to_string(), TokenType::Increment);
        keywords.insert("MINVALUE".to_string(), TokenType::Minvalue);
        keywords.insert("MAXVALUE".to_string(), TokenType::Maxvalue);
        keywords.insert("CYCLE".to_string(), TokenType::Cycle);
        keywords.insert("NOCYCLE".to_string(), TokenType::NoCycle);
        keywords.insert("PRIOR".to_string(), TokenType::Prior);
        // MATCH_RECOGNIZE keywords
        keywords.insert("MATCH".to_string(), TokenType::Match);
        keywords.insert("MATCH_RECOGNIZE".to_string(), TokenType::MatchRecognize);
        keywords.insert("MEASURES".to_string(), TokenType::Measures);
        keywords.insert("PATTERN".to_string(), TokenType::Pattern);
        keywords.insert("DEFINE".to_string(), TokenType::Define);
        keywords.insert("RUNNING".to_string(), TokenType::Running);
        keywords.insert("FINAL".to_string(), TokenType::Final);
        keywords.insert("OWNED".to_string(), TokenType::Owned);
        keywords.insert("AFTER".to_string(), TokenType::After);
        keywords.insert("BEFORE".to_string(), TokenType::Before);
        keywords.insert("INSTEAD".to_string(), TokenType::Instead);
        keywords.insert("EACH".to_string(), TokenType::Each);
        keywords.insert("STATEMENT".to_string(), TokenType::Statement);
        keywords.insert("REFERENCING".to_string(), TokenType::Referencing);
        keywords.insert("OLD".to_string(), TokenType::Old);
        keywords.insert("NEW".to_string(), TokenType::New);
        keywords.insert("OF".to_string(), TokenType::Of);
        keywords.insert("CHECK".to_string(), TokenType::Check);
        keywords.insert("START".to_string(), TokenType::Start);
        keywords.insert("ENUM".to_string(), TokenType::Enum);
        keywords.insert("AUTHORIZATION".to_string(), TokenType::Authorization);
        keywords.insert("RESTART".to_string(), TokenType::Restart);
        // Date/time literal keywords
        keywords.insert("DATE".to_string(), TokenType::Date);
        keywords.insert("TIME".to_string(), TokenType::Time);
        keywords.insert("TIMESTAMP".to_string(), TokenType::Timestamp);
        keywords.insert("DATETIME".to_string(), TokenType::DateTime);
        keywords.insert("GENERATED".to_string(), TokenType::Generated);
        keywords.insert("IDENTITY".to_string(), TokenType::Identity);
        keywords.insert("ALWAYS".to_string(), TokenType::Always);
        // LOAD DATA keywords
        keywords.insert("LOAD".to_string(), TokenType::Load);
        keywords.insert("LOCAL".to_string(), TokenType::Local);
        keywords.insert("INPATH".to_string(), TokenType::Inpath);
        keywords.insert("INPUTFORMAT".to_string(), TokenType::InputFormat);
        keywords.insert("SERDE".to_string(), TokenType::Serde);
        keywords.insert("SERDEPROPERTIES".to_string(), TokenType::SerdeProperties);
        keywords.insert("FORMAT".to_string(), TokenType::Format);
        // SQLite
        keywords.insert("PRAGMA".to_string(), TokenType::Pragma);
        // SHOW statement
        keywords.insert("SHOW".to_string(), TokenType::Show);
        // Oracle ORDER SIBLINGS BY (hierarchical queries)
        keywords.insert("SIBLINGS".to_string(), TokenType::Siblings);
        // COPY and PUT statements (Snowflake, PostgreSQL)
        keywords.insert("COPY".to_string(), TokenType::Copy);
        keywords.insert("PUT".to_string(), TokenType::Put);
        keywords.insert("GET".to_string(), TokenType::Get);
        // EXEC/EXECUTE statement (TSQL, etc.)
        keywords.insert("EXEC".to_string(), TokenType::Execute);
        keywords.insert("EXECUTE".to_string(), TokenType::Execute);
        // Postfix null check operators (PostgreSQL/SQLite)
        keywords.insert("ISNULL".to_string(), TokenType::IsNull);
        keywords.insert("NOTNULL".to_string(), TokenType::NotNull);

        let mut single_tokens = std::collections::HashMap::new();
        single_tokens.insert('(', TokenType::LParen);
        single_tokens.insert(')', TokenType::RParen);
        single_tokens.insert('[', TokenType::LBracket);
        single_tokens.insert(']', TokenType::RBracket);
        single_tokens.insert('{', TokenType::LBrace);
        single_tokens.insert('}', TokenType::RBrace);
        single_tokens.insert(',', TokenType::Comma);
        single_tokens.insert('.', TokenType::Dot);
        single_tokens.insert(';', TokenType::Semicolon);
        single_tokens.insert('+', TokenType::Plus);
        single_tokens.insert('-', TokenType::Dash);
        single_tokens.insert('*', TokenType::Star);
        single_tokens.insert('/', TokenType::Slash);
        single_tokens.insert('%', TokenType::Percent);
        single_tokens.insert('&', TokenType::Amp);
        single_tokens.insert('|', TokenType::Pipe);
        single_tokens.insert('^', TokenType::Caret);
        single_tokens.insert('~', TokenType::Tilde);
        single_tokens.insert('<', TokenType::Lt);
        single_tokens.insert('>', TokenType::Gt);
        single_tokens.insert('=', TokenType::Eq);
        single_tokens.insert('!', TokenType::Exclamation);
        single_tokens.insert(':', TokenType::Colon);
        single_tokens.insert('@', TokenType::DAt);
        single_tokens.insert('#', TokenType::Hash);
        single_tokens.insert('$', TokenType::Dollar);
        single_tokens.insert('?', TokenType::Parameter);

        let mut quotes = std::collections::HashMap::new();
        quotes.insert("'".to_string(), "'".to_string());
        // Triple-quoted strings (e.g., """x""")
        quotes.insert("\"\"\"".to_string(), "\"\"\"".to_string());

        let mut identifiers = std::collections::HashMap::new();
        identifiers.insert('"', '"');
        identifiers.insert('`', '`');
        // Note: TSQL bracket-quoted identifiers [name] are handled in the parser
        // because [ is also used for arrays and subscripts

        let mut comments = std::collections::HashMap::new();
        comments.insert("--".to_string(), None);
        comments.insert("/*".to_string(), Some("*/".to_string()));

        Self {
            keywords,
            single_tokens,
            quotes,
            identifiers,
            comments,
            // Standard SQL: only '' (doubled quote) escapes a quote
            // Backslash escapes are dialect-specific (MySQL, etc.)
            string_escapes: vec!['\''],
            nested_comments: true,
            // By default, no escape_follow_chars means preserve backslash for unrecognized escapes
            escape_follow_chars: vec![],
            // Default: b'...' is bit string (standard SQL), not byte string (BigQuery)
            b_prefix_is_byte_string: false,
            numeric_literals: std::collections::HashMap::new(),
            identifiers_can_start_with_digit: false,
            hex_number_strings: false,
            hex_string_is_integer_type: false,
            // Default: backslash escapes ARE allowed in raw strings (sqlglot default)
            // Spark/Databricks set this to false
            string_escapes_allowed_in_raw_strings: true,
            hash_comments: false,
        }
    }
}

/// SQL Tokenizer
pub struct Tokenizer {
    config: TokenizerConfig,
}

impl Tokenizer {
    /// Create a new tokenizer with the given configuration
    pub fn new(config: TokenizerConfig) -> Self {
        Self { config }
    }

    /// Create a tokenizer with default configuration
    pub fn default_config() -> Self {
        Self::new(TokenizerConfig::default())
    }

    /// Tokenize a SQL string
    pub fn tokenize(&self, sql: &str) -> Result<Vec<Token>> {
        let mut state = TokenizerState::new(sql, &self.config);
        state.tokenize()
    }
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self::default_config()
    }
}

/// Internal state for tokenization
struct TokenizerState<'a> {
    chars: Vec<char>,
    size: usize,
    tokens: Vec<Token>,
    start: usize,
    current: usize,
    line: usize,
    column: usize,
    comments: Vec<String>,
    config: &'a TokenizerConfig,
}

impl<'a> TokenizerState<'a> {
    fn new(sql: &str, config: &'a TokenizerConfig) -> Self {
        let chars: Vec<char> = sql.chars().collect();
        let size = chars.len();
        Self {
            chars,
            size,
            tokens: Vec::new(),
            start: 0,
            current: 0,
            line: 1,
            column: 1,
            comments: Vec::new(),
            config,
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>> {
        while !self.is_at_end() {
            self.skip_whitespace();
            if self.is_at_end() {
                break;
            }

            self.start = self.current;
            self.scan_token()?;
        }

        // Handle edge case: comments with no tokens (e.g., SQL that's just a comment)
        // In this case, self.comments contains leading comments that couldn't be attached
        // to any token. We can't do much with them here.
        // Note: After the first token is created, comments go directly to trailing_comments
        // via scan_block_comment/scan_line_comment, so self.comments should typically be empty.

        Ok(std::mem::take(&mut self.tokens))
    }

    fn is_at_end(&self) -> bool {
        self.current >= self.size
    }

    fn peek(&self) -> char {
        if self.is_at_end() {
            '\0'
        } else {
            self.chars[self.current]
        }
    }

    fn peek_next(&self) -> char {
        if self.current + 1 >= self.size {
            '\0'
        } else {
            self.chars[self.current + 1]
        }
    }

    fn advance(&mut self) -> char {
        let c = self.peek();
        self.current += 1;
        if c == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        c
    }

    fn skip_whitespace(&mut self) {
        while !self.is_at_end() {
            let c = self.peek();
            match c {
                ' ' | '\t' | '\r' | '\n'
                | '\u{00A0}' // non-breaking space
                | '\u{2000}'..='\u{200B}' // various Unicode spaces + zero-width space
                | '\u{3000}' // ideographic (full-width) space
                | '\u{FEFF}' // BOM / zero-width no-break space
                => {
                    self.advance();
                }
                '-' if self.peek_next() == '-' => {
                    self.scan_line_comment();
                }
                '/' if self.peek_next() == '*' => {
                    // Check if this is a hint comment /*+ ... */
                    if self.current + 2 < self.size && self.chars[self.current + 2] == '+' {
                        // This is a hint comment, handle it as a token instead of skipping
                        break;
                    }
                    if self.scan_block_comment().is_err() {
                        return;
                    }
                }
                '#' if self.config.hash_comments => {
                    self.scan_hash_line_comment();
                }
                _ => break,
            }
        }
    }

    fn scan_hash_line_comment(&mut self) {
        self.advance(); // #
        let start = self.current;
        while !self.is_at_end() && self.peek() != '\n' {
            self.advance();
        }
        let comment: String = self.chars[start..self.current].iter().collect();
        let comment_text = comment.trim().to_string();
        if let Some(last) = self.tokens.last_mut() {
            last.trailing_comments.push(comment_text);
        } else {
            self.comments.push(comment_text);
        }
    }

    fn scan_line_comment(&mut self) {
        self.advance(); // -
        self.advance(); // -
        let start = self.current;
        while !self.is_at_end() && self.peek() != '\n' {
            self.advance();
        }
        let comment: String = self.chars[start..self.current].iter().collect();
        let comment_text = comment.trim().to_string();

        // Attach to previous token as trailing comment, or buffer for next token
        if let Some(last) = self.tokens.last_mut() {
            last.trailing_comments.push(comment_text);
        } else {
            self.comments.push(comment_text);
        }
    }

    fn scan_block_comment(&mut self) -> Result<()> {
        self.advance(); // /
        self.advance(); // *
        let content_start = self.current;
        let mut depth = 1;

        while !self.is_at_end() && depth > 0 {
            if self.peek() == '/' && self.peek_next() == '*' && self.config.nested_comments {
                self.advance();
                self.advance();
                depth += 1;
            } else if self.peek() == '*' && self.peek_next() == '/' {
                depth -= 1;
                if depth > 0 {
                    self.advance();
                    self.advance();
                }
            } else {
                self.advance();
            }
        }

        if depth > 0 {
            return Err(Error::tokenize(
                "Unterminated block comment",
                self.line,
                self.column,
            ));
        }

        // Get the content between /* and */ (preserving internal whitespace for nested comments)
        let content: String = self.chars[content_start..self.current].iter().collect();
        self.advance(); // *
        self.advance(); // /

        // For round-trip fidelity, preserve the exact comment content including nested comments
        let comment_text = format!("/*{}*/", content);

        // Attach to previous token as trailing comment, or buffer for next token
        if let Some(last) = self.tokens.last_mut() {
            last.trailing_comments.push(comment_text);
        } else {
            self.comments.push(comment_text);
        }

        Ok(())
    }

    /// Scan a hint comment /*+ ... */ and return it as a Hint token
    fn scan_hint(&mut self) -> Result<()> {
        self.advance(); // /
        self.advance(); // *
        self.advance(); // +
        let hint_start = self.current;

        // Scan until we find */
        while !self.is_at_end() {
            if self.peek() == '*' && self.peek_next() == '/' {
                break;
            }
            self.advance();
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated hint comment",
                self.line,
                self.column,
            ));
        }

        let hint_text: String = self.chars[hint_start..self.current].iter().collect();
        self.advance(); // *
        self.advance(); // /

        self.add_token_with_text(TokenType::Hint, hint_text.trim().to_string());

        Ok(())
    }

    /// Scan a positional parameter: $1, $2, etc.
    fn scan_positional_parameter(&mut self) -> Result<()> {
        self.advance(); // consume $
        let start = self.current;

        while !self.is_at_end() && self.peek().is_ascii_digit() {
            self.advance();
        }

        let number: String = self.chars[start..self.current].iter().collect();
        self.add_token_with_text(TokenType::Parameter, number);
        Ok(())
    }

    /// Try to scan a tagged dollar-quoted string: $tag$content$tag$
    /// Returns Some(()) if successful, None if this isn't a tagged dollar string.
    ///
    /// The token text is stored as "tag\x00content" to preserve the tag for later use.
    fn try_scan_tagged_dollar_string(&mut self) -> Result<Option<()>> {
        let saved_pos = self.current;

        // We're at '$', next char is alphabetic
        self.advance(); // consume opening $

        // Scan the tag (identifier: alphanumeric + underscore, including Unicode)
        // Tags can contain Unicode characters like emojis (e.g., $🦆$)
        let tag_start = self.current;
        while !self.is_at_end() && (self.peek().is_alphanumeric() || self.peek() == '_' || !self.peek().is_ascii()) {
            self.advance();
        }
        let tag: String = self.chars[tag_start..self.current].iter().collect();

        // Must have a closing $ after the tag
        if self.is_at_end() || self.peek() != '$' {
            // Not a tagged dollar string - restore position
            self.current = saved_pos;
            return Ok(None);
        }
        self.advance(); // consume closing $ of opening tag

        // Now scan content until we find $tag$
        let content_start = self.current;
        let closing_tag = format!("${}$", tag);
        let closing_chars: Vec<char> = closing_tag.chars().collect();

        loop {
            if self.is_at_end() {
                // Unterminated - restore and fall through
                self.current = saved_pos;
                return Ok(None);
            }

            // Check if we've reached the closing tag
            if self.peek() == '$' && self.current + closing_chars.len() <= self.size {
                let matches = closing_chars.iter().enumerate().all(|(j, &ch)| {
                    self.current + j < self.size && self.chars[self.current + j] == ch
                });
                if matches {
                    let content: String = self.chars[content_start..self.current].iter().collect();
                    // Consume closing tag
                    for _ in 0..closing_chars.len() {
                        self.advance();
                    }
                    // Store as "tag\x00content" to preserve the tag
                    let token_text = format!("{}\x00{}", tag, content);
                    self.add_token_with_text(TokenType::DollarString, token_text);
                    return Ok(Some(()));
                }
            }
            self.advance();
        }
    }

    /// Scan a dollar-quoted string: $$content$$ or $tag$content$tag$
    ///
    /// For $$...$$ (no tag), the token text is just the content.
    /// For $tag$...$tag$, use try_scan_tagged_dollar_string instead.
    fn scan_dollar_quoted_string(&mut self) -> Result<()> {
        self.advance(); // consume first $
        self.advance(); // consume second $

        // For $$...$$ (no tag), just scan until closing $$
        let start = self.current;
        while !self.is_at_end() {
            if self.peek() == '$' && self.current + 1 < self.size && self.chars[self.current + 1] == '$' {
                break;
            }
            self.advance();
        }

        let content: String = self.chars[start..self.current].iter().collect();

        if !self.is_at_end() {
            self.advance(); // consume first $
            self.advance(); // consume second $
        }

        self.add_token_with_text(TokenType::DollarString, content);
        Ok(())
    }

    fn scan_token(&mut self) -> Result<()> {
        let c = self.peek();

        // Check for string literal
        if c == '\'' {
            // Check for triple-quoted string '''...''' if configured
            if self.config.quotes.contains_key("'''")
               && self.peek_next() == '\''
               && self.current + 2 < self.size && self.chars[self.current + 2] == '\'' {
                return self.scan_triple_quoted_string('\'');
            }
            return self.scan_string();
        }

        // Check for triple-quoted string """...""" if configured
        if c == '"' && self.config.quotes.contains_key("\"\"\"")
           && self.peek_next() == '"'
           && self.current + 2 < self.size && self.chars[self.current + 2] == '"' {
            return self.scan_triple_quoted_string('"');
        }

        // Check for double-quoted strings when dialect supports them (e.g., BigQuery)
        // This must come before identifier quotes check
        if c == '"' && self.config.quotes.contains_key("\"") && !self.config.identifiers.contains_key(&'"') {
            return self.scan_double_quoted_string();
        }

        // Check for identifier quotes
        if let Some(&end_quote) = self.config.identifiers.get(&c) {
            return self.scan_quoted_identifier(end_quote);
        }

        // Check for numbers (including numbers starting with a dot like .25)
        if c.is_ascii_digit() {
            return self.scan_number();
        }

        // Check for numbers starting with a dot (e.g., .25, .5)
        // This must come before single character token handling
        // Don't treat as a number if:
        // - Previous char was also a dot (e.g., 1..2 should be 1, ., ., 2)
        // - Previous char is an identifier character (e.g., foo.25 should be foo, ., 25)
        //   This handles BigQuery numeric table parts like project.dataset.25
        if c == '.' && self.peek_next().is_ascii_digit() {
            let prev_char = if self.current > 0 { self.chars[self.current - 1] } else { '\0' };
            let is_after_ident = prev_char.is_alphanumeric() || prev_char == '_'
                || prev_char == '`' || prev_char == '"' || prev_char == ']'
                || prev_char == ')';
            if prev_char != '.' && !is_after_ident {
                return self.scan_number_starting_with_dot();
            }
        }

        // Check for hint comment /*+ ... */
        if c == '/' && self.peek_next() == '*' && self.current + 2 < self.size && self.chars[self.current + 2] == '+' {
            return self.scan_hint();
        }

        // Check for multi-character operators first
        if let Some(token_type) = self.try_scan_multi_char_operator() {
            self.add_token(token_type);
            return Ok(());
        }

        // Check for tagged dollar-quoted strings: $tag$content$tag$
        // Tags can contain Unicode characters (including emojis like 🦆) and digits (e.g., $1$)
        if c == '$'
            && (self.peek_next().is_alphanumeric() || self.peek_next() == '_' || !self.peek_next().is_ascii())
        {
            if let Some(()) = self.try_scan_tagged_dollar_string()? {
                return Ok(());
            }
        }

        // Check for dollar-quoted strings: $$...$$
        if c == '$' && self.peek_next() == '$' {
            return self.scan_dollar_quoted_string();
        }

        // Check for positional parameters: $1, $2, etc.
        if c == '$' && self.peek_next().is_ascii_digit() {
            return self.scan_positional_parameter();
        }

        // TSQL: Check for identifiers starting with # (temp tables) or @ (variables)
        // e.g., #temp, ##global_temp, @variable
        if (c == '#' || c == '@') && (self.peek_next().is_alphanumeric() || self.peek_next() == '_' || self.peek_next() == '#') {
            return self.scan_tsql_identifier();
        }

        // Check for single character tokens
        if let Some(&token_type) = self.config.single_tokens.get(&c) {
            self.advance();
            self.add_token(token_type);
            return Ok(());
        }

        // Unicode minus (U+2212) → treat as regular minus
        if c == '\u{2212}' {
            self.advance();
            self.add_token(TokenType::Dash);
            return Ok(());
        }

        // Unicode fraction slash (U+2044) → treat as regular slash
        if c == '\u{2044}' {
            self.advance();
            self.add_token(TokenType::Slash);
            return Ok(());
        }

        // Unicode curly/smart quotes → treat as regular string quotes
        if c == '\u{2018}' || c == '\u{2019}' {
            // Left/right single quotation marks → scan as string with matching end
            return self.scan_unicode_quoted_string(c);
        }
        if c == '\u{201C}' || c == '\u{201D}' {
            // Left/right double quotation marks → scan as quoted identifier
            return self.scan_unicode_quoted_identifier(c);
        }

        // Must be an identifier or keyword
        self.scan_identifier_or_keyword()
    }

    fn try_scan_multi_char_operator(&mut self) -> Option<TokenType> {
        let c = self.peek();
        let next = self.peek_next();
        let third = if self.current + 2 < self.size { self.chars[self.current + 2] } else { '\0' };

        // Check for three-character operators first
        // -|- (Adjacent - PostgreSQL range adjacency)
        if c == '-' && next == '|' && third == '-' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::Adjacent);
        }

        // ||/ (Cube root - PostgreSQL)
        if c == '|' && next == '|' && third == '/' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DPipeSlash);
        }

        // #>> (JSONB path text extraction - PostgreSQL)
        if c == '#' && next == '>' && third == '>' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DHashArrow);
        }

        // ->> (JSON text extraction - PostgreSQL/MySQL)
        if c == '-' && next == '>' && third == '>' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DArrow);
        }

        // <=> (NULL-safe equality - MySQL)
        if c == '<' && next == '=' && third == '>' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::NullsafeEq);
        }

        // <-> (Distance operator - PostgreSQL)
        if c == '<' && next == '-' && third == '>' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::LrArrow);
        }

        // <@ (Contained by - PostgreSQL)
        if c == '<' && next == '@' {
            self.advance();
            self.advance();
            return Some(TokenType::LtAt);
        }

        // @> (Contains - PostgreSQL)
        if c == '@' && next == '>' {
            self.advance();
            self.advance();
            return Some(TokenType::AtGt);
        }

        // ~~~ (Glob - PostgreSQL)
        if c == '~' && next == '~' && third == '~' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::Glob);
        }

        // ~~* (ILike - PostgreSQL)
        if c == '~' && next == '~' && third == '*' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::ILike);
        }

        // !~~* (Not ILike - PostgreSQL)
        let fourth = if self.current + 3 < self.size { self.chars[self.current + 3] } else { '\0' };
        if c == '!' && next == '~' && third == '~' && fourth == '*' {
            self.advance();
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::NotILike);
        }

        // !~~ (Not Like - PostgreSQL)
        if c == '!' && next == '~' && third == '~' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::NotLike);
        }

        // !~* (Not Regexp ILike - PostgreSQL)
        if c == '!' && next == '~' && third == '*' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::NotIRLike);
        }

        // !:> (Not cast / Try cast - SingleStore)
        if c == '!' && next == ':' && third == '>' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::NColonGt);
        }

        // ?:: (TRY_CAST shorthand - Databricks)
        if c == '?' && next == ':' && third == ':' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::QDColon);
        }

        // !~ (Not Regexp - PostgreSQL)
        if c == '!' && next == '~' {
            self.advance();
            self.advance();
            return Some(TokenType::NotRLike);
        }

        // ~~ (Like - PostgreSQL)
        if c == '~' && next == '~' {
            self.advance();
            self.advance();
            return Some(TokenType::Like);
        }

        // ~* (Regexp ILike - PostgreSQL)
        if c == '~' && next == '*' {
            self.advance();
            self.advance();
            return Some(TokenType::IRLike);
        }

        // SingleStore three-character JSON path operators (must be checked before :: two-char)
        // ::$ (JSON extract string), ::% (JSON extract double), ::? (JSON match)
        if c == ':' && next == ':' && third == '$' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DColonDollar);
        }
        if c == ':' && next == ':' && third == '%' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DColonPercent);
        }
        if c == ':' && next == ':' && third == '?' {
            self.advance();
            self.advance();
            self.advance();
            return Some(TokenType::DColonQMark);
        }

        // Two-character operators
        let token_type = match (c, next) {
            ('.', ':') => Some(TokenType::DotColon),
            ('=', '=') => Some(TokenType::Eq),  // Hive/Spark == equality operator
            ('<', '=') => Some(TokenType::Lte),
            ('>', '=') => Some(TokenType::Gte),
            ('!', '=') => Some(TokenType::Neq),
            ('<', '>') => Some(TokenType::Neq),
            ('^', '=') => Some(TokenType::Neq),
            ('<', '<') => Some(TokenType::LtLt),
            ('>', '>') => Some(TokenType::GtGt),
            ('|', '|') => Some(TokenType::DPipe),
            ('|', '/') => Some(TokenType::PipeSlash),  // Square root - PostgreSQL
            (':', ':') => Some(TokenType::DColon),
            (':', '=') => Some(TokenType::ColonEq),    // := (assignment, named args)
            (':', '>') => Some(TokenType::ColonGt),    // ::> (TSQL)
            ('-', '>') => Some(TokenType::Arrow),      // JSON object access
            ('=', '>') => Some(TokenType::FArrow),     // Fat arrow (lambda)
            ('&', '&') => Some(TokenType::DAmp),
            ('&', '<') => Some(TokenType::AmpLt),      // PostgreSQL range operator
            ('&', '>') => Some(TokenType::AmpGt),      // PostgreSQL range operator
            ('@', '@') => Some(TokenType::AtAt),       // Text search match
            ('?', '|') => Some(TokenType::QMarkPipe),  // JSONB contains any key
            ('?', '&') => Some(TokenType::QMarkAmp),   // JSONB contains all keys
            ('?', '?') => Some(TokenType::DQMark),     // Double question mark
            ('#', '>') => Some(TokenType::HashArrow),  // JSONB path extraction
            ('#', '-') => Some(TokenType::HashDash),   // JSONB delete
            ('^', '@') => Some(TokenType::CaretAt),    // PostgreSQL starts-with operator
            ('*', '*') => Some(TokenType::DStar),      // Power operator
            ('|', '>') => Some(TokenType::PipeGt),     // Pipe-greater (some dialects)
            _ => None,
        };

        if token_type.is_some() {
            self.advance();
            self.advance();
        }

        token_type
    }

    fn scan_string(&mut self) -> Result<()> {
        self.advance(); // Opening quote
        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.peek();
            if c == '\'' {
                if self.peek_next() == '\'' {
                    // Escaped quote
                    value.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    break;
                }
            } else if c == '\\' && self.config.string_escapes.contains(&'\\') {
                // Handle escape sequences
                self.advance(); // Consume the backslash
                if !self.is_at_end() {
                    let escaped = self.advance();
                    match escaped {
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        '0' => value.push('\0'),
                        'Z' => value.push('\x1A'), // Ctrl+Z (MySQL)
                        'a' => value.push('\x07'), // Alert/bell
                        'b' => value.push('\x08'), // Backspace
                        'f' => value.push('\x0C'), // Form feed
                        'v' => value.push('\x0B'), // Vertical tab
                        '\\' => value.push('\\'),
                        '\'' => value.push('\''),
                        '"' => value.push('"'),
                        '%' => {
                            // MySQL: \% in LIKE patterns
                            value.push('%');
                        }
                        '_' => {
                            // MySQL: \_ in LIKE patterns
                            value.push('_');
                        }
                        // For unrecognized escape sequences:
                        // If escape_follow_chars is set, only preserve backslash for chars in that list
                        // Otherwise (empty list), preserve backslash + char for unrecognized escapes
                        _ => {
                            if !self.config.escape_follow_chars.is_empty() {
                                // MySQL-style: discard backslash for unrecognized escapes
                                value.push(escaped);
                            } else {
                                // Standard: preserve backslash + char
                                value.push('\\');
                                value.push(escaped);
                            }
                        }
                    }
                }
            } else {
                value.push(self.advance());
            }
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated string",
                self.line,
                self.column,
            ));
        }

        self.advance(); // Closing quote
        self.add_token_with_text(TokenType::String, value);
        Ok(())
    }

    /// Scan a double-quoted string (for dialects like BigQuery where " is a string delimiter)
    fn scan_double_quoted_string(&mut self) -> Result<()> {
        self.advance(); // Opening quote
        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.peek();
            if c == '"' {
                if self.peek_next() == '"' {
                    // Escaped quote
                    value.push('"');
                    self.advance();
                    self.advance();
                } else {
                    break;
                }
            } else if c == '\\' && self.config.string_escapes.contains(&'\\') {
                // Handle escape sequences
                self.advance(); // Consume the backslash
                if !self.is_at_end() {
                    let escaped = self.advance();
                    match escaped {
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        '0' => value.push('\0'),
                        'Z' => value.push('\x1A'), // Ctrl+Z (MySQL)
                        'a' => value.push('\x07'), // Alert/bell
                        'b' => value.push('\x08'), // Backspace
                        'f' => value.push('\x0C'), // Form feed
                        'v' => value.push('\x0B'), // Vertical tab
                        '\\' => value.push('\\'),
                        '\'' => value.push('\''),
                        '"' => value.push('"'),
                        '%' => {
                            // MySQL: \% in LIKE patterns
                            value.push('%');
                        }
                        '_' => {
                            // MySQL: \_ in LIKE patterns
                            value.push('_');
                        }
                        // For unrecognized escape sequences:
                        // If escape_follow_chars is set, only preserve backslash for chars in that list
                        // Otherwise (empty list), preserve backslash + char for unrecognized escapes
                        _ => {
                            if !self.config.escape_follow_chars.is_empty() {
                                // MySQL-style: discard backslash for unrecognized escapes
                                value.push(escaped);
                            } else {
                                // Standard: preserve backslash + char
                                value.push('\\');
                                value.push(escaped);
                            }
                        }
                    }
                }
            } else {
                value.push(self.advance());
            }
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated double-quoted string",
                self.line,
                self.column,
            ));
        }

        self.advance(); // Closing quote
        self.add_token_with_text(TokenType::String, value);
        Ok(())
    }

    fn scan_triple_quoted_string(&mut self, quote_char: char) -> Result<()> {
        // Advance past the three opening quotes
        self.advance();
        self.advance();
        self.advance();
        let mut value = String::new();

        while !self.is_at_end() {
            // Check for closing triple quote
            if self.peek() == quote_char
                && self.current + 1 < self.size && self.chars[self.current + 1] == quote_char
                && self.current + 2 < self.size && self.chars[self.current + 2] == quote_char
            {
                // Found closing """
                break;
            }
            value.push(self.advance());
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated triple-quoted string",
                self.line,
                self.column,
            ));
        }

        // Advance past the three closing quotes
        self.advance();
        self.advance();
        self.advance();
        let token_type = if quote_char == '"' {
            TokenType::TripleDoubleQuotedString
        } else {
            TokenType::TripleSingleQuotedString
        };
        self.add_token_with_text(token_type, value);
        Ok(())
    }

    fn scan_quoted_identifier(&mut self, end_quote: char) -> Result<()> {
        self.advance(); // Opening quote
        let start = self.current;

        while !self.is_at_end() && self.peek() != end_quote {
            if self.peek() == end_quote && self.peek_next() == end_quote {
                // Escaped quote
                self.advance();
            }
            self.advance();
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated identifier",
                self.line,
                self.column,
            ));
        }

        let value: String = self.chars[start..self.current].iter().collect();
        self.advance(); // Closing quote
        self.add_token_with_text(TokenType::QuotedIdentifier, value);
        Ok(())
    }

    /// Scan a string delimited by Unicode curly single quotes (U+2018/U+2019)
    fn scan_unicode_quoted_string(&mut self, open_quote: char) -> Result<()> {
        self.advance(); // Opening curly quote
        let start = self.current;
        // Accept either left or right single quote as closing
        while !self.is_at_end() && self.peek() != '\u{2018}' && self.peek() != '\u{2019}' && self.peek() != '\'' {
            self.advance();
        }
        let value: String = self.chars[start..self.current].iter().collect();
        if !self.is_at_end() {
            self.advance(); // Closing quote
        }
        let _ = open_quote;
        self.add_token_with_text(TokenType::String, value);
        Ok(())
    }

    /// Scan an identifier delimited by Unicode curly double quotes (U+201C/U+201D)
    fn scan_unicode_quoted_identifier(&mut self, open_quote: char) -> Result<()> {
        self.advance(); // Opening curly quote
        let start = self.current;
        while !self.is_at_end() && self.peek() != '\u{201C}' && self.peek() != '\u{201D}' && self.peek() != '"' {
            self.advance();
        }
        let value: String = self.chars[start..self.current].iter().collect();
        if !self.is_at_end() {
            self.advance(); // Closing quote
        }
        let _ = open_quote;
        self.add_token_with_text(TokenType::QuotedIdentifier, value);
        Ok(())
    }

    fn scan_number(&mut self) -> Result<()> {
        // Check for 0x/0X hex number prefix (SQLite-style)
        if self.config.hex_number_strings && self.peek() == '0' && !self.is_at_end() {
            let next = if self.current + 1 < self.size { self.chars[self.current + 1] } else { '\0' };
            if next == 'x' || next == 'X' {
                // Advance past '0' and 'x'/'X'
                self.advance();
                self.advance();
                // Collect hex digits (allow underscores as separators, e.g., 0xbad_cafe)
                let hex_start = self.current;
                while !self.is_at_end() && (self.peek().is_ascii_hexdigit() || self.peek() == '_') {
                    if self.peek() == '_' && !self.peek_next().is_ascii_hexdigit() {
                        break;
                    }
                    self.advance();
                }
                if self.current > hex_start {
                    let hex_value: String = self.chars[hex_start..self.current].iter().collect();
                    if self.config.hex_string_is_integer_type {
                        // BigQuery: 0xA represents an integer in hex notation
                        self.add_token_with_text(TokenType::HexNumber, hex_value);
                    } else {
                        // SQLite/Teradata: 0xCC represents a binary/blob hex string
                        self.add_token_with_text(TokenType::HexString, hex_value);
                    }
                    return Ok(());
                }
                // No hex digits after 0x - fall through to normal number parsing
                // (reset current back to after '0')
                self.current = self.start + 1;
            }
        }

        // Allow underscores as digit separators (e.g., 20_000, 1_000_000)
        while !self.is_at_end() && (self.peek().is_ascii_digit() || self.peek() == '_') {
            // Don't allow underscore at the end (must be followed by digit)
            if self.peek() == '_' && (self.is_at_end() || !self.peek_next().is_ascii_digit()) {
                break;
            }
            self.advance();
        }

        // Look for decimal part - allow trailing dot (e.g., "1.")
        // In PostgreSQL (and sqlglot), "1.x" parses as float "1." with alias "x"
        // So we always consume the dot as part of the number, even if followed by an identifier
        if self.peek() == '.' {
            let next = self.peek_next();
            // Only consume the dot if:
            // 1. Followed by a digit (normal decimal like 1.5)
            // 2. Followed by an identifier start (like 1.x -> becomes 1. with alias x)
            // 3. End of input or other non-dot character (trailing decimal like "1.")
            // Do NOT consume if it's a double dot (..) which is a range operator
            if next != '.' {
                self.advance(); // consume the .
                // Only consume digits after the decimal point (not identifiers)
                while !self.is_at_end() && (self.peek().is_ascii_digit() || self.peek() == '_') {
                    if self.peek() == '_' && !self.peek_next().is_ascii_digit() {
                        break;
                    }
                    self.advance();
                }
            }
        }

        // Look for exponent
        if self.peek() == 'e' || self.peek() == 'E' {
            self.advance();
            if self.peek() == '+' || self.peek() == '-' {
                self.advance();
            }
            while !self.is_at_end() && (self.peek().is_ascii_digit() || self.peek() == '_') {
                if self.peek() == '_' && !self.peek_next().is_ascii_digit() {
                    break;
                }
                self.advance();
            }
        }

        let text: String = self.chars[self.start..self.current].iter().collect();

        // Check for numeric literal suffixes (e.g., 1L -> BIGINT, 1s -> SMALLINT in Hive/Spark)
        if !self.config.numeric_literals.is_empty() && !self.is_at_end() {
            let next_char = self.peek().to_uppercase().to_string();
            // Try 2-char suffix first (e.g., "BD"), then 1-char
            let suffix_match = if self.current + 1 < self.size {
                let two_char: String = vec![self.chars[self.current], self.chars[self.current + 1]]
                    .iter().collect::<String>().to_uppercase();
                if self.config.numeric_literals.contains_key(&two_char) {
                    // Make sure the 2-char suffix is not followed by more identifier chars
                    let after_suffix = if self.current + 2 < self.size { self.chars[self.current + 2] } else { ' ' };
                    if !after_suffix.is_alphanumeric() && after_suffix != '_' {
                        Some((two_char, 2))
                    } else {
                        None
                    }
                } else if self.config.numeric_literals.contains_key(&next_char) {
                    // 1-char suffix - make sure not followed by more identifier chars
                    let after_suffix = if self.current + 1 < self.size { self.chars[self.current + 1] } else { ' ' };
                    if !after_suffix.is_alphanumeric() && after_suffix != '_' {
                        Some((next_char, 1))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if self.config.numeric_literals.contains_key(&next_char) {
                // At end of input, 1-char suffix
                Some((next_char, 1))
            } else {
                None
            };

            if let Some((suffix, len)) = suffix_match {
                // Consume the suffix characters
                for _ in 0..len {
                    self.advance();
                }
                // Emit as a special number-with-suffix token
                // We'll encode as "number::TYPE" so the parser can split it
                let type_name = self.config.numeric_literals.get(&suffix).expect("suffix verified by contains_key above").clone();
                let combined = format!("{}::{}", text, type_name);
                self.add_token_with_text(TokenType::Number, combined);
                return Ok(());
            }
        }

        // Check for identifiers that start with a digit (e.g., 1a, 1_a, 1a_1a)
        // In Hive/Spark/MySQL/ClickHouse, these are valid unquoted identifiers
        if self.config.identifiers_can_start_with_digit && !self.is_at_end() {
            let next = self.peek();
            if next.is_alphabetic() || next == '_' {
                // Continue scanning as an identifier
                while !self.is_at_end() {
                    let ch = self.peek();
                    if ch.is_alphanumeric() || ch == '_' {
                        self.advance();
                    } else {
                        break;
                    }
                }
                let ident_text: String = self.chars[self.start..self.current].iter().collect();
                self.add_token_with_text(TokenType::Identifier, ident_text);
                return Ok(());
            }
        }

        self.add_token_with_text(TokenType::Number, text);
        Ok(())
    }

    /// Scan a number that starts with a dot (e.g., .25, .5, .123e10)
    fn scan_number_starting_with_dot(&mut self) -> Result<()> {
        // Consume the leading dot
        self.advance();

        // Consume the fractional digits
        while !self.is_at_end() && (self.peek().is_ascii_digit() || self.peek() == '_') {
            if self.peek() == '_' && !self.peek_next().is_ascii_digit() {
                break;
            }
            self.advance();
        }

        // Look for exponent
        if self.peek() == 'e' || self.peek() == 'E' {
            self.advance();
            if self.peek() == '+' || self.peek() == '-' {
                self.advance();
            }
            while !self.is_at_end() && (self.peek().is_ascii_digit() || self.peek() == '_') {
                if self.peek() == '_' && !self.peek_next().is_ascii_digit() {
                    break;
                }
                self.advance();
            }
        }

        let text: String = self.chars[self.start..self.current].iter().collect();
        self.add_token_with_text(TokenType::Number, text);
        Ok(())
    }

    fn scan_identifier_or_keyword(&mut self) -> Result<()> {
        // Guard against unrecognized characters that could cause infinite loops
        let first_char = self.peek();
        if !first_char.is_alphanumeric() && first_char != '_' {
            // Unknown character - skip it and return an error
            let c = self.advance();
            return Err(Error::tokenize(
                format!("Unexpected character: '{}'", c),
                self.line,
                self.column,
            ));
        }

        while !self.is_at_end() {
            let c = self.peek();
            // Allow alphanumeric, underscore, $, # and @ in identifiers
            // PostgreSQL allows $, TSQL allows # and @
            // But stop consuming # if followed by > or >> (PostgreSQL #> and #>> operators)
            if c == '#' {
                let next_c = if self.current + 1 < self.size { self.chars[self.current + 1] } else { '\0' };
                if next_c == '>' || next_c == '-' {
                    break; // Don't consume # — it's part of #>, #>>, or #- operator
                }
                self.advance();
            } else if c.is_alphanumeric() || c == '_' || c == '$' || c == '@' {
                self.advance();
            } else {
                break;
            }
        }

        let text: String = self.chars[self.start..self.current].iter().collect();
        let upper = text.to_uppercase();

        // Special-case NOT= (Teradata and other dialects)
        if upper == "NOT" && self.peek() == '=' {
            self.advance(); // consume '='
            self.add_token(TokenType::Neq);
            return Ok(());
        }

        // Check for special string prefixes like N'...', X'...', B'...', U&'...', r'...', b'...'
        // Also handle double-quoted variants for dialects that support them (e.g., BigQuery)
        let next_char = self.peek();
        let is_single_quote = next_char == '\'';
        let is_double_quote = next_char == '"' && self.config.quotes.contains_key("\"");
        // For raw strings (r"..." or r'...'), we allow double quotes even if " is not in quotes config
        // because raw strings are a special case used in Spark/Databricks where " is for identifiers
        let is_double_quote_for_raw = next_char == '"';

        // Handle raw strings first - they're special because they work with both ' and "
        // even in dialects where " is normally an identifier delimiter (like Databricks)
        if upper == "R" && (is_single_quote || is_double_quote_for_raw) {
            // Raw string r'...' or r"..." or r'''...''' or r"""...""" (BigQuery style)
            // In raw strings, backslashes are treated literally (no escape processing)
            let quote_char = if is_single_quote { '\'' } else { '"' };
            self.advance(); // consume the first opening quote

            // Check for triple-quoted raw string (r"""...""" or r'''...''')
            if self.peek() == quote_char && self.peek_next() == quote_char {
                // Triple-quoted raw string
                self.advance(); // consume second quote
                self.advance(); // consume third quote
                let string_value = self.scan_raw_triple_quoted_content(quote_char)?;
                self.add_token_with_text(TokenType::RawString, string_value);
            } else {
                let string_value = self.scan_raw_string_content(quote_char)?;
                self.add_token_with_text(TokenType::RawString, string_value);
            }
            return Ok(());
        }

        if is_single_quote || is_double_quote {
            match upper.as_str() {
                "N" => {
                    // National string N'...'
                    self.advance(); // consume the opening quote
                    let string_value = if is_single_quote {
                        self.scan_string_content()?
                    } else {
                        self.scan_double_quoted_string_content()?
                    };
                    self.add_token_with_text(TokenType::NationalString, string_value);
                    return Ok(());
                }
                "E" => {
                    // PostgreSQL escape string E'...' or e'...'
                    // Preserve the case by prefixing with "e:" or "E:"
                    // Always use backslash escapes for escape strings (e.g., \' is an escaped quote)
                    let lowercase = text == "e";
                    let prefix = if lowercase { "e:" } else { "E:" };
                    self.advance(); // consume the opening quote
                    let string_value = self.scan_string_content_with_escapes(true)?;
                    self.add_token_with_text(TokenType::EscapeString, format!("{}{}", prefix, string_value));
                    return Ok(());
                }
                "X" => {
                    // Hex string X'...'
                    self.advance(); // consume the opening quote
                    let string_value = if is_single_quote {
                        self.scan_string_content()?
                    } else {
                        self.scan_double_quoted_string_content()?
                    };
                    self.add_token_with_text(TokenType::HexString, string_value);
                    return Ok(());
                }
                "B" if is_double_quote => {
                    // Byte string b"..." (BigQuery style) - MUST check before single quote B'...'
                    self.advance(); // consume the opening quote
                    let string_value = self.scan_double_quoted_string_content()?;
                    self.add_token_with_text(TokenType::ByteString, string_value);
                    return Ok(());
                }
                "B" if is_single_quote => {
                    // For BigQuery: b'...' is a byte string (bytes data)
                    // For standard SQL: B'...' is a bit string (binary digits)
                    self.advance(); // consume the opening quote
                    let string_value = self.scan_string_content()?;
                    if self.config.b_prefix_is_byte_string {
                        self.add_token_with_text(TokenType::ByteString, string_value);
                    } else {
                        self.add_token_with_text(TokenType::BitString, string_value);
                    }
                    return Ok(());
                }
                _ => {}
            }
        }

        // Check for U&'...' Unicode string syntax (SQL standard)
        if upper == "U" && self.peek() == '&' && self.current + 1 < self.size && self.chars[self.current + 1] == '\'' {
            self.advance(); // consume '&'
            self.advance(); // consume opening quote
            let string_value = self.scan_string_content()?;
            self.add_token_with_text(TokenType::UnicodeString, string_value);
            return Ok(());
        }

        let token_type = self
            .config
            .keywords
            .get(&upper)
            .copied()
            .unwrap_or(TokenType::Var);

        self.add_token_with_text(token_type, text);
        Ok(())
    }

    /// Scan string content (everything between quotes)
    /// If `force_backslash_escapes` is true, backslash is always treated as an escape character
    /// (used for PostgreSQL E'...' escape strings)
    fn scan_string_content_with_escapes(&mut self, force_backslash_escapes: bool) -> Result<String> {
        let mut value = String::new();
        let use_backslash_escapes = force_backslash_escapes || self.config.string_escapes.contains(&'\\');

        while !self.is_at_end() {
            let c = self.peek();
            if c == '\'' {
                if self.peek_next() == '\'' {
                    // Escaped quote ''
                    value.push('\'');
                    self.advance();
                    self.advance();
                } else {
                    break;
                }
            } else if c == '\\' && use_backslash_escapes {
                // Preserve escape sequences literally (including \' for escape strings)
                value.push(self.advance());
                if !self.is_at_end() {
                    value.push(self.advance());
                }
            } else {
                value.push(self.advance());
            }
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated string",
                self.line,
                self.column,
            ));
        }

        self.advance(); // Closing quote
        Ok(value)
    }

    /// Scan string content (everything between quotes)
    fn scan_string_content(&mut self) -> Result<String> {
        self.scan_string_content_with_escapes(false)
    }

    /// Scan double-quoted string content (for dialects like BigQuery where " is a string delimiter)
    /// This is used for prefixed strings like b"..." or N"..."
    fn scan_double_quoted_string_content(&mut self) -> Result<String> {
        let mut value = String::new();
        let use_backslash_escapes = self.config.string_escapes.contains(&'\\');

        while !self.is_at_end() {
            let c = self.peek();
            if c == '"' {
                if self.peek_next() == '"' {
                    // Escaped quote ""
                    value.push('"');
                    self.advance();
                    self.advance();
                } else {
                    break;
                }
            } else if c == '\\' && use_backslash_escapes {
                // Handle escape sequences
                self.advance(); // Consume backslash
                if !self.is_at_end() {
                    let escaped = self.advance();
                    match escaped {
                        'n' => value.push('\n'),
                        'r' => value.push('\r'),
                        't' => value.push('\t'),
                        '0' => value.push('\0'),
                        '\\' => value.push('\\'),
                        '"' => value.push('"'),
                        '\'' => value.push('\''),
                        'x' => {
                            // Hex escape \xNN - collect hex digits
                            let mut hex = String::new();
                            for _ in 0..2 {
                                if !self.is_at_end() && self.peek().is_ascii_hexdigit() {
                                    hex.push(self.advance());
                                }
                            }
                            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                                value.push(byte as char);
                            } else {
                                // Invalid hex escape, keep it literal
                                value.push('\\');
                                value.push('x');
                                value.push_str(&hex);
                            }
                        }
                        _ => {
                            // For unrecognized escapes, preserve backslash + char
                            value.push('\\');
                            value.push(escaped);
                        }
                    }
                }
            } else {
                value.push(self.advance());
            }
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated double-quoted string",
                self.line,
                self.column,
            ));
        }

        self.advance(); // Closing quote
        Ok(value)
    }

    /// Scan raw string content (limited escape processing for quotes)
    /// Used for BigQuery r'...' and r"..." strings
    /// In raw strings, backslashes are literal EXCEPT that escape sequences for the
    /// quote character still work (e.g., \' in r'...' escapes the quote, '' also works)
    fn scan_raw_string_content(&mut self, quote_char: char) -> Result<String> {
        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.peek();
            if c == quote_char {
                if self.peek_next() == quote_char {
                    // Escaped quote (doubled) - e.g., '' inside r'...'
                    value.push(quote_char);
                    self.advance();
                    self.advance();
                } else {
                    break;
                }
            } else if c == '\\' && self.peek_next() == quote_char && self.config.string_escapes_allowed_in_raw_strings {
                // Backslash-escaped quote - works in raw strings when string_escapes_allowed_in_raw_strings is true
                // e.g., \' inside r'...' becomes literal ' (BigQuery behavior)
                // Spark/Databricks has this set to false, so backslash is always literal there
                value.push(quote_char);
                self.advance(); // consume backslash
                self.advance(); // consume quote
            } else {
                // In raw strings, everything including backslashes is literal
                value.push(self.advance());
            }
        }

        if self.is_at_end() {
            return Err(Error::tokenize(
                "Unterminated raw string",
                self.line,
                self.column,
            ));
        }

        self.advance(); // Closing quote
        Ok(value)
    }

    /// Scan raw triple-quoted string content (r"""...""" or r'''...''')
    /// Terminates when three consecutive quote_chars are found
    fn scan_raw_triple_quoted_content(&mut self, quote_char: char) -> Result<String> {
        let mut value = String::new();

        while !self.is_at_end() {
            let c = self.peek();
            if c == quote_char && self.peek_next() == quote_char {
                // Check for third quote
                if self.current + 2 < self.size && self.chars[self.current + 2] == quote_char {
                    // Found three consecutive quotes - end of string
                    self.advance(); // first closing quote
                    self.advance(); // second closing quote
                    self.advance(); // third closing quote
                    return Ok(value);
                }
            }
            // In raw strings, everything including backslashes is literal
            let ch = self.advance();
            value.push(ch);
        }

        Err(Error::tokenize(
            "Unterminated raw triple-quoted string",
            self.line,
            self.column,
        ))
    }

    /// Scan TSQL identifiers that start with # (temp tables) or @ (variables)
    /// Examples: #temp, ##global_temp, @variable
    fn scan_tsql_identifier(&mut self) -> Result<()> {
        // Consume the leading # or @ (or ##)
        let first = self.advance();

        // For ##, consume the second #
        if first == '#' && self.peek() == '#' {
            self.advance();
        }

        // Now scan the rest of the identifier
        while !self.is_at_end() {
            let c = self.peek();
            if c.is_alphanumeric() || c == '_' || c == '$' || c == '#' || c == '@' {
                self.advance();
            } else {
                break;
            }
        }

        let text: String = self.chars[self.start..self.current].iter().collect();
        // These are always identifiers (variables or temp table names), never keywords
        self.add_token_with_text(TokenType::Var, text);
        Ok(())
    }

    fn add_token(&mut self, token_type: TokenType) {
        let text: String = self.chars[self.start..self.current].iter().collect();
        self.add_token_with_text(token_type, text);
    }

    fn add_token_with_text(&mut self, token_type: TokenType, text: String) {
        let span = Span::new(self.start, self.current, self.line, self.column);
        let mut token = Token::new(token_type, text, span);
        token.comments.append(&mut self.comments);
        self.tokens.push(token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("SELECT 1").unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].token_type, TokenType::Select);
        assert_eq!(tokens[1].token_type, TokenType::Number);
        assert_eq!(tokens[1].text, "1");
    }

    #[test]
    fn test_select_with_identifier() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("SELECT a, b FROM t").unwrap();

        assert_eq!(tokens.len(), 6);
        assert_eq!(tokens[0].token_type, TokenType::Select);
        assert_eq!(tokens[1].token_type, TokenType::Var);
        assert_eq!(tokens[1].text, "a");
        assert_eq!(tokens[2].token_type, TokenType::Comma);
        assert_eq!(tokens[3].token_type, TokenType::Var);
        assert_eq!(tokens[3].text, "b");
        assert_eq!(tokens[4].token_type, TokenType::From);
        assert_eq!(tokens[5].token_type, TokenType::Var);
        assert_eq!(tokens[5].text, "t");
    }

    #[test]
    fn test_string_literal() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("SELECT 'hello'").unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[1].token_type, TokenType::String);
        assert_eq!(tokens[1].text, "hello");
    }

    #[test]
    fn test_escaped_string() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("SELECT 'it''s'").unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[1].token_type, TokenType::String);
        assert_eq!(tokens[1].text, "it's");
    }

    #[test]
    fn test_comments() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("SELECT -- comment\n1").unwrap();

        assert_eq!(tokens.len(), 2);
        // Comments are attached to the PREVIOUS token as trailing_comments
        // This is better for round-trip fidelity (e.g., SELECT c /* comment */ FROM)
        assert_eq!(tokens[0].trailing_comments.len(), 1);
        assert_eq!(tokens[0].trailing_comments[0], "comment");
    }

    #[test]
    fn test_operators() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("1 + 2 * 3").unwrap();

        assert_eq!(tokens.len(), 5);
        assert_eq!(tokens[0].token_type, TokenType::Number);
        assert_eq!(tokens[1].token_type, TokenType::Plus);
        assert_eq!(tokens[2].token_type, TokenType::Number);
        assert_eq!(tokens[3].token_type, TokenType::Star);
        assert_eq!(tokens[4].token_type, TokenType::Number);
    }

    #[test]
    fn test_comparison_operators() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("a <= b >= c != d").unwrap();

        assert_eq!(tokens[1].token_type, TokenType::Lte);
        assert_eq!(tokens[3].token_type, TokenType::Gte);
        assert_eq!(tokens[5].token_type, TokenType::Neq);
    }

    #[test]
    fn test_national_string() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("N'abc'").unwrap();

        assert_eq!(tokens.len(), 1, "Expected 1 token for N'abc', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::NationalString);
        assert_eq!(tokens[0].text, "abc");
    }

    #[test]
    fn test_hex_string() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("X'ABCD'").unwrap();

        assert_eq!(tokens.len(), 1, "Expected 1 token for X'ABCD', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::HexString);
        assert_eq!(tokens[0].text, "ABCD");
    }

    #[test]
    fn test_bit_string() {
        let tokenizer = Tokenizer::default();
        let tokens = tokenizer.tokenize("B'01010'").unwrap();

        assert_eq!(tokens.len(), 1, "Expected 1 token for B'01010', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::BitString);
        assert_eq!(tokens[0].text, "01010");
    }

    #[test]
    fn test_trailing_dot_number() {
        let tokenizer = Tokenizer::default();

        // Test trailing dot
        let tokens = tokenizer.tokenize("SELECT 1.").unwrap();
        assert_eq!(tokens.len(), 2, "Expected 2 tokens for 'SELECT 1.', got {:?}", tokens);
        assert_eq!(tokens[1].token_type, TokenType::Number);
        assert_eq!(tokens[1].text, "1.");

        // Test normal decimal
        let tokens = tokenizer.tokenize("SELECT 1.5").unwrap();
        assert_eq!(tokens[1].text, "1.5");

        // Test number followed by dot and identifier
        // In PostgreSQL (and sqlglot), "1.x" parses as float "1." with alias "x"
        let tokens = tokenizer.tokenize("SELECT 1.a").unwrap();
        assert_eq!(tokens.len(), 3, "Expected 3 tokens for 'SELECT 1.a', got {:?}", tokens);
        assert_eq!(tokens[1].token_type, TokenType::Number);
        assert_eq!(tokens[1].text, "1.");
        assert_eq!(tokens[2].token_type, TokenType::Var);

        // Test two dots (range operator) - dot is NOT consumed when followed by another dot
        let tokens = tokenizer.tokenize("SELECT 1..2").unwrap();
        assert_eq!(tokens[1].token_type, TokenType::Number);
        assert_eq!(tokens[1].text, "1");
        assert_eq!(tokens[2].token_type, TokenType::Dot);
        assert_eq!(tokens[3].token_type, TokenType::Dot);
        assert_eq!(tokens[4].token_type, TokenType::Number);
        assert_eq!(tokens[4].text, "2");
    }

    #[test]
    fn test_leading_dot_number() {
        let tokenizer = Tokenizer::default();

        // Test leading dot number (e.g., .25 for 0.25)
        let tokens = tokenizer.tokenize(".25").unwrap();
        assert_eq!(tokens.len(), 1, "Expected 1 token for '.25', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::Number);
        assert_eq!(tokens[0].text, ".25");

        // Test leading dot in context (Oracle SAMPLE clause)
        let tokens = tokenizer.tokenize("SAMPLE (.25)").unwrap();
        assert_eq!(tokens.len(), 4, "Expected 4 tokens for 'SAMPLE (.25)', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::Sample);
        assert_eq!(tokens[1].token_type, TokenType::LParen);
        assert_eq!(tokens[2].token_type, TokenType::Number);
        assert_eq!(tokens[2].text, ".25");
        assert_eq!(tokens[3].token_type, TokenType::RParen);

        // Test leading dot with exponent
        let tokens = tokenizer.tokenize(".5e10").unwrap();
        assert_eq!(tokens.len(), 1, "Expected 1 token for '.5e10', got {:?}", tokens);
        assert_eq!(tokens[0].token_type, TokenType::Number);
        assert_eq!(tokens[0].text, ".5e10");

        // Test that plain dot is still a Dot token
        let tokens = tokenizer.tokenize("a.b").unwrap();
        assert_eq!(tokens.len(), 3, "Expected 3 tokens for 'a.b', got {:?}", tokens);
        assert_eq!(tokens[1].token_type, TokenType::Dot);
    }

    #[test]
    fn test_unrecognized_character() {
        let tokenizer = Tokenizer::default();

        // Unicode curly quotes are now handled as string delimiters
        let result = tokenizer.tokenize("SELECT \u{2018}hello\u{2019}");
        assert!(result.is_ok(), "Curly quotes should be tokenized as strings");

        // Unicode bullet character should still error
        let result = tokenizer.tokenize("SELECT • FROM t");
        assert!(result.is_err());
    }

    #[test]
    fn test_colon_eq_tokenization() {
        let tokenizer = Tokenizer::default();

        // := should be a single ColonEq token
        let tokens = tokenizer.tokenize("a := 1").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].token_type, TokenType::Var);
        assert_eq!(tokens[1].token_type, TokenType::ColonEq);
        assert_eq!(tokens[2].token_type, TokenType::Number);

        // : followed by non-= should still be Colon
        let tokens = tokenizer.tokenize("a:b").unwrap();
        assert!(tokens.iter().any(|t| t.token_type == TokenType::Colon));
        assert!(!tokens.iter().any(|t| t.token_type == TokenType::ColonEq));

        // :: should still be DColon
        let tokens = tokenizer.tokenize("a::INT").unwrap();
        assert!(tokens.iter().any(|t| t.token_type == TokenType::DColon));
    }

    #[test]
    fn test_colon_eq_parsing() {
        use crate::parser::Parser;
        use crate::generator::Generator;

        // MySQL @var := value in SELECT
        let ast = Parser::parse_sql("SELECT @var1 := 1, @var2").expect("Failed to parse MySQL @var := expr");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT @var1 := 1, @var2");

        // MySQL @var := @var in SELECT
        let ast = Parser::parse_sql("SELECT @var1, @var2 := @var1").expect("Failed to parse MySQL @var2 := @var1");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT @var1, @var2 := @var1");

        // MySQL @var := COUNT(*)
        let ast = Parser::parse_sql("SELECT @var1 := COUNT(*) FROM t1").expect("Failed to parse MySQL @var := COUNT(*)");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT @var1 := COUNT(*) FROM t1");

        // MySQL SET @var := 1 (should normalize to = in output)
        let ast = Parser::parse_sql("SET @var1 := 1").expect("Failed to parse SET @var1 := 1");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SET @var1 = 1");

        // Function named args with :=
        let ast = Parser::parse_sql("UNION_VALUE(k1 := 1)").expect("Failed to parse named arg with :=");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "UNION_VALUE(k1 := 1)");

        // UNNEST with recursive := TRUE
        let ast = Parser::parse_sql("SELECT UNNEST(col, recursive := TRUE) FROM t").expect("Failed to parse UNNEST with :=");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT UNNEST(col, recursive := TRUE) FROM t");

        // DuckDB prefix alias: foo: 1 means 1 AS foo
        let ast = Parser::parse_sql("SELECT foo: 1").expect("Failed to parse DuckDB prefix alias foo: 1");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT 1 AS foo");

        // DuckDB prefix alias with multiple columns
        let ast = Parser::parse_sql("SELECT foo: 1, bar: 2, baz: 3").expect("Failed to parse DuckDB multiple prefix aliases");
        let output = Generator::sql(&ast[0]).expect("Failed to generate");
        assert_eq!(output, "SELECT 1 AS foo, 2 AS bar, 3 AS baz");
    }

    #[test]
    fn test_colon_eq_dialect_roundtrip() {
        use crate::dialects::{Dialect, DialectType};

        fn check(dialect: DialectType, sql: &str, expected: Option<&str>) {
            let d = Dialect::get(dialect);
            let ast = d.parse(sql).unwrap_or_else(|e| panic!("Parse error for '{}': {}", sql, e));
            assert!(!ast.is_empty(), "Empty AST for: {}", sql);
            let transformed = d.transform(ast[0].clone()).unwrap_or_else(|e| panic!("Transform error for '{}': {}", sql, e));
            let output = d.generate(&transformed).unwrap_or_else(|e| panic!("Generate error for '{}': {}", sql, e));
            let expected = expected.unwrap_or(sql);
            assert_eq!(output, expected, "Roundtrip failed for: {}", sql);
        }

        // MySQL := tests
        check(DialectType::MySQL, "SELECT @var1 := 1, @var2", None);
        check(DialectType::MySQL, "SELECT @var1, @var2 := @var1", None);
        check(DialectType::MySQL, "SELECT @var1 := COUNT(*) FROM t1", None);
        check(DialectType::MySQL, "SET @var1 := 1", Some("SET @var1 = 1"));

        // DuckDB := tests
        check(DialectType::DuckDB, "SELECT UNNEST(col, recursive := TRUE) FROM t", None);
        check(DialectType::DuckDB, "UNION_VALUE(k1 := 1)", None);

        // STRUCT_PACK(a := 'b')::json should at least parse without error
        // (The STRUCT_PACK -> Struct transformation is a separate feature)
        {
            let d = Dialect::get(DialectType::DuckDB);
            let ast = d.parse("STRUCT_PACK(a := 'b')::json").expect("Failed to parse STRUCT_PACK(a := 'b')::json");
            assert!(!ast.is_empty(), "Empty AST for STRUCT_PACK(a := 'b')::json");
        }

        // DuckDB prefix alias tests
        check(DialectType::DuckDB, "SELECT foo: 1", Some("SELECT 1 AS foo"));
        check(DialectType::DuckDB, "SELECT foo: 1, bar: 2, baz: 3", Some("SELECT 1 AS foo, 2 AS bar, 3 AS baz"));
    }

    #[test]
    fn test_comment_roundtrip() {
        use crate::parser::Parser;
        use crate::generator::Generator;

        fn check_roundtrip(sql: &str) -> Option<String> {
            let ast = match Parser::parse_sql(sql) {
                Ok(a) => a,
                Err(e) => return Some(format!("Parse error: {:?}", e)),
            };
            if ast.is_empty() {
                return Some("Empty AST".to_string());
            }
            let mut generator = Generator::default();
            let output = match generator.generate(&ast[0]) {
                Ok(o) => o,
                Err(e) => return Some(format!("Gen error: {:?}", e)),
            };
            if output == sql {
                None
            } else {
                Some(format!("Mismatch:\n  input:  {}\n  output: {}", sql, output))
            }
        }

        let tests = vec![
            // Nested comments
            "SELECT c /* c1 /* c2 */ c3 */",
            "SELECT c /* c1 /* c2 /* c3 */ */ */",
            // Simple alias with comments
            "SELECT c /* c1 */ AS alias /* c2 */",
            // Multiple columns with comments
            "SELECT a /* x */, b /* x */",
            // Multiple comments after column
            "SELECT a /* x */ /* y */ /* z */, b /* k */ /* m */",
            // FROM tables with comments
            "SELECT * FROM foo /* x */, bla /* x */",
            // Arithmetic with comments
            "SELECT 1 /* comment */ + 1",
            "SELECT 1 /* c1 */ + 2 /* c2 */",
            "SELECT 1 /* c1 */ + /* c2 */ 2 /* c3 */",
            // CAST with comments
            "SELECT CAST(x AS INT) /* comment */ FROM foo",
            // Function arguments with comments
            "SELECT FOO(x /* c */) /* FOO */, b /* b */",
            // Multi-part table names with comments
            "SELECT x FROM a.b.c /* x */, e.f.g /* x */",
            // INSERT with comments
            "INSERT INTO t1 (tc1 /* tc1 */, tc2 /* tc2 */) SELECT c1 /* sc1 */, c2 /* sc2 */ FROM t",
            // Leading comments on statements
            "/* c */ WITH x AS (SELECT 1) SELECT * FROM x",
            "/* comment1 */ INSERT INTO x /* comment2 */ VALUES (1, 2, 3)",
            "/* comment1 */ UPDATE tbl /* comment2 */ SET x = 2 WHERE x < 2",
            "/* comment1 */ DELETE FROM x /* comment2 */ WHERE y > 1",
            "/* comment */ CREATE TABLE foo AS SELECT 1",
            // Trailing comments on statements
            "INSERT INTO foo SELECT * FROM bar /* comment */",
            // Complex nested expressions with comments
            "SELECT FOO(x /* c1 */ + y /* c2 */ + BLA(5 /* c3 */)) FROM (VALUES (1 /* c4 */, \"test\" /* c5 */)) /* c6 */",
        ];

        let mut failures = Vec::new();
        for sql in tests {
            if let Some(e) = check_roundtrip(sql) {
                failures.push(e);
            }
        }

        if !failures.is_empty() {
            panic!("Comment roundtrip failures:\n{}", failures.join("\n\n"));
        }
    }

    #[test]
    fn test_dollar_quoted_string_parsing() {
        use crate::dialects::{Dialect, DialectType};

        // Test dollar string token parsing utility function
        let (tag, content) = super::parse_dollar_string_token("FOO\x00content here");
        assert_eq!(tag, Some("FOO".to_string()));
        assert_eq!(content, "content here");

        let (tag, content) = super::parse_dollar_string_token("just content");
        assert_eq!(tag, None);
        assert_eq!(content, "just content");

        // Test roundtrip for Databricks dialect with dollar-quoted function body
        fn check_databricks(sql: &str, expected: Option<&str>) {
            let d = Dialect::get(DialectType::Databricks);
            let ast = d.parse(sql).unwrap_or_else(|e| panic!("Parse error for '{}': {}", sql, e));
            assert!(!ast.is_empty(), "Empty AST for: {}", sql);
            let transformed = d.transform(ast[0].clone()).unwrap_or_else(|e| panic!("Transform error for '{}': {}", sql, e));
            let output = d.generate(&transformed).unwrap_or_else(|e| panic!("Generate error for '{}': {}", sql, e));
            let expected = expected.unwrap_or(sql);
            assert_eq!(output, expected, "Roundtrip failed for: {}", sql);
        }

        // Test [42]: $$...$$ heredoc
        check_databricks(
            "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $$def add_one(x):\n  return x+1$$",
            None
        );

        // Test [43]: $FOO$...$FOO$ tagged heredoc
        check_databricks(
            "CREATE FUNCTION add_one(x INT) RETURNS INT LANGUAGE PYTHON AS $FOO$def add_one(x):\n  return x+1$FOO$",
            None
        );
    }
}
