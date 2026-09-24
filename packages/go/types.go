package polyglot

import "encoding/json"

const sdkVersion = "0.13.0"

func Version() string {
	return sdkVersion
}

type TranspileOptions struct {
	Pretty           bool                    `json:"pretty,omitempty"`
	UnsupportedLevel UnsupportedLevel        `json:"unsupportedLevel,omitempty"`
	MaxUnsupported   int                     `json:"maxUnsupported,omitempty"`
	ComplexityGuard  *ComplexityGuardOptions `json:"complexityGuard,omitempty"`
}

type ComplexityGuardOptions struct {
	MaxParserDepth       GuardLimit `json:"maxParserDepth"`
	MaxInputBytes        GuardLimit `json:"maxInputBytes"`
	MaxTokens            GuardLimit `json:"maxTokens"`
	MaxASTNodes          GuardLimit `json:"maxAstNodes"`
	MaxASTDepth          GuardLimit `json:"maxAstDepth"`
	MaxParenthesisDepth  GuardLimit `json:"maxParenthesisDepth"`
	MaxFunctionCallDepth GuardLimit `json:"maxFunctionCallDepth"`
}

// GuardLimit distinguishes an omitted limit from a number (including zero) and
// explicit disabling. Raising or disabling a parser limit can permit stack exhaustion.
type GuardLimit struct {
	value *uint64
	set   bool
}

// NewGuardLimit supplies an explicit, nonnegative resource limit.
func NewGuardLimit(value uint64) GuardLimit { return GuardLimit{value: &value, set: true} }

// DisabledGuardLimit disables this check only; other guards remain active.
func DisabledGuardLimit() GuardLimit { return GuardLimit{set: true} }

func (limit GuardLimit) MarshalJSON() ([]byte, error) { return json.Marshal(limit.value) }

func (limit *GuardLimit) UnmarshalJSON(data []byte) error {
	var value *uint64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*limit = GuardLimit{value: value, set: true}
	return nil
}

// Custom omission is needed on Go 1.22: omitempty does not omit a value struct.
func (options ComplexityGuardOptions) MarshalJSON() ([]byte, error) {
	limits := map[string]GuardLimit{
		"maxParserDepth":       options.MaxParserDepth,
		"maxInputBytes":        options.MaxInputBytes,
		"maxTokens":            options.MaxTokens,
		"maxAstNodes":          options.MaxASTNodes,
		"maxAstDepth":          options.MaxASTDepth,
		"maxParenthesisDepth":  options.MaxParenthesisDepth,
		"maxFunctionCallDepth": options.MaxFunctionCallDepth,
	}
	for name, limit := range limits {
		if !limit.set {
			delete(limits, name)
		}
	}
	return json.Marshal(limits)
}

func (options *ComplexityGuardOptions) UnmarshalJSON(data []byte) error {
	type fields ComplexityGuardOptions
	var decoded fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*options = ComplexityGuardOptions(decoded)
	return nil
}

type UnsupportedLevel string

const (
	UnsupportedIgnore    UnsupportedLevel = "ignore"
	UnsupportedWarn      UnsupportedLevel = "warn"
	UnsupportedRaise     UnsupportedLevel = "raise"
	UnsupportedImmediate UnsupportedLevel = "immediate"
)

type FormatOptions struct {
	MaxInputBytes *int `json:"maxInputBytes,omitempty"`
	MaxTokens     *int `json:"maxTokens,omitempty"`
	MaxASTNodes   *int `json:"maxAstNodes,omitempty"`
	MaxSetOpChain *int `json:"maxSetOpChain,omitempty"`
}

type OptimizeOptions struct{}

type GenerateOptions struct{}

// ParseOptions configures parsing without changing dialect-specific defaults.
type ParseOptions struct {
	ComplexityGuard *ComplexityGuardOptions `json:"complexityGuard,omitempty"`
}

type AnalyzeQueryOptions struct {
	ComplexityGuard *ComplexityGuardOptions `json:"complexityGuard,omitempty"`
	Dialect         string                  `json:"dialect,omitempty"`
	Schema          *ValidationSchema       `json:"schema,omitempty"`
}

type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors"`
}

type ValidationOptions struct {
	ComplexityGuard *ComplexityGuardOptions `json:"complexityGuard,omitempty"`
	StrictSyntax    bool                    `json:"strictSyntax,omitempty"`
	Semantic        bool                    `json:"semantic,omitempty"`
}

// SchemaValidationOptions controls the shared Rust schema validator. Unknown
// identifiers are checked by default. Strict overrides ValidationSchema.Strict;
// nil inherits the schema setting (which defaults to true).
type SchemaValidationOptions struct {
	ComplexityGuard *ComplexityGuardOptions `json:"complexityGuard,omitempty"`
	CheckTypes      bool                    `json:"check_types,omitempty"`
	CheckReferences bool                    `json:"check_references,omitempty"`
	Strict          *bool                   `json:"strict,omitempty"`
	Semantic        bool                    `json:"semantic,omitempty"`
	StrictSyntax    bool                    `json:"strict_syntax,omitempty"`
}

type ValidationError struct {
	Message  string `json:"message"`
	Line     *int   `json:"line,omitempty"`
	Column   *int   `json:"column,omitempty"`
	Severity string `json:"severity"`
	Code     string `json:"code"`
	// Start and End are Unicode character offsets, suitable for slicing []rune(sql).
	// End is exclusive; nil means the source range is unavailable.
	Start *int `json:"start,omitempty"`
	End   *int `json:"end,omitempty"`
}

type SchemaColumnReference struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Schema string `json:"schema,omitempty"`
}

type SchemaTableReference struct {
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
	Schema  string   `json:"schema,omitempty"`
}

type SchemaForeignKey struct {
	Name       string               `json:"name,omitempty"`
	Columns    []string             `json:"columns"`
	References SchemaTableReference `json:"references"`
}

type SchemaColumn struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type,omitempty"`
	Nullable   *bool                  `json:"nullable,omitempty"`
	PrimaryKey bool                   `json:"primaryKey,omitempty"`
	Unique     bool                   `json:"unique,omitempty"`
	References *SchemaColumnReference `json:"references,omitempty"`
}

type SchemaTable struct {
	Name        string             `json:"name"`
	Schema      string             `json:"schema,omitempty"`
	Columns     []SchemaColumn     `json:"columns"`
	Aliases     []string           `json:"aliases,omitempty"`
	PrimaryKey  []string           `json:"primaryKey,omitempty"`
	UniqueKeys  [][]string         `json:"uniqueKeys,omitempty"`
	ForeignKeys []SchemaForeignKey `json:"foreignKeys,omitempty"`
}

type ValidationSchema struct {
	Tables []SchemaTable `json:"tables"`
	Strict *bool         `json:"strict,omitempty"`
}

// MarshalJSON represents nil table/column slices as empty arrays, matching the
// shared schema contract. An empty column list denotes an open schema.
func (schema ValidationSchema) MarshalJSON() ([]byte, error) {
	type wireSchema ValidationSchema
	wire := wireSchema(schema)
	wire.Tables = append([]SchemaTable{}, schema.Tables...)
	for i := range wire.Tables {
		if wire.Tables[i].Columns == nil {
			wire.Tables[i].Columns = []SchemaColumn{}
		}
	}
	return json.Marshal(wire)
}

type LineageNode struct {
	Name              string          `json:"name"`
	Expression        json.RawMessage `json:"expression"`
	Source            json.RawMessage `json:"source"`
	Downstream        []LineageNode   `json:"downstream"`
	SourceName        string          `json:"source_name"`
	SourceKind        string          `json:"source_kind"`
	SourceAlias       *string         `json:"source_alias,omitempty"`
	SetBranch         *SetBranch      `json:"set_branch,omitempty"`
	ReferenceNodeName string          `json:"reference_node_name"`
}

type SetOperator string

const (
	SetOperatorUnion     SetOperator = "union"
	SetOperatorIntersect SetOperator = "intersect"
	SetOperatorExcept    SetOperator = "except"
)

type SetBranch struct {
	Operator SetOperator `json:"operator"`
	Ordinal  int         `json:"ordinal"`
	All      bool        `json:"all"`
}

type OutputColumnKind string

const (
	OutputColumnNamed    OutputColumnKind = "named"
	OutputColumnUnnamed  OutputColumnKind = "unnamed"
	OutputColumnWildcard OutputColumnKind = "wildcard"
)

// OutputColumn describes one projection slot. Fields that do not apply to the
// selected Kind are nil.
type OutputColumn struct {
	Kind         OutputColumnKind `json:"kind"`
	Name         *string          `json:"name,omitempty"`
	Ordinal      *int             `json:"ordinal"`
	Qualifier    *string          `json:"qualifier,omitempty"`
	StartOrdinal *int             `json:"startOrdinal,omitempty"`
}

type QueryOutput struct {
	Columns         []OutputColumn `json:"columns"`
	OrdinalComplete bool           `json:"ordinalComplete"`
}

type QueryAnalysis struct {
	Shape           string               `json:"shape"`
	CTEs            []string             `json:"ctes"`
	CTEFacts        []CTEFact            `json:"cteFacts"`
	Projections     []ProjectionFact     `json:"projections"`
	Relations       []RelationFact       `json:"relations"`
	BaseTables      []RelationFact       `json:"baseTables"`
	StarProjections []StarProjectionFact `json:"starProjections"`
	SetOperations   []SetOperationFact   `json:"setOperations"`
	ColumnUses      []ColumnUseFact      `json:"columnUses"`
}

// QuerySourceSpan is a half-open range of Unicode characters in the original SQL.
type QuerySourceSpan struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// ColumnUseReferenceFact locates the use, not its upstream column definition.
type ColumnUseReferenceFact struct {
	ColumnReferenceFact
	Span *QuerySourceSpan `json:"span,omitempty"`
}

type ColumnUseContext string

const (
	ColumnUseJoin               ColumnUseContext = "join"
	ColumnUseFilter             ColumnUseContext = "filter"
	ColumnUseGroup              ColumnUseContext = "group"
	ColumnUseHaving             ColumnUseContext = "having"
	ColumnUseQualify            ColumnUseContext = "qualify"
	ColumnUseWindowPartition    ColumnUseContext = "window_partition"
	ColumnUseWindowOrder        ColumnUseContext = "window_order"
	ColumnUseWindowFrame        ColumnUseContext = "window_frame"
	ColumnUseOrder              ColumnUseContext = "order"
	ColumnUseAggregateOrder     ColumnUseContext = "aggregate_order"
	ColumnUseSetOperationFilter ColumnUseContext = "set_operation_filter"
)

// ColumnUseFact groups references by their containing expression. Paths identify
// locations within an analysis, not persistent IDs across SQL edits. ExpressionSQL
// is dialect-rendered SQL; Span is absent unless a complete source range is known.
type ColumnUseFact struct {
	Context        ColumnUseContext         `json:"context"`
	ScopePath      string                   `json:"scopePath"`
	ExpressionPath string                   `json:"expressionPath"`
	ExpressionSQL  string                   `json:"expressionSql"`
	Span           *QuerySourceSpan         `json:"span,omitempty"`
	References     []ColumnUseReferenceFact `json:"references"`
}

type ProjectionFact struct {
	Index             int                    `json:"index"`
	Name              *string                `json:"name"`
	IsStar            bool                   `json:"isStar"`
	StarTable         *string                `json:"starTable"`
	TransformKind     string                 `json:"transformKind"`
	TransformFunction *TransformFunctionFact `json:"transformFunction,omitempty"`
	CastType          *string                `json:"castType"`
	TypeHint          *string                `json:"typeHint"`
	Nullability       string                 `json:"nullability"`
	Upstream          []ColumnReferenceFact  `json:"upstream"`
}

type TransformFunctionFact struct {
	Name        string                `json:"name"`
	LiteralArgs []string              `json:"literalArgs"`
	ColumnArgs  []ColumnReferenceFact `json:"columnArgs"`
}

type CTEFact struct {
	Name          string   `json:"name"`
	Columns       []string `json:"columns"`
	BodySQL       string   `json:"bodySql"`
	OutputColumns []string `json:"outputColumns"`
}

type StarProjectionFact struct {
	Index           int      `json:"index"`
	Table           *string  `json:"table"`
	ExpandedColumns []string `json:"expandedColumns"`
}

type ColumnReferenceFact struct {
	SourceName  *string `json:"sourceName"`
	SourceAlias *string `json:"sourceAlias"`
	SourceKind  string  `json:"sourceKind"`
	Table       *string `json:"table"`
	Column      string  `json:"column"`
	Unqualified bool    `json:"unqualified"`
	Confidence  string  `json:"confidence"`
}

type RelationFact struct {
	Name    string   `json:"name"`
	Alias   *string  `json:"alias"`
	Kind    string   `json:"kind"`
	Columns []string `json:"columns"`
	Catalog *string  `json:"catalog"`
	Schema  *string  `json:"schema"`
	Table   *string  `json:"table"`
}

type SetOperationFact struct {
	Kind          string                   `json:"kind"`
	All           bool                     `json:"all"`
	Distinct      bool                     `json:"distinct"`
	OutputColumns []string                 `json:"outputColumns"`
	Branches      []SetOperationBranchFact `json:"branches"`
}

type SetOperationBranchFact struct {
	Index       int                    `json:"index"`
	Role        SetOperationBranchRole `json:"role"`
	Projections []ProjectionFact       `json:"projections"`
}

type SetOperationBranchRole string

const (
	SetOperationBranchRoleValue  SetOperationBranchRole = "value"
	SetOperationBranchRoleFilter SetOperationBranchRole = "filter"
)

type RenameTablesOptions struct {
	AliasRenamedTables      *bool `json:"aliasRenamedTables,omitempty"`
	PreserveExistingAliases *bool `json:"preserveExistingAliases,omitempty"`
}

type QualifyTablesOptions struct {
	DB                              string `json:"db,omitempty"`
	Catalog                         string `json:"catalog,omitempty"`
	Dialect                         string `json:"dialect,omitempty"`
	CanonicalizeTableAliases        *bool  `json:"canonicalizeTableAliases,omitempty"`
	AliasUnaliasedTables            *bool  `json:"aliasUnaliasedTables,omitempty"`
	AliasUnaliasedSubqueries        *bool  `json:"aliasUnaliasedSubqueries,omitempty"`
	AliasPrefix                     string `json:"aliasPrefix,omitempty"`
	NormalizeSetOperationSubqueries *bool  `json:"normalizeSetOperationSubqueries,omitempty"`
}

type OpenLineageDatasetID struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type OpenLineageRunEventType string

const (
	OpenLineageRunEventStart    OpenLineageRunEventType = "START"
	OpenLineageRunEventRunning  OpenLineageRunEventType = "RUNNING"
	OpenLineageRunEventComplete OpenLineageRunEventType = "COMPLETE"
	OpenLineageRunEventAbort    OpenLineageRunEventType = "ABORT"
	OpenLineageRunEventFail     OpenLineageRunEventType = "FAIL"
	OpenLineageRunEventOther    OpenLineageRunEventType = "OTHER"
)

type OpenLineageOptions struct {
	Dialect          string                          `json:"dialect,omitempty"`
	Producer         string                          `json:"producer"`
	DatasetNamespace string                          `json:"datasetNamespace,omitempty"`
	DatasetMappings  map[string]OpenLineageDatasetID `json:"datasetMappings"`
	OutputDataset    *OpenLineageDatasetID           `json:"outputDataset,omitempty"`
	Schema           *ValidationSchema               `json:"schema,omitempty"`
	JobNamespace     string                          `json:"jobNamespace,omitempty"`
	JobName          string                          `json:"jobName,omitempty"`
	EventTime        string                          `json:"eventTime,omitempty"`
	RunID            string                          `json:"runId,omitempty"`
	EventType        OpenLineageRunEventType         `json:"eventType,omitempty"`
}

type OpenLineageWarning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type OpenLineageTransformation struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	Description string `json:"description,omitempty"`
	Masking     *bool  `json:"masking,omitempty"`
}

type OpenLineageInputField struct {
	Namespace       string                      `json:"namespace"`
	Name            string                      `json:"name"`
	Field           string                      `json:"field"`
	Transformations []OpenLineageTransformation `json:"transformations,omitempty"`
}

type OpenLineageColumnLineageField struct {
	InputFields []OpenLineageInputField `json:"inputFields"`
}

type OpenLineageColumnLineageFacet struct {
	Producer  string                                   `json:"_producer"`
	SchemaURL string                                   `json:"_schemaURL"`
	Fields    map[string]OpenLineageColumnLineageField `json:"fields"`
}

type OpenLineageDataset struct {
	Namespace string                     `json:"namespace"`
	Name      string                     `json:"name"`
	Facets    map[string]json.RawMessage `json:"facets,omitempty"`
}

type OpenLineageColumnLineageResult struct {
	Facet    OpenLineageColumnLineageFacet `json:"facet"`
	Inputs   []OpenLineageDataset          `json:"inputs"`
	Outputs  []OpenLineageDataset          `json:"outputs"`
	Warnings []OpenLineageWarning          `json:"warnings"`
}

type OpenLineageEventResult struct {
	Event    json.RawMessage      `json:"event"`
	Warnings []OpenLineageWarning `json:"warnings"`
}
