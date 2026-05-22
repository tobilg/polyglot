package polyglot

import "encoding/json"

const sdkVersion = "0.4.0"

func Version() string {
	return sdkVersion
}

type TranspileOptions struct {
	Pretty bool `json:"pretty,omitempty"`
}

type FormatOptions struct {
	MaxInputBytes *int `json:"maxInputBytes,omitempty"`
	MaxTokens     *int `json:"maxTokens,omitempty"`
	MaxASTNodes   *int `json:"maxAstNodes,omitempty"`
	MaxSetOpChain *int `json:"maxSetOpChain,omitempty"`
}

type OptimizeOptions struct{}

type GenerateOptions struct{}

type ValidationResult struct {
	Valid  bool              `json:"valid"`
	Errors []ValidationError `json:"errors"`
}

type ValidationError struct {
	Message  string `json:"message"`
	Line     *int   `json:"line,omitempty"`
	Column   *int   `json:"column,omitempty"`
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Start    *int   `json:"start,omitempty"`
	End      *int   `json:"end,omitempty"`
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

type LineageNode struct {
	Name              string          `json:"name"`
	Expression        json.RawMessage `json:"expression"`
	Source            json.RawMessage `json:"source"`
	Downstream        []LineageNode   `json:"downstream"`
	SourceName        string          `json:"source_name"`
	ReferenceNodeName string          `json:"reference_node_name"`
}

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
