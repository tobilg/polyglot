package polyglot

import (
	"encoding/json"
	"sync"
)

var defaultClient struct {
	sync.RWMutex
	client *Client
}

func SetDefaultClient(client *Client) {
	defaultClient.Lock()
	defaultClient.client = client
	defaultClient.Unlock()
}

func ClearDefaultClient() {
	SetDefaultClient(nil)
}

func DefaultClient() (*Client, error) {
	defaultClient.RLock()
	client := defaultClient.client
	defaultClient.RUnlock()
	if client == nil {
		return nil, ErrNoDefaultClient
	}
	return client, nil
}

func RuntimeVersion() (string, error) {
	client, err := DefaultClient()
	if err != nil {
		return "", err
	}
	return client.RuntimeVersion()
}

func Transpile(sql, fromDialect, toDialect string, options ...TranspileOptions) ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Transpile(sql, fromDialect, toDialect, options...)
}

func Format(sql, dialect string, options ...FormatOptions) ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Format(sql, dialect, options...)
}

func Optimize(sql, dialect string, options ...OptimizeOptions) ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Optimize(sql, dialect, options...)
}

func Generate(ast json.RawMessage, dialect string, options ...GenerateOptions) ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Generate(ast, dialect, options...)
}

func GenerateDataType(dataType json.RawMessage, dialect string) (string, error) {
	client, err := DefaultClient()
	if err != nil {
		return "", err
	}
	return client.GenerateDataType(dataType, dialect)
}

func Validate(sql, dialect string, options ...ValidationOptions) (ValidationResult, error) {
	client, err := DefaultClient()
	if err != nil {
		return ValidationResult{}, err
	}
	return client.Validate(sql, dialect, options...)
}

func Dialects() ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Dialects()
}

func DialectCount() (int, error) {
	client, err := DefaultClient()
	if err != nil {
		return 0, err
	}
	return client.DialectCount()
}

func Parse(sql, dialect string) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Parse(sql, dialect)
}

func ParseOne(sql, dialect string) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.ParseOne(sql, dialect)
}

func ParseDataType(sql, dialect string) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.ParseDataType(sql, dialect)
}

func Tokenize(sql, dialect string) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Tokenize(sql, dialect)
}

func AnnotateTypes(sql, dialect string, schema *ValidationSchema) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.AnnotateTypes(sql, dialect, schema)
}

func Diff(sql1, sql2, dialect string) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.Diff(sql1, sql2, dialect)
}

func QualifyTables(ast json.RawMessage, options QualifyTablesOptions) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.QualifyTables(ast, options)
}

func SetLimit(ast json.RawMessage, limit int) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.SetLimit(ast, limit)
}

func SetOffset(ast json.RawMessage, offset int) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.SetOffset(ast, offset)
}

func SetOrderBy(ast json.RawMessage, orderBy json.RawMessage) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.SetOrderBy(ast, orderBy)
}

func RenameTables(ast json.RawMessage, mapping map[string]string, options RenameTablesOptions) (json.RawMessage, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.RenameTables(ast, mapping, options)
}

func Lineage(column, sql, dialect string) (LineageNode, error) {
	client, err := DefaultClient()
	if err != nil {
		return LineageNode{}, err
	}
	return client.Lineage(column, sql, dialect)
}

func LineageWithSchema(column, sql string, schema ValidationSchema, dialect string) (LineageNode, error) {
	client, err := DefaultClient()
	if err != nil {
		return LineageNode{}, err
	}
	return client.LineageWithSchema(column, sql, schema, dialect)
}

func SourceTables(column, sql, dialect string) ([]string, error) {
	client, err := DefaultClient()
	if err != nil {
		return nil, err
	}
	return client.SourceTables(column, sql, dialect)
}

func OpenLineageColumnLineage(sql string, options OpenLineageOptions) (OpenLineageColumnLineageResult, error) {
	client, err := DefaultClient()
	if err != nil {
		return OpenLineageColumnLineageResult{}, err
	}
	return client.OpenLineageColumnLineage(sql, options)
}

func OpenLineageJobEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error) {
	client, err := DefaultClient()
	if err != nil {
		return OpenLineageEventResult{}, err
	}
	return client.OpenLineageJobEvent(sql, options)
}

func OpenLineageRunEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error) {
	client, err := DefaultClient()
	if err != nil {
		return OpenLineageEventResult{}, err
	}
	return client.OpenLineageRunEvent(sql, options)
}

func AnalyzeQuery(sql string, options AnalyzeQueryOptions) (QueryAnalysis, error) {
	client, err := DefaultClient()
	if err != nil {
		return QueryAnalysis{}, err
	}
	return client.AnalyzeQuery(sql, options)
}
