package polyglot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tobilg/polyglot/packages/go/internal/ffi"
)

type Client struct {
	mu     sync.RWMutex
	lib    *ffi.Library
	closed bool
}

func Open(path string) (*Client, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("polyglot: library path is required")
	}
	lib, err := ffi.Open(path)
	if err != nil {
		return nil, fmt.Errorf("polyglot: open native library %q: %w", path, err)
	}
	return &Client{lib: lib}, nil
}

func OpenDefault() (*Client, error) {
	var errs []error
	for _, candidate := range defaultLibraryCandidates() {
		client, err := Open(candidate)
		if err == nil {
			return client, nil
		}
		errs = append(errs, err)
	}

	return nil, fmt.Errorf(
		"polyglot: could not open native library; set %s or install %s: %w",
		LibraryPathEnv,
		libraryFileName(),
		errors.Join(errs...),
	)
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	if c.lib == nil {
		return nil
	}
	err := c.lib.Close()
	c.lib = nil
	return err
}

func (c *Client) Version() string {
	return sdkVersion
}

func (c *Client) RuntimeVersion() (string, error) {
	lib, unlock, err := c.use()
	if err != nil {
		return "", err
	}
	defer unlock()

	ptr := lib.Version()
	if ptr == 0 {
		return "", &Error{Operation: "version", Status: ffi.StatusInternalError, Message: "polyglot_version returned NULL"}
	}
	return ffi.CString(ptr), nil
}

func (c *Client) Transpile(sql, fromDialect, toDialect string, options ...TranspileOptions) ([]string, error) {
	if err := rejectNUL(sql, fromDialect, toDialect); err != nil {
		return nil, err
	}
	fromDialect = defaultDialect(fromDialect)
	toDialect = defaultDialect(toDialect)

	if len(options) > 0 && options[0] != (TranspileOptions{}) {
		optionsJSON, err := marshalOptions(options[0])
		if err != nil {
			return nil, err
		}
		return c.callStringSlice("transpile", func(lib *ffi.Library) ffi.Result {
			return lib.TranspileWithOptions(sql, fromDialect, toDialect, optionsJSON)
		})
	}

	return c.callStringSlice("transpile", func(lib *ffi.Library) ffi.Result {
		return lib.Transpile(sql, fromDialect, toDialect)
	})
}

func (c *Client) Format(sql, dialect string, options ...FormatOptions) ([]string, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	dialect = defaultDialect(dialect)

	if len(options) > 0 && options[0] != (FormatOptions{}) {
		optionsJSON, err := marshalOptions(options[0])
		if err != nil {
			return nil, err
		}
		return c.callStringSlice("format", func(lib *ffi.Library) ffi.Result {
			return lib.FormatWithOptions(sql, dialect, optionsJSON)
		})
	}

	return c.callStringSlice("format", func(lib *ffi.Library) ffi.Result {
		return lib.Format(sql, dialect)
	})
}

func (c *Client) Optimize(sql, dialect string, options ...OptimizeOptions) ([]string, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	return c.callStringSlice("optimize", func(lib *ffi.Library) ffi.Result {
		return lib.Optimize(sql, defaultDialect(dialect))
	})
}

func (c *Client) Generate(ast json.RawMessage, dialect string, options ...GenerateOptions) ([]string, error) {
	if err := rejectNUL(string(ast), dialect); err != nil {
		return nil, err
	}
	return c.callStringSlice("generate", func(lib *ffi.Library) ffi.Result {
		return lib.Generate(string(ast), defaultDialect(dialect))
	})
}

func (c *Client) GenerateDataType(dataType json.RawMessage, dialect string) (string, error) {
	if err := rejectNUL(string(dataType), dialect); err != nil {
		return "", err
	}
	return c.callPayload("generate_data_type", func(lib *ffi.Library) ffi.Result {
		return lib.GenerateDataType(string(dataType), defaultDialect(dialect))
	})
}

func (c *Client) Validate(sql, dialect string) (ValidationResult, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return ValidationResult{}, err
	}

	lib, unlock, err := c.use()
	if err != nil {
		return ValidationResult{}, err
	}
	defer unlock()

	result := lib.Validate(sql, defaultDialect(dialect))
	defer lib.FreeValidationResult(result)

	message := ffi.CString(result.Error)
	if result.Status != ffi.StatusSuccess && result.Status != ffi.StatusValidationError {
		return ValidationResult{}, &Error{Operation: "validate", Status: result.Status, Message: message}
	}

	errorsJSON := ffi.CString(result.ErrorsJSON)
	var validationErrors []ValidationError
	if errorsJSON != "" {
		if err := json.Unmarshal([]byte(errorsJSON), &validationErrors); err != nil {
			return ValidationResult{}, fmt.Errorf("polyglot validate: decode validation errors: %w", err)
		}
	}

	return ValidationResult{
		Valid:  result.Valid != 0,
		Errors: validationErrors,
	}, nil
}

func (c *Client) Dialects() ([]string, error) {
	lib, unlock, err := c.use()
	if err != nil {
		return nil, err
	}
	defer unlock()

	ptr := lib.DialectList()
	if ptr == 0 {
		return nil, &Error{Operation: "dialects", Status: ffi.StatusInternalError, Message: "polyglot_dialect_list returned NULL"}
	}
	payload := ffi.CString(ptr)
	lib.FreeString(ptr)

	var dialects []string
	if err := json.Unmarshal([]byte(payload), &dialects); err != nil {
		return nil, fmt.Errorf("polyglot dialects: decode response: %w", err)
	}
	return dialects, nil
}

func (c *Client) DialectCount() (int, error) {
	lib, unlock, err := c.use()
	if err != nil {
		return 0, err
	}
	defer unlock()
	return int(lib.DialectCount()), nil
}

func (c *Client) Parse(sql, dialect string) (json.RawMessage, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	return c.callRaw("parse", func(lib *ffi.Library) ffi.Result {
		return lib.Parse(sql, defaultDialect(dialect))
	})
}

func (c *Client) ParseOne(sql, dialect string) (json.RawMessage, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	return c.callRaw("parse_one", func(lib *ffi.Library) ffi.Result {
		return lib.ParseOne(sql, defaultDialect(dialect))
	})
}

func (c *Client) ParseDataType(sql, dialect string) (json.RawMessage, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	return c.callRaw("parse_data_type", func(lib *ffi.Library) ffi.Result {
		return lib.ParseDataType(sql, defaultDialect(dialect))
	})
}

func (c *Client) Tokenize(sql, dialect string) (json.RawMessage, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	return c.callRaw("tokenize", func(lib *ffi.Library) ffi.Result {
		return lib.Tokenize(sql, defaultDialect(dialect))
	})
}

func (c *Client) AnnotateTypes(sql, dialect string, schema *ValidationSchema) (json.RawMessage, error) {
	if err := rejectNUL(sql, dialect); err != nil {
		return nil, err
	}
	schemaJSON, err := marshalOptionalSchema(schema)
	if err != nil {
		return nil, err
	}
	if err := rejectNUL(schemaJSON); err != nil {
		return nil, err
	}
	return c.callRaw("annotate_types", func(lib *ffi.Library) ffi.Result {
		return lib.AnnotateTypes(sql, defaultDialect(dialect), schemaJSON)
	})
}

func (c *Client) Diff(sql1, sql2, dialect string) (json.RawMessage, error) {
	if err := rejectNUL(sql1, sql2, dialect); err != nil {
		return nil, err
	}
	return c.callRaw("diff", func(lib *ffi.Library) ffi.Result {
		return lib.Diff(sql1, sql2, defaultDialect(dialect))
	})
}

func (c *Client) QualifyTables(ast json.RawMessage, options QualifyTablesOptions) (json.RawMessage, error) {
	optionsJSON, err := marshalOptions(options)
	if err != nil {
		return nil, err
	}
	if err := rejectNUL(string(ast), optionsJSON); err != nil {
		return nil, err
	}
	return c.callRaw("qualify_tables", func(lib *ffi.Library) ffi.Result {
		return lib.QualifyTables(string(ast), optionsJSON)
	})
}

func (c *Client) SetLimit(ast json.RawMessage, limit int) (json.RawMessage, error) {
	if limit < 0 {
		return nil, fmt.Errorf("polyglot: limit must be non-negative")
	}
	if err := rejectNUL(string(ast)); err != nil {
		return nil, err
	}
	return c.callRaw("set_limit", func(lib *ffi.Library) ffi.Result {
		return lib.SetLimit(string(ast), uint64(limit))
	})
}

func (c *Client) SetOffset(ast json.RawMessage, offset int) (json.RawMessage, error) {
	if offset < 0 {
		return nil, fmt.Errorf("polyglot: offset must be non-negative")
	}
	if err := rejectNUL(string(ast)); err != nil {
		return nil, err
	}
	return c.callRaw("set_offset", func(lib *ffi.Library) ffi.Result {
		return lib.SetOffset(string(ast), uint64(offset))
	})
}

func (c *Client) SetOrderBy(ast json.RawMessage, orderBy json.RawMessage) (json.RawMessage, error) {
	if err := rejectNUL(string(ast), string(orderBy)); err != nil {
		return nil, err
	}
	return c.callRaw("set_order_by", func(lib *ffi.Library) ffi.Result {
		return lib.SetOrderBy(string(ast), string(orderBy))
	})
}

func (c *Client) RenameTables(ast json.RawMessage, mapping map[string]string, options RenameTablesOptions) (json.RawMessage, error) {
	mappingJSON, err := marshalOptions(mapping)
	if err != nil {
		return nil, err
	}
	optionsJSON, err := marshalOptions(options)
	if err != nil {
		return nil, err
	}
	if err := rejectNUL(string(ast), mappingJSON, optionsJSON); err != nil {
		return nil, err
	}
	return c.callRaw("rename_tables", func(lib *ffi.Library) ffi.Result {
		return lib.RenameTablesWithOptions(string(ast), mappingJSON, optionsJSON)
	})
}

func (c *Client) Lineage(column, sql, dialect string) (LineageNode, error) {
	if err := rejectNUL(column, sql, dialect); err != nil {
		return LineageNode{}, err
	}
	var node LineageNode
	err := c.callJSON("lineage", func(lib *ffi.Library) ffi.Result {
		return lib.Lineage(column, sql, defaultDialect(dialect))
	}, &node)
	return node, err
}

func (c *Client) LineageWithSchema(column, sql string, schema ValidationSchema, dialect string) (LineageNode, error) {
	schemaJSON, err := marshalOptions(schema)
	if err != nil {
		return LineageNode{}, err
	}
	if err := rejectNUL(column, sql, schemaJSON, dialect); err != nil {
		return LineageNode{}, err
	}
	var node LineageNode
	err = c.callJSON("lineage_with_schema", func(lib *ffi.Library) ffi.Result {
		return lib.LineageWithSchema(column, sql, schemaJSON, defaultDialect(dialect))
	}, &node)
	return node, err
}

func (c *Client) SourceTables(column, sql, dialect string) ([]string, error) {
	if err := rejectNUL(column, sql, dialect); err != nil {
		return nil, err
	}
	return c.callStringSlice("source_tables", func(lib *ffi.Library) ffi.Result {
		return lib.SourceTables(column, sql, defaultDialect(dialect))
	})
}

func (c *Client) OpenLineageColumnLineage(sql string, options OpenLineageOptions) (OpenLineageColumnLineageResult, error) {
	optionsJSON, err := marshalOpenLineageOptions(options)
	if err != nil {
		return OpenLineageColumnLineageResult{}, err
	}
	if err := rejectNUL(sql, optionsJSON); err != nil {
		return OpenLineageColumnLineageResult{}, err
	}
	var result OpenLineageColumnLineageResult
	err = c.callJSON("openlineage_column_lineage", func(lib *ffi.Library) ffi.Result {
		return lib.OpenLineageColumnLineage(sql, optionsJSON)
	}, &result)
	return result, err
}

func (c *Client) OpenLineageJobEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error) {
	optionsJSON, err := marshalOpenLineageOptions(options)
	if err != nil {
		return OpenLineageEventResult{}, err
	}
	if err := rejectNUL(sql, optionsJSON); err != nil {
		return OpenLineageEventResult{}, err
	}
	var result OpenLineageEventResult
	err = c.callJSON("openlineage_job_event", func(lib *ffi.Library) ffi.Result {
		return lib.OpenLineageJobEvent(sql, optionsJSON)
	}, &result)
	return result, err
}

func (c *Client) OpenLineageRunEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error) {
	optionsJSON, err := marshalOpenLineageOptions(options)
	if err != nil {
		return OpenLineageEventResult{}, err
	}
	if err := rejectNUL(sql, optionsJSON); err != nil {
		return OpenLineageEventResult{}, err
	}
	var result OpenLineageEventResult
	err = c.callJSON("openlineage_run_event", func(lib *ffi.Library) ffi.Result {
		return lib.OpenLineageRunEvent(sql, optionsJSON)
	}, &result)
	return result, err
}

func (c *Client) AnalyzeQuery(sql string, options AnalyzeQueryOptions) (QueryAnalysis, error) {
	optionsJSON, err := marshalAnalyzeQueryOptions(options)
	if err != nil {
		return QueryAnalysis{}, err
	}
	if err := rejectNUL(sql, optionsJSON); err != nil {
		return QueryAnalysis{}, err
	}
	var analysis QueryAnalysis
	err = c.callJSON("analyze_query", func(lib *ffi.Library) ffi.Result {
		return lib.AnalyzeQuery(sql, optionsJSON)
	}, &analysis)
	return analysis, err
}

func (c *Client) callStringSlice(operation string, call func(*ffi.Library) ffi.Result) ([]string, error) {
	var output []string
	if err := c.callJSON(operation, call, &output); err != nil {
		return nil, err
	}
	return output, nil
}

func (c *Client) callRaw(operation string, call func(*ffi.Library) ffi.Result) (json.RawMessage, error) {
	payload, err := c.callPayload(operation, call)
	if err != nil {
		return nil, err
	}
	if !json.Valid([]byte(payload)) {
		return nil, fmt.Errorf("polyglot %s: invalid JSON response", operation)
	}
	return json.RawMessage(payload), nil
}

func (c *Client) callJSON(operation string, call func(*ffi.Library) ffi.Result, out any) error {
	payload, err := c.callPayload(operation, call)
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(payload), out); err != nil {
		return fmt.Errorf("polyglot %s: decode response: %w", operation, err)
	}
	return nil
}

func (c *Client) callPayload(operation string, call func(*ffi.Library) ffi.Result) (string, error) {
	lib, unlock, err := c.use()
	if err != nil {
		return "", err
	}
	defer unlock()

	result := call(lib)
	defer lib.FreeResult(result)

	if result.Status != ffi.StatusSuccess {
		message := ffi.CString(result.Error)
		if message == "" {
			message = statusName(result.Status)
		}
		return "", &Error{Operation: operation, Status: result.Status, Message: message}
	}
	return ffi.CString(result.Data), nil
}

func (c *Client) use() (*ffi.Library, func(), error) {
	if c == nil {
		return nil, func() {}, ErrClosed
	}
	c.mu.RLock()
	if c.closed || c.lib == nil {
		c.mu.RUnlock()
		return nil, func() {}, ErrClosed
	}
	return c.lib, c.mu.RUnlock, nil
}

func defaultDialect(dialect string) string {
	if strings.TrimSpace(dialect) == "" {
		return "generic"
	}
	return dialect
}

func marshalOptions(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("polyglot: encode options JSON: %w", err)
	}
	return string(data), nil
}

func marshalOptionalSchema(schema *ValidationSchema) (string, error) {
	if schema == nil {
		return "", nil
	}
	return marshalOptions(schema)
}

func marshalOpenLineageOptions(options OpenLineageOptions) (string, error) {
	if strings.TrimSpace(options.Dialect) == "" {
		options.Dialect = "generic"
	}
	if options.DatasetMappings == nil {
		options.DatasetMappings = map[string]OpenLineageDatasetID{}
	}
	return marshalOptions(options)
}

func marshalAnalyzeQueryOptions(options AnalyzeQueryOptions) (string, error) {
	if strings.TrimSpace(options.Dialect) == "" {
		options.Dialect = "generic"
	}
	return marshalOptions(options)
}

func rejectNUL(values ...string) error {
	for _, value := range values {
		if strings.ContainsRune(value, '\x00') {
			return fmt.Errorf("polyglot: strings passed to the native library must not contain NUL bytes")
		}
	}
	return nil
}
