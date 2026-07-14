package ffi

import (
	"fmt"

	"github.com/ebitengine/purego"
)

type Library struct {
	dl   *dynamicLibrary
	path string

	Transpile                func(string, string, string) Result
	TranspileWithOptions     func(string, string, string, string) Result
	Format                   func(string, string) Result
	FormatWithOptions        func(string, string, string) Result
	Optimize                 func(string, string) Result
	Generate                 func(string, string) Result
	GenerateDataType         func(string, string) Result
	Validate                 func(string, string) ValidationResult
	ValidateWithOptions      func(string, string, string) ValidationResult
	Parse                    func(string, string) Result
	ParseOne                 func(string, string) Result
	ParseDataType            func(string, string) Result
	Tokenize                 func(string, string) Result
	AnnotateTypes            func(string, string, string) Result
	Diff                     func(string, string, string) Result
	QualifyTables            func(string, string) Result
	SetLimit                 func(string, uint64) Result
	SetOffset                func(string, uint64) Result
	SetOrderBy               func(string, string) Result
	RenameTablesWithOptions  func(string, string, string) Result
	Lineage                  func(string, string, string) Result
	LineageWithSchema        func(string, string, string, string) Result
	SourceTables             func(string, string, string) Result
	OpenLineageColumnLineage func(string, string) Result
	OpenLineageJobEvent      func(string, string) Result
	OpenLineageRunEvent      func(string, string) Result
	AnalyzeQuery             func(string, string) Result
	DialectList              func() uintptr
	DialectCount             func() int32
	Version                  func() uintptr
	FreeString               func(uintptr)
	FreeResult               func(Result)
	FreeValidationResult     func(ValidationResult)
}

func Open(path string) (*Library, error) {
	dl, err := openDynamicLibrary(path)
	if err != nil {
		return nil, err
	}

	lib := &Library{dl: dl, path: path}
	if err := lib.registerAll(); err != nil {
		_ = dl.close()
		return nil, err
	}
	return lib, nil
}

func (l *Library) Path() string {
	return l.path
}

func (l *Library) Close() error {
	if l == nil || l.dl == nil {
		return nil
	}
	err := l.dl.close()
	l.dl = nil
	return err
}

func (l *Library) registerAll() error {
	symbols := []struct {
		name string
		fn   any
	}{
		{"polyglot_transpile", &l.Transpile},
		{"polyglot_transpile_with_options", &l.TranspileWithOptions},
		{"polyglot_format", &l.Format},
		{"polyglot_format_with_options", &l.FormatWithOptions},
		{"polyglot_optimize", &l.Optimize},
		{"polyglot_generate", &l.Generate},
		{"polyglot_generate_data_type", &l.GenerateDataType},
		{"polyglot_validate", &l.Validate},
		{"polyglot_validate_with_options", &l.ValidateWithOptions},
		{"polyglot_parse", &l.Parse},
		{"polyglot_parse_one", &l.ParseOne},
		{"polyglot_parse_data_type", &l.ParseDataType},
		{"polyglot_tokenize", &l.Tokenize},
		{"polyglot_annotate_types", &l.AnnotateTypes},
		{"polyglot_diff", &l.Diff},
		{"polyglot_qualify_tables", &l.QualifyTables},
		{"polyglot_set_limit", &l.SetLimit},
		{"polyglot_set_offset", &l.SetOffset},
		{"polyglot_set_order_by", &l.SetOrderBy},
		{"polyglot_rename_tables_with_options", &l.RenameTablesWithOptions},
		{"polyglot_lineage", &l.Lineage},
		{"polyglot_lineage_with_schema", &l.LineageWithSchema},
		{"polyglot_source_tables", &l.SourceTables},
		{"polyglot_openlineage_column_lineage", &l.OpenLineageColumnLineage},
		{"polyglot_openlineage_job_event", &l.OpenLineageJobEvent},
		{"polyglot_openlineage_run_event", &l.OpenLineageRunEvent},
		{"polyglot_analyze_query", &l.AnalyzeQuery},
		{"polyglot_dialect_list", &l.DialectList},
		{"polyglot_dialect_count", &l.DialectCount},
		{"polyglot_version", &l.Version},
		{"polyglot_free_string", &l.FreeString},
		{"polyglot_free_result", &l.FreeResult},
		{"polyglot_free_validation_result", &l.FreeValidationResult},
	}

	for _, symbol := range symbols {
		if err := l.register(symbol.name, symbol.fn); err != nil {
			return err
		}
	}
	return nil
}

func (l *Library) register(name string, target any) (err error) {
	addr, err := l.dl.lookup(name)
	if err != nil {
		return fmt.Errorf("missing symbol %s: %w", name, err)
	}

	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = fmt.Errorf("register symbol %s: %v", name, panicValue)
		}
	}()
	purego.RegisterFunc(target, addr)
	return nil
}
