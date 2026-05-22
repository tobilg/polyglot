package polyglot

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func integrationClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv(LibraryPathEnv) == "" {
		t.Skipf("%s is not set", LibraryPathEnv)
	}
	client, err := OpenDefault()
	if err != nil {
		t.Fatalf("OpenDefault: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	return client
}

func integrationLibraryPath(t *testing.T) string {
	t.Helper()
	path := os.Getenv(LibraryPathEnv)
	if path == "" {
		t.Skipf("%s is not set", LibraryPathEnv)
	}
	return path
}

func integrationSchema() ValidationSchema {
	return ValidationSchema{
		Tables: []SchemaTable{
			{
				Name: "users",
				Columns: []SchemaColumn{
					{Name: "id", Type: "INT"},
					{Name: "name", Type: "TEXT"},
				},
			},
			{
				Name: "orders",
				Columns: []SchemaColumn{
					{Name: "order_id", Type: "INT"},
					{Name: "user_id", Type: "INT"},
					{Name: "total", Type: "INT"},
				},
			},
		},
	}
}

func integrationSchemaPtr() *ValidationSchema {
	schema := integrationSchema()
	return &schema
}

func integrationOpenLineageOptions() OpenLineageOptions {
	return OpenLineageOptions{
		Producer:         "https://github.com/tobilg/polyglot",
		DatasetNamespace: "warehouse",
		OutputDataset:    &OpenLineageDatasetID{Namespace: "warehouse", Name: "daily_orders"},
	}
}

func assertValidJSON(t *testing.T, name string, payload json.RawMessage) {
	t.Helper()
	if !json.Valid(payload) {
		t.Fatalf("%s returned invalid JSON: %s", name, payload)
	}
}

func TestIntegrationExplicitOpenAndLifecycle(t *testing.T) {
	path := integrationLibraryPath(t)

	client, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	if client.Version() != Version() {
		t.Fatalf("client Version() = %q, package Version() = %q", client.Version(), Version())
	}
	if _, err := client.RuntimeVersion(); err != nil {
		t.Fatalf("RuntimeVersion before close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := client.RuntimeVersion(); !errors.Is(err, ErrClosed) {
		t.Fatalf("RuntimeVersion after close err = %v, want ErrClosed", err)
	}
	if _, err := Open(""); err == nil {
		t.Fatal("Open accepted empty path")
	}
	if _, err := Open(path + ".missing"); err == nil {
		t.Fatal("Open accepted missing library path")
	}
}

func TestIntegrationCoreAPIs(t *testing.T) {
	client := integrationClient(t)

	runtimeVersion, err := client.RuntimeVersion()
	if err != nil {
		t.Fatalf("RuntimeVersion: %v", err)
	}
	if strings.TrimSpace(runtimeVersion) == "" {
		t.Fatal("empty runtime version")
	}

	dialects, err := client.Dialects()
	if err != nil {
		t.Fatalf("Dialects: %v", err)
	}
	if len(dialects) == 0 {
		t.Fatal("empty dialect list")
	}

	count, err := client.DialectCount()
	if err != nil {
		t.Fatalf("DialectCount: %v", err)
	}
	if count != len(dialects) {
		t.Fatalf("DialectCount = %d, len(Dialects) = %d", count, len(dialects))
	}

	transpiled, err := client.Transpile("SELECT IFNULL(a, b) FROM t", "mysql", "postgres")
	if err != nil {
		t.Fatalf("Transpile: %v", err)
	}
	if len(transpiled) != 1 || !strings.Contains(strings.ToUpper(transpiled[0]), "COALESCE") {
		t.Fatalf("unexpected transpile result: %#v", transpiled)
	}

	prettyTranspiled, err := client.Transpile(
		"SELECT a, b, c, d FROM t UNION SELECT a, b, c, d FROM u",
		"postgres",
		"postgres",
		TranspileOptions{Pretty: true},
	)
	if err != nil {
		t.Fatalf("Transpile with options: %v", err)
	}
	if len(prettyTranspiled) != 1 || !strings.Contains(prettyTranspiled[0], "\n") {
		t.Fatalf("expected pretty transpile output, got %#v", prettyTranspiled)
	}

	formatted, err := client.Format("SELECT a,b FROM t WHERE x=1", "postgres")
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	if len(formatted) != 1 || !strings.Contains(strings.ToUpper(formatted[0]), "SELECT") {
		t.Fatalf("unexpected format result: %#v", formatted)
	}

	maxSetOpChain := 8
	formatted, err = client.Format(
		"SELECT 1 UNION ALL SELECT 2",
		"generic",
		FormatOptions{MaxSetOpChain: &maxSetOpChain},
	)
	if err != nil {
		t.Fatalf("Format with options: %v", err)
	}
	if len(formatted) != 1 {
		t.Fatalf("unexpected format-with-options result: %#v", formatted)
	}

	optimized, err := client.Optimize("SELECT a FROM t WHERE NOT (NOT (b = 1))", "generic")
	if err != nil {
		t.Fatalf("Optimize: %v", err)
	}
	if len(optimized) != 1 || !strings.Contains(strings.ToUpper(optimized[0]), "SELECT") {
		t.Fatalf("unexpected optimize result: %#v", optimized)
	}

	validation, err := client.Validate("SELECT 1", "generic")
	if err != nil {
		t.Fatalf("Validate valid SQL: %v", err)
	}
	if !validation.Valid || len(validation.Errors) != 0 {
		t.Fatalf("valid SQL validation = %#v", validation)
	}

	validation, err = client.Validate("SELECT FROM", "generic")
	if err != nil {
		t.Fatalf("Validate invalid SQL should return data, not error: %v", err)
	}
	if validation.Valid || len(validation.Errors) == 0 {
		t.Fatalf("invalid SQL validation = %#v", validation)
	}

	parsed, err := client.Parse("SELECT a FROM t", "generic")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !json.Valid(parsed) {
		t.Fatalf("parse returned invalid JSON: %s", parsed)
	}

	parsedOne, err := client.ParseOne("SELECT a FROM t", "generic")
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	assertValidJSON(t, "ParseOne", parsedOne)

	tokens, err := client.Tokenize("SELECT a FROM t", "generic")
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	assertValidJSON(t, "Tokenize", tokens)
}

func TestIntegrationLineageAndOpenLineage(t *testing.T) {
	client := integrationClient(t)

	node, err := client.Lineage("total", "SELECT o.total FROM orders o", "generic")
	if err != nil {
		t.Fatalf("Lineage: %v", err)
	}
	if node.Name == "" {
		t.Fatalf("unexpected lineage node: %#v", node)
	}

	tables, err := client.SourceTables("total", "SELECT o.total FROM orders o", "generic")
	if err != nil {
		t.Fatalf("SourceTables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("empty source tables")
	}

	schemaNode, err := client.LineageWithSchema(
		"id",
		"SELECT id FROM users u JOIN orders o ON u.id = o.user_id",
		integrationSchema(),
		"generic",
	)
	if err != nil {
		t.Fatalf("LineageWithSchema: %v", err)
	}
	if schemaNode.Name == "" {
		t.Fatalf("unexpected schema lineage node: %#v", schemaNode)
	}

	options := integrationOpenLineageOptions()

	columnLineage, err := client.OpenLineageColumnLineage("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageColumnLineage: %v", err)
	}
	if len(columnLineage.Facet.Fields) == 0 {
		t.Fatalf("missing column lineage fields: %#v", columnLineage)
	}

	options.JobNamespace = "jobs"
	options.JobName = "daily_orders"
	options.EventTime = "2026-05-21T00:00:00Z"
	jobEvent, err := client.OpenLineageJobEvent("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageJobEvent: %v", err)
	}
	if !json.Valid(jobEvent.Event) {
		t.Fatalf("invalid job event JSON: %s", jobEvent.Event)
	}

	options.RunID = "run-1"
	options.EventType = OpenLineageRunEventComplete
	runEvent, err := client.OpenLineageRunEvent("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageRunEvent: %v", err)
	}
	if !json.Valid(runEvent.Event) {
		t.Fatalf("invalid run event JSON: %s", runEvent.Event)
	}

	if _, err := client.OpenLineageColumnLineage("SELECT total FROM orders", OpenLineageOptions{}); err == nil {
		t.Fatal("OpenLineageColumnLineage accepted missing producer/output dataset")
	}
}

func TestIntegrationASTHelpers(t *testing.T) {
	client := integrationClient(t)

	parsed, err := client.Parse("SELECT a FROM old_table", "generic")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	qualified, err := client.QualifyTables(parsed, QualifyTablesOptions{Dialect: "generic"})
	if err != nil {
		t.Fatalf("QualifyTables: %v", err)
	}
	assertValidJSON(t, "QualifyTables", qualified)

	renamed, err := client.RenameTables(parsed, map[string]string{"old_table": "new_table"}, RenameTablesOptions{})
	if err != nil {
		t.Fatalf("RenameTables: %v", err)
	}

	generated, err := client.Generate(renamed, "generic")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(generated) != 1 || !strings.Contains(generated[0], "new_table") {
		t.Fatalf("unexpected generated SQL: %#v", generated)
	}

	annotated, err := client.AnnotateTypes("SELECT 1", "generic", nil)
	if err != nil {
		t.Fatalf("AnnotateTypes: %v", err)
	}
	assertValidJSON(t, "AnnotateTypes", annotated)

	annotated, err = client.AnnotateTypes("SELECT total FROM orders", "generic", integrationSchemaPtr())
	if err != nil {
		t.Fatalf("AnnotateTypes with schema: %v", err)
	}
	assertValidJSON(t, "AnnotateTypes with schema", annotated)

	diff, err := client.Diff("SELECT a FROM t", "SELECT b FROM t", "generic")
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	assertValidJSON(t, "Diff", diff)
}

func TestIntegrationPackageLevelAPI(t *testing.T) {
	client := integrationClient(t)
	SetDefaultClient(client)
	t.Cleanup(ClearDefaultClient)

	if got, err := DefaultClient(); err != nil || got != client {
		t.Fatalf("DefaultClient() = %p, %v; want %p", got, err, client)
	}

	if version, err := RuntimeVersion(); err != nil || strings.TrimSpace(version) == "" {
		t.Fatalf("RuntimeVersion() = %q, %v", version, err)
	}

	dialects, err := Dialects()
	if err != nil {
		t.Fatalf("Dialects: %v", err)
	}
	count, err := DialectCount()
	if err != nil {
		t.Fatalf("DialectCount: %v", err)
	}
	if count != len(dialects) {
		t.Fatalf("DialectCount = %d, len(Dialects) = %d", count, len(dialects))
	}

	transpiled, err := Transpile("SELECT IFNULL(a, b) FROM t", "mysql", "postgres", TranspileOptions{})
	if err != nil {
		t.Fatalf("Transpile wrapper: %v", err)
	}
	if len(transpiled) != 1 {
		t.Fatalf("unexpected transpile wrapper result: %#v", transpiled)
	}

	formatted, err := Format("SELECT a,b FROM t", "generic", FormatOptions{})
	if err != nil {
		t.Fatalf("Format wrapper: %v", err)
	}
	if len(formatted) != 1 {
		t.Fatalf("unexpected format wrapper result: %#v", formatted)
	}

	optimized, err := Optimize("SELECT a FROM t", "generic")
	if err != nil {
		t.Fatalf("Optimize wrapper: %v", err)
	}
	if len(optimized) != 1 {
		t.Fatalf("unexpected optimize wrapper result: %#v", optimized)
	}

	parsed, err := Parse("SELECT a FROM old_table", "generic")
	if err != nil {
		t.Fatalf("Parse wrapper: %v", err)
	}
	assertValidJSON(t, "Parse wrapper", parsed)

	parsedOne, err := ParseOne("SELECT a FROM old_table", "generic")
	if err != nil {
		t.Fatalf("ParseOne wrapper: %v", err)
	}
	assertValidJSON(t, "ParseOne wrapper", parsedOne)

	tokens, err := Tokenize("SELECT a FROM old_table", "generic")
	if err != nil {
		t.Fatalf("Tokenize wrapper: %v", err)
	}
	assertValidJSON(t, "Tokenize wrapper", tokens)

	generated, err := Generate(parsed, "generic")
	if err != nil {
		t.Fatalf("Generate wrapper: %v", err)
	}
	if len(generated) != 1 {
		t.Fatalf("unexpected generate wrapper result: %#v", generated)
	}

	validation, err := Validate("SELECT 1", "generic")
	if err != nil {
		t.Fatalf("Validate wrapper: %v", err)
	}
	if !validation.Valid {
		t.Fatalf("unexpected validate wrapper result: %#v", validation)
	}

	annotated, err := AnnotateTypes("SELECT total FROM orders", "generic", integrationSchemaPtr())
	if err != nil {
		t.Fatalf("AnnotateTypes wrapper: %v", err)
	}
	assertValidJSON(t, "AnnotateTypes wrapper", annotated)

	diff, err := Diff("SELECT a FROM t", "SELECT b FROM t", "generic")
	if err != nil {
		t.Fatalf("Diff wrapper: %v", err)
	}
	assertValidJSON(t, "Diff wrapper", diff)

	qualified, err := QualifyTables(parsed, QualifyTablesOptions{Dialect: "generic"})
	if err != nil {
		t.Fatalf("QualifyTables wrapper: %v", err)
	}
	assertValidJSON(t, "QualifyTables wrapper", qualified)

	renamed, err := RenameTables(parsed, map[string]string{"old_table": "new_table"}, RenameTablesOptions{})
	if err != nil {
		t.Fatalf("RenameTables wrapper: %v", err)
	}
	assertValidJSON(t, "RenameTables wrapper", renamed)

	node, err := Lineage("total", "SELECT total FROM orders", "generic")
	if err != nil {
		t.Fatalf("Lineage wrapper: %v", err)
	}
	if node.Name == "" {
		t.Fatalf("unexpected lineage wrapper result: %#v", node)
	}

	node, err = LineageWithSchema("total", "SELECT total FROM orders", integrationSchema(), "generic")
	if err != nil {
		t.Fatalf("LineageWithSchema wrapper: %v", err)
	}
	if node.Name == "" {
		t.Fatalf("unexpected lineage-with-schema wrapper result: %#v", node)
	}

	tables, err := SourceTables("total", "SELECT total FROM orders", "generic")
	if err != nil {
		t.Fatalf("SourceTables wrapper: %v", err)
	}
	if len(tables) == 0 {
		t.Fatalf("unexpected source tables wrapper result: %#v", tables)
	}

	options := integrationOpenLineageOptions()
	columnLineage, err := OpenLineageColumnLineage("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageColumnLineage wrapper: %v", err)
	}
	if len(columnLineage.Facet.Fields) == 0 {
		t.Fatalf("unexpected OpenLineageColumnLineage wrapper result: %#v", columnLineage)
	}

	options.JobNamespace = "jobs"
	options.JobName = "daily_orders"
	options.EventTime = "2026-05-21T00:00:00Z"
	jobEvent, err := OpenLineageJobEvent("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageJobEvent wrapper: %v", err)
	}
	assertValidJSON(t, "OpenLineageJobEvent wrapper", jobEvent.Event)

	options.RunID = "run-1"
	options.EventType = OpenLineageRunEventComplete
	runEvent, err := OpenLineageRunEvent("SELECT total FROM orders", options)
	if err != nil {
		t.Fatalf("OpenLineageRunEvent wrapper: %v", err)
	}
	assertValidJSON(t, "OpenLineageRunEvent wrapper", runEvent.Event)
}
