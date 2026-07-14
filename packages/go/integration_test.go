package polyglot

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
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

func benchmarkClient(b *testing.B) *Client {
	b.Helper()
	if os.Getenv(LibraryPathEnv) == "" {
		b.Skipf("%s is not set", LibraryPathEnv)
	}
	client, err := OpenDefault()
	if err != nil {
		b.Fatalf("OpenDefault: %v", err)
	}
	b.Cleanup(func() {
		if err := client.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	})
	return client
}

func BenchmarkNativeProfileParse(b *testing.B) {
	client := benchmarkClient(b)
	columns := make([]string, 1_000)
	for index := range columns {
		columns[index] = "c" + strconv.Itoa(index)
	}
	numbers := make([]string, 10_000)
	for value := range numbers {
		numbers[value] = strconv.Itoa(value)
	}
	queries := []struct {
		name string
		sql  string
	}{
		{"representative", "SELECT a, b, SUM(c) AS total FROM events WHERE created_at >= '2025-01-01' GROUP BY a, b"},
		{"many_columns", "SELECT " + strings.Join(columns, ", ") + " FROM events"},
		{"nested_functions", "SELECT " + strings.Repeat("COALESCE(", 20) +
			"value" + strings.Repeat(", NULL)", 20) + " FROM events"},
		{"large_strings", "SELECT " + strings.Repeat("'"+strings.Repeat("x", 100)+"', ", 499) +
			"'" + strings.Repeat("x", 100) + "' FROM events"},
		{"many_numbers", "SELECT " + strings.Join(numbers, ", ") + " FROM events"},
	}

	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := client.Parse(query.sql, "postgres"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNativeProfileTranspile(b *testing.B) {
	client := benchmarkClient(b)
	const sql = "SELECT a, b, SUM(c) AS total FROM events WHERE created_at >= '2025-01-01' GROUP BY a, b"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.Transpile(sql, "postgres", "duckdb"); err != nil {
			b.Fatal(err)
		}
	}
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
	nullable := true
	nonNull := false

	return ValidationSchema{
		Tables: []SchemaTable{
			{
				Name: "users",
				Columns: []SchemaColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "name", Type: "TEXT", Nullable: &nonNull},
				},
			},
			{
				Name: "orders",
				Columns: []SchemaColumn{
					{Name: "order_id", Type: "INT", Nullable: &nonNull},
					{Name: "user_id", Type: "INT"},
					{Name: "amount", Type: "DECIMAL(10,2)", Nullable: &nullable},
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

func collectLineageNames(node LineageNode) []string {
	names := []string{node.Name}
	for _, child := range node.Downstream {
		names = append(names, collectLineageNames(child)...)
	}
	return names
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func hasUpstream(values []ColumnReferenceFact, table string, column string) bool {
	for _, value := range values {
		if value.Table != nil && *value.Table == table && value.Column == column {
			return true
		}
	}
	return false
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

	validation, err = client.Validate(
		"SELECT * FROM users LIMIT 10",
		"generic",
		ValidationOptions{Semantic: true},
	)
	if err != nil {
		t.Fatalf("Validate semantic SQL: %v", err)
	}
	if !validation.Valid || len(validation.Errors) < 2 {
		t.Fatalf("semantic SQL validation = %#v", validation)
	}

	validation, err = client.Validate(
		"SELECT *, FROM users",
		"generic",
		ValidationOptions{StrictSyntax: true, Semantic: true},
	)
	if err != nil {
		t.Fatalf("Validate strict SQL: %v", err)
	}
	if validation.Valid || len(validation.Errors) != 1 || validation.Errors[0].Code != "E005" {
		t.Fatalf("strict SQL validation = %#v", validation)
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

	dataType, err := client.ParseDataType("DECIMAL(10, 2)", "duckdb")
	if err != nil {
		t.Fatalf("ParseDataType: %v", err)
	}
	assertValidJSON(t, "ParseDataType", dataType)

	var decodedDataType map[string]any
	if err := json.Unmarshal(dataType, &decodedDataType); err != nil {
		t.Fatalf("decode ParseDataType: %v", err)
	}
	if decodedDataType["data_type"] != "decimal" {
		t.Fatalf("unexpected data type payload: %#v", decodedDataType)
	}

	generatedType, err := client.GenerateDataType(dataType, "postgres")
	if err != nil {
		t.Fatalf("GenerateDataType: %v", err)
	}
	if generatedType != "DECIMAL(10, 2)" {
		t.Fatalf("GenerateDataType = %q", generatedType)
	}

	if _, err := client.ParseDataType("DECIMAL(10, 2) SELECT 1", "duckdb"); err == nil {
		t.Fatal("ParseDataType accepted trailing SQL")
	}

	tokens, err := client.Tokenize("SELECT a FROM t", "generic")
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	assertValidJSON(t, "Tokenize", tokens)

	analysis, err := client.AnalyzeQuery("SELECT a FROM t", AnalyzeQueryOptions{})
	if err != nil {
		t.Fatalf("AnalyzeQuery: %v", err)
	}
	if analysis.Shape != "select" || len(analysis.Projections) != 1 {
		t.Fatalf("unexpected AnalyzeQuery result: %#v", analysis)
	}
	if analysis.Projections[0].Name == nil || *analysis.Projections[0].Name != "a" {
		t.Fatalf("unexpected AnalyzeQuery projection: %#v", analysis.Projections[0])
	}

	eventsSchema := ValidationSchema{
		Tables: []SchemaTable{
			{
				Name:    "events",
				Columns: []SchemaColumn{{Name: "created_at", Type: "TIMESTAMP"}},
			},
		},
	}
	analysis, err = client.AnalyzeQuery(
		"SELECT DATE_TRUNC('month', created_at) AS bucket FROM events",
		AnalyzeQueryOptions{Dialect: "duckdb", Schema: &eventsSchema},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery transform function: %v", err)
	}
	transformFunction := analysis.Projections[0].TransformFunction
	if transformFunction == nil || transformFunction.Name != "DATE_TRUNC" {
		t.Fatalf("unexpected AnalyzeQuery transform function: %#v", analysis.Projections[0])
	}
	if len(transformFunction.LiteralArgs) != 1 || transformFunction.LiteralArgs[0] != "month" {
		t.Fatalf("unexpected AnalyzeQuery literal args: %#v", transformFunction)
	}
	if len(transformFunction.ColumnArgs) != 1 || transformFunction.ColumnArgs[0].Column != "created_at" {
		t.Fatalf("unexpected AnalyzeQuery column args: %#v", transformFunction)
	}

	analysis, err = client.AnalyzeQuery(
		"SELECT o.order_id, SUM(o.amount) AS amount_sum FROM orders AS o GROUP BY o.order_id",
		AnalyzeQueryOptions{Schema: integrationSchemaPtr()},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery with schema: %v", err)
	}
	if len(analysis.BaseTables) != 1 || analysis.BaseTables[0].Name != "orders" {
		t.Fatalf("unexpected AnalyzeQuery baseTables: %#v", analysis.BaseTables)
	}
	if analysis.BaseTables[0].Catalog != nil || analysis.BaseTables[0].Schema != nil || analysis.BaseTables[0].Table == nil || *analysis.BaseTables[0].Table != "orders" {
		t.Fatalf("unexpected AnalyzeQuery structured base table identity: %#v", analysis.BaseTables[0])
	}
	if analysis.Projections[0].Upstream[0].SourceAlias == nil || *analysis.Projections[0].Upstream[0].SourceAlias != "o" {
		t.Fatalf("unexpected AnalyzeQuery source alias: %#v", analysis.Projections[0].Upstream)
	}
	if analysis.Projections[1].TransformKind != "aggregation" {
		t.Fatalf("unexpected AnalyzeQuery transform kind: %#v", analysis.Projections[1])
	}
	if analysis.Projections[1].TypeHint == nil || *analysis.Projections[1].TypeHint != "DECIMAL(10, 2)" {
		t.Fatalf("unexpected AnalyzeQuery type hint: %#v", analysis.Projections[1])
	}
	if analysis.Projections[0].Nullability != "non_null" {
		t.Fatalf("unexpected AnalyzeQuery nullability: %#v", analysis.Projections[0])
	}

	analysis, err = client.AnalyzeQuery(
		"WITH base AS (SELECT order_id, amount FROM orders) SELECT * FROM base",
		AnalyzeQueryOptions{Schema: integrationSchemaPtr()},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery with CTE/star: %v", err)
	}
	if len(analysis.CTEFacts) != 1 || analysis.CTEFacts[0].Name != "base" {
		t.Fatalf("unexpected AnalyzeQuery CTE facts: %#v", analysis.CTEFacts)
	}
	if analysis.CTEFacts[0].BodySQL != "SELECT order_id, amount FROM orders" {
		t.Fatalf("unexpected AnalyzeQuery CTE body SQL: %#v", analysis.CTEFacts[0])
	}
	if len(analysis.StarProjections) != 1 || len(analysis.StarProjections[0].ExpandedColumns) != 2 {
		t.Fatalf("unexpected AnalyzeQuery star projections: %#v", analysis.StarProjections)
	}

	analysis, err = client.AnalyzeQuery(
		"SELECT region2, p1 FROM (SELECT region, q, amt FROM sales) PIVOT(SUM(amt) FOR q IN ('Q1')) AS p(region2, p1)",
		AnalyzeQueryOptions{Dialect: "duckdb"},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery with pivot alias columns: %v", err)
	}
	var regionProjection *ProjectionFact
	var pivotProjection *ProjectionFact
	for idx := range analysis.Projections {
		projection := &analysis.Projections[idx]
		if projection.Name != nil && *projection.Name == "region2" {
			regionProjection = projection
		}
		if projection.Name != nil && *projection.Name == "p1" {
			pivotProjection = projection
		}
	}
	if regionProjection == nil || pivotProjection == nil {
		t.Fatalf("missing AnalyzeQuery pivot projections: %#v", analysis.Projections)
	}
	hasRegion := false
	for _, upstream := range regionProjection.Upstream {
		if upstream.Table != nil && *upstream.Table == "sales" && upstream.Column == "region" {
			hasRegion = true
			break
		}
	}
	if !hasRegion {
		t.Fatalf("unexpected AnalyzeQuery region upstream: %#v", regionProjection.Upstream)
	}
	hasPivotInput := false
	for _, upstream := range pivotProjection.Upstream {
		if upstream.Table != nil && *upstream.Table == "sales" && upstream.Column == "amt" {
			hasPivotInput = true
			break
		}
	}
	if !hasPivotInput {
		t.Fatalf("unexpected AnalyzeQuery pivot upstream: %#v", pivotProjection.Upstream)
	}

	setSchema := ValidationSchema{
		Tables: []SchemaTable{
			{Name: "t1", Columns: []SchemaColumn{{Name: "v", Type: "INT"}}},
			{Name: "t2", Columns: []SchemaColumn{{Name: "v", Type: "INT"}}},
			{Name: "t3", Columns: []SchemaColumn{{Name: "v", Type: "INT"}}},
		},
	}
	analysis, err = client.AnalyzeQuery(
		"SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) UNION ALL SELECT v FROM t3) u",
		AnalyzeQueryOptions{Dialect: "duckdb", Schema: &setSchema},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery nested set operation with schema: %v", err)
	}
	setUpstream := analysis.Projections[0].Upstream
	if !hasUpstream(setUpstream, "t1", "v") ||
		!hasUpstream(setUpstream, "t2", "v") ||
		!hasUpstream(setUpstream, "t3", "v") {
		t.Fatalf("unexpected AnalyzeQuery nested set upstream: %#v", setUpstream)
	}

	unnestSchema := ValidationSchema{
		Tables: []SchemaTable{
			{Name: "t", Columns: []SchemaColumn{{Name: "arr", Type: "INT"}}},
		},
	}
	analysis, err = client.AnalyzeQuery(
		"SELECT i FROM t, UNNEST(t.arr) AS i",
		AnalyzeQueryOptions{Dialect: "duckdb", Schema: &unnestSchema},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery UNNEST output alias with schema: %v", err)
	}
	if !hasUpstream(analysis.Projections[0].Upstream, "t", "arr") {
		t.Fatalf("unexpected AnalyzeQuery UNNEST upstream: %#v", analysis.Projections[0].Upstream)
	}

	partialSchema := ValidationSchema{
		Tables: []SchemaTable{
			{Name: "t", Columns: []SchemaColumn{{Name: "amount", Type: "INT"}}},
		},
	}
	analysis, err = client.AnalyzeQuery(
		"SELECT order_id, amount FROM t",
		AnalyzeQueryOptions{Dialect: "duckdb", Schema: &partialSchema},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery partial schema: %v", err)
	}
	if len(analysis.Projections) != 2 {
		t.Fatalf("unexpected AnalyzeQuery partial schema projections: %#v", analysis.Projections)
	}
	if !hasUpstream(analysis.Projections[0].Upstream, "t", "order_id") {
		t.Fatalf("unexpected AnalyzeQuery unknown-column upstream: %#v", analysis.Projections[0].Upstream)
	}
	if !hasUpstream(analysis.Projections[1].Upstream, "t", "amount") {
		t.Fatalf("unexpected AnalyzeQuery known-column upstream: %#v", analysis.Projections[1].Upstream)
	}
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

	cteNode, err := client.Lineage(
		"s",
		"WITH c AS (SELECT * FROM t) SELECT SUM(c.x) AS s FROM c GROUP BY 1",
		"generic",
	)
	if err != nil {
		t.Fatalf("CTE star Lineage: %v", err)
	}
	cteNames := collectLineageNames(cteNode)
	hasBaseColumn := false
	for _, name := range cteNames {
		if name == "t.x" {
			hasBaseColumn = true
			break
		}
	}
	if !hasBaseColumn {
		t.Fatalf("expected t.x in CTE star lineage, got %#v", cteNames)
	}

	setNode, err := client.Lineage(
		"v",
		"SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) UNION ALL SELECT v FROM t3) u",
		"duckdb",
	)
	if err != nil {
		t.Fatalf("nested set operation Lineage: %v", err)
	}
	setNames := collectLineageNames(setNode)
	if !containsString(setNames, "t1.v") || !containsString(setNames, "t2.v") || !containsString(setNames, "t3.v") {
		t.Fatalf("expected nested set operation input columns in lineage, got %#v", setNames)
	}

	bigQueryNode, err := client.Lineage(
		"week_start",
		"SELECT date_val AS week_start FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-01-31')) AS date_val",
		"bigquery",
	)
	if err != nil {
		t.Fatalf("BigQuery UNNEST Lineage: %v", err)
	}
	if len(bigQueryNode.Downstream) != 1 {
		t.Fatalf("unexpected BigQuery UNNEST lineage node: %#v", bigQueryNode)
	}
	virtualSource := bigQueryNode.Downstream[0]
	if virtualSource.Name != "_0.date_val" ||
		virtualSource.SourceName != "_0" ||
		virtualSource.SourceKind != "virtual" ||
		virtualSource.SourceAlias == nil ||
		*virtualSource.SourceAlias != "date_val" {
		t.Fatalf("unexpected BigQuery UNNEST virtual source: %#v", virtualSource)
	}

	tables, err := client.SourceTables("total", "SELECT o.total FROM orders o", "generic")
	if err != nil {
		t.Fatalf("SourceTables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("empty source tables")
	}

	ddlNode, err := client.Lineage(
		"x",
		"CREATE TABLE tgt AS SELECT x FROM src",
		"generic",
	)
	if err != nil {
		t.Fatalf("DDL wrapper Lineage: %v", err)
	}
	ddlNames := collectLineageNames(ddlNode)
	hasSourceColumn := false
	for _, name := range ddlNames {
		if name == "src.x" {
			hasSourceColumn = true
			break
		}
	}
	if !hasSourceColumn {
		t.Fatalf("expected src.x in DDL wrapper lineage, got %#v", ddlNames)
	}

	windowNode, err := client.Lineage(
		"out",
		"WITH c AS (SELECT user_id, ts FROM events) SELECT ROW_NUMBER() OVER (PARTITION BY c.user_id ORDER BY c.ts) AS out FROM c",
		"generic",
	)
	if err != nil {
		t.Fatalf("window Lineage: %v", err)
	}
	windowNames := collectLineageNames(windowNode)
	hasUserID := false
	hasTS := false
	for _, name := range windowNames {
		if name == "events.user_id" {
			hasUserID = true
		}
		if name == "events.ts" {
			hasTS = true
		}
	}
	if !hasUserID || !hasTS {
		t.Fatalf("expected window input columns in lineage, got %#v", windowNames)
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

	partialSchema := ValidationSchema{
		Tables: []SchemaTable{
			{Name: "t", Columns: []SchemaColumn{{Name: "amount", Type: "INT"}}},
		},
	}
	partialSchemaNode, err := client.LineageWithSchema(
		"amount",
		"SELECT order_id, amount FROM t",
		partialSchema,
		"duckdb",
	)
	if err != nil {
		t.Fatalf("LineageWithSchema partial schema: %v", err)
	}
	if !containsString(collectLineageNames(partialSchemaNode), "t.amount") {
		t.Fatalf("unexpected partial-schema lineage node: %#v", partialSchemaNode)
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

	setOperation, err := client.Parse("SELECT id FROM a UNION ALL SELECT id FROM b", "generic")
	if err != nil {
		t.Fatalf("Parse set operation: %v", err)
	}
	setOperation, err = client.SetLimit(setOperation, 5)
	if err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	setOperation, err = client.SetOffset(setOperation, 10)
	if err != nil {
		t.Fatalf("SetOffset: %v", err)
	}

	orderExpressionSource, err := client.Parse("SELECT id", "generic")
	if err != nil {
		t.Fatalf("Parse order expression source: %v", err)
	}
	var parsedOrderExpression []map[string]any
	if err := json.Unmarshal(orderExpressionSource, &parsedOrderExpression); err != nil {
		t.Fatalf("decode order expression source: %v", err)
	}
	selectData := parsedOrderExpression[0]["select"].(map[string]any)
	orderExpressions := selectData["expressions"].([]any)
	orderBy, err := json.Marshal([]any{orderExpressions[0]})
	if err != nil {
		t.Fatalf("encode order by: %v", err)
	}
	setOperation, err = client.SetOrderBy(setOperation, orderBy)
	if err != nil {
		t.Fatalf("SetOrderBy: %v", err)
	}
	generatedSetOperation, err := client.Generate(setOperation, "generic")
	if err != nil {
		t.Fatalf("Generate set operation: %v", err)
	}
	if len(generatedSetOperation) != 1 || generatedSetOperation[0] != "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 5 OFFSET 10" {
		t.Fatalf("unexpected generated set operation: %#v", generatedSetOperation)
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

	dataType, err := ParseDataType("VARCHAR(255)", "duckdb")
	if err != nil {
		t.Fatalf("ParseDataType wrapper: %v", err)
	}
	assertValidJSON(t, "ParseDataType wrapper", dataType)

	generatedType, err := GenerateDataType(dataType, "postgres")
	if err != nil {
		t.Fatalf("GenerateDataType wrapper: %v", err)
	}
	if generatedType != "VARCHAR(255)" {
		t.Fatalf("unexpected GenerateDataType wrapper result: %q", generatedType)
	}

	tokens, err := Tokenize("SELECT a FROM old_table", "generic")
	if err != nil {
		t.Fatalf("Tokenize wrapper: %v", err)
	}
	assertValidJSON(t, "Tokenize wrapper", tokens)

	analysis, err := AnalyzeQuery("SELECT a FROM t", AnalyzeQueryOptions{})
	if err != nil {
		t.Fatalf("AnalyzeQuery wrapper: %v", err)
	}
	if analysis.Shape != "select" || len(analysis.Projections) != 1 {
		t.Fatalf("unexpected AnalyzeQuery wrapper result: %#v", analysis)
	}

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

	setOperation, err := Parse("SELECT id FROM a UNION ALL SELECT id FROM b", "generic")
	if err != nil {
		t.Fatalf("Parse set operation wrapper: %v", err)
	}
	setOperation, err = SetLimit(setOperation, 5)
	if err != nil {
		t.Fatalf("SetLimit wrapper: %v", err)
	}
	setOperation, err = SetOffset(setOperation, 10)
	if err != nil {
		t.Fatalf("SetOffset wrapper: %v", err)
	}
	setOperation, err = SetOrderBy(setOperation, json.RawMessage(`[{"column":{"name":{"name":"id","quoted":false},"table":null,"join_mark":false,"trailing_comments":[]}}]`))
	if err != nil {
		t.Fatalf("SetOrderBy wrapper: %v", err)
	}
	assertValidJSON(t, "SetOrderBy wrapper", setOperation)

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
