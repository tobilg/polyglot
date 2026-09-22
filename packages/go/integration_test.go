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

func TestIntegrationNullOrderingPreservation(t *testing.T) {
	client := integrationClient(t)
	for _, dialect := range []string{"snowflake", "duckdb", "postgres"} {
		for _, ordering := range []string{
			"category NULLS LAST, created_at DESC NULLS FIRST",
			"category, created_at DESC",
		} {
			sql := "SELECT id FROM items ORDER BY " + ordering
			for _, options := range [][]FormatOptions{nil, {{}}} {
				result, err := client.Format(sql, dialect, options...)
				if err != nil || len(result) != 1 || strings.Join(strings.Fields(result[0]), " ") != sql {
					t.Fatalf("format %s: %v, %v", dialect, result, err)
				}
			}
			if dialect != "postgres" {
				result, err := client.Transpile(sql, dialect, dialect)
				if err != nil || len(result) != 1 || result[0] != sql {
					t.Fatalf("transpile %s: %v, %v", dialect, result, err)
				}
			}
		}
	}
}

func TestIntegrationParserDepthGuard(t *testing.T) {
	client := integrationClient(t)
	sql := "SELECT " + strings.Repeat("~ ", 12) + "1"
	for _, tc := range []struct {
		limit GuardLimit
		fails bool
	}{
		{GuardLimit{}, false}, {NewGuardLimit(0), true}, {NewGuardLimit(8), true},
		{NewGuardLimit(64), false}, {DisabledGuardLimit(), false},
	} {
		_, err := client.Transpile(sql, "generic", "generic", TranspileOptions{
			ComplexityGuard: &ComplexityGuardOptions{MaxParserDepth: tc.limit},
		})
		if tc.fails {
			if err == nil || !strings.Contains(err.Error(), "E_GUARD_PARSER_DEPTH_EXCEEDED") {
				t.Fatalf("expected depth error, got %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Transpile("SELECT "+strings.Repeat("~ ", 4000)+"1", "generic", "generic"); err == nil || !strings.Contains(err.Error(), "E_GUARD_PARSER_DEPTH_EXCEEDED") {
		t.Fatalf("default guard: %v", err)
	}
	if _, err := client.Parse("SELECT 1", "generic"); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationParseValidationAnalysisGuardOptions(t *testing.T) {
	client := integrationClient(t)
	schema := ValidationSchema{Tables: []SchemaTable{{Name: "records", Columns: []SchemaColumn{{Name: "value", Type: "INTEGER"}}}}}
	for _, depth := range []int{64, 65} {
		sql := "SELECT " + strings.Repeat("COALESCE(", depth) + "value" + strings.Repeat(", 0)", depth) + " FROM records"
		for _, tc := range []struct {
			guard    *ComplexityGuardOptions
			accepted bool
		}{
			{nil, depth == 64}, {&ComplexityGuardOptions{}, depth == 64},
			{&ComplexityGuardOptions{MaxFunctionCallDepth: NewGuardLimit(128)}, true},
			{&ComplexityGuardOptions{MaxFunctionCallDepth: DisabledGuardLimit()}, true},
		} {
			for _, parse := range []func(string, string, ...ParseOptions) (json.RawMessage, error){client.Parse, client.ParseOne} {
				_, err := parse(sql, "snowflake", ParseOptions{ComplexityGuard: tc.guard})
				if (err == nil) != tc.accepted {
					t.Fatalf("depth=%d, accepted=%v: %v", depth, tc.accepted, err)
				}
				if err != nil && !strings.Contains(err.Error(), "E_GUARD_FUNCTION_NESTING_DEPTH_EXCEEDED") {
					t.Fatal(err)
				}
			}
			result, err := client.Validate(sql, "snowflake", ValidationOptions{ComplexityGuard: tc.guard})
			if err != nil || result.Valid != tc.accepted {
				t.Fatalf("validate: %#v, %v", result, err)
			}
			result, err = client.ValidateWithSchema(sql, schema, "snowflake", SchemaValidationOptions{ComplexityGuard: tc.guard})
			if err != nil || result.Valid != tc.accepted {
				t.Fatalf("validate schema: %#v, %v", result, err)
			}
			_, err = client.AnalyzeQuery(sql, AnalyzeQueryOptions{Dialect: "snowflake", ComplexityGuard: tc.guard})
			if (err == nil) != tc.accepted {
				t.Fatalf("analyze: %v", err)
			}
		}
	}
	_, err := client.Parse("SELECT 1", "generic", ParseOptions{ComplexityGuard: &ComplexityGuardOptions{MaxFunctionCallDepth: DisabledGuardLimit(), MaxInputBytes: NewGuardLimit(1)}})
	if err == nil || !strings.Contains(err.Error(), "E_GUARD_INPUT_TOO_LARGE") {
		t.Fatalf("independent guard: %v", err)
	}
	_, err = client.ParseDataType("DECIMAL(10, 2)", "snowflake", ParseOptions{ComplexityGuard: &ComplexityGuardOptions{MaxASTNodes: NewGuardLimit(0)}})
	if err == nil || !strings.Contains(err.Error(), "E_GUARD_AST_BUDGET_EXCEEDED") {
		t.Fatalf("data type guard: %v", err)
	}
	if _, err := client.ParseDataType("DECIMAL(10, 2)", "snowflake"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ParseOne("SELECT 1; SELECT 2", "generic", ParseOptions{}); err == nil {
		t.Fatal("accepted multiple statements")
	}
	if _, err := client.Parse("SELECT 1", "generic"); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationAnalyzeQueryCTECastType(t *testing.T) {
	client := integrationClient(t)
	sql := "WITH transformed AS (SELECT CAST(amount AS INTEGER) AS amount FROM raw_orders), final AS (SELECT amount FROM transformed) SELECT amount FROM final"
	schema := &ValidationSchema{Tables: []SchemaTable{{Name: "raw_orders", Columns: []SchemaColumn{{Name: "amount", Type: "VARCHAR"}}}}}
	for _, inputSchema := range []*ValidationSchema{nil, schema} {
		analysis, err := client.AnalyzeQuery(sql, AnalyzeQueryOptions{Dialect: "snowflake", Schema: inputSchema})
		if err != nil {
			t.Fatal(err)
		}
		if len(analysis.Projections) != 1 {
			t.Fatalf("unexpected projections: %#v", analysis.Projections)
		}
		projection := analysis.Projections[0]
		if projection.TypeHint == nil || *projection.TypeHint != "INT" || projection.TransformKind != "direct" || projection.CastType != nil {
			t.Fatalf("unexpected CTE projection: %#v", projection)
		}
	}
}

func TestIntegrationAnalyzeQueryColumnUses(t *testing.T) {
	client := integrationClient(t)
	sql := "SELECT '😀', o.id FROM orders o WHERE o.amount > 0 OR o.amount < -1"
	analysis, err := client.AnalyzeQuery(sql, AnalyzeQueryOptions{Dialect: "duckdb"})
	if err != nil {
		t.Fatal(err)
	}
	if len(analysis.ColumnUses) != 1 {
		t.Fatalf("unexpected uses: %#v", analysis.ColumnUses)
	}
	fact := analysis.ColumnUses[0]
	if fact.Context != ColumnUseFilter || fact.ScopePath != "root" || fact.ExpressionPath != "where_clause.this" || len(fact.References) != 2 {
		t.Fatalf("unexpected use: %#v", fact)
	}
	if *fact.References[0].Span == *fact.References[1].Span {
		t.Fatal("occurrences were deduplicated")
	}
	for _, ref := range fact.References {
		if ref.SourceName == nil || *ref.SourceName != "orders" || ref.SourceAlias == nil || *ref.SourceAlias != "o" || ref.Column != "amount" || ref.Confidence != "resolved" {
			t.Fatalf("unexpected reference: %#v", ref)
		}
		if ref.Span == nil || string([]rune(sql)[ref.Span.Start:ref.Span.End]) != "o.amount" {
			t.Fatalf("invalid occurrence span: %#v", ref)
		}
	}
	if len(analysis.Projections[1].Upstream) != 1 || analysis.Projections[1].Upstream[0].Column != "id" {
		t.Fatal("projection lineage changed")
	}
	analysis, err = client.AnalyzeQuery("WITH base AS (SELECT id, amount FROM orders) SELECT id FROM base WHERE amount > 0 EXCEPT SELECT id FROM blocked", AnalyzeQueryOptions{Dialect: "duckdb"})
	if err != nil {
		t.Fatal(err)
	}
	seenFilter, seenBranch := false, false
	for _, use := range analysis.ColumnUses {
		if use.Context == ColumnUseFilter {
			seenFilter = use.ScopePath == "root.branches[0]" && *use.References[0].Table == "orders" && use.References[0].Column == "amount"
		}
		if use.Context == ColumnUseSetOperationFilter {
			seenBranch = use.ScopePath == "root.branches[1]" && *use.References[0].Table == "blocked"
		}
	}
	if !seenFilter || !seenBranch {
		t.Fatalf("missing scoped facts: %#v", analysis.ColumnUses)
	}
}

func TestIntegrationValidateWithSchema(t *testing.T) {
	client := integrationClient(t)
	schema := ValidationSchema{Tables: []SchemaTable{
		{Name: "orders", Columns: []SchemaColumn{{Name: "order_id", Type: "INT"}, {Name: "active", Type: "BOOLEAN"}}},
		{Name: "customers", Columns: []SchemaColumn{{Name: "order_id", Type: "INT"}}},
	}}
	for _, tc := range []struct{ sql, code, token string }{
		{"SELECT o.order_id FROM orders o WHERE o.missing_column = TRUE", "E201", "missing_column"},
		{"SELECT x.order_id FROM orders", "E222", "x"},
		{"SELECT * FROM missing", "E200", "missing"},
		{"SELECT '😀', o.\"míssing\" FROM orders o", "E201", "\"míssing\""},
		{"SELECT order_id FROM orders o JOIN customers c ON o.order_id=c.order_id", "E221", "order_id"},
	} {
		result, err := client.ValidateWithSchema(tc.sql, schema, "snowflake", SchemaValidationOptions{CheckTypes: true, CheckReferences: true})
		if err != nil {
			t.Fatal(err)
		}
		if result.Valid {
			t.Fatalf("expected invalid SQL: %s", tc.sql)
		}
		found := false
		for _, finding := range result.Errors {
			if finding.Code != tc.code {
				continue
			}
			found = true
			if finding.Start == nil || finding.End == nil {
				t.Fatalf("missing range: %+v", finding)
			}
			if token := string([]rune(tc.sql)[*finding.Start:*finding.End]); token != tc.token {
				t.Fatalf("token = %q, want %q", token, tc.token)
			}
		}
		if !found {
			t.Fatalf("missing %s: %+v", tc.code, result.Errors)
		}
	}
	for _, sql := range []string{
		"WITH a AS (SELECT order_id AS id FROM orders), b AS (SELECT id FROM a) SELECT id FROM b",
		"SELECT o.order_id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.order_id=o.order_id)",
	} {
		result, err := client.ValidateWithSchema(sql, schema, "snowflake", SchemaValidationOptions{CheckReferences: true})
		if err != nil || !result.Valid {
			t.Fatalf("%s: %+v, %v", sql, result, err)
		}
	}
	result, err := client.ValidateWithSchema("SELECT order_id FROM q WHERE EXISTS (WITH q AS (SELECT order_id FROM orders) SELECT order_id FROM q)", schema, "snowflake")
	if err != nil || result.Valid {
		t.Fatalf("CTE visibility: %+v, %v", result, err)
	}

	strict := false
	schema.Strict = &strict
	result, err = client.ValidateWithSchema("SELECT missing FROM orders", schema, "")
	if err != nil || !result.Valid || result.Errors[0].Severity != "warning" {
		t.Fatalf("non-strict: %+v, %v", result, err)
	}
	forceStrict := true
	result, err = client.ValidateWithSchema("SELECT missing FROM orders", schema, "", SchemaValidationOptions{Strict: &forceStrict})
	if err != nil || result.Valid {
		t.Fatalf("strict override: %+v, %v", result, err)
	}
	schema.Strict = nil
	result, err = client.ValidateWithSchema("SELECT order_id + active FROM orders", schema, "", SchemaValidationOptions{CheckTypes: true})
	if err != nil || result.Valid {
		t.Fatalf("types: %+v, %v", result, err)
	}
	result, err = client.ValidateWithSchema("SELECT *, FROM orders", schema, "", SchemaValidationOptions{StrictSyntax: true, Semantic: true})
	if err != nil || result.Valid || len(result.Errors) != 1 || result.Errors[0].Code != "E005" {
		t.Fatalf("syntax precedence: %+v, %v", result, err)
	}
	result, err = client.ValidateWithSchema("SELECT * FROM orders LIMIT 10", schema, "", SchemaValidationOptions{Semantic: true})
	if err != nil || !result.Valid || len(result.Errors) != 2 {
		t.Fatalf("semantic warnings: %+v, %v", result, err)
	}
	for _, test := range []struct{ sql, code string }{
		{"SELECT order_id, SUM(active) FROM orders", "E230"},
		{"SELECT order_id FROM orders WHERE SUM(order_id)>0", "E231"},
		{"SELECT order_id FROM orders WHERE ROW_NUMBER() OVER()=1", "E232"},
	} {
		result, err = client.ValidateWithSchema(test.sql, schema, "snowflake", SchemaValidationOptions{Semantic: true, Strict: &strict})
		found := false
		for _, finding := range result.Errors {
			found = found || (finding.Code == test.code && finding.Severity == "error")
		}
		if err != nil || result.Valid || !found {
			t.Fatalf("semantic correctness %s: %+v, %v", test.sql, result, err)
		}
	}
	for _, columns := range [][]SchemaColumn{nil, {{Name: "*"}}} {
		schema.Tables[0].Columns = columns
		result, err = client.ValidateWithSchema("SELECT missing FROM orders o JOIN customers c ON TRUE", schema, "")
		if err != nil || !result.Valid {
			t.Fatalf("open schema: %+v, %v", result, err)
		}
	}
	if _, err = client.ValidateWithSchema("SELECT 1", schema, "bad_dialect"); err == nil {
		t.Fatal("unknown dialect accepted")
	}
	result, err = client.ValidateWithSchema("SELECT 1", ValidationSchema{}, "")
	if err != nil || !result.Valid {
		t.Fatalf("zero-value schema: %+v, %v", result, err)
	}
	SetDefaultClient(client)
	t.Cleanup(ClearDefaultClient)
	result, err = ValidateWithSchema("SELECT order_id FROM customers", schema, "snowflake")
	if err != nil || !result.Valid {
		t.Fatalf("package wrapper: %+v, %v", result, err)
	}
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

func TestIntegrationParseSourceSpans(t *testing.T) {
	client := integrationClient(t)
	type sourceSpan struct{ Start, End, Line, Column int }
	for _, tc := range []struct {
		sql    string
		index  int
		source string
	}{
		{"SELECT customer_id FROM orders", 0, "customer_id"},
		{"SELECT \"é😀\", \"a\".\"b\" FROM \"t\"", 1, "\"a\".\"b\""},
	} {
		data, err := client.ParseOne(tc.sql, "snowflake")
		if err != nil {
			t.Fatal(err)
		}
		var ast struct {
			Select struct {
				Expressions []struct {
					Column struct {
						Name struct{ Span *sourceSpan }
						Span *sourceSpan
					}
				}
			}
		}
		if err := json.Unmarshal(data, &ast); err != nil {
			t.Fatal(err)
		}
		column := ast.Select.Expressions[tc.index].Column
		if column.Span == nil || column.Name.Span == nil {
			t.Fatalf("missing spans: %s", data)
		}
		span := column.Span
		if source := string([]rune(tc.sql)[span.Start:span.End]); source != tc.source {
			t.Fatalf("source %q, want %q", source, tc.source)
		}
	}
}

func TestIntegrationIntegerDataTypes(t *testing.T) {
	client := integrationClient(t)
	for _, tc := range []struct{ dialect, input, output, tag string }{
		{"duckdb", "HUGEINT", "INT128", "int128"},
		{"duckdb", "INT128", "INT128", "int128"},
		{"clickhouse", "Int128", "Int128", "int128"},
		{"starrocks", "LARGEINT", "LARGEINT", "int128"},
		{"duckdb", "UTINYINT", "UTINYINT", "uint8"},
		{"duckdb", "USMALLINT", "USMALLINT", "uint16"},
		{"duckdb", "UINTEGER", "UINTEGER", "uint32"},
		{"duckdb", "UBIGINT", "UBIGINT", "uint64"},
		{"duckdb", "UHUGEINT", "UINT128", "uint128"},
		{"duckdb", "UINT128", "UINT128", "uint128"},
	} {
		t.Run(tc.dialect+"/"+tc.input, func(t *testing.T) {
			dataType, err := client.ParseDataType(tc.input, tc.dialect)
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(dataType, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded["data_type"] != tc.tag {
				t.Fatalf("unexpected type: %s", dataType)
			}
			sql, err := client.GenerateDataType(dataType, tc.dialect)
			if err != nil {
				t.Fatal(err)
			}
			if sql != tc.output {
				t.Fatalf("generated %q, want %q", sql, tc.output)
			}
		})
	}
}

func TestIntegrationGenerateDataTypeQuotedFieldNames(t *testing.T) {
	client := integrationClient(t)
	for _, tc := range []struct{ name, identifier string }{
		{"field name", `"field name"`},
		{`a"b`, `"a""b"`},
		{"a INT, b", `"a INT, b"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, err := json.Marshal(map[string]any{
				"data_type": "struct", "nested": false,
				"fields": []any{map[string]any{"name": tc.name, "data_type": map[string]any{"data_type": "text"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			sql, err := client.GenerateDataType(input, "duckdb")
			if err != nil {
				t.Fatal(err)
			}
			if sql != "STRUCT("+tc.identifier+" TEXT)" {
				t.Fatalf("unexpected SQL: %s", sql)
			}
			parsed, err := client.ParseDataType(sql, "duckdb")
			if err != nil {
				t.Fatal(err)
			}
			var value struct{ Fields []struct{ Name string } }
			if err := json.Unmarshal(parsed, &value); err != nil {
				t.Fatal(err)
			}
			if len(value.Fields) != 1 || value.Fields[0].Name != tc.identifier {
				t.Fatalf("field changed: %s", parsed)
			}
		})
	}
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

func TestBuilderIntegration(t *testing.T) {
	client := integrationClient(t)
	base := Select("x").From("tbl").Where("x > 0")
	extended := base.Where("x < 9")

	baseSQL, err := client.BuildSQL(base, "generic")
	if err != nil {
		t.Fatal(err)
	}
	extendedSQL, err := client.BuildSQL(extended, "generic")
	if err != nil {
		t.Fatal(err)
	}
	if baseSQL != "SELECT x FROM tbl WHERE x > 0" {
		t.Fatalf("base SQL = %q", baseSQL)
	}
	if extendedSQL != "SELECT x FROM tbl WHERE x > 0 AND x < 9" {
		t.Fatalf("extended SQL = %q", extendedSQL)
	}
	if _, err := client.Build(extended); err != nil {
		t.Fatal(err)
	}
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

	tidbDDL, err := client.ParseOne(
		"CREATE TABLE posts (id BIGINT AUTO_RANDOM PRIMARY KEY, title VARCHAR(255))",
		"tidb",
	)
	if err != nil {
		t.Fatalf("ParseOne TiDB DDL: %v", err)
	}
	assertValidJSON(t, "ParseOne TiDB DDL", tidbDDL)
	tidbGenerated, err := client.Generate(json.RawMessage("["+string(tidbDDL)+"]"), "tidb")
	if err != nil {
		t.Fatalf("Generate TiDB DDL: %v", err)
	}
	if len(tidbGenerated) != 1 || tidbGenerated[0] != "CREATE TABLE posts (id BIGINT AUTO_RANDOM PRIMARY KEY, title VARCHAR(255))" {
		t.Fatalf("unexpected TiDB DDL roundtrip: %#v", tidbGenerated)
	}

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

	analysis, err = client.AnalyzeQuery(
		"SELECT a FROM x EXCEPT SELECT b FROM y",
		AnalyzeQueryOptions{Dialect: "generic"},
	)
	if err != nil {
		t.Fatalf("AnalyzeQuery set-operation roles: %v", err)
	}
	if len(analysis.SetOperations) != 1 ||
		len(analysis.SetOperations[0].Branches) != 2 ||
		analysis.SetOperations[0].Branches[0].Role != SetOperationBranchRoleValue ||
		analysis.SetOperations[0].Branches[1].Role != SetOperationBranchRoleFilter {
		t.Fatalf("unexpected AnalyzeQuery set-operation roles: %#v", analysis.SetOperations)
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
	for _, reference := range analysis.Projections[0].Upstream {
		if reference.Confidence != "unknown" {
			t.Fatalf("missing column claimed resolved: %#v", reference)
		}
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

	ordinalNode, err := client.LineageAt(
		1,
		"SELECT a, b FROM t1 UNION ALL SELECT x, y FROM t2",
		"generic",
	)
	if err != nil {
		t.Fatalf("LineageAt: %v", err)
	}
	ordinalNames := collectLineageNames(ordinalNode)
	if !containsString(ordinalNames, "t1.b") || !containsString(ordinalNames, "t2.y") {
		t.Fatalf("unexpected ordinal lineage: %#v", ordinalNames)
	}
	if len(ordinalNode.Downstream) != 2 ||
		ordinalNode.Downstream[0].SetBranch == nil ||
		ordinalNode.Downstream[0].SetBranch.Operator != SetOperatorUnion ||
		ordinalNode.Downstream[0].SetBranch.Ordinal != 0 ||
		!ordinalNode.Downstream[0].SetBranch.All ||
		ordinalNode.Downstream[1].SetBranch == nil ||
		ordinalNode.Downstream[1].SetBranch.Ordinal != 1 {
		t.Fatalf("unexpected ordinal lineage branch metadata: %#v", ordinalNode.Downstream)
	}

	_, err = client.LineageAt(2, "SELECT a FROM t", "generic")
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("LineageAt error = %v, want ErrColumnNotFound", err)
	}

	output, err := client.OutputColumns("SELECT 1, t.*, b FROM t", "generic")
	if err != nil {
		t.Fatalf("OutputColumns: %v", err)
	}
	if output.OrdinalComplete || len(output.Columns) != 3 || output.Columns[1].Kind != OutputColumnWildcard {
		t.Fatalf("unexpected query output: %#v", output)
	}

	schemaOutput, err := client.OutputColumnsWithSchema(
		"SELECT * FROM users",
		integrationSchema(),
		"generic",
	)
	if err != nil {
		t.Fatalf("OutputColumnsWithSchema: %v", err)
	}
	if !schemaOutput.OrdinalComplete || len(schemaOutput.Columns) == 0 {
		t.Fatalf("unexpected schema-aware query output: %#v", schemaOutput)
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

func TestIntegrationBigQueryNameAlignedSetOperation(t *testing.T) {
	client := integrationClient(t)
	const sql = "SELECT 1 AS a, 2 AS b FULL OUTER UNION ALL BY NAME SELECT 3 AS b, 4 AS c"

	output, err := client.OutputColumns(sql, "bigquery")
	if err != nil {
		t.Fatalf("BigQuery name-aligned OutputColumns: %v", err)
	}
	if !output.OrdinalComplete || len(output.Columns) != 3 ||
		output.Columns[0].Name == nil || *output.Columns[0].Name != "a" ||
		output.Columns[1].Name == nil || *output.Columns[1].Name != "b" ||
		output.Columns[2].Name == nil || *output.Columns[2].Name != "c" {
		t.Fatalf("unexpected BigQuery name-aligned output: %#v", output)
	}

	analysis, err := client.AnalyzeQuery(sql, AnalyzeQueryOptions{Dialect: "bigquery"})
	if err != nil {
		t.Fatalf("BigQuery name-aligned AnalyzeQuery: %v", err)
	}
	if len(analysis.SetOperations) != 1 ||
		len(analysis.SetOperations[0].OutputColumns) != 3 ||
		analysis.SetOperations[0].OutputColumns[0] != "a" ||
		analysis.SetOperations[0].OutputColumns[1] != "b" ||
		analysis.SetOperations[0].OutputColumns[2] != "c" {
		t.Fatalf("unexpected BigQuery name-aligned analysis: %#v", analysis.SetOperations)
	}
	if len(analysis.Projections) != 3 ||
		analysis.Projections[0].Nullability != "nullable" ||
		analysis.Projections[1].Nullability != "non_null" ||
		analysis.Projections[2].Nullability != "nullable" {
		t.Fatalf("unexpected BigQuery name-aligned nullability: %#v", analysis.Projections)
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

func TestIntegrationHana(t *testing.T) {
	client := integrationClient(t)
	sql := "SELECT * FROM t FOR JSON ('arraywrap' = 'NO')"
	output, err := client.Transpile(sql, "hana", "hana")
	if err != nil || len(output) != 1 || output[0] != sql {
		t.Fatalf("HANA native round trip: %v, %v", output, err)
	}
	if _, err := client.Transpile(sql, "hana", "duckdb"); err == nil {
		t.Fatal("expected unsupported HANA serialization error")
	}
	output, err = client.Transpile("SELECT LOCATE('abcabc', 'bc')", "hana", "duckdb")
	if err != nil || len(output) != 1 || output[0] != "SELECT STRPOS('abcabc', 'bc')" {
		t.Fatalf("HANA LOCATE: %v, %v", output, err)
	}
}
