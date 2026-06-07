# Polyglot Go SDK

Official Go SDK for Polyglot SQL.

The SDK uses [PureGo](https://github.com/ebitengine/purego) to call the
`polyglot-sql-ffi` shared library without cgo. It does not download or bundle
native libraries at runtime. Build or download the matching FFI library yourself
and point the SDK at it.

Important: `go get github.com/tobilg/polyglot/packages/go` installs only the Go
module. Runtime calls such as `Transpile`, `Parse`, `Validate`, lineage, and
OpenLineage generation require a separate `polyglot-sql-ffi` shared library
(`.so`, `.dylib`, or `.dll`) that matches the SDK release version. Provide that
library with `Open(path)` or `POLYGLOT_SQL_FFI_PATH` plus `OpenDefault()`.

## Install

```bash
go get github.com/tobilg/polyglot/packages/go
```

Go module releases use nested tags that match the root Polyglot release, for
example `packages/go/v0.4.0`.

## Native Library Setup

Build the shared library from this repository, or download the matching FFI
artifact from the same Polyglot release as the Go SDK tag:

```bash
cargo build -p polyglot-sql-ffi --profile ffi_release
```

Then either open it explicitly:

```go
client, err := polyglot.Open("../../target/ffi_release/libpolyglot_sql_ffi.so")
```

or set `POLYGLOT_SQL_FFI_PATH` and use `OpenDefault`:

```bash
export POLYGLOT_SQL_FFI_PATH="$PWD/target/ffi_release/libpolyglot_sql_ffi.so"
```

`OpenDefault` checks `POLYGLOT_SQL_FFI_PATH` first, then common local build
locations relative to the current working directory, its parent, two levels up,
and the executable directory. As a final fallback it asks the system loader to
resolve the platform library name.

Library names by platform:

- Linux: `libpolyglot_sql_ffi.so`
- macOS: `libpolyglot_sql_ffi.dylib`
- Windows: `polyglot_sql_ffi.dll`

## Basic Usage

```go
package main

import (
	"fmt"
	"log"

	polyglot "github.com/tobilg/polyglot/packages/go"
)

func main() {
	client, err := polyglot.OpenDefault()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sql, err := client.Transpile(
		"SELECT IFNULL(a, b) FROM t",
		"mysql",
		"postgres",
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sql[0])
}
```

## API Reference

The primary API is client-scoped. Package-level convenience functions mirror
the operational calls and require an explicitly configured default client, as
shown in [Default Client](#default-client).

### Lifecycle and Versions

| API | Description |
| --- | --- |
| `Open(path string) (*Client, error)` | Load a specific native FFI library. |
| `OpenDefault() (*Client, error)` | Resolve and load the native FFI library from `POLYGLOT_SQL_FFI_PATH`, local candidates, or system loader lookup. |
| `client.Close() error` | Release the loaded native library handle. Safe to call more than once. |
| `Version() string` | Return the Go SDK version. |
| `client.Version() string` | Return the Go SDK version. |
| `client.RuntimeVersion() (string, error)` | Return the version reported by the loaded native FFI library. |
| `RuntimeVersion() (string, error)` | Default-client wrapper for `client.RuntimeVersion`. |
| `LibraryPathEnv` | Exported constant for the `POLYGLOT_SQL_FFI_PATH` environment variable name. |

### SQL Operations

These methods are available on `*Client` and as package-level wrappers:

| API | Description |
| --- | --- |
| `Transpile(sql, fromDialect, toDialect string, options ...TranspileOptions) ([]string, error)` | Parse SQL in one dialect and generate SQL in another. |
| `Format(sql, dialect string, options ...FormatOptions) ([]string, error)` | Format SQL for a dialect, with optional formatting guards. |
| `Optimize(sql, dialect string, options ...OptimizeOptions) ([]string, error)` | Apply optimizer rewrites and return SQL. |
| `Generate(ast json.RawMessage, dialect string, options ...GenerateOptions) ([]string, error)` | Generate SQL from a JSON AST returned by `Parse` or related APIs. |
| `Validate(sql, dialect string) (ValidationResult, error)` | Validate SQL and return diagnostics as data when validation fails. |
| `Dialects() ([]string, error)` | Return supported dialect names. |
| `DialectCount() (int, error)` | Return the number of supported dialects. |

An empty dialect string defaults to `generic`.

```go
dialects, err := client.Dialects()
if err != nil {
	log.Fatal(err)
}

formatted, err := client.Format("SELECT a,b FROM t", "postgres")
if err != nil {
	log.Fatal(err)
}

optimized, err := client.Optimize("SELECT a FROM t WHERE NOT (NOT (b = 1))", "generic")
if err != nil {
	log.Fatal(err)
}

fmt.Println(len(dialects), formatted[0], optimized[0])
```

### JSON AST APIs

These methods return `json.RawMessage` so applications can decode only the
parts they need:

| API | Description |
| --- | --- |
| `Parse(sql, dialect string) (json.RawMessage, error)` | Parse one or more SQL statements into a JSON AST array. |
| `ParseOne(sql, dialect string) (json.RawMessage, error)` | Parse one SQL statement into a single JSON AST node. |
| `Tokenize(sql, dialect string) (json.RawMessage, error)` | Tokenize SQL and return token JSON. |
| `AnnotateTypes(sql, dialect string, schema *ValidationSchema) (json.RawMessage, error)` | Parse SQL and annotate expression types, optionally using schema metadata. |
| `Diff(sql1, sql2, dialect string) (json.RawMessage, error)` | Return an AST diff between two SQL strings. |

```go
ast, err := client.Parse("SELECT total FROM orders", "generic")
if err != nil {
	log.Fatal(err)
}

one, err := client.ParseOne("SELECT total FROM orders", "generic")
if err != nil {
	log.Fatal(err)
}

tokens, err := client.Tokenize("SELECT total FROM orders", "generic")
if err != nil {
	log.Fatal(err)
}

annotated, err := client.AnnotateTypes("SELECT total FROM orders", "generic", nil)
if err != nil {
	log.Fatal(err)
}

diff, err := client.Diff("SELECT a FROM t", "SELECT b FROM t", "generic")
if err != nil {
	log.Fatal(err)
}

fmt.Println(len(ast), len(one), len(tokens), len(annotated), len(diff))
```

### AST Transforms

| API | Description |
| --- | --- |
| `QualifyTables(ast json.RawMessage, options QualifyTablesOptions) (json.RawMessage, error)` | Qualify table names and optionally generate stable aliases. |
| `RenameTables(ast json.RawMessage, mapping map[string]string, options RenameTablesOptions) (json.RawMessage, error)` | Rename table references in a JSON AST. |

```go
ast, err := client.Parse("SELECT a FROM old_table", "generic")
if err != nil {
	log.Fatal(err)
}

qualified, err := client.QualifyTables(ast, polyglot.QualifyTablesOptions{
	Dialect: "generic",
	DB:      "analytics",
})
if err != nil {
	log.Fatal(err)
}

renamed, err := client.RenameTables(
	qualified,
	map[string]string{"old_table": "new_table"},
	polyglot.RenameTablesOptions{},
)
if err != nil {
	log.Fatal(err)
}

sql, err := client.Generate(renamed, "generic")
if err != nil {
	log.Fatal(err)
}
fmt.Println(sql[0])
```

### Lineage

| API | Description |
| --- | --- |
| `Lineage(column, sql, dialect string) (LineageNode, error)` | Return a lineage tree for a selected output column. |
| `LineageWithSchema(column, sql string, schema ValidationSchema, dialect string) (LineageNode, error)` | Return lineage using schema metadata for improved resolution. |
| `SourceTables(column, sql, dialect string) ([]string, error)` | Return source table names for a selected output column. |

```go
node, err := client.Lineage("total", "SELECT o.total FROM orders o", "generic")
if err != nil {
	log.Fatal(err)
}

tables, err := client.SourceTables("total", "SELECT o.total FROM orders o", "generic")
if err != nil {
	log.Fatal(err)
}
fmt.Println(node.Name, tables)
```

`LineageNode.SourceKind` identifies whether a source is a real table, CTE,
derived table, virtual source, or unknown. `LineageNode.SourceAlias` is set for
virtual sources such as BigQuery `UNNEST(...) AS alias`.

### OpenLineage

| API | Description |
| --- | --- |
| `OpenLineageColumnLineage(sql string, options OpenLineageOptions) (OpenLineageColumnLineageResult, error)` | Build a standalone OpenLineage-compatible `columnLineage` dataset facet plus inferred datasets. |
| `OpenLineageJobEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error)` | Build an OpenLineage-compatible job event payload. |
| `OpenLineageRunEvent(sql string, options OpenLineageOptions) (OpenLineageEventResult, error)` | Build an OpenLineage-compatible run event payload. |

The SDK builds OpenLineage-compatible payloads only. Transport and client
emission are intentionally out of scope.

```go
options := polyglot.OpenLineageOptions{
	Dialect:          "generic",
	Producer:         "https://github.com/tobilg/polyglot",
	DatasetNamespace: "warehouse",
	OutputDataset: &polyglot.OpenLineageDatasetID{
		Namespace: "warehouse",
		Name:      "daily_orders",
	},
}

columnLineage, err := client.OpenLineageColumnLineage("SELECT total FROM orders", options)
if err != nil {
	log.Fatal(err)
}

options.JobNamespace = "jobs"
options.JobName = "daily_orders"
options.EventTime = "2026-05-22T00:00:00Z"
jobEvent, err := client.OpenLineageJobEvent("SELECT total FROM orders", options)
if err != nil {
	log.Fatal(err)
}

options.RunID = "run-1"
options.EventType = polyglot.OpenLineageRunEventComplete
runEvent, err := client.OpenLineageRunEvent("SELECT total FROM orders", options)
if err != nil {
	log.Fatal(err)
}

fmt.Println(columnLineage.Facet.Fields, jobEvent.Event, runEvent.Event)
```

### Options and Result Types

| Type | Fields |
| --- | --- |
| `TranspileOptions` | `Pretty`, `UnsupportedLevel`, `MaxUnsupported` |
| `UnsupportedLevel` | `UnsupportedIgnore`, `UnsupportedWarn`, `UnsupportedRaise`, `UnsupportedImmediate` |
| `FormatOptions` | `MaxInputBytes`, `MaxTokens`, `MaxASTNodes`, `MaxSetOpChain` |
| `OptimizeOptions` | Reserved for future optimizer options. |
| `GenerateOptions` | Reserved for future generator options. |
| `ValidationResult` | `Valid`, `Errors` |
| `ValidationError` | `Message`, `Line`, `Column`, `Severity`, `Code`, `Start`, `End` |
| `ValidationSchema` | `Tables`, `Strict` |
| `SchemaTable` | `Name`, `Schema`, `Columns`, `Aliases`, `PrimaryKey`, `UniqueKeys`, `ForeignKeys` |
| `SchemaColumn` | `Name`, `Type`, `Nullable`, `PrimaryKey`, `Unique`, `References` |
| `SchemaForeignKey` | `Name`, `Columns`, `References` |
| `SchemaColumnReference` | `Table`, `Column`, `Schema` |
| `SchemaTableReference` | `Table`, `Columns`, `Schema` |
| `LineageNode` | `Name`, `Expression`, `Source`, `Downstream`, `SourceName`, `SourceKind`, `SourceAlias`, `ReferenceNodeName` |
| `QualifyTablesOptions` | `DB`, `Catalog`, `Dialect`, `CanonicalizeTableAliases`, `AliasUnaliasedTables`, `AliasUnaliasedSubqueries`, `AliasPrefix`, `NormalizeSetOperationSubqueries` |
| `RenameTablesOptions` | `AliasRenamedTables`, `PreserveExistingAliases` |
| `OpenLineageOptions` | `Dialect`, `Producer`, `DatasetNamespace`, `DatasetMappings`, `OutputDataset`, `Schema`, `JobNamespace`, `JobName`, `EventTime`, `RunID`, `EventType` |
| `OpenLineageDatasetID` | `Namespace`, `Name` |
| `OpenLineageColumnLineageResult` | `Facet`, `Inputs`, `Outputs`, `Warnings` |
| `OpenLineageEventResult` | `Event`, `Warnings` |
| `OpenLineageWarning` | `Code`, `Message` |
| `OpenLineageDataset` | `Namespace`, `Name`, `Facets` |
| `OpenLineageColumnLineageFacet` | `Producer`, `SchemaURL`, `Fields` |
| `OpenLineageColumnLineageField` | `InputFields` |
| `OpenLineageInputField` | `Namespace`, `Name`, `Field`, `Transformations` |
| `OpenLineageTransformation` | `Type`, `Subtype`, `Description`, `Masking` |

`OpenLineageRunEventType` constants are `OpenLineageRunEventStart`,
`OpenLineageRunEventRunning`, `OpenLineageRunEventComplete`,
`OpenLineageRunEventAbort`, `OpenLineageRunEventFail`, and
`OpenLineageRunEventOther`.

## Validation

Invalid SQL is returned as validation data, not as a Go error, when the native
call succeeds:

```go
result, err := client.Validate("SELECT FROM", "generic")
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.Valid)
fmt.Println(result.Errors)
```

Go errors are reserved for missing libraries, missing symbols, invalid option
JSON, FFI failures, and lifecycle misuse.

## Error Handling

The SDK exposes `ErrClosed`, `ErrNoDefaultClient`, and `*polyglot.Error`.
`*polyglot.Error` includes the native FFI status code, operation name, and
message.

```go
_, err := client.Transpile("SELECT FROM", "generic", "postgres")
if err != nil {
	var ffiErr *polyglot.Error
	if errors.As(err, &ffiErr) {
		fmt.Println(ffiErr.Operation, ffiErr.Status, ffiErr.Message)
	}
}
```

## Raw JSON APIs

AST-heavy APIs return `json.RawMessage` so applications can decode only the
parts they need:

```go
ast, err := client.Parse("SELECT a FROM t", "generic")
if err != nil {
	log.Fatal(err)
}

sql, err := client.Generate(ast, "generic")
if err != nil {
	log.Fatal(err)
}
fmt.Println(sql[0])
```

## Schema Metadata

Schema metadata is used by validation, type annotation, lineage, and
OpenLineage helpers:

```go
nullable := false
schema := polyglot.ValidationSchema{
	Tables: []polyglot.SchemaTable{
		{
			Name: "orders",
			Columns: []polyglot.SchemaColumn{
				{Name: "id", Type: "INT", PrimaryKey: true, Nullable: &nullable},
				{Name: "total", Type: "DECIMAL"},
			},
		},
	},
}

annotated, err := client.AnnotateTypes("SELECT total FROM orders", "generic", &schema)
if err != nil {
	log.Fatal(err)
}

lineage, err := client.LineageWithSchema("total", "SELECT total FROM orders", schema, "generic")
if err != nil {
	log.Fatal(err)
}

fmt.Println(len(annotated), lineage.Name)
```

## Default Client

Package-level convenience functions use only an explicitly configured default
client:

| API | Description |
| --- | --- |
| `SetDefaultClient(client *Client)` | Configure the process-wide default client used by package-level wrappers. |
| `ClearDefaultClient()` | Remove the configured default client. |
| `DefaultClient() (*Client, error)` | Return the configured default client or `ErrNoDefaultClient`. |

```go
client, err := polyglot.OpenDefault()
if err != nil {
	log.Fatal(err)
}
defer client.Close()

polyglot.SetDefaultClient(client)
sql, err := polyglot.Transpile("SELECT 1", "postgres", "postgres")
if err != nil {
	log.Fatal(err)
}
fmt.Println(sql[0])
```

If no default client is configured, package-level functions return
`ErrNoDefaultClient` instead of panicking.
