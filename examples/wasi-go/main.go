package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Copy the release artifact here before building; see README.md.
//
//go:embed polyglot_sql_wasi.wasm
var moduleWasm []byte

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := newClient(ctx, moduleWasm)
	if err != nil {
		return err
	}
	defer c.Close()
	lines, err := demonstrate(ctx, c)
	if err != nil {
		return err
	}
	for _, line := range lines {
		fmt.Println(line)
	}
	return nil
}

func demonstrate(ctx context.Context, c *client) ([]string, error) {
	transpiled, err := c.Call(ctx, request{
		Action: "transpile", SQL: "SELECT IFNULL(a, b) FROM t",
		ReadDialect: "mysql", WriteDialect: "postgres",
	})
	if err != nil {
		return nil, err
	}
	parsed, err := c.Call(ctx, request{
		Action: "parse", Dialect: "oracle",
		SQL: "SELECT ID FROM SALES.ORDERS WHERE REGION = 'EU'",
	})
	if err != nil {
		return nil, err
	}
	// Keep all AST fields and preserve JSON numbers without converting to float64.
	var ast any
	decoder := json.NewDecoder(bytes.NewReader(parsed))
	decoder.UseNumber()
	if err := decoder.Decode(&ast); err != nil {
		return nil, err
	}
	if count := addPartition(ast); count != 1 {
		return nil, fmt.Errorf("expected one table in the sample AST, got %d", count)
	}
	modified, err := json.Marshal(ast)
	if err != nil {
		return nil, err
	}
	generated, err := c.Call(ctx, request{Action: "generate", AST: modified, Dialect: "oracle"})
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, result := range []json.RawMessage{transpiled, generated} {
		var statements []string
		if err := json.Unmarshal(result, &statements); err != nil {
			return nil, fmt.Errorf("expected SQL statement array: %w", err)
		}
		lines = append(lines, statements...)
	}
	return lines, nil
}

// This example adds an Oracle partition to each table in this sample query.
// AST field names belong to the pinned module version; preserve other fields.
func addPartition(value any) int {
	count := 0
	switch node := value.(type) {
	case map[string]any:
		if table, ok := node["table"].(map[string]any); ok {
			table["partitions"] = []any{map[string]any{"name": "P1", "quoted": false}}
			count++
		}
		for _, child := range node {
			count += addPartition(child)
		}
	case []any:
		for _, child := range node {
			count += addPartition(child)
		}
	}
	return count
}
