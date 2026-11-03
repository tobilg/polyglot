package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := newClient(ctx, moduleWasm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	lines, err := demonstrate(ctx, c)
	want := []string{"SELECT COALESCE(a, b) FROM t", "SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'"}
	if err != nil || !reflect.DeepEqual(lines, want) {
		t.Fatalf("example: got %v, %v; want %v", lines, err, want)
	}
	formatted, err := c.Call(ctx, request{Action: "format", SQL: "select a from t where b=1", Dialect: "postgres"})
	var statements []string
	if err != nil || json.Unmarshal(formatted, &statements) != nil || len(statements) != 1 || !strings.Contains(statements[0], "\n") {
		t.Fatalf("format: %s, %v", formatted, err)
	}
	for _, sql := range []string{"", "SELECT 1; SELECT 2"} {
		result, err := c.Call(ctx, request{Action: "transpile", SQL: sql})
		if err != nil || json.Unmarshal(result, &statements) != nil {
			t.Fatalf("transpile %q: %s, %v", sql, result, err)
		}
		want := []string{}
		if sql != "" {
			want = []string{"SELECT 1", "SELECT 2"}
		}
		if !reflect.DeepEqual(statements, want) {
			t.Fatalf("transpile %q: got %v, want %v", sql, statements, want)
		}
	}
	// A normal WASI exit can still contain a protocol/SQL failure.
	_, err = c.Call(ctx, request{Action: "parse", SQL: "SELECT 1", Dialect: "not-a-dialect"})
	var sqlErr *sqlError
	if !errors.As(err, &sqlErr) || !strings.Contains(err.Error(), "Unknown dialect") {
		t.Fatalf("expected structured SQL error, got %v", err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := c.Call(cancelled, request{Action: "parse", SQL: "SELECT 1"}); err == nil {
		t.Fatal("cancelled call succeeded")
	}
	// Cancelling one instance must leave the compiled module reusable.
	if _, err := c.Call(ctx, request{Action: "parse", SQL: "SELECT 1"}); err != nil {
		t.Fatal(err)
	}
}
