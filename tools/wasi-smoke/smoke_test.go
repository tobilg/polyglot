// Package main holds protocol tests for the polyglot-sql-wasi module, run
// against a real WebAssembly runtime.
//
// These exist because the crate's own host-target tests cannot catch
// target-specific behaviour. The WASM stack is sized by the linker, and
// overflowing it surfaces to the host as an opaque trap with no message rather
// than as an error response, so the only way to know the module is safe at a
// given input depth is to run it where that stack applies.
//
// Set POLYGLOT_WASI_MODULE to the built artifact, or run `make
// test-wasi-runtime`, which builds it first.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

// maxRequestJSONDepth mirrors MAX_REQUEST_JSON_DEPTH in the module. The
// module's bound is the authority; this copy only lets the tests probe either
// side of it.
const maxRequestJSONDepth = 1536

type response struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *string         `json:"error"`
}

func (r response) reason() string {
	if r.Error == nil {
		return "<nil>"
	}
	return *r.Error
}

type runner struct {
	ctx      context.Context
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
}

func newRunner(t *testing.T) *runner {
	t.Helper()
	path := os.Getenv("POLYGLOT_WASI_MODULE")
	if path == "" {
		t.Skip("POLYGLOT_WASI_MODULE is not set; run `make test-wasi-runtime`")
	}
	wasmBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}

	ctx := context.Background()
	rt := wazero.NewRuntime(ctx)
	t.Cleanup(func() { rt.Close(ctx) })
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		t.Fatalf("instantiating WASI: %v", err)
	}
	compiled, err := rt.CompileModule(ctx, wasmBytes)
	if err != nil {
		t.Fatalf("compiling %s: %v", path, err)
	}
	return &runner{ctx: ctx, runtime: rt, compiled: compiled}
}

// raw sends one request and reports the response, or trapped=true when the
// guest died instead of answering. A trap is the failure mode worth
// distinguishing: it carries no message a caller could act on.
func (r *runner) raw(t *testing.T, body string) (response, bool) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	config := wazero.NewModuleConfig().
		WithStdin(strings.NewReader(body)).
		WithStdout(&stdout).
		WithStderr(&stderr).
		WithName("")

	if _, err := r.runtime.InstantiateModule(r.ctx, r.compiled, config); err != nil {
		if exitErr, ok := err.(*sys.ExitError); !ok || exitErr.ExitCode() != 0 {
			return response{}, true
		}
	}
	var resp response
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		t.Fatalf("response was not JSON (%q): %v", stdout.String(), err)
	}
	return resp, false
}

func (r *runner) call(t *testing.T, request map[string]interface{}) response {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshalling request: %v", err)
	}
	resp, trapped := r.raw(t, string(body))
	if trapped {
		t.Fatalf("the module trapped instead of answering %v", request["action"])
	}
	return resp
}

func longExpression(terms int) string {
	return "SELECT " + strings.Repeat("a + ", terms-1) + "a FROM t"
}

func jsonDepth(s string) int {
	depth, max := 0, 0
	inString, escaped := false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{', '[':
			depth++
			if depth > max {
				max = depth
			}
		case '}', ']':
			depth--
		}
	}
	return max
}

// All four actions have to work through the real protocol.
func TestActions(t *testing.T) {
	r := newRunner(t)

	parsed := r.call(t, map[string]interface{}{
		"action": "parse", "sql": "SELECT a FROM t WHERE b = 1", "dialect": "oracle"})
	if !parsed.OK {
		t.Fatalf("parse: %s", parsed.reason())
	}

	generated := r.call(t, map[string]interface{}{
		"action": "generate", "ast": parsed.Result, "dialect": "oracle"})
	if !generated.OK {
		t.Fatalf("generate: %s", generated.reason())
	}
	var rendered []string
	if err := json.Unmarshal(generated.Result, &rendered); err != nil {
		t.Fatalf("generate result: %v", err)
	}
	if len(rendered) != 1 || rendered[0] != "SELECT a FROM t WHERE b = 1" {
		t.Errorf("generate returned %q", rendered)
	}

	transpiled := r.call(t, map[string]interface{}{
		"action": "transpile", "sql": "SELECT a FROM t",
		"read_dialect": "oracle", "write_dialect": "duckdb"})
	if !transpiled.OK {
		t.Fatalf("transpile: %s", transpiled.reason())
	}

	formatted := r.call(t, map[string]interface{}{
		"action": "format", "sql": "SELECT a,b FROM t ORDER BY a NULLS LAST",
		"dialect": "postgres"})
	if !formatted.OK {
		t.Fatalf("format: %s", formatted.reason())
	}
}

// format must use the library's formatter, not a same-dialect transpile. The
// difference shows up as multiline output and as clauses a normalising round
// trip drops.
func TestFormatParity(t *testing.T) {
	r := newRunner(t)
	resp := r.call(t, map[string]interface{}{
		"action": "format", "sql": "SELECT a,b FROM t ORDER BY a NULLS LAST",
		"dialect": "postgres"})
	if !resp.OK {
		t.Fatalf("format: %s", resp.reason())
	}
	var out []string
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("format result: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("format returned %d statements", len(out))
	}
	if !strings.Contains(out[0], "\n") {
		t.Errorf("format returned single line output, so it is not using the formatter: %q", out[0])
	}
	if !strings.Contains(strings.ToUpper(out[0]), "NULLS LAST") {
		t.Errorf("format dropped the explicit null ordering: %q", out[0])
	}
}

func TestMultipleStatements(t *testing.T) {
	r := newRunner(t)
	parsed := r.call(t, map[string]interface{}{
		"action": "parse", "sql": "SELECT 1 FROM DUAL; SELECT 2 FROM DUAL", "dialect": "oracle"})
	if !parsed.OK {
		t.Fatalf("parse: %s", parsed.reason())
	}
	generated := r.call(t, map[string]interface{}{
		"action": "generate", "ast": parsed.Result, "dialect": "oracle"})
	if !generated.OK {
		t.Fatalf("generate: %s", generated.reason())
	}
	var out []string
	if err := json.Unmarshal(generated.Result, &out); err != nil {
		t.Fatalf("result: %v", err)
	}
	if len(out) != 2 {
		t.Errorf("expected two statements, got %q", out)
	}
}

// Anything the parser emits must be acceptable to generate, in the runtime
// where the stack is the linker's.
//
// Two separate failures lived here. At 40 terms generation trapped with "out
// of bounds memory access", because the WASI target was linked with the
// default 1 MiB stack instead of the 64 MiB the other WASM target already
// used. At 61 terms the AST passed serde_json's default nesting limit of 128
// levels, so the protocol rejected an AST it had just produced.
func TestParserOutputRoundTrips(t *testing.T) {
	r := newRunner(t)

	for _, terms := range []int{2, 40, 61, 120, 250, 500} {
		sql := longExpression(terms)
		parsed := r.call(t, map[string]interface{}{
			"action": "parse", "sql": sql, "dialect": "oracle"})
		if !parsed.OK {
			t.Fatalf("%d terms: parse: %s", terms, parsed.reason())
		}

		// Measured on the whole request, which is what the module counts: the
		// outer object adds a level above the AST itself.
		request := `{"action":"generate","dialect":"oracle","ast":` + string(parsed.Result) + `}`
		if depth := jsonDepth(request); depth > maxRequestJSONDepth {
			t.Fatalf("%d terms: parse emitted an AST that makes a %d level request, past the "+
				"protocol limit of %d, so the module cannot accept its own output",
				terms, depth, maxRequestJSONDepth)
		}

		generated := r.call(t, map[string]interface{}{
			"action": "generate", "ast": parsed.Result, "dialect": "oracle"})
		if !generated.OK {
			t.Fatalf("%d terms: generate rejected an AST this module produced: %s",
				terms, generated.reason())
		}
		var out []string
		if err := json.Unmarshal(generated.Result, &out); err != nil {
			t.Fatalf("%d terms: result: %v", terms, err)
		}
		if len(out) != 1 || out[0] != sql {
			t.Errorf("%d terms did not round trip: %q", terms, out)
		}
	}
}

// Past the supported depth the answer has to be a reported error rather than a
// trap, and at the boundary a request has to still be accepted. A hand built
// AST is the only way to get here, since the parser's own guard refuses to
// emit anything this deep.
func TestDepthBoundary(t *testing.T) {
	r := newRunner(t)

	// nestedAdd builds n nested `add` nodes, each costing two JSON levels.
	nestedAdd := func(n int) string {
		const leaf = `{"literal":{"literal_type":"number","value":"1"}}`
		expr := leaf
		for i := 0; i < n; i++ {
			expr = `{"add":{"left":` + expr + `,"right":` + leaf +
				`,"left_comments":[],"operator_comments":[],"trailing_comments":[]}}`
		}
		return `[{"select":{"expressions":[` + expr +
			`],"from":{"expressions":[{"table":{"name":{"name":"t","quoted":false}}}]}}}]`
	}

	for _, nodes := range []int{700, 763, 764, 765, 800, 1200, 2000} {
		ast := nestedAdd(nodes)
		// The depth the module enforces is that of the whole request, so that
		// is what the expectations here are built from.
		request := `{"action":"generate","dialect":"oracle","ast":` + ast + `}`
		depth := jsonDepth(request)
		resp, trapped := r.raw(t, request)

		if trapped {
			t.Fatalf("a request %d levels deep trapped instead of being answered; "+
				"the depth limit must stay below the stack limit", depth)
		}
		if depth > maxRequestJSONDepth {
			if resp.OK || !strings.Contains(resp.reason(), "levels deep") {
				t.Errorf("a request %d levels deep was not refused by the depth limit: ok=%v %s",
					depth, resp.OK, resp.reason())
			}
			continue
		}
		// Within the limit the request is decoded. These hand built nodes are
		// missing fields on purpose, so a decode error is the expected answer:
		// what matters is that the module answers at all.
		if strings.Contains(resp.reason(), "levels deep") {
			t.Errorf("a request %d levels deep was refused, but the limit is %d",
				depth, maxRequestJSONDepth)
		}
	}
}

// Malformed input has to be reported, never trap.
func TestMalformedRequests(t *testing.T) {
	r := newRunner(t)

	for _, body := range []string{
		"",
		"not json",
		"{",
		"[1, 2, 3]",
		`{"action": }`,
		`{"action": "parse", "sql": 7}`,
		`{"action": "nope"}`,
		`{"action": "parse", "dialect": "oracle"}`,
		`{"action": "generate", "dialect": "oracle"}`,
		`{"action": "parse", "sql": "SELECT 1", "dialect": "not_a_dialect"}`,
		`{"action": "parse", "sql": "SELECT FROM", "dialect": "oracle"}`,
		`{"action": "generate", "ast": "not an ast", "dialect": "oracle"}`,
	} {
		resp, trapped := r.raw(t, body)
		if trapped {
			t.Errorf("%q trapped instead of being reported", body)
			continue
		}
		if resp.OK {
			t.Errorf("%q was accepted", body)
			continue
		}
		if resp.Error == nil || *resp.Error == "" {
			t.Errorf("%q failed with no reason given", body)
		}
	}
}
