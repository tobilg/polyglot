package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

// These types describe the experimental WASI protocol, not the native Go SDK.
type request struct {
	Action       string          `json:"action"`
	SQL          string          `json:"sql"`
	AST          json.RawMessage `json:"ast,omitempty"`
	Dialect      string          `json:"dialect,omitempty"`
	ReadDialect  string          `json:"read_dialect,omitempty"`
	WriteDialect string          `json:"write_dialect,omitempty"`
}

type response struct {
	OK     *bool           `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  *string         `json:"error"`
}

type sqlError struct{ message string }

func (e *sqlError) Error() string { return "polyglot: " + e.message }

type client struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
}

func newClient(ctx context.Context, wasm []byte) (*client, error) {
	// Enable cancellation of guest execution, not just host-side operations.
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().WithCloseOnContextDone(true))
	if _, err := wasi_snapshot_preview1.Instantiate(ctx, rt); err != nil {
		rt.Close(context.Background())
		return nil, fmt.Errorf("initialize WASI: %w", err)
	}
	compiled, err := rt.CompileModule(ctx, wasm)
	if err != nil {
		rt.Close(context.Background())
		return nil, fmt.Errorf("compile WASI module: %w", err)
	}
	return &client{runtime: rt, compiled: compiled}, nil
}

func (c *client) Close() error {
	// Runtime.Close also closes its compiled modules and live instances.
	return c.runtime.Close(context.Background())
}

func (c *client) Call(ctx context.Context, req request) (json.RawMessage, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	var stdout, stderr bytes.Buffer
	// Reuse compiled code, but create one instance with private I/O per request.
	// Empty names allow concurrent calls on this client.
	mod, err := c.runtime.InstantiateModule(ctx, c.compiled, wazero.NewModuleConfig().
		WithName("").WithStdin(bytes.NewReader(body)).WithStdout(&stdout).WithStderr(&stderr))
	if mod != nil {
		defer mod.Close(context.Background())
	}
	// wazero 1.11 returns nil for exit code zero. Traps/nonzero exits are errors.
	if err != nil {
		return nil, fmt.Errorf("execute WASI module (stderr: %s): %w", stderr.String(), err)
	}
	var resp response
	if err := json.Unmarshal(stdout.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("decode WASI response (stderr: %s): %w", stderr.String(), err)
	}
	if resp.OK == nil {
		return nil, errors.New("WASI response has no ok field")
	}
	if !*resp.OK {
		if resp.Error == nil {
			return nil, errors.New("WASI error response has no error message")
		}
		return nil, &sqlError{message: *resp.Error}
	}
	if len(resp.Result) == 0 || bytes.Equal(resp.Result, []byte("null")) || resp.Error != nil {
		return nil, errors.New("invalid WASI success response")
	}
	return resp.Result, nil
}
