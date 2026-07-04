package polyglot

import (
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	if Version() == "" {
		t.Fatal("Version() is empty")
	}
	if expected := os.Getenv("POLYGLOT_GO_EXPECTED_VERSION"); expected != "" && Version() != expected {
		t.Fatalf("Version() = %q, want %q", Version(), expected)
	}
}

func TestTranspileOptionsJSON(t *testing.T) {
	payload, err := marshalOptions(TranspileOptions{
		Pretty:           true,
		UnsupportedLevel: UnsupportedRaise,
		MaxUnsupported:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload != `{"pretty":true,"unsupportedLevel":"raise","maxUnsupported":2}` {
		t.Fatalf("payload = %s", payload)
	}
}

func TestTranspileOptionsComplexityGuardJSON(t *testing.T) {
	limit := 128
	payload, err := marshalOptions(TranspileOptions{
		ComplexityGuard: &ComplexityGuardOptions{
			MaxFunctionCallDepth: &limit,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if payload != `{"complexityGuard":{"maxFunctionCallDepth":128}}` {
		t.Fatalf("payload = %s", payload)
	}
}

func TestFormatOptionsJSON(t *testing.T) {
	limit := 128
	payload, err := marshalOptions(FormatOptions{MaxSetOpChain: &limit})
	if err != nil {
		t.Fatal(err)
	}
	if payload != `{"maxSetOpChain":128}` {
		t.Fatalf("payload = %s", payload)
	}
}

func TestOpenLineageOptionsDefaults(t *testing.T) {
	payload, err := marshalOpenLineageOptions(OpenLineageOptions{
		Producer: "test",
		OutputDataset: &OpenLineageDatasetID{
			Namespace: "warehouse",
			Name:      "out",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["dialect"] != "generic" {
		t.Fatalf("dialect = %#v", decoded["dialect"])
	}
	if _, ok := decoded["datasetMappings"].(map[string]any); !ok {
		t.Fatalf("datasetMappings missing or wrong type: %#v", decoded["datasetMappings"])
	}
}

func TestAnalyzeQueryOptionsDefaults(t *testing.T) {
	payload, err := marshalAnalyzeQueryOptions(AnalyzeQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["dialect"] != "generic" {
		t.Fatalf("dialect = %#v", decoded["dialect"])
	}
}

func TestDefaultClientMissingReturnsError(t *testing.T) {
	ClearDefaultClient()
	if _, err := DefaultClient(); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("DefaultClient err = %v, want ErrNoDefaultClient", err)
	}
	_, err := Transpile("SELECT 1", "generic", "generic")
	if !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := ParseDataType("INT", "generic"); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("ParseDataType err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := GenerateDataType(json.RawMessage(`{"data_type":"int"}`), "generic"); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("GenerateDataType err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := AnalyzeQuery("SELECT 1", AnalyzeQueryOptions{}); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("AnalyzeQuery err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := SetLimit(json.RawMessage(`[]`), 1); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("SetLimit err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := SetOffset(json.RawMessage(`[]`), 1); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("SetOffset err = %v, want ErrNoDefaultClient", err)
	}
	if _, err := SetOrderBy(json.RawMessage(`[]`), json.RawMessage(`[]`)); !errors.Is(err, ErrNoDefaultClient) {
		t.Fatalf("SetOrderBy err = %v, want ErrNoDefaultClient", err)
	}
}

func TestClosedClientReturnsError(t *testing.T) {
	client := &Client{closed: true}
	_, err := client.DialectCount()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestErrorWrapping(t *testing.T) {
	err := &Error{Operation: "transpile", Status: 3, Message: "bad sql"}
	if !strings.Contains(err.Error(), "transpile") || !strings.Contains(err.Error(), "bad sql") {
		t.Fatalf("unexpected error string: %s", err.Error())
	}
	if !errors.Is(err, &Error{Status: 3}) {
		t.Fatalf("errors.Is did not match status")
	}
	if !errors.Is(err, &Error{Operation: "transpile"}) {
		t.Fatalf("errors.Is did not match operation")
	}
}

func TestRejectNUL(t *testing.T) {
	if err := rejectNUL("abc"); err != nil {
		t.Fatalf("rejectNUL safe string: %v", err)
	}
	if err := rejectNUL("a\x00b"); err == nil {
		t.Fatalf("rejectNUL accepted embedded NUL")
	}
}

func TestLibraryFileName(t *testing.T) {
	name := libraryFileName()
	switch runtime.GOOS {
	case "darwin":
		if name != "libpolyglot_sql_ffi.dylib" {
			t.Fatalf("name = %q", name)
		}
	case "windows":
		if name != "polyglot_sql_ffi.dll" {
			t.Fatalf("name = %q", name)
		}
	default:
		if name != "libpolyglot_sql_ffi.so" {
			t.Fatalf("name = %q", name)
		}
	}
}

func TestDefaultLibraryCandidatesIncludeEnvFirst(t *testing.T) {
	t.Setenv(LibraryPathEnv, "/tmp/custom-polyglot-lib")
	candidates := defaultLibraryCandidates()
	if len(candidates) == 0 {
		t.Fatal("no candidates")
	}
	if candidates[0] != "/tmp/custom-polyglot-lib" {
		t.Fatalf("first candidate = %q", candidates[0])
	}
	if candidates[len(candidates)-1] != libraryFileName() {
		t.Fatalf("last candidate = %q, want system name", candidates[len(candidates)-1])
	}
}
