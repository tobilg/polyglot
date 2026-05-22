package polyglot

import (
	"os"
	"path/filepath"
	"runtime"
)

const LibraryPathEnv = "POLYGLOT_SQL_FFI_PATH"

func libraryFileName() string {
	switch runtime.GOOS {
	case "darwin":
		return "libpolyglot_sql_ffi.dylib"
	case "windows":
		return "polyglot_sql_ffi.dll"
	default:
		return "libpolyglot_sql_ffi.so"
	}
}

func defaultLibraryCandidates() []string {
	name := libraryFileName()
	var candidates []string

	if envPath := os.Getenv(LibraryPathEnv); envPath != "" {
		candidates = append(candidates, envPath)
	}

	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, localCandidates(cwd, name)...)
		if parent := filepath.Dir(cwd); parent != cwd {
			candidates = append(candidates, localCandidates(parent, name)...)
		}
		if root := filepath.Clean(filepath.Join(cwd, "..", "..")); root != cwd {
			candidates = append(candidates, localCandidates(root, name)...)
		}
	}

	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, localCandidates(filepath.Dir(exe), name)...)
	}

	candidates = append(candidates, name)
	return dedupeStrings(candidates)
}

func localCandidates(base, name string) []string {
	return []string{
		filepath.Join(base, name),
		filepath.Join(base, "target", "ffi_release", name),
		filepath.Join(base, "target", "debug", name),
		filepath.Join(base, "target", "release", name),
	}
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := values[:0]
	for _, value := range values {
		if value == "" {
			continue
		}
		key := value
		if filepath.Base(value) == value {
			key = "loader:" + value
		} else if abs, err := filepath.Abs(value); err == nil {
			key = abs
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}
