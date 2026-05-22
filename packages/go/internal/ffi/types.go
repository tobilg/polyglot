package ffi

import "unsafe"

const (
	StatusSuccess            int32 = 0
	StatusParseError         int32 = 1
	StatusGenerateError      int32 = 2
	StatusTranspileError     int32 = 3
	StatusValidationError    int32 = 4
	StatusInvalidArgument    int32 = 5
	StatusSerializationError int32 = 6
	StatusInternalError      int32 = 99
)

// Result mirrors polyglot_result_t from crates/polyglot-sql-ffi.
type Result struct {
	Data   uintptr
	Error  uintptr
	Status int32
}

// ValidationResult mirrors polyglot_validation_result_t from crates/polyglot-sql-ffi.
type ValidationResult struct {
	Valid      int32
	ErrorsJSON uintptr
	Error      uintptr
	Status     int32
}

func CString(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}

	p := unsafe.Pointer(ptr)
	n := 0
	for *(*byte)(unsafe.Add(p, n)) != 0 {
		n++
	}
	return string(unsafe.Slice((*byte)(p), n))
}
