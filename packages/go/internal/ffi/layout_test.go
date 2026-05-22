package ffi

import (
	"testing"
	"unsafe"
)

func TestResultLayout(t *testing.T) {
	ptrSize := unsafe.Sizeof(uintptr(0))
	wantSize := ptrSize*2 + 4
	if ptrSize == 8 {
		wantSize = 24
	}

	if got := unsafe.Sizeof(Result{}); got != wantSize {
		t.Fatalf("Result size = %d, want %d", got, wantSize)
	}
	if got := unsafe.Offsetof(Result{}.Data); got != 0 {
		t.Fatalf("Result.Data offset = %d, want 0", got)
	}
	if got := unsafe.Offsetof(Result{}.Error); got != ptrSize {
		t.Fatalf("Result.Error offset = %d, want %d", got, ptrSize)
	}
	if got := unsafe.Offsetof(Result{}.Status); got != ptrSize*2 {
		t.Fatalf("Result.Status offset = %d, want %d", got, ptrSize*2)
	}
}

func TestValidationResultLayout(t *testing.T) {
	ptrSize := unsafe.Sizeof(uintptr(0))
	errorsOffset := ptrSize
	errorOffset := ptrSize * 2
	statusOffset := ptrSize * 3
	wantSize := statusOffset + 4
	if ptrSize == 8 {
		errorsOffset = 8
		errorOffset = 16
		statusOffset = 24
		wantSize = 32
	}

	if got := unsafe.Sizeof(ValidationResult{}); got != wantSize {
		t.Fatalf("ValidationResult size = %d, want %d", got, wantSize)
	}
	if got := unsafe.Offsetof(ValidationResult{}.Valid); got != 0 {
		t.Fatalf("ValidationResult.Valid offset = %d, want 0", got)
	}
	if got := unsafe.Offsetof(ValidationResult{}.ErrorsJSON); got != errorsOffset {
		t.Fatalf("ValidationResult.ErrorsJSON offset = %d, want %d", got, errorsOffset)
	}
	if got := unsafe.Offsetof(ValidationResult{}.Error); got != errorOffset {
		t.Fatalf("ValidationResult.Error offset = %d, want %d", got, errorOffset)
	}
	if got := unsafe.Offsetof(ValidationResult{}.Status); got != statusOffset {
		t.Fatalf("ValidationResult.Status offset = %d, want %d", got, statusOffset)
	}
}
