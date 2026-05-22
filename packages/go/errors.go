package polyglot

import (
	"errors"
	"fmt"

	"github.com/tobilg/polyglot/packages/go/internal/ffi"
)

var (
	ErrClosed          = errors.New("polyglot: client is closed")
	ErrNoDefaultClient = errors.New("polyglot: default client is not configured")
)

type Error struct {
	Operation string
	Status    int32
	Message   string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return fmt.Sprintf("polyglot %s failed with status %d", e.Operation, e.Status)
	}
	return fmt.Sprintf("polyglot %s failed with status %d: %s", e.Operation, e.Status, e.Message)
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Status != 0 && e.Status != t.Status {
		return false
	}
	if t.Operation != "" && e.Operation != t.Operation {
		return false
	}
	return true
}

func statusName(status int32) string {
	switch status {
	case ffi.StatusSuccess:
		return "success"
	case ffi.StatusParseError:
		return "parse error"
	case ffi.StatusGenerateError:
		return "generate error"
	case ffi.StatusTranspileError:
		return "transpile error"
	case ffi.StatusValidationError:
		return "validation error"
	case ffi.StatusInvalidArgument:
		return "invalid argument"
	case ffi.StatusSerializationError:
		return "serialization error"
	case ffi.StatusInternalError:
		return "internal error"
	default:
		return fmt.Sprintf("status %d", status)
	}
}
