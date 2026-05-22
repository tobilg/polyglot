// Package polyglot is the official Go SDK for Polyglot SQL.
//
// The SDK loads the polyglot-sql-ffi shared library at runtime through PureGo,
// so callers can keep CGO_ENABLED=0. It does not download or bundle native
// libraries; provide the shared library path with Open or POLYGLOT_SQL_FFI_PATH.
package polyglot
