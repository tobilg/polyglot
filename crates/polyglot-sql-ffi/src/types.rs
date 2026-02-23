use std::os::raw::c_char;
use std::ptr;

pub const STATUS_SUCCESS: i32 = 0;
pub const STATUS_PARSE_ERROR: i32 = 1;
pub const STATUS_GENERATE_ERROR: i32 = 2;
pub const STATUS_TRANSPILE_ERROR: i32 = 3;
pub const STATUS_VALIDATION_ERROR: i32 = 4;
pub const STATUS_INVALID_ARGUMENT: i32 = 5;
pub const STATUS_SERIALIZATION_ERROR: i32 = 6;
pub const STATUS_INTERNAL_ERROR: i32 = 99;

/// Common result payload for string-returning FFI functions.
#[repr(C)]
#[derive(Debug)]
pub struct PolyglotResult {
    /// Result payload. Owned by caller and must be freed with `polyglot_free_string`.
    pub data: *mut c_char,
    /// Error message payload. Owned by caller and must be freed with `polyglot_free_string`.
    pub error: *mut c_char,
    /// Status code. `0` means success.
    pub status: i32,
}

impl PolyglotResult {
    pub fn success(data: *mut c_char) -> Self {
        Self {
            data,
            error: ptr::null_mut(),
            status: STATUS_SUCCESS,
        }
    }

    pub fn error(status: i32, error: *mut c_char) -> Self {
        Self {
            data: ptr::null_mut(),
            error,
            status,
        }
    }
}

/// Validation result payload for FFI.
#[repr(C)]
#[derive(Debug)]
pub struct PolyglotValidationResult {
    /// `1` if valid, `0` otherwise.
    pub valid: i32,
    /// JSON array of validation errors. Owned by caller and must be freed with `polyglot_free_string`.
    pub errors_json: *mut c_char,
    /// Optional top-level error message.
    pub error: *mut c_char,
    /// Status code. `0` means success.
    pub status: i32,
}

impl PolyglotValidationResult {
    pub fn success(valid: i32, errors_json: *mut c_char) -> Self {
        Self {
            valid,
            errors_json,
            error: ptr::null_mut(),
            status: STATUS_SUCCESS,
        }
    }

    pub fn error(status: i32, error: *mut c_char) -> Self {
        Self {
            valid: 0,
            errors_json: ptr::null_mut(),
            error,
            status,
        }
    }
}
