use crate::types::{PolyglotResult, PolyglotValidationResult};
use std::ffi::CString;
use std::os::raw::c_char;

/// Free a C string returned by this library.
#[no_mangle]
// This C ABI entry point validates the pointer before reclaiming library-owned memory.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub extern "C" fn polyglot_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }

    unsafe {
        let _ = CString::from_raw(s);
    }
}

/// Free a `polyglot_result_t`.
#[no_mangle]
pub extern "C" fn polyglot_free_result(result: PolyglotResult) {
    polyglot_free_string(result.data);
    polyglot_free_string(result.error);
}

/// Free a `polyglot_validation_result_t`.
#[no_mangle]
pub extern "C" fn polyglot_free_validation_result(result: PolyglotValidationResult) {
    polyglot_free_string(result.errors_json);
    polyglot_free_string(result.error);
}
