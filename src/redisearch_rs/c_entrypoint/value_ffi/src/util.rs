use value::RsValue;

/// Get a reference to an RsValue object from a pointer.
///
/// # SAFETY
///
/// value must point to a valid RsValue object.
pub(crate) unsafe fn expect_value<'a>(value: *const RsValue) -> &'a RsValue {
    // SAFETY: value points to a valid RsValue object.
    let value = unsafe { value.as_ref() };

    if cfg!(debug_assertions) {
        value.expect("value must not be null")
    } else {
        // SAFETY: value points to a valid RsValue object.
        unsafe { value.unwrap_unchecked() }
    }
}
