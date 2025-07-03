/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{fmt, ops::Deref};

use icu_casemap::CaseMapper;

/// A normalized string type that ensures the inner string is in lower-case and utf case-folded, e.g. 'ß' becomes 'ss'.
///
/// Internally it uses the `icu_casemap` library to perform case folding, which is a Unicode-aware way of normalizing strings for comparison.
/// That is a big change from using `libnu` in C to the Rust implementation, which will be tackled in the future.
/// Read the remark section for more details.
///
/// The method [`NormalizedString::new_unchecked`] is used to create an instance by assuming the input is
/// already normalized. We use that for the FFI glue and depend on libnu for now.
///
/// ## Remarks on icu_casemap and libnu
///
/// The C code is using the function `normalizeStr` to normalize strings (utf case-folding). This functionality depends on `libnu`.
/// The `icu_casemap` crate is a Rust implementation that provides `similar`` functionality, allowing us to perform Unicode-aware case folding.
/// But we need to build confidence, e.g. with property tests, that `similar` is `same` to `libnu` in here.
/// See [Jira Ticket MOD-10320](https://redislabs.atlassian.net/browse/MOD-10320)
///
/// Miri complains about the `icu_casemap` crate because of stacked borrows. Probably a false positive, due to un-released safety models.
/// We raised a discussion in the ICU4X project to address this [issue #6723](https://github.com/unicode-org/icu4x/issues/6723),
/// as it affects our ability to use `icu_casemap` in Miri tests.
///
/// The ICU is aware of this issue and discussing current workarounds.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedString(String);

impl NormalizedString {
    /// Creates a new `NormalizedString` from an input convertible to [`AsRef<str>`].
    pub fn new<T: AsRef<str>>(input: T) -> Self {
        let casemapper = CaseMapper::new();
        let normalized = casemapper.fold_string(input.as_ref()).into_owned();

        // Safety: We ensure the string is normalized before creating the instance
        unsafe { Self::new_unchecked(normalized) }
    }

    /// Creates a new `NormalizedString` from a given `String` assuming it is already normalized.
    /// This is unsafe because it does not check the normalization of the input.
    ///
    /// # Safety
    /// The caller must ensure that the input string is already normalized.
    /// If the input is not normalized, it may lead to incorrect behavior when comparing or sorting strings.
    pub unsafe fn new_unchecked(inner: String) -> Self {
        NormalizedString(inner)
    }

    /// Returns the inner string as a `&str`.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Get Strings from NormalizedString
impl From<NormalizedString> for String {
    fn from(normalized: NormalizedString) -> Self {
        normalized.0
    }
}

/// Convert from `String` or `&str` to `NormalizedString`
impl<T: AsRef<str>> From<T> for NormalizedString {
    fn from(input: T) -> Self {
        let casemapper = CaseMapper::new();
        let normalized = casemapper.fold_string(input.as_ref()).into_owned();
        // Safety: We ensured above that the string is normalized
        unsafe { Self::new_unchecked(normalized) }
    }
}

// Deref to &str for convenience
impl Deref for NormalizedString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Display for printing
impl fmt::Display for NormalizedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
