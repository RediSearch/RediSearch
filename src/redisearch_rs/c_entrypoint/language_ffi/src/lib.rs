/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to look up, from C, the [`Language`] of a name and the names of a
//! [`Language`], with languages crossing the boundary as [`RSLanguage`].
//!
//! Functions that take a language take it as a raw `u32`, not as
//! [`RSLanguage`]: C can store any integer in an `RSLanguage`, and receiving an
//! out-of-range discriminant into a Rust enum is undefined behavior.

use std::{
    ffi::{CStr, c_char},
    ptr, slice,
};

use language::Language;
use strum::{EnumIter, FromRepr, IntoEnumIterator};

/// A [`Language`] as C stores it, plus the two C-only states.
///
/// The discriminants of the languages are persisted in the RDB (as a schema
/// rule's default language) and hashed into the index fingerprint, so they must
/// never change: every one is spelled out. A new language takes the value of
/// [`Unsupported`](Self::Unsupported), not its alphabetical place, and the two
/// sentinels move up: the RDB loader rejects every value from
/// [`Unsupported`](Self::Unsupported) up.
///
/// These numbers are a storage format, not only C glue: when the RDB code moves
/// to Rust, the mapping must move with it rather than be dropped with this crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, FromRepr)]
#[repr(C)]
#[cheadergen::config(export)]
pub enum RSLanguage {
    #[cheadergen(rename = "RS_LANG_ENGLISH")]
    English = 0,
    #[cheadergen(rename = "RS_LANG_ARABIC")]
    Arabic = 1,
    #[cheadergen(rename = "RS_LANG_BASQUE")]
    Basque = 2,
    #[cheadergen(rename = "RS_LANG_CATALAN")]
    Catalan = 3,
    #[cheadergen(rename = "RS_LANG_CHINESE")]
    Chinese = 4,
    #[cheadergen(rename = "RS_LANG_DANISH")]
    Danish = 5,
    #[cheadergen(rename = "RS_LANG_DUTCH")]
    Dutch = 6,
    #[cheadergen(rename = "RS_LANG_FINNISH")]
    Finnish = 7,
    #[cheadergen(rename = "RS_LANG_FRENCH")]
    French = 8,
    #[cheadergen(rename = "RS_LANG_GERMAN")]
    German = 9,
    #[cheadergen(rename = "RS_LANG_GREEK")]
    Greek = 10,
    #[cheadergen(rename = "RS_LANG_HINDI")]
    Hindi = 11,
    #[cheadergen(rename = "RS_LANG_HUNGARIAN")]
    Hungarian = 12,
    #[cheadergen(rename = "RS_LANG_ITALIAN")]
    Italian = 13,
    #[cheadergen(rename = "RS_LANG_INDONESIAN")]
    Indonesian = 14,
    #[cheadergen(rename = "RS_LANG_IRISH")]
    Irish = 15,
    #[cheadergen(rename = "RS_LANG_LITHUANIAN")]
    Lithuanian = 16,
    #[cheadergen(rename = "RS_LANG_NEPALI")]
    Nepali = 17,
    #[cheadergen(rename = "RS_LANG_NORWEGIAN")]
    Norwegian = 18,
    #[cheadergen(rename = "RS_LANG_PORTUGUESE")]
    Portuguese = 19,
    #[cheadergen(rename = "RS_LANG_ROMANIAN")]
    Romanian = 20,
    #[cheadergen(rename = "RS_LANG_RUSSIAN")]
    Russian = 21,
    #[cheadergen(rename = "RS_LANG_SPANISH")]
    Spanish = 22,
    #[cheadergen(rename = "RS_LANG_SWEDISH")]
    Swedish = 23,
    #[cheadergen(rename = "RS_LANG_TAMIL")]
    Tamil = 24,
    #[cheadergen(rename = "RS_LANG_TURKISH")]
    Turkish = 25,
    #[cheadergen(rename = "RS_LANG_ARMENIAN")]
    Armenian = 26,
    #[cheadergen(rename = "RS_LANG_SERBIAN")]
    Serbian = 27,
    #[cheadergen(rename = "RS_LANG_YIDDISH")]
    Yiddish = 28,
    #[cheadergen(rename = "RS_LANG_TAGALOG")]
    Tagalog = 29,
    #[cheadergen(rename = "RS_LANG_MALAY")]
    Malay = 30,
    /// A name lookup failed. Never persisted.
    #[cheadergen(rename = "RS_LANG_UNSUPPORTED")]
    Unsupported = 31,
    /// The query did not set a language, so the index default applies.
    /// Never persisted.
    #[cheadergen(rename = "RS_LANG_UNSET")]
    Unset = 32,
}

impl From<Language> for RSLanguage {
    fn from(language: Language) -> Self {
        match language {
            Language::English => Self::English,
            Language::Arabic => Self::Arabic,
            Language::Basque => Self::Basque,
            Language::Catalan => Self::Catalan,
            Language::Chinese => Self::Chinese,
            Language::Danish => Self::Danish,
            Language::Dutch => Self::Dutch,
            Language::Finnish => Self::Finnish,
            Language::French => Self::French,
            Language::German => Self::German,
            Language::Greek => Self::Greek,
            Language::Hindi => Self::Hindi,
            Language::Hungarian => Self::Hungarian,
            Language::Italian => Self::Italian,
            Language::Indonesian => Self::Indonesian,
            Language::Irish => Self::Irish,
            Language::Lithuanian => Self::Lithuanian,
            Language::Nepali => Self::Nepali,
            Language::Norwegian => Self::Norwegian,
            Language::Portuguese => Self::Portuguese,
            Language::Romanian => Self::Romanian,
            Language::Russian => Self::Russian,
            Language::Spanish => Self::Spanish,
            Language::Swedish => Self::Swedish,
            Language::Tamil => Self::Tamil,
            Language::Turkish => Self::Turkish,
            Language::Armenian => Self::Armenian,
            Language::Serbian => Self::Serbian,
            Language::Yiddish => Self::Yiddish,
            Language::Tagalog => Self::Tagalog,
            Language::Malay => Self::Malay,
        }
    }
}

impl RSLanguage {
    /// The [`Language`] this value stands for, or `None` for
    /// [`Unsupported`](Self::Unsupported) and [`Unset`](Self::Unset).
    ///
    /// Searches the `From<Language>` mapping, so the two directions cannot
    /// disagree.
    pub fn language(self) -> Option<Language> {
        Language::iter().find(|&lang| Self::from(lang) == self)
    }
}

/// Looks up a language by name. The match is ASCII case-insensitive over
/// exactly `len` bytes, so `language` need not be NUL-terminated (see
/// [`Language::from_name`]).
///
/// - If `language` is NULL, returns `DEFAULT_LANGUAGE`
///   ([`Language::default`]), whatever `len` is.
/// - If `len` is 0, returns `RS_LANG_UNSUPPORTED` without reading `language`.
/// - If no language has that name, returns `RS_LANG_UNSUPPORTED`.
///
/// # Safety
///
/// 1. When `language` is non-null and `len` is non-zero, `language` must point
///    to a [valid] region of memory of length `len`, whose bytes are
///    initialized and not mutated for the duration of the call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSLanguage_Find(language: *const c_char, len: usize) -> RSLanguage {
    if language.is_null() {
        // Must agree with C's `DEFAULT_LANGUAGE`, which `cheadergen.toml` defines.
        return Language::default().into();
    }
    if len == 0 {
        return RSLanguage::Unsupported;
    }

    // SAFETY: 1. both early returns above ruled out a null `language` and a
    // zero `len`, so the caller guarantees `len` readable bytes at `language`.
    let name = unsafe { slice::from_raw_parts(language.cast::<u8>(), len) };

    Language::from_name(name).map_or(RSLanguage::Unsupported, RSLanguage::from)
}

/// Returns the lowercase name of `language`, such as `"english"`
/// ([`Language::name`]), as a static string the caller must not free.
///
/// Returns NULL for `RS_LANG_UNSUPPORTED`, `RS_LANG_UNSET`, and any value that
/// is not an `RSLanguage`.
#[unsafe(no_mangle)]
pub extern "C" fn RSLanguage_ToString(language: u32) -> *const c_char {
    to_c_str(language, Language::name)
}

/// Returns the name of the libstemmer (snowball) algorithm for `language`, to
/// pass to `sb_stemmer_new`, as a static string the caller must not free.
///
/// The same as [`RSLanguage_ToString`], except for the languages
/// [`Language::snowball_stemmer`] remaps. Returns NULL for the same values as
/// [`RSLanguage_ToString`].
#[unsafe(no_mangle)]
pub extern "C" fn RSLanguage_ToSnowballStemmer(language: u32) -> *const c_char {
    to_c_str(language, Language::snowball_stemmer)
}

/// Applies `f` to the [`Language`] that `language` stands for, or returns NULL
/// when it stands for none; the [crate] docs say why it arrives as a raw `u32`.
fn to_c_str(language: u32, f: fn(Language) -> &'static CStr) -> *const c_char {
    usize::try_from(language)
        .ok()
        .and_then(RSLanguage::from_repr)
        .and_then(RSLanguage::language)
        .map_or(ptr::null(), |lang| f(lang).as_ptr())
}
