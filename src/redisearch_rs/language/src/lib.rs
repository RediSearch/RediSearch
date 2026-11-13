/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The languages RediSearch can stem and tokenize, and their names.

use std::ffi::CStr;

use strum::{EnumIter, FromRepr, IntoEnumIterator};

/// A language supported for stemming and tokenization.
///
/// The discriminants of named languages are persisted in the RDB (as a schema
/// rule's default language) and hashed into the index fingerprint, so they must
/// never change: every one is spelled out. A new language takes the value of
/// [`Unsupported`](Self::Unsupported), not its alphabetical place, and the two
/// sentinels move up: the RDB loader rejects every value from
/// [`Unsupported`](Self::Unsupported) up.
///
/// C sees this type as `typedef enum RSLanguage`, with `RS_LANG_*` variant
/// names.
///
/// Use [`Language::from_repr`] to validate a raw value received from C.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, EnumIter, FromRepr)]
#[repr(C)]
#[cheadergen::config(export, rename = "RSLanguage")]
pub enum Language {
    #[default]
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

impl Language {
    /// The lowercase user-facing name, as accepted by `LANGUAGE` arguments.
    ///
    /// Returns `None` for [`Unsupported`](Self::Unsupported) and
    /// [`Unset`](Self::Unset).
    pub const fn name(self) -> Option<&'static CStr> {
        Some(match self {
            Self::English => c"english",
            Self::Arabic => c"arabic",
            Self::Basque => c"basque",
            Self::Catalan => c"catalan",
            Self::Chinese => c"chinese",
            Self::Danish => c"danish",
            Self::Dutch => c"dutch",
            Self::Finnish => c"finnish",
            Self::French => c"french",
            Self::German => c"german",
            Self::Greek => c"greek",
            Self::Hindi => c"hindi",
            Self::Hungarian => c"hungarian",
            Self::Italian => c"italian",
            Self::Indonesian => c"indonesian",
            Self::Irish => c"irish",
            Self::Lithuanian => c"lithuanian",
            Self::Nepali => c"nepali",
            Self::Norwegian => c"norwegian",
            Self::Portuguese => c"portuguese",
            Self::Romanian => c"romanian",
            Self::Russian => c"russian",
            Self::Spanish => c"spanish",
            Self::Swedish => c"swedish",
            Self::Tamil => c"tamil",
            Self::Turkish => c"turkish",
            Self::Armenian => c"armenian",
            Self::Serbian => c"serbian",
            Self::Yiddish => c"yiddish",
            Self::Tagalog => c"tagalog",
            Self::Malay => c"malay",
            Self::Unsupported | Self::Unset => return None,
        })
    }

    /// Looks up a supported language by its [name](Self::name).
    ///
    /// The comparison is ASCII case-insensitive over the whole slice: a prefix,
    /// a trailing byte (NUL included) or a non-ASCII case variant does not match.
    pub fn from_name(name: &[u8]) -> Option<Self> {
        Self::iter().find(|lang| {
            lang.name()
                .is_some_and(|n| n.to_bytes().eq_ignore_ascii_case(name))
        })
    }

    /// The name of the libstemmer (snowball) algorithm that stems this language.
    ///
    /// The same as [`name`](Self::name), except for a language libstemmer has
    /// no algorithm for but has one for a linguistically close language: Malay
    /// maps onto Indonesian. Languages without a close algorithm keep their own
    /// name, and libstemmer provides no stemmer for them.
    pub const fn snowball_stemmer(self) -> Option<&'static CStr> {
        match self {
            Self::Malay => Self::Indonesian.name(),
            _ => self.name(),
        }
    }
}
