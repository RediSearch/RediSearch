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

use strum::{EnumIter, IntoEnumIterator};

/// A language supported for stemming and tokenization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, EnumIter)]
pub enum Language {
    #[default]
    English,
    Arabic,
    Basque,
    Catalan,
    Chinese,
    Danish,
    Dutch,
    Finnish,
    French,
    German,
    Greek,
    Hindi,
    Hungarian,
    Italian,
    Indonesian,
    Irish,
    Lithuanian,
    Nepali,
    Norwegian,
    Portuguese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Tamil,
    Turkish,
    Armenian,
    Serbian,
    Yiddish,
    Tagalog,
    Malay,
}

impl Language {
    /// The lowercase user-facing name, as accepted by `LANGUAGE` arguments.
    pub const fn name(self) -> &'static CStr {
        match self {
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
        }
    }

    /// Looks up a language by its [name](Self::name).
    ///
    /// The comparison is ASCII case-insensitive over the whole slice: a prefix,
    /// a trailing byte (NUL included) or a non-ASCII case variant does not match.
    pub fn from_name(name: &[u8]) -> Option<Self> {
        Self::iter().find(|lang| lang.name().to_bytes().eq_ignore_ascii_case(name))
    }

    /// The name of the libstemmer (snowball) algorithm that stems this language.
    ///
    /// The same as [`name`](Self::name), except for a language libstemmer has
    /// no algorithm for but has one for a linguistically close language:
    /// [`Malay`](Self::Malay) maps onto [`Indonesian`](Self::Indonesian).
    /// Languages without a close algorithm keep their own name, and libstemmer
    /// provides no stemmer for them.
    pub const fn snowball_stemmer(self) -> &'static CStr {
        match self {
            Self::Malay => Self::Indonesian.name(),
            _ => self.name(),
        }
    }
}
