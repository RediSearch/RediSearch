/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CStr, c_char},
    ptr,
};

use language_ffi::{
    RSLanguage, RSLanguage_Find, RSLanguage_ToSnowballStemmer, RSLanguage_ToString,
};

/// Every stored value with its name, written out by hand: RDB files and index
/// fingerprints store these numbers, so renumbering must fail this table.
const STORED: [(u32, &CStr); 31] = [
    (0, c"english"),
    (1, c"arabic"),
    (2, c"basque"),
    (3, c"catalan"),
    (4, c"chinese"),
    (5, c"danish"),
    (6, c"dutch"),
    (7, c"finnish"),
    (8, c"french"),
    (9, c"german"),
    (10, c"greek"),
    (11, c"hindi"),
    (12, c"hungarian"),
    (13, c"italian"),
    (14, c"indonesian"),
    (15, c"irish"),
    (16, c"lithuanian"),
    (17, c"nepali"),
    (18, c"norwegian"),
    (19, c"portuguese"),
    (20, c"romanian"),
    (21, c"russian"),
    (22, c"spanish"),
    (23, c"swedish"),
    (24, c"tamil"),
    (25, c"turkish"),
    (26, c"armenian"),
    (27, c"serbian"),
    (28, c"yiddish"),
    (29, c"tagalog"),
    (30, c"malay"),
];

const WITHOUT_A_LANGUAGE: [u32; 4] = [31, 32, 1000, u32::MAX];

fn as_c_str(name: *const c_char) -> Option<&'static CStr> {
    // SAFETY: the functions under test return NULL or a pointer to a static
    // NUL-terminated string.
    (!name.is_null()).then(|| unsafe { CStr::from_ptr(name) })
}

#[test]
fn to_string_names_every_stored_value() {
    for (stored, name) in STORED {
        assert_eq!(
            as_c_str(RSLanguage_ToString(stored)),
            Some(name),
            "{stored}"
        );
    }
}

#[test]
fn to_string_is_null_for_values_without_a_language() {
    for value in WITHOUT_A_LANGUAGE {
        assert_eq!(as_c_str(RSLanguage_ToString(value)), None, "{value}");
    }
}

#[test]
fn snowball_stemmer_is_null_for_values_without_a_language() {
    for value in WITHOUT_A_LANGUAGE {
        assert_eq!(
            as_c_str(RSLanguage_ToSnowballStemmer(value)),
            None,
            "{value}"
        );
    }
}

#[test]
fn snowball_stemmer_of_malay_is_indonesian() {
    assert_eq!(
        as_c_str(RSLanguage_ToSnowballStemmer(RSLanguage::Malay as u32)),
        Some(c"indonesian")
    );
}

#[test]
fn find_of_null_is_the_default() {
    // SAFETY: 1. a null `language` is never read.
    let found = unsafe { RSLanguage_Find(ptr::null(), 5) };
    assert_eq!(found, RSLanguage::English);
}

#[test]
fn find_of_zero_length_is_unsupported() {
    // SAFETY: 1. a zero `len` is never read.
    let found = unsafe { RSLanguage_Find(c"english".as_ptr(), 0) };
    assert_eq!(found, RSLanguage::Unsupported);
}

#[test]
fn find_reads_only_len_bytes() {
    // SAFETY: 1. the string is static and longer than `len`.
    let found = unsafe { RSLanguage_Find(c"englishXXX".as_ptr(), 7) };
    assert_eq!(found, RSLanguage::English);
}

#[test]
fn find_of_an_unknown_name_is_unsupported() {
    // SAFETY: 1. the string is static and exactly `len` bytes long.
    let found = unsafe { RSLanguage_Find(c"klingon".as_ptr(), 7) };
    assert_eq!(found, RSLanguage::Unsupported);
}
