/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`string_utils::unicode_tolower_capped`].

use string_utils::{unicode_tolower, unicode_tolower_capped};

#[test]
fn within_cap_matches_unicode_tolower() {
    let sut = "Hello";

    let actual = unicode_tolower_capped(sut, 100);

    assert_eq!(actual.as_deref(), Some(unicode_tolower(sut).as_str()));
}

#[test]
fn exactly_at_cap_is_accepted() {
    let sut = "a".repeat(5);

    let actual = unicode_tolower_capped(&sut, 5);

    assert_eq!(actual, Some(sut));
}

#[test]
fn one_over_cap_is_rejected() {
    let actual = unicode_tolower_capped(&"a".repeat(6), 5);

    assert_eq!(actual, None);
}

#[test]
fn cap_counts_lowercased_codepoints() {
    // 'İ' (U+0130) lowercases to two codepoints ("i̇"), so three of them
    // become six lowercased codepoints — over a cap of 5, even though the
    // input is only three characters.
    let sut = "İ".repeat(3);
    assert_eq!(sut.chars().count(), 3);

    assert_eq!(unicode_tolower_capped(&sut, 5), None);
    // The same input fits under a cap of 6.
    assert_eq!(unicode_tolower_capped(&sut, 6), Some(unicode_tolower(&sut)));
}

#[test]
fn zero_cap_admits_only_empty() {
    assert_eq!(unicode_tolower_capped("", 0), Some(String::new()));
    assert_eq!(unicode_tolower_capped("a", 0), None);
}
