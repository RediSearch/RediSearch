/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use sorting_vector::normalized_string::NormalizedString;

#[test]
#[cfg(not(miri))]
fn test_lowercase() {
    let ns = NormalizedString::new("Hello World");
    assert_eq!(ns.as_str(), "hello world");
}

#[test]
#[cfg(not(miri))]
fn test_utf_case_folding() {
    let ns = NormalizedString::new("Héllo Wörld");
    assert_eq!(ns.as_str(), "héllo wörld");

    let ns2 = NormalizedString::new("Straße with sharp s");
    assert_eq!(ns2.as_str(), "strasse with sharp s");
}
