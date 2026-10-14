/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_spec_cache::{CachedField, IndexSpecCache};

/// A lookup name matches a field only if it has the field name's length and
/// bytes. A name with an interior NUL therefore matches nothing, not even its
/// prefix up to the NUL.
#[test]
fn names_match_by_length_and_content() {
    let cache = IndexSpecCache::new(
        [
            CachedField::new(b"title").with_ft_id(1),
            CachedField::new(b"tit\0le").with_ft_id(2),
        ],
        [],
    );

    assert_eq!(cache.find_field(c"title").map(CachedField::ft_id), Some(1));
    assert!(cache.find_field(c"tit").is_none());
    assert!(cache.find_field(c"titles").is_none());
    assert!(cache.find_field(c"Title").is_none());
}

/// A path reads up to its first NUL, as it does for C code holding it as a C
/// string, whether it is the field's own path or its name.
#[test]
fn path_ends_at_the_first_nul() {
    assert_eq!(
        CachedField::new(b"name").with_path(b"$.a\0b").path(),
        c"$.a"
    );
    assert_eq!(CachedField::new(b"na\0me").path(), c"na");
}

/// Field names, paths and rule field names are user data, so the `Debug`
/// output leaves them out.
#[test]
fn debug_output_leaves_out_names() {
    let field = CachedField::new(b"name")
        .with_path(b"$.path")
        .with_types(8)
        .with_options(1)
        .with_sort_idx(2)
        .with_ft_id(3);
    let expected_field = "CachedField { types: 8, options: 1, sort_idx: 2, ft_id: 3, .. }";
    assert_eq!(format!("{field:?}"), expected_field);

    let cache = IndexSpecCache::new([field], [c"score"]);
    assert_eq!(
        format!("{cache:?}"),
        format!("IndexSpecCache {{ fields: [{expected_field}], .. }}")
    );
}
