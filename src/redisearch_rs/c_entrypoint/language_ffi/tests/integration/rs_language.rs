/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use language::Language;
use language_ffi::RSLanguage;
use strum::IntoEnumIterator;

#[test]
fn every_language_converts_back_to_itself() {
    for lang in Language::iter() {
        assert_eq!(RSLanguage::from(lang).language(), Some(lang), "{lang:?}");
    }
}

#[test]
fn sentinels_stand_for_no_language() {
    for value in [RSLanguage::Unsupported, RSLanguage::Unset] {
        assert_eq!(value.language(), None, "{value:?}");
    }
}

#[test]
fn only_values_below_unsupported_stand_for_a_language() {
    // The RDB loader rejects stored languages from `Unsupported` up, so a
    // language numbered there could be set on an index that then fails to reload.
    for value in RSLanguage::iter() {
        assert_eq!(
            value.language().is_some(),
            (value as u32) < RSLanguage::Unsupported as u32,
            "{value:?}"
        );
    }
}
