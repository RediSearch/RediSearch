/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use language::Language;

/// The expected name of every supported language, written out independently of
/// [`Language::name`] so a typo there cannot make a round-trip test pass.
const NAMES: [(Language, &str); 31] = [
    (Language::English, "english"),
    (Language::Arabic, "arabic"),
    (Language::Basque, "basque"),
    (Language::Catalan, "catalan"),
    (Language::Chinese, "chinese"),
    (Language::Danish, "danish"),
    (Language::Dutch, "dutch"),
    (Language::Finnish, "finnish"),
    (Language::French, "french"),
    (Language::German, "german"),
    (Language::Greek, "greek"),
    (Language::Hindi, "hindi"),
    (Language::Hungarian, "hungarian"),
    (Language::Italian, "italian"),
    (Language::Indonesian, "indonesian"),
    (Language::Irish, "irish"),
    (Language::Lithuanian, "lithuanian"),
    (Language::Nepali, "nepali"),
    (Language::Norwegian, "norwegian"),
    (Language::Portuguese, "portuguese"),
    (Language::Romanian, "romanian"),
    (Language::Russian, "russian"),
    (Language::Spanish, "spanish"),
    (Language::Swedish, "swedish"),
    (Language::Tamil, "tamil"),
    (Language::Turkish, "turkish"),
    (Language::Armenian, "armenian"),
    (Language::Serbian, "serbian"),
    (Language::Yiddish, "yiddish"),
    (Language::Tagalog, "tagalog"),
    (Language::Malay, "malay"),
];

#[test]
fn name_returns_the_lowercase_name() {
    for (lang, name) in NAMES {
        assert_eq!(lang.name().to_str().unwrap(), name, "{lang:?}");
    }
}

#[test]
fn from_name_finds_every_language_in_any_ascii_case() {
    for (lang, name) in NAMES {
        let mut mixed = name.to_owned();
        mixed[..1].make_ascii_uppercase();
        for spelling in [name.to_owned(), name.to_ascii_uppercase(), mixed] {
            assert_eq!(
                Language::from_name(spelling.as_bytes()),
                Some(lang),
                "{spelling}"
            );
        }
    }
}

#[test]
fn from_name_rejects_anything_but_a_whole_name() {
    let cases: [(&str, &[u8]); 6] = [
        ("empty", b""),
        ("prefix", b"engl"),
        ("extension", b"englishx"),
        ("trailing NUL", b"english\0"),
        // `İ` is not ASCII, so it is not an uppercase `i`.
        ("non-ASCII case variant", "\u{130}rish".as_bytes()),
        ("C sentinel name", b"unsupported"),
    ];
    for (case, input) in cases {
        assert_eq!(Language::from_name(input), None, "{case}");
    }
}

#[test]
fn malay_is_stemmed_as_indonesian() {
    assert_eq!(Language::Malay.snowball_stemmer(), c"indonesian");
}

#[test]
fn other_languages_are_stemmed_under_their_own_name() {
    // This includes Chinese and Tagalog, which libstemmer lacks: they must get
    // no stemmer rather than a substitute one.
    for (lang, _) in NAMES.into_iter().filter(|&(l, _)| l != Language::Malay) {
        assert_eq!(lang.snowball_stemmer(), lang.name(), "{lang:?}");
    }
}
