use crate::{
    matches_fixed_len, no_match_fixed_len, partial_match_fixed_len, utils::chunk_to_string,
};
use wildcard::WildcardPattern;

#[test]
#[should_panic(
    expected = "`matches_fixed_len` must not be called on a token stream that contains a '*' wildcard"
)]
fn test_matches_fixed_len_panics_with_wildcards() {
    matches_fixed_len!(b"fo*", [b"foo"]);
}

#[test]
fn test_matches_fixed_len() {
    // no wildcard
    matches_fixed_len!(br"foo", [br"foo"]);
    partial_match_fixed_len!(br"foo", [br"fo"]);
    no_match_fixed_len!(br"foo", [b"fooo", b"bar"]);

    // ? at end
    matches_fixed_len!(b"fo?", [b"foo"]);
    partial_match_fixed_len!(b"fo?", [b"fo"]);
    no_match_fixed_len!(b"fo?", [b"fooo", b"bar"]);

    // ? at beginning
    matches_fixed_len!(b"?oo", [b"foo"]);
    partial_match_fixed_len!(b"?oo", [b"fo"]);
    no_match_fixed_len!(b"?oo", [b"fooo", b"bar"]);

    // just ?
    no_match_fixed_len!(b"????", [b"biker", b"cider", b"cooler"]);
    partial_match_fixed_len!(b"????", [b"b", b"bi", b"bik"]);
    matches_fixed_len!(b"????", [b"bike", b"cool"]);

    // weird cases
    matches_fixed_len!(b"", [b""]);
    no_match_fixed_len!(b"", [b"foo"]);
}
