use crate::{assert_matches, assert_no_match, utils::chunk_to_string};
use wildcard::TokenStream;

#[test]
fn test_matches() {
    // no wildcard
    assert_matches!(br"foo", [br"foo"]);

    // ? at end
    assert_matches!(b"fo?", [b"foo"]);

    // ? at beginning
    assert_matches!(b"?oo", [b"foo"]);

    // // just ?
    assert_no_match!(b"????", [b"biker", b"cider", b"cooler"]);
    assert_matches!(b"????", [b"bike", b"cool"]);

    // * at end
    assert_matches!(b"fo*", [b"foo", b"fo", b"fooo"]);

    // * at beginning
    assert_matches!(b"*oo", [b"foo", b"fooo"]);
    assert_matches!(b"*", [b"bar", b""]);
    assert_matches!(b"*oo", [b"fofoo", b"foofoo"]);

    // mix
    assert_matches!(b"f?o*bar", [b"foobar", b"fooooobar"]);

    // weird cases
    assert_matches!(b"*foo*bar*foo", [b"foo_bar_foo"]);
    assert_matches!(b"", [b""]);
    assert_no_match!(b"", [b"foo"]);
    assert_no_match!(b"*?A", [b"\0"])
}
