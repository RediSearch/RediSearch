use crate::{matches, no_match, partial_match, utils::chunk_to_string};
use wildcard::WildcardPattern;

#[test]
fn test_matches() {
    // no wildcard
    matches!(br"foo", [br"foo"]);
    partial_match!(br"foo", [br"fo"]);
    no_match!(br"foo", [b"fooo", b"bar"]);

    // ? at end
    matches!(b"fo?", [b"foo"]);
    partial_match!(b"fo?", [b"fo"]);
    no_match!(b"fo?", [b"fooo", b"bar"]);

    // ? at beginning
    matches!(b"?oo", [b"foo"]);
    partial_match!(b"?oo", [b"fo"]);
    no_match!(b"?oo", [b"fooo", b"bar"]);

    // just ?
    no_match!(b"????", [b"biker", b"cider", b"cooler"]);
    partial_match!(b"????", [b"b", b"bi", b"bik"]);
    matches!(b"????", [b"bike", b"cool"]);

    // * at end
    matches!(b"fo*", [b"foo", b"fo", b"fooo"]);
    no_match!(b"fo*", [b"bar"]);

    // * at beginning
    matches!(b"*oo", [b"foo", b"fooo", b"fofoo", b"foofoo"]);
    partial_match!(b"*oo", [b"fo", b"bar"]);
    matches!(b"*", [b"bar", b""]);

    // mix
    matches!(b"f?o*bar", [b"foobar", b"fooooobar"]);
    no_match!(b"f?o*bar", [b"fobar", b"barfoo", b"bar"]);
    partial_match!(b"*f?o*bar", [b"bar"]);

    // weird cases
    matches!(b"*foo*bar*foo", [b"foo_bar_foo"]);
    matches!(b"", [b""]);
    no_match!(b"", [b"foo"]);
    partial_match!(b"*?A", [b"\0"])
}
