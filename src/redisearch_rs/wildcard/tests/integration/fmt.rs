use wildcard::WildcardPattern;

#[test]
fn test_wildcard_pattern_debug() {
    let pattern = WildcardPattern::parse(b"foo*bar?baz");
    assert_eq!(
        format!("{:#?}", pattern),
        r#"WildcardPattern {
    tokens: [
        Token::Literal(br"foo"),
        Token::Any,
        Token::Literal(br"bar"),
        Token::One,
        Token::Literal(br"baz"),
    ],
    expected_length: None,
}"#
    );
}

#[test]
fn test_wildcard_pattern_display() {
    // Ensure the display output matches the original pattern
    let pattern = WildcardPattern::parse(b"foo*bar?baz");
    assert_eq!(format!("{}", pattern), "foo*bar?baz");

    // Ensure the display output resolves escapes
    let pattern_with_escapes = WildcardPattern::parse(br"foo\*bar\?baz");
    assert_eq!(format!("{}", pattern_with_escapes), "foo*bar?baz");

    // Ensure invalid UTF-8 is replaced with the Unicode replacement character
    let invalid_utf8 = WildcardPattern::parse(&[0x66, 0x6F, 0x80, b'*', b'b', b'a', b'z']);
    assert_eq!(format!("{}", invalid_utf8), "fo�*baz");
}
