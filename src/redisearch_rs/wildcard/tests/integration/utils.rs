use wildcard::CharLike;

pub fn chunk_to_string<C: CharLike>(chunk: &[C]) -> String {
    String::from_utf8_lossy(
        chunk
            .iter()
            .map(|c| c.as_u8())
            .collect::<Vec<u8>>()
            .as_slice(),
    )
    .into_owned()
}

#[macro_export]
macro_rules! assert_matches {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = TokenStream::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert!(
                tokens.matches(expected),
                r#"{:?} should match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! assert_no_match {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = TokenStream::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert!(
                !tokens.matches(expected),
                r#"{:?} should not match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}
