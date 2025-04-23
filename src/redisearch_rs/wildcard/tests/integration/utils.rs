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
// For consistency, this macro should be called `match!`, but `match`
// is a keyword in Rust, so we use `matches!` instead.
macro_rules! matches {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches(expected),
                wildcard::MatchOutcome::Match,
                r#"{:?} should match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! no_match {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches(expected),
                wildcard::MatchOutcome::NoMatch,
                r#"{:?} should not match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! partial_match {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches(expected),
                wildcard::MatchOutcome::PartialMatch,
                r#"{:?} should be a partial match for pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! matches_fixed_len {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches_fixed_len(expected),
                wildcard::MatchOutcome::Match,
                r#"{:?} should match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! no_match_fixed_len {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches_fixed_len(expected),
                wildcard::MatchOutcome::NoMatch,
                r#"{:?} should not match pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}

#[macro_export]
macro_rules! partial_match_fixed_len {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches_fixed_len(expected),
                wildcard::MatchOutcome::PartialMatch,
                r#"{:?} should be a partial match for pattern {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
        }
    }};
}
