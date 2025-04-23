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
macro_rules! _assert_match {
    ($pattern:expr, $expected_results:expr $(,)?, $outcome:expr) => {{
        let tokens = WildcardPattern::parse($pattern);

        let results: &[&[u8]] = &$expected_results;
        for expected in results {
            assert_eq!(
                tokens.matches(expected),
                $outcome,
                r#"Unexpected match outcome for {:?} when trying to match against {:?}"#,
                chunk_to_string(expected),
                chunk_to_string($pattern)
            );
            if tokens.tokens().iter().all(|t| *t != wildcard::Token::Any) {
                assert_eq!(
                    tokens.matches(expected),
                    tokens.matches_fixed_len(expected),
                    r#"{:?} should yield the same result when trying to match {:?} normally \
                    and via the fixed-length optimized algorithm"#,
                    chunk_to_string(expected),
                    chunk_to_string($pattern)
                )
            }
        }
    }};
}

#[macro_export]
// For consistency, this macro should be called `match!`, but `match`
// is a keyword in Rust, so we use `matches!` instead.
macro_rules! matches {
    ($pattern:expr, $expected_results:expr $(,)?) => {{ crate::_assert_match!($pattern, $expected_results, wildcard::MatchOutcome::Match) }};
}

#[macro_export]
macro_rules! no_match {
    ($pattern:expr, $expected_results:expr $(,)?) => {{ crate::_assert_match!($pattern, $expected_results, wildcard::MatchOutcome::NoMatch) }};
}

#[macro_export]
macro_rules! partial_match {
    ($pattern:expr, $expected_results:expr $(,)?) => {{
        crate::_assert_match!(
            $pattern,
            $expected_results,
            wildcard::MatchOutcome::PartialMatch
        )
    }};
}
