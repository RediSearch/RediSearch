use criterion::{Criterion, black_box, criterion_group, criterion_main};
use wildcard::WildcardPattern;

fn criterion_benchmark_matching(c: &mut Criterion) {
    let cases = vec![
        (b"foobar".to_vec(), b"foobar".to_vec()), // Exact match, no wildcards
        (b"fo?bar".to_vec(), b"foobar".to_vec()), // Match with `?`
        (b"fo?bar".to_vec(), b"foobarz".to_vec()), // Too long to match, without `*`
        (b"foo*baz".to_vec(), b"foobarbaz".to_vec()), // Multi-character wildcard match, requires backtracking
        (
            b"foo*bar*red??*l*bs?".to_vec(),
            b"foobarbazredxxlxxbsx".to_vec(),
        ), // Complex pattern match
        (b"*".to_vec(), b"randomkey".to_vec()),       // Match everything
    ];

    for (pattern, key) in &cases {
        let pattern = WildcardPattern::parse(pattern);

        let id = format!("{} vs {}", pattern, String::from_utf8_lossy(&key));
        c.bench_function(&id, |b| {
            b.iter(|| pattern.matches(black_box(key)));
        });
    }
}

criterion_group!(matching, criterion_benchmark_matching);
criterion_main!(matching);
