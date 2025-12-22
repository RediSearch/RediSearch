use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use redisearch_disk::document_id_key::DocumentIdKey;
use redisearch_disk::search_disk::{AsKeyExt, FromKeyExt};

fn get_test_cases() -> Vec<(&'static str, u64)> {
    vec![
        ("small", 42u64),
        ("medium", 1_000_000u64),
        ("large", u64::MAX / 2),
        ("max", u64::MAX),
    ]
}

fn benchmark_document_id_key_as_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocumentIdKey::as_key");

    for (name, value) in get_test_cases() {
        group.bench_with_input(BenchmarkId::from_parameter(name), &value, |b, &val| {
            let doc_id: DocumentIdKey = val.into();
            b.iter(|| black_box(doc_id.as_key()));
        });
    }

    group.finish();
}

fn benchmark_document_id_key_from_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocumentIdKey::from_key");

    for (name, value) in get_test_cases() {
        let doc_id: DocumentIdKey = value.into();
        let key = doc_id.as_key();
        group.bench_with_input(BenchmarkId::from_parameter(name), &key, |b, k| {
            b.iter(|| black_box(DocumentIdKey::from_key(k)));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_document_id_key_as_key,
    benchmark_document_id_key_from_key,
);

criterion_main!(benches);
