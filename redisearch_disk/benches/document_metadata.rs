use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use redisearch_disk::index_spec::Key;
use redisearch_disk::index_spec::doc_table::{DocumentFlag, DocumentFlags, DocumentMetadata};

fn get_test_cases() -> Vec<(&'static str, DocumentMetadata)> {
    vec![
        (
            "small_key",
            DocumentMetadata {
                key: Key::from("doc_1"),
                score: 1.0,
                flags: DocumentFlags::empty(),
                max_term_freq: 10,
                doc_len: 50,
            },
        ),
        (
            "medium_key",
            DocumentMetadata {
                key: Key::from("document_with_medium_length_key_12345"),
                score: 3.5,
                flags: DocumentFlag::HasExpiration | DocumentFlag::HasOffsetVector,
                max_term_freq: 100,
                doc_len: 500,
            },
        ),
        (
            "large_key",
            DocumentMetadata {
                key: Key::from(
                    "very_long_document_key_with_many_characters_that_represents_a_realistic_use_case_in_production_environments_12345678901234567890",
                ),
                score: 9.99,
                flags: DocumentFlag::HasSortVector
                    | DocumentFlag::HasOffsetVector
                    | DocumentFlag::HasExpiration
                    | DocumentFlag::FailedToOpen,
                max_term_freq: 1000,
                doc_len: 5000,
            },
        ),
        (
            "max_values",
            DocumentMetadata {
                key: Key::from("document_max"),
                score: f32::MAX,
                flags: DocumentFlags::all(),
                max_term_freq: u32::MAX,
                doc_len: u32::MAX,
            },
        ),
    ]
}

fn benchmark_document_metadata_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocumentMetadata::serialize");

    let test_cases = get_test_cases();

    for (name, metadata) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &metadata, |b, meta| {
            b.iter(|| black_box(meta.serialize()));
        });
    }

    group.finish();
}

fn benchmark_document_metadata_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocumentMetadata::deserialize");

    let test_cases = get_test_cases();

    for (name, metadata) in test_cases {
        let serialized = metadata.serialize();
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &serialized,
            |b, bytes| {
                b.iter(|| black_box(DocumentMetadata::deserialize(bytes)));
            },
        );
    }

    group.finish();
}

fn benchmark_document_metadata_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocumentMetadata::roundtrip");

    // Use a subset of test cases for roundtrip (excluding max_values)
    let test_cases: Vec<_> = get_test_cases()
        .into_iter()
        .filter(|(name, _)| *name != "max_values")
        .collect();

    for (name, metadata) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &metadata, |b, meta| {
            b.iter(|| {
                let serialized = meta.serialize();
                black_box(DocumentMetadata::deserialize(&serialized))
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_document_metadata_serialize,
    benchmark_document_metadata_deserialize,
    benchmark_document_metadata_roundtrip,
);

criterion_main!(benches);
