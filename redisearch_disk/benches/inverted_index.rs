use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use redisearch_disk::{
    index_spec::inverted_index::{
        block_traits::SerializableBlock,
        term::{self, PostingsListBlock},
    },
    value_traits::ValueExt,
};

fn get_single_element_test_cases() -> Vec<(&'static str, term::Document)> {
    vec![
        (
            "small_values",
            term::Document {
                doc_id: 1,
                metadata: term::Metadata {
                    field_mask: 0x1,
                    frequency: 1,
                },
            },
        ),
        (
            "medium_values",
            term::Document {
                doc_id: 1_000_000,
                metadata: term::Metadata {
                    field_mask: 0xDEADBEEFCAFEBABE,
                    frequency: 100,
                },
            },
        ),
        (
            "large_values",
            term::Document {
                doc_id: u64::MAX / 2,
                metadata: term::Metadata {
                    field_mask: u128::MAX / 2,
                    frequency: 10_000,
                },
            },
        ),
        (
            "max_values",
            term::Document {
                doc_id: u64::MAX,
                metadata: term::Metadata {
                    field_mask: u128::MAX,
                    frequency: u64::MAX,
                },
            },
        ),
    ]
}

fn get_block_size_test_cases() -> Vec<(&'static str, usize)> {
    vec![
        ("tiny_block", 5),
        ("small_block", 10),
        ("medium_block", 50),
        ("large_block", 100),
        ("xlarge_block", 200),
        ("max_block", 255),
    ]
}

fn create_test_documents(size: usize) -> Vec<term::Document> {
    (0..size)
        .map(|i| term::Document {
            doc_id: i as u64,
            metadata: term::Metadata {
                field_mask: (i as u128) << 64 | i as u128,
                frequency: i as u64 * 10,
            },
        })
        .collect()
}

fn benchmark_single_element_as_speedb_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::single_element_serialize");

    let test_cases = get_single_element_test_cases();

    for (name, doc) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &doc, |b, d| {
            b.iter(|| {
                let block: PostingsListBlock = d.clone().into();
                black_box(block.as_speedb_value())
            });
        });
    }

    group.finish();
}

fn benchmark_single_element_archive_from_speedb_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::single_element_deserialize");

    let test_cases = get_single_element_test_cases();

    for (name, doc) in test_cases {
        let block: PostingsListBlock = doc.clone().into();
        let data = block.as_speedb_value();

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, bytes| {
            b.iter(|| {
                let archived = PostingsListBlock::archive_from_speedb_value(bytes);
                black_box(term::Document::from(archived.get(0).unwrap()))
            });
        });
    }

    group.finish();
}

fn benchmark_block_as_speedb_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::block_serialize");

    for (name, size) in get_block_size_test_cases() {
        let documents = create_test_documents(size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &documents, |b, docs| {
            b.iter(|| {
                let mut block = PostingsListBlock::with_capacity(docs.len());
                for doc in docs {
                    block.push(doc.clone());
                }
                black_box(block.as_speedb_value())
            });
        });
    }

    group.finish();
}

fn benchmark_block_archive_from_speedb_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::block_deserialize");

    for (name, size) in get_block_size_test_cases() {
        let documents = create_test_documents(size);

        let mut block = PostingsListBlock::with_capacity(documents.len());
        for doc in &documents {
            block.push(doc.clone());
        }
        let data = block.as_speedb_value();

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, bytes| {
            b.iter(|| {
                let archived = PostingsListBlock::archive_from_speedb_value(bytes);
                let num_docs = archived.num_docs();
                let mut results = Vec::with_capacity(num_docs as usize);
                for i in 0..num_docs {
                    results.push(term::Document::from(archived.get(i).unwrap()));
                }
                black_box(results)
            });
        });
    }

    group.finish();
}

fn benchmark_block_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::block_roundtrip");

    for (name, size) in get_block_size_test_cases() {
        let documents = create_test_documents(size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &documents, |b, docs| {
            b.iter(|| {
                // Serialize
                let mut block = PostingsListBlock::with_capacity(docs.len());
                for doc in docs {
                    block.push(doc.clone());
                }
                let data = block.as_speedb_value();

                // Deserialize
                let archived = PostingsListBlock::archive_from_speedb_value(&data);
                let num_docs = archived.num_docs();
                let mut results = Vec::with_capacity(num_docs as usize);
                for i in 0..num_docs {
                    results.push(term::Document::from(archived.get(i).unwrap()));
                }
                black_box(results)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_single_element_as_speedb_value,
    benchmark_single_element_archive_from_speedb_value,
    benchmark_block_as_speedb_value,
    benchmark_block_archive_from_speedb_value,
    benchmark_block_roundtrip,
);

criterion_main!(benches);
