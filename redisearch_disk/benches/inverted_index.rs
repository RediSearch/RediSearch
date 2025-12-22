use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use redisearch_disk::index_spec::inverted_index::full_term_block::ArchivedFullTermBlock;
use redisearch_disk::index_spec::inverted_index::{
    FullTermDocument, FullTermMetadata, PostingsListBlock,
};

fn get_single_element_test_cases() -> Vec<(&'static str, FullTermDocument)> {
    vec![
        (
            "small_values",
            FullTermDocument {
                doc_id: 1,
                metadata: FullTermMetadata {
                    field_mask: 0x1,
                    frequency: 1,
                },
            },
        ),
        (
            "medium_values",
            FullTermDocument {
                doc_id: 1_000_000,
                metadata: FullTermMetadata {
                    field_mask: 0xDEADBEEFCAFEBABE,
                    frequency: 100,
                },
            },
        ),
        (
            "large_values",
            FullTermDocument {
                doc_id: u64::MAX / 2,
                metadata: FullTermMetadata {
                    field_mask: u128::MAX / 2,
                    frequency: 10_000,
                },
            },
        ),
        (
            "max_values",
            FullTermDocument {
                doc_id: u64::MAX,
                metadata: FullTermMetadata {
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

fn create_test_documents(size: usize) -> Vec<FullTermDocument> {
    (0..size)
        .map(|i| FullTermDocument {
            doc_id: i as u64,
            metadata: FullTermMetadata {
                field_mask: (i as u128) << 64 | i as u128,
                frequency: i as u64 * 10,
            },
        })
        .collect()
}

fn benchmark_single_element_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::single_element_serialize");

    let test_cases = get_single_element_test_cases();

    for (name, doc) in test_cases {
        group.bench_with_input(BenchmarkId::from_parameter(name), &doc, |b, d| {
            b.iter(|| {
                let block: PostingsListBlock = d.clone().into();
                black_box(block.serialize())
            });
        });
    }

    group.finish();
}

fn benchmark_single_element_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::single_element_deserialize");

    let test_cases = get_single_element_test_cases();

    for (name, doc) in test_cases {
        let block: PostingsListBlock = doc.clone().into();
        let data = block.serialize();

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, bytes| {
            b.iter(|| {
                let archived = ArchivedFullTermBlock::from_bytes(bytes.clone().into_boxed_slice());
                black_box(FullTermDocument::from(archived.get(0).unwrap()))
            });
        });
    }

    group.finish();
}

fn benchmark_block_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::block_serialize");

    for (name, size) in get_block_size_test_cases() {
        let documents = create_test_documents(size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &documents, |b, docs| {
            b.iter(|| {
                let mut block = PostingsListBlock::with_capacity(docs.len());
                for doc in docs {
                    block.push(doc.clone());
                }
                black_box(block.serialize())
            });
        });
    }

    group.finish();
}

fn benchmark_block_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("InvertedIndex::block_deserialize");

    for (name, size) in get_block_size_test_cases() {
        let documents = create_test_documents(size);

        let mut block = PostingsListBlock::with_capacity(documents.len());
        for doc in &documents {
            block.push(doc.clone());
        }
        let data = block.serialize();

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, bytes| {
            b.iter(|| {
                let archived = ArchivedFullTermBlock::from_bytes(bytes.clone().into_boxed_slice());
                let num_terms = archived.num_terms();
                let mut results = Vec::with_capacity(num_terms as usize);
                for i in 0..num_terms {
                    results.push(FullTermDocument::from(archived.get(i).unwrap()));
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
                let data = block.serialize();

                // Deserialize
                let archived = ArchivedFullTermBlock::from_bytes(data.into_boxed_slice());
                let num_terms = archived.num_terms();
                let mut results = Vec::with_capacity(num_terms as usize);
                for i in 0..num_terms {
                    results.push(FullTermDocument::from(archived.get(i).unwrap()));
                }
                black_box(results)
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_single_element_serialize,
    benchmark_single_element_deserialize,
    benchmark_block_serialize,
    benchmark_block_deserialize,
    benchmark_block_roundtrip,
);

criterion_main!(benches);
