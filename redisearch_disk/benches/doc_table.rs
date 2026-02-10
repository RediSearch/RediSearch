use std::hint::black_box;

use criterion::{Criterion, criterion_group};
use ffi::DocumentType;
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use redisearch_disk::{
    database::SpeedbMultithreadedDatabase,
    index_spec::{
        deleted_ids::DeletedIdsStore,
        doc_table::{DocTable, DocumentFlags},
    },
};
use redisearch_rs as _;
use tempfile::TempDir;

mock_or_stub_missing_redis_c_symbols!();

fn doc_table_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocTable::insert");

    group.bench_function("On-Disk", |b| {
        let (_temp_dir, doc_table) = get_on_disk_doc_table();
        let mut last_key = 0;

        b.iter(|| {
            let (new_id, old_id) = doc_table
                .insert_document(
                    format!("doc_{last_key}"),
                    1.0,
                    DocumentFlags::empty(),
                    3,
                    2,
                    None,
                )
                .unwrap();

            black_box((new_id, old_id));

            last_key += 1;
        });
    });

    group.bench_function("In-Memory", |b| {
        let mut doc_table = get_in_memory_doc_table();
        let mut last_key = 0;

        b.iter(|| {
            let key = format!("doc_{last_key}");
            let key = key.as_bytes();
            let dmd = unsafe {
                ffi::DocTable_Put(
                    &mut doc_table,
                    key.as_ptr().cast(),
                    key.len(),
                    1.0,
                    ffi::RSDocumentFlags_Document_DefaultFlags,
                    [].as_ptr(),
                    0,
                    DocumentType::Hash,
                )
            };

            black_box(dmd);

            last_key += 1;
        })
    });
}

fn doc_table_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("DocTable::get");

    group.bench_function("On-Disk", |b| {
        let (_temp_dir, doc_table) = get_on_disk_doc_table();

        // Pre-populate with documents
        let mut doc_ids = Vec::new();
        for i in 0..100 {
            let (new_id, _) = doc_table
                .insert_document(format!("doc_{i}"), 1.0, DocumentFlags::empty(), 3, 2, None)
                .unwrap();
            doc_ids.push(new_id);
        }

        b.iter(|| {
            for &doc_id in &doc_ids {
                let doc = doc_table.get_document_metadata(doc_id);
                let _ = black_box(doc);
            }
        });
    });

    group.bench_function("In-Memory", |b| {
        let mut doc_table = get_in_memory_doc_table();
        redis_mock::init_redis_module_mock();

        // Pre-populate with documents
        let mut doc_ids = Vec::new();
        for i in 0..100 {
            let key = format!("doc_{i}");
            let dmd = unsafe {
                ffi::DocTable_Put(
                    &mut doc_table,
                    key.as_ptr().cast(),
                    key.len(),
                    1.0,
                    ffi::RSDocumentFlags_Document_DefaultFlags,
                    [].as_ptr(),
                    0,
                    DocumentType::Hash,
                )
            };
            doc_ids.push(unsafe { (*dmd).id });
        }

        b.iter(|| {
            for &doc_id in &doc_ids {
                let doc = unsafe { ffi::DocTable_Borrow(&doc_table, doc_id) };
                black_box(doc);
            }
        });
    });
}

fn get_on_disk_doc_table() -> (TempDir, DocTable) {
    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let db = SpeedbMultithreadedDatabase::open_cf(&opts, &path, ["doc_table", "reverse_lookup"])
        .unwrap();

    let doc_table = DocTable::new(DocumentType::Hash, db, DeletedIdsStore::default()).unwrap();

    (path, doc_table)
}

fn get_in_memory_doc_table() -> ffi::DocTable {
    unsafe { ffi::NewDocTable(1000, 10_000) }
}

criterion_group!(benches, doc_table_insert, doc_table_get);

fn main() {
    // The C doc table will write log output during its operations, so we need to make sure the
    // `RedisModule_Log` is setup correctly
    redis_mock::init_redis_module_mock();

    benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
