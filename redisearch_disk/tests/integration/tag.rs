use ffi::t_docId;
use redisearch_disk::{
    database::SpeedbMultithreadedDatabase,
    index_spec::inverted_index::{TagIndexConfig, TagInvertedIndex},
};
use rqe_iterators::RQEIterator;
use tempfile::TempDir;

const FIELD_INDEX: ffi::t_fieldIndex = 1;

fn get_temp_tag_index() -> (TempDir, TagInvertedIndex) {
    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Use simple CF name like the term test - no prefix extractor needed for basic tests
    let cf_name = TagIndexConfig::cf_name(FIELD_INDEX);
    let db = SpeedbMultithreadedDatabase::open_cf(&opts, &path, [cf_name]).unwrap();

    (path, TagInvertedIndex::new(db, FIELD_INDEX))
}

fn drain_iterator<'index>(mut it: impl RQEIterator<'index>) -> Vec<t_docId> {
    let mut doc_ids = Vec::new();

    while let Ok(Some(result)) = it.read() {
        doc_ids.push(result.doc_id);
    }

    doc_ids
}

#[test]
fn basic() {
    let (_temp_dir, ti) = get_temp_tag_index();

    // Add a tag with some documents
    ti.insert("electronics", 1).unwrap();
    ti.insert("electronics", 3).unwrap();
    ti.insert("books", 2).unwrap();

    // Get the iterator for the "electronics" tag
    let mut it = ti.tag_iterator("electronics", 1.0).unwrap();

    // Check the initial state of the iterator
    assert_eq!(
        it.num_estimated(),
        3,
        "Expected estimate of 3 for electronics tag"
    );
    assert!(!it.at_eof());
    assert_eq!(it.last_doc_id(), 0);

    // Read the first document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(result.weight, 1.0);

    // Read the second document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 3);
    assert_eq!(result.weight, 1.0);

    // Read past the end of the iterator
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn empty_iterator() {
    let (_temp_dir, ti) = get_temp_tag_index();

    // Get the iterator when there are no tags
    let it = ti.tag_iterator("nonexistent", 1.0).unwrap();
    assert_eq!(it.num_estimated(), 0);
}

#[test]
fn iterator_stays_with_tag() {
    let (_temp_dir, ti) = get_temp_tag_index();

    // Add two different tags
    ti.insert("tag1", 1).unwrap();
    ti.insert("tag2", 2).unwrap();

    // Get the iterator for tag1
    let mut it = ti.tag_iterator("tag1", 1.0).unwrap();

    // Read the first document
    let result = it.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);

    // It should not read into tag2's data
    assert!(it.read().unwrap().is_none());
}

#[test]
fn multiple_docs_per_tag() {
    let (_temp_dir, ti) = get_temp_tag_index();

    // Add multiple documents to same tag
    ti.insert("category", 1).unwrap();
    ti.insert("category", 2).unwrap();
    ti.insert("category", 3).unwrap();
    ti.insert("category", 5).unwrap();

    let it = ti.tag_iterator("category", 1.0).unwrap();
    let doc_ids = drain_iterator(it);

    assert_eq!(doc_ids, vec![1, 2, 3, 5]);
}
