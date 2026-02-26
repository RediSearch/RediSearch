use pretty_assertions::assert_eq;
use redisearch_disk::{
    compaction::TextCompactionCollector,
    index_spec::{
        deleted_ids::DeletedIdsStore,
        inverted_index::{
            InvertedIndexKey, TermIndexConfig,
            block_traits::{ArchivedBlock, IndexConfig, SerializableBlock, TermIndexCfConfig},
            term::{Document, Metadata, PostingsListBlock},
        },
    },
    key_traits::AsKeyExt,
    value_traits::ValueExt,
};
use speedb::{BottommostLevelCompaction, CompactOptions, DB, IteratorMode};
use tempfile::TempDir;

// Test that compaction merges postings list blocks and applies aggregation (deletion filtering).
// This ensures the column family descriptor is set up correctly with the merge operator.
#[test]
fn test_compaction_aggregation() {
    let deleted_ids = DeletedIdsStore::new();
    let collector = TextCompactionCollector::new();
    let config = TermIndexCfConfig::new(deleted_ids.clone(), collector);
    let cf_descriptor = TermIndexConfig::cf_descriptor(config);

    let path = TempDir::new().unwrap();
    let mut opts = speedb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let db = DB::open_cf_descriptors(&opts, path.path(), vec![cf_descriptor]).unwrap();

    // Insert some test data into the "fulltext" column family
    let cf_handle = db.cf_handle("fulltext").unwrap();
    let doc1 = Document {
        doc_id: 1,
        metadata: Metadata {
            field_mask: 0xDEADBEEF,
            frequency: 42,
        },
    };
    let doc2 = Document {
        doc_id: 2,
        metadata: Metadata {
            field_mask: 0xCAFEBABE,
            frequency: 84,
        },
    };
    let doc3 = Document {
        doc_id: 3,
        metadata: Metadata {
            field_mask: 0xFEEDFACE,
            frequency: 126,
        },
    };
    let mut block = PostingsListBlock::new();
    block.push(doc1.clone());
    block.push(doc2.clone());
    block.push(doc3.clone());
    let key = InvertedIndexKey::new("term", Some(3));
    db.put_cf(&cf_handle, key.as_key(), block.serialize())
        .unwrap();

    // Get the block from the DB
    let value = db.get_cf(&cf_handle, key.as_key()).unwrap().unwrap();
    let archive = PostingsListBlock::archive_from_speedb_value(&value);

    // Verify the block contents
    assert_eq!(
        archive.iter().map(Document::from).collect::<Vec<_>>(),
        vec![doc1.clone(), doc2.clone(), doc3]
    );

    // Add a new block with more documents
    let doc4 = Document {
        doc_id: 4,
        metadata: Metadata {
            field_mask: 0xBAADF00D,
            frequency: 168,
        },
    };
    let doc5 = Document {
        doc_id: 5,
        metadata: Metadata {
            field_mask: 0xDEADC0DE,
            frequency: 210,
        },
    };
    let mut block = PostingsListBlock::new();
    block.push(doc4.clone());
    block.push(doc5.clone());
    let key = InvertedIndexKey::new("term", Some(5));

    db.put_cf(&cf_handle, key.as_key(), block.serialize())
        .unwrap();

    // Delete an ID to verify it is filtered out
    deleted_ids.mark_deleted(3);

    // Trigger compaction (Force bottommost to prevent trivial moves that skip the merge operator)
    let mut compact_opts = CompactOptions::default();
    compact_opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    db.compact_range_cf_opt(&cf_handle, None::<&[u8]>, None::<&[u8]>, &compact_opts);

    // Verify that the two block are now one block with 4 documents
    let value = db.get_cf(&cf_handle, key.as_key()).unwrap().unwrap();
    let archive = PostingsListBlock::archive_from_speedb_value(&value);

    assert_eq!(
        archive.iter().map(Document::from).collect::<Vec<_>>(),
        vec![doc1.clone(), doc2.clone(), doc4.clone(), doc5.clone()]
    );

    // Verify the compacted block is no longer accessible
    let old_key = InvertedIndexKey::new("term", Some(3));
    assert_eq!(db.get_cf(&cf_handle, old_key.as_key()).unwrap(), None);

    // Add a new block with more documents
    let doc6 = Document {
        doc_id: 6,
        metadata: Metadata {
            field_mask: 0xABADBABE,
            frequency: 252,
        },
    };
    let doc7 = Document {
        doc_id: 7,
        metadata: Metadata {
            field_mask: 0xDEADFA11,
            frequency: 294,
        },
    };

    let mut block = PostingsListBlock::new();
    block.push(doc6.clone());
    block.push(doc7.clone());
    let key = InvertedIndexKey::new("term", Some(7));

    db.put_cf(&cf_handle, key.as_key(), block.serialize())
        .unwrap();

    // Delete the last ID in the block to ensure compaction uses the correct ID for the new key
    deleted_ids.mark_deleted(7);

    // Trigger compaction again (Force bottommost to prevent trivial moves)
    let mut compact_opts = CompactOptions::default();
    compact_opts.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    db.compact_range_cf_opt(&cf_handle, None::<&[u8]>, None::<&[u8]>, &compact_opts);

    // Verify the keys in the column family are correct.
    // Merge accumulation merges all same-prefix entries into a single block,
    // so both blocks are combined. After filtering doc 7, the remaining docs
    // are {1,2,4,5,6} under a single key with last_doc_id = 6.
    let entries: Vec<_> = db
        .iterator_cf(&cf_handle, IteratorMode::Start)
        .flatten()
        .collect();

    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0.to_vec(),
        InvertedIndexKey::new("term", Some(6)).as_key(),
    );

    let archive = PostingsListBlock::archive_from_speedb_value(&entries[0].1);
    assert_eq!(
        archive.iter().map(Document::from).collect::<Vec<_>>(),
        vec![doc1, doc2, doc4, doc5, doc6]
    );
}
