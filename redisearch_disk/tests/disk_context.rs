//! Tests for DiskContext, particularly output_info_metrics with mock InfoSink.
//!
//! These tests are excluded from Miri because they use FFI calls to SpeedB/RocksDB.

#![cfg(not(miri))]

use std::ffi::CStr;

use redisearch_disk::disk_context::DiskContext;
use redisearch_disk::info_sink::InfoSink;
use redisearch_disk::metrics::{AsyncReadMetrics, ColumnFamilyMetrics, IndexMetrics};

/// Mock implementation of InfoSink for testing.
#[derive(Debug, Default)]
struct MockInfoSink {
    sections: Vec<String>,
    dicts: Vec<String>,
    dict_depth: usize,
    /// Fields with their associated dict name: (dict_name, field_name, value)
    u64_fields: Vec<(String, String, u64)>,
    /// Current dict name for tracking which dict fields belong to
    current_dict: Option<String>,
}

impl InfoSink for MockInfoSink {
    fn add_u64(&mut self, name: &CStr, value: u64) {
        let name_str = name.to_string_lossy().into_owned();
        let dict_name = self.current_dict.clone().unwrap_or_default();
        self.u64_fields.push((dict_name, name_str, value));
    }

    fn with_section(&mut self, name: &CStr, f: impl FnOnce(&mut Self)) {
        self.sections.push(name.to_string_lossy().into_owned());
        f(self);
    }

    fn with_dict(&mut self, name: &CStr, f: impl FnOnce(&mut Self)) {
        let name_str = name.to_string_lossy().into_owned();
        self.dicts.push(name_str.clone());
        let prev_dict = self.current_dict.take();
        self.current_dict = Some(name_str);
        self.dict_depth += 1;

        f(self);

        self.dict_depth -= 1;
        self.current_dict = prev_dict;
    }
}

#[test]
fn test_output_info_metrics_empty() {
    let ctx = DiskContext::new("/tmp/test", true);
    let mut sink = MockInfoSink::default();

    ctx.output_info_metrics(&mut sink);

    // Should have one section
    assert_eq!(sink.sections, vec!["disk"]);

    // Should have two dicts (doc_table and inverted_index)
    assert_eq!(
        sink.dicts,
        vec!["disk_doc_table", "disk_text_inverted_index"]
    );

    // All dicts should be closed
    assert_eq!(sink.dict_depth, 0);

    // Should have fields with zero values (empty metrics)
    assert!(!sink.u64_fields.is_empty());

    // Check some expected field names exist
    let field_names: Vec<&str> = sink.u64_fields.iter().map(|(_, n, _)| n.as_str()).collect();
    assert!(field_names.contains(&"num_immutable_memtables"));
    assert!(field_names.contains(&"estimate_num_keys"));
    assert!(field_names.contains(&"async_total_reads_requested"));
}

#[test]
fn test_output_info_metrics_with_data() {
    let mut ctx = DiskContext::new("/tmp/test", true);

    // Inject metrics directly using store_index_metrics
    let metrics = IndexMetrics {
        doc_table: ColumnFamilyMetrics {
            estimate_num_keys: 100,
            active_memtable_size: 1024,
            ..Default::default()
        },
        inverted_index: ColumnFamilyMetrics {
            estimate_num_keys: 50,
            live_sst_files_size: 2048,
            ..Default::default()
        },
        async_read: AsyncReadMetrics {
            total_reads_requested: 10,
            reads_found: 8,
            reads_not_found: 1,
            reads_errors: 1,
        },
    };
    ctx.store_index_metrics("test_index", metrics);

    let mut sink = MockInfoSink::default();

    ctx.output_info_metrics(&mut sink);

    // Verify structure is correct
    assert_eq!(sink.sections, vec!["disk"]);
    assert_eq!(
        sink.dicts,
        vec!["disk_doc_table", "disk_text_inverted_index"]
    );
    assert_eq!(sink.dict_depth, 0);

    // Verify some specific values were output
    let find_field = |dict: &str, name: &str| {
        sink.u64_fields
            .iter()
            .find(|(d, n, _)| d == dict && n == name)
            .map(|(_, _, v)| *v)
    };

    // Doc table metrics
    assert_eq!(
        find_field("disk_doc_table", "active_memtable_size"),
        Some(1024)
    );

    // Inverted index metrics
    assert_eq!(
        find_field("disk_text_inverted_index", "live_sst_files_size"),
        Some(2048)
    );

    // Async read metrics (only in doc_table dict)
    assert_eq!(
        find_field("disk_doc_table", "async_total_reads_requested"),
        Some(10)
    );
    assert_eq!(find_field("disk_doc_table", "async_reads_found"), Some(8));
    assert_eq!(
        find_field("disk_doc_table", "async_reads_not_found"),
        Some(1)
    );
    assert_eq!(find_field("disk_doc_table", "async_reads_errors"), Some(1));
}
