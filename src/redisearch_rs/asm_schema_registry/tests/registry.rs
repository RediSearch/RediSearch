/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use asm_schema_registry::{AsmSchemaRegistry, SchemaRegistryEncoding};
use rdb_io::RdbIO;

#[test]
fn drop_blocks_only_names_seen_during_import() {
    let mut imports = AsmSchemaRegistry::default();
    imports.record_drop(b"before");
    imports.start();
    imports.record_drop(b"index");
    assert!(imports.was_dropped(b"index"));
    assert!(!imports.was_dropped(b"other"));
    assert!(!imports.was_dropped(b"before"));
}

#[test]
fn duplicate_start_preserves_drops() {
    let mut imports = AsmSchemaRegistry::default();
    imports.start();
    imports.record_drop(b"index");
    imports.start();
    assert!(imports.was_dropped(b"index"));
}

#[test]
fn terminal_event_releases_records_and_stops_tracking() {
    let mut imports = AsmSchemaRegistry::default();
    imports.start();
    imports.record_drop(b"index");
    imports.end();
    assert!(!imports.was_dropped(b"index"));
    assert!(!imports.record_drop(b"after"));
}

#[test]
fn retry_starts_without_previous_attempts_drops() {
    let mut imports = AsmSchemaRegistry::default();
    imports.start();
    imports.record_drop(b"index");
    imports.end();
    imports.start();
    assert!(!imports.was_dropped(b"index"));
}

#[test]
fn record_drop_reports_only_new_insertions() {
    let mut imports = AsmSchemaRegistry::default();
    assert!(!imports.record_drop(b"index"));
    imports.start();
    assert!(imports.record_drop(b"index"));
    assert!(!imports.record_drop(b"index"));
}

fn saved_records() -> Vec<u8> {
    let mut imports = AsmSchemaRegistry::default();
    imports.start();
    imports.record_drop(b"index");
    imports.record_drop(b"other");
    let mut bytes = Vec::new();
    imports.save(&mut &mut std::io::Cursor::new(&mut bytes));
    bytes
}

#[test]
fn full_sync_restores_deletions_without_an_index() {
    let mut bytes = saved_records();
    let mut replica = AsmSchemaRegistry::default();
    replica.start();
    replica
        .load(
            &mut &mut std::io::Cursor::new(&mut bytes),
            SchemaRegistryEncoding::Names,
        )
        .unwrap();
    assert!(replica.was_dropped(b"index"));
    assert!(replica.was_dropped(b"other"));
    replica.end();
    assert!(!replica.has_drops());
}

#[test]
fn full_sync_replaces_records_even_without_a_new_start_notification() {
    let mut bytes = saved_records();
    let mut replica = AsmSchemaRegistry::default();
    replica.start();
    replica.record_drop(b"pre_snapshot");
    replica.begin_loading();
    replica
        .load(
            &mut &mut std::io::Cursor::new(&mut bytes),
            SchemaRegistryEncoding::Names,
        )
        .unwrap();
    assert!(!replica.was_dropped(b"pre_snapshot"));
    assert!(replica.was_dropped(b"index"));
}

#[test]
fn cold_restart_discards_persisted_records() {
    let mut replica = AsmSchemaRegistry::default();
    let mut bytes = saved_records();
    let mut cursor = std::io::Cursor::new(&mut bytes);
    replica
        .load(&mut &mut cursor, SchemaRegistryEncoding::Names)
        .unwrap();
    assert_eq!(cursor.position() as usize, bytes.len());
    assert!(!replica.has_drops());
    assert!(!replica.record_drop(b"after"));
}

#[test]
fn legacy_payload_consumes_task_id_and_restores_names() {
    let mut bytes = Vec::new();
    let mut writer = std::io::Cursor::new(&mut bytes);
    let mut io = &mut writer;
    io.write_u64(1);
    io.write_buffer(b"legacy-task");
    io.write_u64(1);
    io.write_buffer(b"index");
    let mut replica = AsmSchemaRegistry::default();
    replica.start();
    let mut cursor = std::io::Cursor::new(&mut bytes);
    replica
        .load(&mut &mut cursor, SchemaRegistryEncoding::TaskIdAndNames)
        .unwrap();
    assert_eq!(cursor.position() as usize, bytes.len());
    assert!(replica.was_dropped(b"index"));
}

#[test]
fn loading_an_old_snapshot_without_auxiliary_data_clears_drops() {
    let mut replica = AsmSchemaRegistry::default();
    replica.start();
    replica.record_drop(b"old");
    replica.begin_loading();
    assert!(!replica.has_drops());
    assert!(replica.record_drop(b"new"));
}

#[test]
fn malformed_snapshot_does_not_install_partial_records() {
    let bytes = saved_records();
    for length in 0..bytes.len() {
        let mut truncated = bytes[..length].to_vec();
        let mut replica = AsmSchemaRegistry::default();
        replica.start();
        assert!(
            replica
                .load(
                    &mut &mut std::io::Cursor::new(&mut truncated),
                    SchemaRegistryEncoding::Names
                )
                .is_err()
        );
        assert!(!replica.has_drops());
    }
    let mut invalid_marker = 2_u64.to_le_bytes().to_vec();
    assert!(
        AsmSchemaRegistry::default()
            .load(
                &mut &mut std::io::Cursor::new(&mut invalid_marker),
                SchemaRegistryEncoding::Names
            )
            .is_err()
    );
}

#[test]
fn empty_snapshot_clears_existing_records() {
    let mut bytes = Vec::new();
    AsmSchemaRegistry::default().save(&mut &mut std::io::Cursor::new(&mut bytes));
    let mut replica = AsmSchemaRegistry::default();
    replica.start();
    replica.record_drop(b"old");
    replica
        .load(
            &mut &mut std::io::Cursor::new(&mut bytes),
            SchemaRegistryEncoding::Names,
        )
        .unwrap();
    assert!(!replica.has_drops());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config {
        // Keep interpreted runs within the Miri test timeout; native runs retain full sampling.
        cases: if cfg!(miri) { 8 } else { proptest::test_runner::Config::default().cases },
        failure_persistence: if cfg!(miri) {
            None
        } else {
            proptest::test_runner::Config::default().failure_persistence
        },
        ..proptest::test_runner::Config::default()
    })]
    #[test]
    fn persisted_names_round_trip_exactly(
        names in proptest::collection::btree_set(
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40), 0..32),
        query in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40),
    ) {
        let mut source = AsmSchemaRegistry::default();
        source.start();
        for name in &names {
            source.record_drop(name);
        }
        let mut bytes = Vec::new();
        source.save(&mut &mut std::io::Cursor::new(&mut bytes));
        let mut replica = AsmSchemaRegistry::default();
        replica.start();
        let mut cursor = std::io::Cursor::new(&mut bytes);
        replica.load(&mut &mut cursor, SchemaRegistryEncoding::Names).unwrap();
        proptest::prop_assert_eq!(cursor.position() as usize, bytes.len());
        for name in &names {
            proptest::prop_assert!(replica.was_dropped(name));
        }
        proptest::prop_assert_eq!(replica.was_dropped(&query), names.contains(&query));
        let mut restored = Vec::new();
        replica.save(&mut &mut std::io::Cursor::new(&mut restored));
        proptest::prop_assert_eq!(bytes, restored);
    }
}
