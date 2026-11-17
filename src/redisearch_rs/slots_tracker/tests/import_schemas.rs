/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use slots_tracker::ImportSchemas;

#[test]
fn drop_blocks_only_names_seen_during_import() {
    let mut imports = ImportSchemas::default();
    imports.record_drop(b"before");
    imports.start(b"task");
    imports.record_drop(b"index");
    assert!(imports.was_dropped(b"index"));
    assert!(!imports.was_dropped(b"other"));
    assert!(!imports.was_dropped(b"before"));
}

#[test]
fn duplicate_start_preserves_drops() {
    let mut imports = ImportSchemas::default();
    imports.start(b"task");
    imports.record_drop(b"index");
    imports.start(b"task");
    assert!(imports.was_dropped(b"index"));
}

#[test]
fn terminal_event_releases_only_its_import() {
    let mut imports = ImportSchemas::default();
    imports.start(b"first");
    imports.start(b"second");
    imports.record_drop(b"index");
    imports.end(b"unknown");
    imports.end(b"first");
    assert!(imports.was_dropped(b"index"));
    imports.end(b"second");
    assert!(!imports.was_dropped(b"index"));
}

#[test]
fn retry_starts_without_previous_attempts_drops() {
    let mut imports = ImportSchemas::default();
    imports.start(b"task");
    imports.record_drop(b"index");
    imports.end(b"task");
    imports.start(b"task");
    assert!(!imports.was_dropped(b"index"));
}

#[test]
fn only_new_drop_records_need_propagation() {
    let mut imports = ImportSchemas::default();
    assert!(!imports.record_drop(b"index"));
    imports.start(b"task");
    assert!(imports.record_drop(b"index"));
    assert!(!imports.record_drop(b"index"));
}

#[test]
fn a_new_task_replaces_previous_records() {
    let mut imports = ImportSchemas::default();
    imports.start(b"old");
    imports.record_drop(b"index");
    imports.start(b"new");
    assert!(!imports.was_dropped(b"index"));
    imports.record_drop(b"replacement");
    imports.end(b"old");
    assert!(imports.was_dropped(b"replacement"));
}

fn saved_records() -> Vec<u8> {
    let mut imports = ImportSchemas::default();
    imports.start(b"task");
    imports.record_drop(b"index");
    imports.record_drop(b"other");
    let mut bytes = Vec::new();
    imports.save(&mut &mut std::io::Cursor::new(&mut bytes));
    bytes
}

#[test]
fn full_sync_restores_deletions_without_an_index() {
    let mut bytes = saved_records();
    let mut replica = ImportSchemas::default();
    replica.start(b"task");
    replica
        .load(&mut &mut std::io::Cursor::new(&mut bytes))
        .unwrap();
    assert!(replica.was_dropped(b"index"));
    assert!(replica.was_dropped(b"other"));
    replica.end(b"task");
    assert!(!replica.has_drops());
}

#[test]
fn full_sync_replaces_records_even_without_a_new_start_notification() {
    let mut bytes = saved_records();
    let mut replica = ImportSchemas::default();
    replica.start(b"task");
    replica.record_drop(b"pre_snapshot");
    replica.begin_loading();
    replica
        .load(&mut &mut std::io::Cursor::new(&mut bytes))
        .unwrap();
    assert!(!replica.was_dropped(b"pre_snapshot"));
    assert!(replica.was_dropped(b"index"));
}

#[test]
fn unrelated_import_and_cold_restart_discard_persisted_records() {
    for task in [None, Some(b"unrelated".as_slice())] {
        let mut replica = ImportSchemas::default();
        if let Some(task) = task {
            replica.start(task);
        }
        let mut bytes = saved_records();
        let mut cursor = std::io::Cursor::new(&mut bytes);
        replica.load(&mut &mut cursor).unwrap();
        assert_eq!(cursor.position() as usize, bytes.len());
        assert!(!replica.has_drops());
    }
}

#[test]
fn loading_an_old_snapshot_without_auxiliary_data_clears_drops() {
    let mut replica = ImportSchemas::default();
    replica.start(b"task");
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
        let mut replica = ImportSchemas::default();
        replica.start(b"task");
        assert!(
            replica
                .load(&mut &mut std::io::Cursor::new(&mut truncated))
                .is_err()
        );
        assert!(!replica.has_drops());
    }
    let mut invalid_marker = 2_u64.to_le_bytes().to_vec();
    assert!(
        ImportSchemas::default()
            .load(&mut &mut std::io::Cursor::new(&mut invalid_marker))
            .is_err()
    );
}

#[test]
fn empty_snapshot_clears_existing_records() {
    let mut bytes = Vec::new();
    ImportSchemas::default().save(&mut &mut std::io::Cursor::new(&mut bytes));
    let mut replica = ImportSchemas::default();
    replica.start(b"task");
    replica.record_drop(b"old");
    replica
        .load(&mut &mut std::io::Cursor::new(&mut bytes))
        .unwrap();
    assert!(!replica.has_drops());
}

proptest::proptest! {
    #[test]
    fn persisted_names_round_trip_exactly(
        task in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..40),
        names in proptest::collection::btree_set(
            proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40), 0..32),
        query in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..40),
    ) {
        let mut source = ImportSchemas::default();
        source.start(&task);
        for name in &names {
            source.record_drop(name);
        }
        let mut bytes = Vec::new();
        source.save(&mut &mut std::io::Cursor::new(&mut bytes));
        let mut replica = ImportSchemas::default();
        replica.start(&task);
        let mut cursor = std::io::Cursor::new(&mut bytes);
        replica.load(&mut &mut cursor).unwrap();
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
