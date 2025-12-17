/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::{RQEIterator, Wildcard, profile::Profile};

#[test]
fn initial_state() {
    let child = Wildcard::new(10, 1.0);
    let profile = Profile::new(child);

    assert_eq!(profile.counters().read, 0);
    assert_eq!(profile.counters().skip_to, 0);
    assert_eq!(profile.counters().eof, false);
    assert_eq!(profile.wall_time_ns(), 0);
}

#[test]
fn profile_read() {
    let child = Wildcard::new(3, 1.0);
    let mut profile = Profile::new(child);

    // Read all docs
    for i in 1..=3 {
        let result = profile.read().unwrap();
        assert!(result.is_some());
        assert_eq!(profile.counters().read, i);
    }

    assert_eq!(profile.counters().skip_to, 0);
    assert!(!profile.counters().eof);
    assert!(profile.wall_time_ns() > 0);

    // Next read returns None -> EOF
    let result = profile.read().unwrap();
    assert!(result.is_none());
    assert!(profile.counters().eof);
}

#[test]
fn profile_skip_to() {
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    let _ = profile.skip_to(5);
    assert_eq!(profile.counters().skip_to, 1);
    assert_eq!(profile.counters().read, 0);
    assert!(!profile.counters().eof);

    // Skip beyond range -> EOF
    let result = profile.skip_to(100).unwrap();
    assert!(result.is_none());
    assert_eq!(profile.counters().skip_to, 2);
    assert!(profile.counters().eof);
}

#[test]
fn profile_delegates_to_child() {
    let child = Wildcard::new(10, 2.5);
    let mut profile = Profile::new(child);

    assert_eq!(profile.last_doc_id(), 0);
    assert_eq!(profile.num_estimated(), 10);
    assert!(!profile.at_eof());

    let result = profile.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(result.weight, 2.5);
    assert_eq!(profile.last_doc_id(), 1);

    // Verify current() returns same as what read() returned
    let current = profile.current().unwrap();
    assert_eq!(current.doc_id, 1);
    assert_eq!(current.weight, 2.5);
}

#[test]
fn profile_rewind() {
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    // Read some docs
    let _ = profile.read(); // doc 1
    let _ = profile.read(); // doc 2
    assert_eq!(profile.last_doc_id(), 2);
    assert_eq!(profile.counters().read, 2);

    // Rewind
    profile.rewind();

    // Verify reset to beginning
    assert_eq!(profile.last_doc_id(), 0);
    assert!(!profile.at_eof());

    // Can read from start again
    let result = profile.read().unwrap().unwrap();
    assert_eq!(result.doc_id, 1);
    assert_eq!(profile.counters().read, 3); // counter keeps incrementing
}

#[test]
fn profile_revalidate() {
    let child = Wildcard::new(10, 1.0);
    let mut profile = Profile::new(child);

    let _ = profile.read(); // doc 1
    let _ = profile.read(); // doc 2

    // Revalidate (Wildcard returns OK)
    let status = profile.revalidate();
    assert!(status.is_ok());

    // Verify delegation still works
    assert_eq!(profile.last_doc_id(), 2);
    assert_eq!(profile.current().unwrap().doc_id, 2);
}
