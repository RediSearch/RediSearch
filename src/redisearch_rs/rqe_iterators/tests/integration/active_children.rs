/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::utils::ActiveChildren;

#[test]
fn new_all_active() {
    let ac = ActiveChildren::new(vec![10, 20, 30]);
    assert_eq!(ac.num_active(), 3);
    assert_eq!(ac.len(), 3);
    assert!(ac.is_active(0));
    assert!(ac.is_active(1));
    assert!(ac.is_active(2));
}

#[test]
fn with_active_range() {
    let ac = ActiveChildren::with_active_range(vec![10, 20, 30, 40], 1, 3);
    assert_eq!(ac.num_active(), 2);
    assert!(!ac.is_active(0));
    assert!(ac.is_active(1));
    assert!(ac.is_active(2));
    assert!(!ac.is_active(3));
}

#[test]
fn deactivate() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30]);
    ac.deactivate(1);
    assert_eq!(ac.num_active(), 2);
    assert!(!ac.is_active(1));
    // Original order preserved
    assert_eq!(*ac.get(0), 10);
    assert_eq!(*ac.get(1), 20);
    assert_eq!(*ac.get(2), 30);
}

#[test]
fn activate_all_after_deactivate() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30]);
    ac.deactivate(0);
    ac.deactivate(2);
    assert_eq!(ac.num_active(), 1);
    ac.activate_all();
    assert_eq!(ac.num_active(), 3);
    assert!(ac.is_active(0));
    assert!(ac.is_active(2));
}

#[test]
fn activate_range() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30, 40]);
    ac.activate_range(1, 3);
    assert_eq!(ac.num_active(), 2);
    assert!(!ac.is_active(0));
    assert!(ac.is_active(1));
    assert!(ac.is_active(2));
    assert!(!ac.is_active(3));
}

#[test]
fn iter_active() {
    let ac = ActiveChildren::with_active_range(vec![10, 20, 30, 40], 1, 3);
    let active: Vec<_> = ac.iter_active().collect();
    assert_eq!(active, vec![(1, &20), (2, &30)]);
}

#[test]
fn iter_active_reverse() {
    let ac = ActiveChildren::with_active_range(vec![10, 20, 30, 40], 1, 3);
    let active: Vec<_> = ac.iter_active().rev().collect();
    assert_eq!(active, vec![(2, &30), (1, &20)]);
}

#[test]
fn iter_active_mut() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30]);
    ac.deactivate(1);
    for (_, val) in ac.iter_active_mut() {
        *val += 1;
    }
    assert_eq!(*ac.get(0), 11);
    assert_eq!(*ac.get(1), 20); // inactive, unchanged
    assert_eq!(*ac.get(2), 31);
}

#[test]
fn remove_active_child() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30]);
    let removed = ac.remove(1);
    assert_eq!(removed, 20);
    assert_eq!(ac.len(), 2);
    assert_eq!(ac.num_active(), 2);
    assert_eq!(*ac.get(0), 10);
    assert_eq!(*ac.get(1), 30);
}

#[test]
fn remove_inactive_child() {
    let mut ac = ActiveChildren::new(vec![10, 20, 30]);
    ac.deactivate(1);
    let removed = ac.remove(1);
    assert_eq!(removed, 20);
    assert_eq!(ac.len(), 2);
    assert_eq!(ac.num_active(), 2);
}

#[test]
fn into_inner() {
    let ac = ActiveChildren::new(vec![10, 20, 30]);
    let inner = ac.into_inner();
    assert_eq!(inner, vec![10, 20, 30]);
}

#[test]
fn empty() {
    let ac = ActiveChildren::<i32>::new(vec![]);
    assert!(ac.is_empty());
    assert_eq!(ac.num_active(), 0);
    assert_eq!(ac.len(), 0);
}
