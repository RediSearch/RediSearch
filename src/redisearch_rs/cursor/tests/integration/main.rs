/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use cursor::{Collection, Kind, Registry};

fn reserve<T>(list: &mut Registry<T>, value: T, interval: u32, random: u32) -> u64 {
    let mut draw = Some(random);
    list.reserve(value, interval, || {
        draw.take().expect("fresh random draw must not collide")
    })
}

#[test]
fn ids_keep_parity_and_retry_collisions() {
    for (kind, first, last) in [
        (Kind::User, 2, 0x8000_0000),
        (Kind::Coordinator, 1, 0x7fff_ffff),
    ] {
        let mut list = Registry::new(kind);
        assert_eq!(reserve(&mut list, (), 0, 0), first);
        let mut draws = [0x8000_0001, u32::MAX].into_iter();
        assert_eq!(list.reserve((), 0, || draws.next().unwrap()), last);
        assert_eq!(list.len(), 2);
    }
}

#[test]
fn empty_defers_active_deletion() {
    let mut list = Registry::new(Kind::User);
    let active = reserve(&mut list, Box::new(1), 0, 0);
    let idle = reserve(&mut list, Box::new(2), 0, 2);
    list.pause(idle, 0);
    list.clear();
    assert_eq!(list.len(), 1);
    assert_eq!(list.idle_len(), 0);
    assert_eq!(list.next_deadline(), None);
    assert_eq!(list.take(active), None);
    assert!(!list.pause(active, 0));
    assert!(list.is_empty());
    assert!(!list.purge(idle));
}

#[test]
fn unowned_ids_leave_cursors_unchanged() {
    let mut list = Registry::new(Kind::User);
    let id = reserve(&mut list, Box::new(19), 1, 0);
    list.pause(id, 0);
    for candidate in [id, 0, u64::MAX] {
        assert!(!list.pause(candidate, 5_000_000));
        list.remove(candidate);
        assert_eq!(list.len(), 1);
        assert_eq!(list.idle_len(), 1);
        assert_eq!(list.next_deadline(), Some(1_000_000));
    }
    assert_eq!(list.take(id).map(|v| **v), Some(19));
    list.remove(id);
    list.remove(id);
    assert!(!list.pause(id, 0));
    assert!(list.is_empty());
}

#[test]
fn expiry_includes_deadline_and_respects_throttle() {
    let mut list = Registry::new(Kind::User);
    let id = reserve(&mut list, (), u32::MAX, 0);
    let deadline = 4_294_968_295_000_000;
    assert!(list.pause(id, 1_000_000_000));
    assert_eq!(list.next_deadline(), Some(deadline));
    assert_eq!(list.collect(deadline - 1, Collection::Forced), None);
    assert_eq!(list.collect(deadline, Collection::Throttled), Some(1));
    let id = reserve(&mut list, (), 0, 0);
    list.pause(id, deadline + 1);
    assert_eq!(
        list.collect(deadline + 999_999_999, Collection::Throttled),
        None
    );
    assert_eq!(
        list.collect(deadline + 1_000_000_000, Collection::Throttled),
        Some(1)
    );
    let id = reserve(&mut list, (), 0, 0);
    list.pause(id, deadline + 1_000_000_001);
    assert_eq!(
        list.collect(deadline + 1_000_000_001, Collection::Forced),
        Some(1)
    );
    assert_eq!(
        list.collect(deadline + 1_000_000_001, Collection::Forced),
        Some(0)
    );
}

#[test]
fn operation_counter_sweeps_every_500_operations() {
    let mut list = Registry::new(Kind::User);
    let id = reserve(&mut list, (), 0, 0);
    list.pause(id, 1);
    for _ in 0..499 {
        list.tick(|| panic!("clock read before a sweep was due"));
    }
    assert_eq!(list.idle_len(), 1);
    let mut reads = 0;
    list.tick(|| {
        reads += 1;
        1_000_000_000
    });
    assert_eq!(reads, 1);
    assert!(list.is_empty());
    let id = reserve(&mut list, (), 0, 0);
    list.pause(id, 1_000_000_001);
    for _ in 0..500 {
        list.tick(|| {
            reads += 1;
            1_999_999_999
        });
    }
    assert_eq!(reads, 2);
    assert_eq!(list.idle_len(), 1);
}

#[test]
fn minimum_is_recomputed_after_removal() {
    let mut list = Registry::new(Kind::User);
    let ids: Vec<_> = (0..5)
        .map(|i| reserve(&mut list, i, [1, 3, 8, 2, 3][i], i as u32 * 2))
        .collect();
    list.pause(ids[0], 1);
    assert_eq!(list.next_deadline(), Some(1_000_001));
    assert!(!list.pause(ids[1], 1));
    assert_eq!(list.take(ids[0]), Some(&0));
    assert!(list.pause(ids[2], 1));
    assert_eq!(list.next_deadline(), Some(3_000_001));
    assert!(!list.pause(ids[4], 1));
    assert!(list.pause(ids[3], 1));
    assert_eq!(list.next_deadline(), Some(2_000_001));
    assert_eq!(list.take(ids[2]), Some(&2));
    assert!(!list.pause(ids[2], 1));
    assert_eq!(list.collect(3_000_001, Collection::Forced), Some(3));
    assert_eq!(list.next_deadline(), Some(8_000_001));
    assert_eq!(list.take(ids[2]), Some(&2));
    list.remove(ids[0]);
    list.remove(ids[2]);
    assert!(list.is_empty());
}

#[test]
fn idle_iteration_excludes_executing_cursors() {
    let mut list = Registry::new(Kind::Coordinator);
    let a = reserve(&mut list, 10, 100, 0);
    let b = reserve(&mut list, 20, 100, 2);
    list.pause(a, 0);
    list.purge(b);
    assert_eq!(list.idle_values().copied().collect::<Vec<_>>(), [10]);
    list.take(a);
    assert_eq!(list.idle_values().count(), 0);
    list.pause(b, 0);
    list.remove(a);
}

#[cfg(not(miri))]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Clone, Copy)]
    enum State {
        Active,
        Deleted,
        Idle(u64),
    }

    proptest! {
        #[test]
        fn periodic_sweeps_obey_elapsed_time(
            batches in prop::collection::vec((0usize..1501,
                prop_oneof![0u32..2001, Just(999), Just(1000), Just(1001)]), 1..8)
        ) {
            let mut list = Registry::new(Kind::User);
            let (mut total, mut now, mut last_sweep) = (0, 1, 0);
            for (operations, elapsed) in batches {
                if list.is_empty() {
                    let id = reserve(&mut list, (), 0, 0);
                    list.pause(id, now);
                }
                now += u64::from(elapsed) * 1_000_000;
                let expected_reads = (total + operations) / 500 - total / 500;
                let collected = expected_reads > 0 && now - last_sweep >= 1_000_000_000;
                let mut reads = 0;
                for _ in 0..operations {
                    list.tick(|| { reads += 1; now });
                }
                prop_assert_eq!(reads, expected_reads);
                prop_assert_eq!(list.is_empty(), collected);
                if collected { last_sweep = now; }
                total += operations;
            }
        }

        #[test]
        fn ids_follow_random_draws(draw in any::<u32>()) {
            for (kind, expected, parity) in [
                (Kind::User, u64::from((draw & 0x7fff_ffff) | 1) + 1, 0),
                (Kind::Coordinator, u64::from((draw & 0x7fff_ffff) & !1) + 1, 1),
            ] {
                let id = reserve(&mut Registry::new(kind), (), 0, draw);
                prop_assert_eq!(id, expected);
                prop_assert_ne!(id, 0);
                prop_assert_eq!(id % 2, parity);
            }
        }

        #[test]
        fn operations_preserve_cursor_lifecycle(
            steps in prop::collection::vec((0u8..8, any::<u8>(),
                prop_oneof![0u32..2001, Just(999), Just(1000), Just(1001)]), 1..150)
        ) {
            let mut list = Registry::new(Kind::User);
            let mut model = BTreeMap::<u64, (u32, u32, State)>::new();
            let mut sequence = 0;
            let mut now = 1;
            let mut last_sweep = 0;
            for (op, selector, elapsed) in steps {
                now += u64::from(elapsed) * 1_000_000;
                let id = model.keys()
                    .nth(usize::from(selector) % model.len().max(1))
                    .copied().unwrap_or(0);
                match op {
                    0 => {
                        sequence += 2;
                        let id = reserve(&mut list, sequence, elapsed, sequence);
                        model.insert(id, (sequence, elapsed, State::Active));
                    }
                    1 => {
                        let expected = if let Some((value, _, state @ State::Idle(_))) = model.get_mut(&id) {
                            *state = State::Active;
                            Some(*value)
                        } else {
                            None
                        };
                        prop_assert_eq!(list.take(id).copied(), expected);
                    }
                    2 => {
                        if let Some(&(value, ms, state)) = model.get(&id) {
                            match state {
                                State::Active => {
                                    list.pause(id, now);
                                    let deadline = now + u64::from(ms) * 1_000_000;
                                    model.insert(id, (value, ms, State::Idle(deadline)));
                                }
                                State::Deleted => {
                                    prop_assert!(!list.pause(id, now));
                                    model.remove(&id);
                                }
                                State::Idle(_) => {}
                            }
                        }
                    }
                    3 => {
                        prop_assert_eq!(list.purge(id), model.contains_key(&id));
                        if let Some((_, _, state)) = model.get_mut(&id) {
                            if matches!(state, State::Idle(_)) {
                                model.remove(&id);
                            } else {
                                *state = State::Deleted;
                            }
                        }
                    }
                    4 | 7 => {
                        let mode = if op == 4 { Collection::Forced } else { Collection::Throttled };
                        let minimum = model.values().filter_map(|(_, _, state)| match state {
                            State::Idle(deadline) => Some(*deadline),
                            _ => None,
                        }).min();
                        let expected = if minimum.is_some_and(|deadline| deadline > now)
                            || (op == 7 && now < last_sweep + 1_000_000_000) {
                            None
                        } else {
                            last_sweep = now;
                            let before = model.len();
                            model.retain(|_, (_, _, state)| {
                                !matches!(state, State::Idle(deadline) if *deadline <= now)
                            });
                            Some(before - model.len())
                        };
                        prop_assert_eq!(list.collect(now, mode), expected);
                    }
                    5 => {
                        list.clear();
                        model.retain(|_, (_, _, state)| !matches!(state, State::Idle(_)));
                        for (_, _, state) in model.values_mut() {
                            *state = State::Deleted;
                        }
                    }
                    _ => {
                        if let Some((_, _, State::Active | State::Deleted)) = model.get(&id) {
                            list.remove(id);
                            model.remove(&id);
                        }
                    }
                }
                let deadlines: Vec<_> = model.values().filter_map(|(_, _, state)| match state {
                    State::Idle(d) => Some(*d),
                    _ => None,
                }).collect();
                prop_assert_eq!(list.len(), model.len());
                prop_assert_eq!(list.is_empty(), model.is_empty());
                prop_assert_eq!(list.idle_len(), deadlines.len());
                prop_assert_eq!(list.next_deadline(), deadlines.iter().copied().min());
                prop_assert_eq!(list.idle_values().copied().collect::<BTreeSet<_>>(),
                    model.values().filter_map(|(value, _, state)|
                        matches!(state, State::Idle(_)).then_some(*value)
                    ).collect::<BTreeSet<_>>());
            }
        }
    }
}
