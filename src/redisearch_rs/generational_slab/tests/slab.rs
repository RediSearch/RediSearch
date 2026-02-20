use generational_slab::*;

use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

#[test]
fn insert_get_remove_one() {
    let mut slab = Slab::new();
    assert!(slab.is_empty());

    let key = slab.insert(10);

    assert_eq!(slab[key], 10);
    assert_eq!(slab.get(key), Some(&10));
    assert!(!slab.is_empty());
    assert!(slab.contains(key));

    assert_eq!(slab.remove(key), 10);
    assert!(!slab.contains(key));
    assert!(slab.get(key).is_none());
}

#[test]
fn insert_get_many() {
    let mut slab = Slab::with_capacity(10);

    for i in 0..10 {
        let key = slab.insert(i + 10);
        assert_eq!(slab[key], i + 10);
    }

    assert_eq!(slab.capacity(), 10);

    // Storing another one grows the slab
    let key = slab.insert(20);
    assert_eq!(slab[key], 20);

    // Capacity grows by 2x
    assert_eq!(slab.capacity(), 20);
}

#[test]
fn insert_get_remove_many() {
    let mut slab = Slab::with_capacity(10);
    let mut keys = vec![];

    for i in 0..10 {
        for j in 0..10 {
            let val = (i * 10) + j;

            let key = slab.insert(val);
            keys.push((key, val));
            assert_eq!(slab[key], val);
        }

        for (key, val) in keys.drain(..) {
            assert_eq!(val, slab.remove(key));
        }
    }

    assert_eq!(10, slab.capacity());
}

#[test]
fn insert_with_vacant_entry() {
    let mut slab = Slab::with_capacity(1);
    let key;

    {
        let entry = slab.vacant_entry();
        key = entry.key();
        entry.insert(123);
    }

    assert_eq!(123, slab[key]);
}

#[test]
fn get_vacant_entry_without_using() {
    let mut slab = Slab::<usize>::with_capacity(1);
    let key = slab.vacant_entry().key();
    assert_eq!(key, slab.vacant_entry().key());
}

#[test]
#[should_panic(expected = "invalid key")]
fn invalid_get_panics() {
    let mut slab = Slab::<usize>::with_capacity(1);
    // Insert and remove to get a valid-looking but vacant slot
    let key = slab.insert(42);
    slab.remove(key);
    // key is now stale (generation mismatch)
    let _ = &slab[key];
}

#[test]
#[should_panic(expected = "invalid key")]
fn invalid_get_mut_panics() {
    let mut slab = Slab::<usize>::new();
    let key = slab.insert(42);
    slab.remove(key);
    let _ = &mut slab[key];
}

#[test]
#[should_panic(expected = "invalid key")]
fn double_remove_panics() {
    let mut slab = Slab::<usize>::with_capacity(1);
    let key = slab.insert(123);
    slab.remove(key);
    slab.remove(key);
}

#[test]
fn slab_get_mut() {
    let mut slab = Slab::new();
    let key = slab.insert(1);

    slab[key] = 2;
    assert_eq!(slab[key], 2);

    *slab.get_mut(key).unwrap() = 3;
    assert_eq!(slab[key], 3);
}

#[test]
fn key_of_tagged() {
    let mut slab = Slab::new();
    let key = slab.insert(0);
    assert_eq!(slab.key_of(&slab[key]), key);
}

#[test]
fn key_of_layout_optimizable() {
    // Entry<&str> doesn't need a discriminant tag because it can use the
    // nonzero-ness of ptr and store Vacant's next at the same offset as len
    let mut slab = Slab::new();
    slab.insert("foo");
    slab.insert("bar");
    let third = slab.insert("baz");
    slab.insert("quux");
    assert_eq!(slab.key_of(&slab[third]), third);
}

#[test]
fn key_of_zst() {
    let mut slab = Slab::new();
    slab.insert(());
    let second = slab.insert(());
    slab.insert(());
    assert_eq!(slab.key_of(&slab[second]), second);
}

#[test]
fn reserve_does_not_allocate_if_available() {
    let mut slab = Slab::with_capacity(10);
    let mut keys = vec![];

    for i in 0..6 {
        keys.push(slab.insert(i));
    }

    for key in &keys[..4] {
        slab.remove(*key);
    }

    assert!(slab.capacity() - slab.len() == 8);

    slab.reserve(8);
    assert_eq!(10, slab.capacity());
}

#[test]
fn reserve_exact_does_not_allocate_if_available() {
    let mut slab = Slab::with_capacity(10);
    let mut keys = vec![];

    for i in 0..6 {
        keys.push(slab.insert(i));
    }

    for key in &keys[..4] {
        slab.remove(*key);
    }

    assert!(slab.capacity() - slab.len() == 8);

    slab.reserve_exact(8);
    assert_eq!(10, slab.capacity());
}

#[test]
#[should_panic(expected = "capacity overflow")]
fn reserve_does_panic_with_capacity_overflow() {
    let mut slab = Slab::with_capacity(10);
    slab.insert(true);
    slab.reserve(isize::MAX as usize);
}

#[test]
#[should_panic(expected = "capacity overflow")]
fn reserve_does_panic_with_capacity_overflow_bytes() {
    let mut slab = Slab::with_capacity(10);
    slab.insert(1u16);
    slab.reserve((isize::MAX as usize) / 2);
}

#[test]
#[should_panic(expected = "capacity overflow")]
fn reserve_exact_does_panic_with_capacity_overflow() {
    let mut slab = Slab::with_capacity(10);
    slab.insert(true);
    slab.reserve_exact(isize::MAX as usize);
}

#[test]
fn retain() {
    let mut slab = Slab::with_capacity(2);

    let key1 = slab.insert(0);
    let key2 = slab.insert(1);

    slab.retain(|key, x| {
        assert_eq!(key.position() as usize, *x);
        *x % 2 == 0
    });

    assert_eq!(slab.len(), 1);
    assert_eq!(slab[key1], 0);
    assert!(!slab.contains(key2));

    // Ensure consistency is retained
    let key = slab.insert(123);
    assert_eq!(key.position(), key2.position());

    assert_eq!(2, slab.len());
    assert_eq!(2, slab.capacity());

    // Inserting another element grows
    let key = slab.insert(345);
    assert_eq!(key.position(), 2);

    assert_eq!(4, slab.capacity());
}

#[test]
fn into_iter() {
    let mut slab = Slab::new();

    let mut keys = Vec::new();
    for i in 0..8 {
        keys.push(slab.insert(i));
    }
    slab.remove(keys[0]);
    slab.remove(keys[4]);
    slab.remove(keys[5]);
    slab.remove(keys[7]);

    let vals: Vec<_> = slab
        .into_iter()
        .inspect(|&(key, val)| assert_eq!(key.position() as usize, val))
        .map(|(_, val)| val)
        .collect();
    assert_eq!(vals, vec![1, 2, 3, 6]);
}

#[test]
fn into_iter_rev() {
    let mut slab = Slab::new();

    for i in 0..4 {
        slab.insert(i);
    }

    let mut iter = slab.into_iter();
    assert_eq!(
        iter.next_back().map(|(k, v)| (k.position(), v)),
        Some((3, 3))
    );
    assert_eq!(
        iter.next_back().map(|(k, v)| (k.position(), v)),
        Some((2, 2))
    );
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((0, 0)));
    assert_eq!(
        iter.next_back().map(|(k, v)| (k.position(), v)),
        Some((1, 1))
    );
    assert_eq!(iter.next_back(), None);
    assert_eq!(iter.next(), None);
}

#[test]
fn iter() {
    let mut slab = Slab::new();

    for i in 0..4 {
        slab.insert(i);
    }

    let vals: Vec<_> = slab
        .iter()
        .enumerate()
        .map(|(i, (key, val))| {
            assert_eq!(i, key.position() as usize);
            *val
        })
        .collect();
    assert_eq!(vals, vec![0, 1, 2, 3]);

    let key1 = slab.iter().nth(1).unwrap().0;
    slab.remove(key1);

    let vals: Vec<_> = slab.iter().map(|(_, r)| *r).collect();
    assert_eq!(vals, vec![0, 2, 3]);
}

#[test]
fn iter_rev() {
    let mut slab = Slab::new();

    let mut keys = Vec::new();
    for i in 0..4 {
        keys.push(slab.insert(i));
    }
    slab.remove(keys[0]);

    let vals: Vec<_> = slab.iter().rev().map(|(k, v)| (k.position(), *v)).collect();
    assert_eq!(vals, vec![(3, 3), (2, 2), (1, 1)]);
}

#[test]
fn iter_mut() {
    let mut slab = Slab::new();

    for i in 0..4 {
        slab.insert(i);
    }

    for (i, (key, e)) in slab.iter_mut().enumerate() {
        assert_eq!(i, key.position() as usize);
        *e += 1;
    }

    let vals: Vec<_> = slab.iter().map(|(_, r)| *r).collect();
    assert_eq!(vals, vec![1, 2, 3, 4]);

    let key2 = slab.iter().nth(2).unwrap().0;
    slab.remove(key2);

    for (_, e) in slab.iter_mut() {
        *e += 1;
    }

    let vals: Vec<_> = slab.iter().map(|(_, r)| *r).collect();
    assert_eq!(vals, vec![2, 3, 5]);
}

#[test]
fn iter_mut_rev() {
    let mut slab = Slab::new();

    let mut keys = Vec::new();
    for i in 0..4 {
        keys.push(slab.insert(i));
    }
    slab.remove(keys[2]);

    {
        let mut iter = slab.iter_mut();
        assert_eq!(
            iter.next().map(|(k, v)| (k.position(), &mut *v)),
            Some((0, &mut 0))
        );
        let mut prev_idx: u32 = u32::MAX;
        for (key, e) in iter.rev() {
            *e += 10;
            assert!(prev_idx > key.position());
            prev_idx = key.position();
        }
    }

    assert_eq!(slab[keys[0]], 0);
    assert_eq!(slab[keys[1]], 11);
    assert_eq!(slab[keys[3]], 13);
    assert!(!slab.contains(keys[2]));
}

#[test]
fn clear() {
    let mut slab = Slab::new();

    for i in 0..4 {
        slab.insert(i);
    }

    // clear full
    slab.clear();
    assert!(slab.is_empty());

    assert_eq!(0, slab.len());
    assert_eq!(4, slab.capacity());

    for i in 0..2 {
        slab.insert(i);
    }

    let vals: Vec<_> = slab.iter().map(|(_, r)| *r).collect();
    assert_eq!(vals, vec![0, 1]);

    // clear half-filled
    slab.clear();
    assert!(slab.is_empty());
}

#[test]
fn shrink_to_fit_empty() {
    let mut slab = Slab::<bool>::with_capacity(20);
    slab.shrink_to_fit();
    assert_eq!(slab.capacity(), 0);
}

#[test]
fn shrink_to_fit_no_vacant() {
    let mut slab = Slab::with_capacity(20);
    slab.insert(String::new());
    slab.shrink_to_fit();
    assert!(slab.capacity() < 10);
}

#[test]
fn shrink_to_fit_doesnt_move() {
    let mut slab = Slab::with_capacity(8);
    let foo = slab.insert("foo");
    let bar = slab.insert("bar");
    slab.insert("baz");
    let quux = slab.insert("quux");
    slab.remove(quux);
    slab.remove(bar);
    slab.shrink_to_fit();
    assert_eq!(slab.len(), 2);
    assert!(slab.capacity() >= 3);
    assert_eq!(slab.get(foo), Some(&"foo"));
    // baz is at position 2, but we can't look it up with bar's key (generation mismatch).
    // Look it up via iterator.
    let baz_key = slab.iter().find(|&(_, &v)| v == "baz").unwrap().0;
    assert_eq!(slab.get(baz_key), Some(&"baz"));
    // bar's position should be the vacant entry
    assert_eq!(slab.vacant_entry().key().position(), bar.position());
}

#[test]
fn shrink_to_fit_doesnt_recreate_list_when_nothing_can_be_done() {
    let mut slab = Slab::with_capacity(16);
    let mut keys = Vec::new();
    for i in 0..4 {
        keys.push(slab.insert(Box::new(i)));
    }
    slab.remove(keys[0]);
    slab.remove(keys[2]);
    slab.remove(keys[1]);
    assert_eq!(slab.vacant_entry().key().position(), keys[1].position());
    slab.shrink_to_fit();
    assert_eq!(slab.len(), 1);
    assert!(slab.capacity() >= 4);
    assert_eq!(slab.vacant_entry().key().position(), keys[1].position());
}

#[test]
fn compact_empty() {
    let mut slab = Slab::new();
    slab.compact(|_, _, _| panic!());
    assert_eq!(slab.len(), 0);
    assert_eq!(slab.capacity(), 0);
    slab.reserve(20);
    slab.compact(|_, _, _| panic!());
    assert_eq!(slab.len(), 0);
    assert_eq!(slab.capacity(), 0);
    let mut keys = Vec::new();
    keys.push(slab.insert(0));
    keys.push(slab.insert(1));
    keys.push(slab.insert(2));
    slab.remove(keys[1]);
    slab.remove(keys[2]);
    slab.remove(keys[0]);
    slab.compact(|_, _, _| panic!());
    assert_eq!(slab.len(), 0);
    assert_eq!(slab.capacity(), 0);
}

#[test]
fn compact_no_moves_needed() {
    let mut slab = Slab::new();
    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push(slab.insert(i));
    }
    slab.remove(keys[8]);
    slab.remove(keys[9]);
    slab.remove(keys[6]);
    slab.remove(keys[7]);
    slab.compact(|_, _, _| panic!());
    assert_eq!(slab.len(), 6);
    for ((key, &value), want) in slab.iter().zip(0..6) {
        assert!(key.position() as usize == value);
        assert_eq!(key.position() as usize, want);
    }
    assert!(slab.capacity() >= 6 && slab.capacity() < 10);
}

#[test]
fn compact_moves_successfully() {
    let mut slab = Slab::with_capacity(20);
    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push(slab.insert(i));
    }
    for &i in &[0, 5, 9, 6, 3] {
        slab.remove(keys[i]);
    }
    let mut moved = 0;
    slab.compact(|&mut v, from, to| {
        assert!(from.position() > to.position());
        assert!(from.position() >= 5);
        assert!(to.position() < 5);
        assert_eq!(from.position() as usize, v);
        moved += 1;
        true
    });
    assert_eq!(slab.len(), 5);
    assert_eq!(moved, 2);
    assert_eq!(slab.vacant_key().position(), 5);
    assert!(slab.capacity() >= 5 && slab.capacity() < 20);
    let mut iter = slab.iter();
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((0, &8)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((1, &1)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((2, &2)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((3, &7)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((4, &4)));
    assert_eq!(iter.next(), None);
}

#[test]
fn compact_doesnt_move_if_closure_errors() {
    let mut slab = Slab::with_capacity(20);
    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push(slab.insert(i));
    }
    for &i in &[9, 3, 1, 4, 0] {
        slab.remove(keys[i]);
    }
    slab.compact(|&mut v, from, to| {
        assert!(from.position() > to.position());
        assert_eq!(from.position() as usize, v);
        v != 6
    });
    assert_eq!(slab.len(), 5);
    assert!(slab.capacity() >= 7 && slab.capacity() < 20);
    assert_eq!(slab.vacant_key().position(), 3);
    let mut iter = slab.iter();
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((0, &8)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((1, &7)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((2, &2)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((5, &5)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((6, &6)));
    assert_eq!(iter.next(), None);
}

#[test]
fn compact_handles_closure_panic() {
    let mut slab = Slab::new();
    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push(slab.insert(i));
    }
    for i in 1..6 {
        slab.remove(keys[i]);
    }
    let result = catch_unwind(AssertUnwindSafe(|| {
        slab.compact(|&mut v, from, to| {
            assert!(from.position() > to.position());
            assert_eq!(from.position() as usize, v);
            if v == 7 {
                panic!("test");
            }
            true
        })
    }));
    match result {
        Err(ref payload) if payload.downcast_ref() == Some(&"test") => {}
        Err(bug) => resume_unwind(bug),
        Ok(()) => unreachable!(),
    }
    assert_eq!(slab.len(), 5 - 1);
    assert_eq!(slab.vacant_key().position(), 3);
    let mut iter = slab.iter();
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((0, &0)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((1, &9)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((2, &8)));
    assert_eq!(iter.next().map(|(k, v)| (k.position(), v)), Some((6, &6)));
    assert_eq!(iter.next(), None);
}

#[test]
fn fully_consumed_drain() {
    let mut slab = Slab::new();

    for i in 0..3 {
        slab.insert(i);
    }

    {
        let mut drain = slab.drain();
        assert_eq!(Some(0), drain.next());
        assert_eq!(Some(1), drain.next());
        assert_eq!(Some(2), drain.next());
        assert_eq!(None, drain.next());
    }

    assert!(slab.is_empty());
}

#[test]
fn partially_consumed_drain() {
    let mut slab = Slab::new();

    for i in 0..3 {
        slab.insert(i);
    }

    {
        let mut drain = slab.drain();
        assert_eq!(Some(0), drain.next());
    }

    assert!(slab.is_empty())
}

#[test]
fn drain_rev() {
    let mut slab = Slab::new();
    let mut keys = Vec::new();
    for i in 0..10 {
        keys.push(slab.insert(i));
    }
    slab.remove(keys[9]);

    let vals: Vec<u64> = slab.drain().rev().collect();
    assert_eq!(vals, (0..9).rev().collect::<Vec<u64>>());
}

#[test]
fn try_remove() {
    let mut slab = Slab::new();

    let key = slab.insert(1);

    assert_eq!(slab.try_remove(key), Some(1));
    assert_eq!(slab.try_remove(key), None);
    assert_eq!(slab.get(key), None);
}

#[test]
fn const_new() {
    static _SLAB: Slab<()> = Slab::new();
}

#[test]
fn clone_from() {
    let mut slab1 = Slab::new();
    let mut slab2 = Slab::new();
    let mut keys1 = Vec::new();
    for i in 0..5 {
        keys1.push(slab1.insert(i));
        slab2.insert(2 * i);
        slab2.insert(2 * i + 1);
    }
    slab1.remove(keys1[1]);
    slab1.remove(keys1[3]);
    slab2.clone_from(&slab1);

    let mut iter2 = slab2.iter();
    assert_eq!(iter2.next().map(|(k, v)| (k.position(), v)), Some((0, &0)));
    assert_eq!(iter2.next().map(|(k, v)| (k.position(), v)), Some((2, &2)));
    assert_eq!(iter2.next().map(|(k, v)| (k.position(), v)), Some((4, &4)));
    assert_eq!(iter2.next(), None);
    assert!(slab2.capacity() >= 10);
}

#[test]
fn get_disjoint_mut() {
    let mut slab = Slab::new();
    let k0 = slab.insert(0);
    let k1 = slab.insert(1);
    let k2 = slab.insert(2);
    let k3 = slab.insert(3);
    let k4 = slab.insert(4);
    slab.remove(k1);
    slab.remove(k3);

    assert_eq!(slab.get_disjoint_mut([]), Ok([]));

    assert_eq!(
        slab.get_disjoint_mut([k4, k2, k0]).unwrap().map(|x| *x),
        [4, 2, 0]
    );

    // Overlapping indices (same key twice)
    assert_eq!(
        slab.get_disjoint_mut([k0, k2, k0, k2]),
        Err(GetDisjointMutError::OverlappingIndices)
    );

    // Vacant index (removed slot is vacant, generation mismatch also applies
    // but Vacant is checked first)
    assert_eq!(
        slab.get_disjoint_mut([k1]),
        Err(GetDisjointMutError::IndexVacant)
    );

    let [a, b] = slab.get_disjoint_mut([k0, k4]).unwrap();
    (*a, *b) = (*b, *a);
    assert_eq!(slab[k0], 4);
    assert_eq!(slab[k4], 0);
}

#[test]
fn get_disjoint_mut_out_of_bounds_index_error() {
    let mut slab: Slab<i32> = Slab::with_capacity(10);
    let k0 = slab.insert(1);
    let k1 = slab.insert(2);

    // Create a key with an out-of-bounds position
    // (we can't use Key::from anymore, so we use vacant_key on a separate slab)
    let mut big_slab: Slab<i32> = Slab::new();
    for _ in 0..6 {
        big_slab.insert(0);
    }
    // key at position 5
    let k5 = big_slab.iter().last().unwrap().0;

    // Index 0 and 1 are valid, but index 5 is out of bounds (beyond len)
    assert_eq!(
        slab.get_disjoint_mut([k0, k1, k5]),
        Err(GetDisjointMutError::IndexOutOfBounds)
    );
}

#[test]
fn mem_usage_empty() {
    let slab = Slab::<u64>::new();
    assert_eq!(slab.mem_usage(), 0);
}

#[test]
fn mem_usage_with_capacity() {
    let slab = Slab::<u64>::with_capacity(10);
    assert!(slab.mem_usage() >= 10 * size_of::<u64>());
    assert!(slab.mem_usage() > 0);
}

#[test]
fn mem_usage_after_insert_and_remove() {
    let mut slab = Slab::<u64>::new();
    let mut keys = Vec::new();
    for i in 0..5 {
        keys.push(slab.insert(i));
    }
    let usage_after_insert = slab.mem_usage();
    assert!(usage_after_insert > 0);

    slab.remove(keys[0]);
    slab.remove(keys[1]);
    // Removing doesn't shrink the allocation
    assert_eq!(slab.mem_usage(), usage_after_insert);
}

// ===== Generational tests =====

#[test]
fn stale_key_get_returns_none() {
    let mut slab = Slab::new();
    let old_key = slab.insert("hello");
    slab.remove(old_key);
    let _new_key = slab.insert("world");

    // old_key points to the same position but has stale generation
    assert_eq!(slab.get(old_key), None);
}

#[test]
fn stale_key_contains_returns_false() {
    let mut slab = Slab::new();
    let old_key = slab.insert(42);
    slab.remove(old_key);
    slab.insert(99);

    assert!(!slab.contains(old_key));
}

#[test]
fn stale_key_try_remove_returns_none() {
    let mut slab = Slab::new();
    let old_key = slab.insert("data");
    slab.remove(old_key);
    slab.insert("new data");

    assert_eq!(slab.try_remove(old_key), None);
    // The new value is still there
    assert_eq!(slab.len(), 1);
}

#[test]
#[should_panic(expected = "invalid key")]
fn stale_key_remove_panics() {
    let mut slab = Slab::new();
    let old_key = slab.insert("hello");
    slab.remove(old_key);
    slab.insert("world");

    slab.remove(old_key); // should panic
}

#[test]
#[should_panic(expected = "invalid key")]
fn stale_key_index_panics() {
    let mut slab = Slab::new();
    let old_key = slab.insert(42);
    slab.remove(old_key);
    slab.insert(99);

    let _ = slab[old_key]; // should panic
}

#[test]
fn stale_key_get_mut_returns_none() {
    let mut slab = Slab::new();
    let old_key = slab.insert(1);
    slab.remove(old_key);
    slab.insert(2);

    assert_eq!(slab.get_mut(old_key), None);
}

#[test]
fn generation_increments_on_remove() {
    let mut slab = Slab::new();
    let key1 = slab.insert("first");
    assert_eq!(key1.generation(), 0);

    slab.remove(key1);
    let key2 = slab.insert("second");
    // Same position, but generation incremented
    assert_eq!(key2.position(), key1.position());
    assert_eq!(key2.generation(), 1);

    slab.remove(key2);
    let key3 = slab.insert("third");
    assert_eq!(key3.position(), key1.position());
    assert_eq!(key3.generation(), 2);
}

#[test]
fn get_disjoint_mut_generation_mismatch() {
    let mut slab = Slab::new();
    let old_key = slab.insert(10);
    slab.remove(old_key);
    let _new_key = slab.insert(20);

    assert_eq!(
        slab.get_disjoint_mut([old_key]),
        Err(GetDisjointMutError::GenerationMismatch)
    );
}

#[test]
fn clear_invalidates_stale_keys() {
    let mut slab = Slab::new();
    let old_key = slab.insert("hello");
    slab.clear();
    let _new_key = slab.insert("world");

    assert_eq!(
        slab.get(old_key),
        None,
        "stale key must not alias after clear"
    );
}

#[test]
fn drain_invalidates_stale_keys() {
    let mut slab = Slab::new();
    let old_key = slab.insert("hello");
    let _: Vec<_> = slab.drain().collect();
    let _new_key = slab.insert("world");

    assert_eq!(
        slab.get(old_key),
        None,
        "stale key must not alias after drain"
    );
}

#[test]
fn compact_invalidates_stale_keys() {
    let mut slab = Slab::new();
    let k0 = slab.insert("a");
    let k1 = slab.insert("b");
    // Remove position 0 so compact moves position 1→0
    slab.remove(k0);
    slab.compact(|_, _, _| true);
    // Position 1 is now gone; insert again to re-create it
    let _new = slab.insert("c");

    assert_eq!(slab.get(k1), None, "stale key must not alias after compact");
}

#[test]
fn clear_then_insert_bumps_generation() {
    let mut slab = Slab::new();
    let _old = slab.insert(42);
    slab.clear();
    let new_key = slab.insert(99);

    assert!(
        new_key.generation() > 0,
        "generation should be bumped after clear, got {}",
        new_key.generation()
    );
}

#[test]
fn shrink_to_fit_invalidates_stale_keys() {
    let mut slab = Slab::new();
    let k0 = slab.insert("a");
    let k1 = slab.insert("b");
    slab.remove(k0);
    slab.remove(k1);
    // Both slots are now vacant; shrink_to_fit pops them.
    slab.shrink_to_fit();
    assert!(slab.is_empty());
    // Re-insert to re-create positions 0 and 1.
    let _new0 = slab.insert("c");
    let _new1 = slab.insert("d");

    assert_eq!(
        slab.get(k0),
        None,
        "stale key must not alias after shrink_to_fit"
    );
    assert_eq!(
        slab.get(k1),
        None,
        "stale key must not alias after shrink_to_fit"
    );
}
