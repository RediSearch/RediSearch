/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use sorting_vector::{IndexOutOfBounds, RSSortingVector};
use value::shared::SHARED_VALUE_CONTENT_SIZE;
use value::{SharedValue, Value};

#[test]
fn creation() {
    let vector: RSSortingVector = RSSortingVector::new(10);
    assert_eq!(vector.len(), 10);
    assert_eq!(vector.iter().count(), 10);

    for value in vector {
        assert!(value.is_null_static());
    }
}

fn build_vector() -> Result<RSSortingVector, IndexOutOfBounds> {
    let mut vector = RSSortingVector::new(4);
    vector.try_insert_num(0, 42.0)?;
    vector.try_insert_string(1, b"abcdefg".to_vec())?;
    vector.try_insert_string_normalize(2, "Hello World")?;
    vector.try_insert_null(3)?;
    Ok(vector)
}

#[test]
fn insert() -> Result<(), IndexOutOfBounds> {
    let vector: &mut RSSortingVector = &mut build_vector()?;

    assert!(matches!(*vector[0], Value::Number(42.0)));
    assert_eq!(vector[1].as_str_bytes(), Some("abcdefg".as_bytes()));
    assert_eq!(vector[2].as_str_bytes(), Some("hello world".as_bytes())); // we normalize --> lowercase
    assert!(vector[3].is_null_static());

    Ok(())
}

#[test]
fn out_of_bounds() -> Result<(), IndexOutOfBounds> {
    let mut vector = build_vector()?;

    assert_eq!(vector.len(), 4);
    let reval = vector.try_insert_num(5, 1.0);
    assert!(reval.is_err());
    Ok(())
}

#[test]
fn override_value() -> Result<(), IndexOutOfBounds> {
    let src = build_vector()?;
    let mut dst: RSSortingVector = RSSortingVector::new(1);
    assert!(dst[0].is_null_static());

    for (idx, val) in src.iter().enumerate() {
        dst.try_insert_val(0, val.clone())?;
        assert!(SharedValue::ptr_eq(&dst[0], &src[idx]));
    }

    // the following is only possible in Rust API
    let mut dupl = src.clone();
    for val in dupl.iter_mut() {
        *val = SharedValue::new_num(42.0)
    }

    assert!(dst[0].is_null_static());
    Ok(())
}

#[test]
fn memory_size() -> Result<(), IndexOutOfBounds> {
    let empty = RSSortingVector::new(0);
    let size = empty.get_memory_size();
    assert!(empty.is_empty());
    assert_eq!(size, 0);

    let mut vec = RSSortingVector::new(1);
    vec.try_insert_num(0, 42.0)?;
    let size = vec.get_memory_size();
    let expected_size = size_of::<SharedValue>() + SHARED_VALUE_CONTENT_SIZE;
    assert_eq!(size, expected_size);

    // test with more complex values
    let vector = build_vector()?;
    let size = vector.get_memory_size();
    let expected_size = 4 * size_of::<SharedValue>() // 4 RSValue pointers
            + (3 * SHARED_VALUE_CONTENT_SIZE) // 3 actually allocated RSValues (the 4th is a null)
            + "abcdefg".len() // size of the string "abcdefg"
            + "Hello World".len(); // size of the string "Hello World"

    assert_eq!(size, expected_size);
    Ok(())
}

#[test]
#[cfg_attr(
    miri,
    ignore = "icu_casemap causes errors under miri, see <https://github.com/unicode-org/icu4x/issues/6723>"
)]
fn case_folding_aka_normalization() -> Result<(), IndexOutOfBounds> {
    let str = "Straße";
    let mut vec: RSSortingVector = RSSortingVector::new(1);
    vec.try_insert_string_normalize(0, str)?;
    assert_eq!(vec[0].as_str_bytes(), Some("strasse".as_bytes()));
    Ok(())
}

fn filled_indexes(n: usize, k: usize, seed: u64) -> Vec<usize> {
    // Deterministic distinct indexes below `n`, in increasing order.
    let mut state = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let mut all: Vec<usize> = (0..n).collect();
    for i in 0..k {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = i + (state >> 33) as usize % (n - i);
        all.swap(i, j);
    }
    let mut chosen = all[..k].to_vec();
    chosen.sort_unstable();
    chosen
}

#[test]
fn sparse_new_is_marked_and_empty() {
    let sv = RSSortingVector::new_sparse(4);
    assert!(sv.is_sparse());
    assert!(!RSSortingVector::new(3).is_sparse());
    assert!(!RSSortingVector::empty().is_sparse());
    assert_eq!(sv.sparse_entries().count(), 0);
    assert!(sv.get_by_index(0).is_none());
}

#[test]
fn sparse_set_get_replace_and_order() {
    let mut sv = RSSortingVector::new_sparse(0);
    // Out of order and with a replacement.
    for idx in [900, 3, 32766, 17, 400, 3] {
        sv.try_insert_num(idx, idx as f64 + 0.5).unwrap();
    }
    sv.try_insert_string(5, b"five".to_vec()).unwrap();
    sv.try_insert_string_normalize(6, "SiX").unwrap();
    sv.try_insert_null(7).unwrap();
    sv.try_insert_num(3, 77.0).unwrap();

    let entries: Vec<usize> = sv.sparse_entries().map(|(idx, _)| idx).collect();
    assert_eq!(entries, [3, 5, 6, 7, 17, 400, 900, 32766]);
    assert!(matches!(**sv.get_by_index(3).unwrap(), Value::Number(77.0)));
    assert_eq!(
        sv.get_by_index(5).unwrap().as_str_bytes(),
        Some(&b"five"[..])
    );
    assert_eq!(
        sv.get_by_index(6).unwrap().as_str_bytes(),
        Some(&b"six"[..])
    );
    assert!(sv.get_by_index(7).unwrap().is_null_static());
    assert!(matches!(**sv.get_by_index(32766).unwrap(), Value::Number(n) if n == 32766.5));
    for absent in [0, 4, 8, 16, 18, 399, 401, 32765, 40000] {
        assert!(sv.get_by_index(absent).is_none(), "{absent}");
    }
    // An index no schema can have is rejected rather than stored.
    assert!(
        sv.try_insert_num(sorting_vector::RS_SORTABLES_MAX, 1.0)
            .is_err()
    );
}

#[test]
fn dense_get_by_index_is_the_slot() {
    let sv = build_vector().unwrap();
    assert!(matches!(**sv.get_by_index(0).unwrap(), Value::Number(42.0)));
    assert!(sv.get_by_index(3).unwrap().is_null_static());
    assert!(sv.get_by_index(4).is_none());
}

#[test]
fn sparse_matches_dense_for_every_index() {
    for (n, k) in [(2048, 1), (2048, 100), (4096, 1500), (32767, 700), (64, 64)] {
        let chosen = filled_indexes(n, k, k as u64);
        let mut dense = RSSortingVector::new(n);
        let mut sparse = RSSortingVector::new_sparse(k);
        // Insert in reverse so the sparse vector sorts on the way in.
        for &idx in chosen.iter().rev() {
            dense.try_insert_num(idx, idx as f64 * 2.0).unwrap();
            sparse.try_insert_num(idx, idx as f64 * 2.0).unwrap();
        }
        for idx in 0..n {
            let d = dense.get_by_index(idx).filter(|v| !v.is_null_static());
            let s = sparse.get_by_index(idx).filter(|v| !v.is_null_static());
            assert_eq!(
                d.map(|v| v.as_num()),
                s.map(|v| v.as_num()),
                "n={n} k={k} idx={idx}"
            );
        }
    }
}

#[test]
fn sparse_memory_size_follows_the_entries() {
    let mut sv = RSSortingVector::new_sparse(0);
    let empty = sv.get_memory_size();
    assert_eq!(empty, std::mem::size_of::<SharedValue>());
    sv.try_insert_num(10, 1.0).unwrap();
    sv.try_insert_string(20, b"abc".to_vec()).unwrap();
    sv.try_insert_null(30).unwrap();
    // Two slots per entry, plus what the owned values take; a null owns nothing.
    let expected =
        empty + 3 * 2 * std::mem::size_of::<SharedValue>() + 2 * SHARED_VALUE_CONTENT_SIZE + 3;
    assert_eq!(sv.get_memory_size(), expected);

    let dense = RSSortingVector::new(1000);
    assert_eq!(
        dense.get_memory_size(),
        1000 * std::mem::size_of::<SharedValue>()
    );
}

#[test]
fn sparse_clone_and_reset() {
    let mut sv = RSSortingVector::new_sparse(2);
    sv.try_insert_num(1, 1.0).unwrap();
    sv.try_insert_num(2, 2.0).unwrap();
    let copy = sv.clone();
    assert!(copy.is_sparse());
    assert_eq!(copy.sparse_entries().count(), 2);
    sv.reset();
    assert!(!sv.is_sparse());
    assert!(sv.is_empty());
    assert_eq!(copy.get_by_index(2).unwrap().as_num(), Some(2.0));
}

#[test]
fn sparse_vectors_can_be_used_from_other_threads() {
    let handles: Vec<_> = (0..4)
        .map(|t| {
            std::thread::spawn(move || {
                let mut sv = RSSortingVector::new_sparse(0);
                for idx in (t..2000).step_by(4) {
                    sv.try_insert_num(idx, idx as f64).unwrap();
                }
                assert_eq!(sv.sparse_entries().count(), 500);
                sv
            })
        })
        .collect();
    for handle in handles {
        let sv = handle.join().unwrap();
        assert_eq!(sv.sparse_entries().count(), 500);
    }
}
