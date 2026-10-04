/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use value::{
    SharedBuffer,
    shared_buffer::{MAX_SHARED_OFFSET, RESERVED_PREFIX},
};

/// A buffer holding `bytes`, from the allocator the Redis module allocator is mocked by.
fn allocate(bytes: &[u8]) -> (NonNull<u8>, usize) {
    let buffer = redis_mock::allocator::alloc_shim(bytes.len().max(1)).cast::<u8>();
    let buffer = NonNull::new(buffer).unwrap();
    // SAFETY: the allocation is at least `bytes.len()` long and fresh.
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.as_ptr(), bytes.len()) };
    (buffer, bytes.len())
}

/// # Safety
///
/// 1. `buffer` must come from [`allocate`] and not be freed since.
unsafe fn release(buffer: NonNull<u8>, _len: usize) {
    redis_mock::allocator::free_shim(buffer.as_ptr().cast());
}

/// A shared buffer over `bytes` whose live bytes are counted by the returned counter.
fn shared(bytes: &[u8]) -> (SharedBuffer, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let (ptr, len) = allocate(bytes);
    // SAFETY: a fresh allocation nothing else refers to, which `release` frees.
    let buffer = unsafe { SharedBuffer::from_raw(ptr, len, release, Some(live.clone())) }
        .expect("long enough to share");
    (buffer, live)
}

/// The bytes of a buffer whose `RESERVED_PREFIX` is followed by `body`.
fn with_prefix(body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0xee; RESERVED_PREFIX];
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn a_buffer_too_short_for_its_prefix_is_not_taken() {
    let (ptr, len) = allocate(&[1, 2, 3]);
    // SAFETY: a fresh allocation nothing else refers to, which `release` frees.
    assert!(unsafe { SharedBuffer::from_raw(ptr, len, release, None) }.is_none());
    // SAFETY: refused, so still the caller's to free.
    unsafe { release(ptr, len) };
}

#[test]
fn a_shared_string_reads_its_bytes_in_place() {
    let (buffer, _) = shared(&with_prefix(b"hello\0"));
    let string = buffer.share(RESERVED_PREFIX, 5).expect("shareable");
    assert!(string.is_shared());
    assert_eq!(string.as_bytes(), b"hello");
    assert_eq!(
        string.as_ptr_len().0.cast::<u8>(),
        buffer.as_bytes()[RESERVED_PREFIX..].as_ptr(),
        "not a copy"
    );
}

#[test]
fn the_buffer_lives_until_its_last_string_is_dropped() {
    let bytes = with_prefix(b"ab\0cd\0");
    let (buffer, live) = shared(&bytes);
    let first = buffer.share(RESERVED_PREFIX, 2).unwrap();
    let second = buffer.share(RESERVED_PREFIX + 3, 2).unwrap();
    assert_eq!(live.load(Ordering::Relaxed), bytes.len());

    drop(buffer);
    drop(first);
    assert_eq!(second.as_bytes(), b"cd");
    assert_eq!(live.load(Ordering::Relaxed), bytes.len());

    drop(second);
    assert_eq!(
        live.load(Ordering::Relaxed),
        0,
        "freed with the last reference"
    );
}

#[test]
fn a_string_without_a_terminator_is_not_shareable() {
    let (buffer, _) = shared(&with_prefix(b"abc"));
    assert!(
        buffer.share(RESERVED_PREFIX, 2).is_none(),
        "no NUL after it"
    );
    assert!(
        buffer.share(RESERVED_PREFIX, 3).is_none(),
        "the NUL would be past the end"
    );
}

#[test]
fn a_string_outside_the_buffer_or_in_its_prefix_is_not_shareable() {
    let (buffer, _) = shared(&with_prefix(b"a\0"));
    assert!(buffer.share(0, 0).is_none(), "inside the prefix");
    assert!(
        buffer.share(RESERVED_PREFIX - 1, 1).is_none(),
        "overlapping the prefix"
    );
    assert!(
        buffer.share(RESERVED_PREFIX + 2, 0).is_none(),
        "past the end"
    );
    assert!(buffer.share(usize::MAX, u32::MAX).is_none(), "overflowing");
    assert!(
        buffer.share(RESERVED_PREFIX + 1, 0).is_some(),
        "an empty string at the NUL"
    );
}

#[test]
#[cfg_attr(miri, ignore = "allocates past the shareable offset")]
fn a_string_past_the_shareable_offset_is_not_shareable() {
    let mut bytes = vec![0u8; MAX_SHARED_OFFSET + 2];
    bytes[MAX_SHARED_OFFSET + 1] = 0;
    let (buffer, _) = shared(&bytes);
    assert!(buffer.share(MAX_SHARED_OFFSET, 0).is_some());
    assert!(buffer.share(MAX_SHARED_OFFSET + 1, 0).is_none());
}

#[test]
fn strings_are_released_from_any_thread() {
    let bytes = with_prefix(b"x\0");
    let (buffer, live) = shared(&bytes);
    let strings: Vec<_> = (0..8)
        .map(|_| buffer.share(RESERVED_PREFIX, 1).unwrap())
        .collect();
    drop(buffer);

    let threads: Vec<_> = strings
        .into_iter()
        .map(|string| std::thread::spawn(move || assert_eq!(string.as_bytes(), b"x")))
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(live.load(Ordering::Relaxed), 0);
}
