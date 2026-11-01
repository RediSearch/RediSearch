/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ffi::{CString, c_char},
    io::Cursor,
    mem,
    ops::ControlFlow,
    ptr,
};

use c_trie::{SuffixMode, SuffixTrie, TrieTerm};
use dict::{KeysDictType, OwnedDict};
use ffi::IndexFlags_Index_DocIdsOnly;
use index_gc::{
    DocExistence, GcError,
    scanners::terms::{
        TermEntry, TermsCursor, apply_terms, apply_terms_with, collect_terms, receive_terms,
        scan_term, scan_terms_after, snapshot_term,
    },
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{DocId, DocLiveness, GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
use index_gc::fork::wire::{deserialize, serialize};
// Provide Redis allocator shims so the C dict/trie functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

fn trie_term(term: &[u8]) -> TrieTerm {
    assert!(!term.is_empty());
    // SAFETY: all callers pass complete, non-empty ASCII terms, which satisfy the
    // trie's validity invariants.
    unsafe { TrieTerm::from_bytes_unchecked(Box::from(term)) }
}

struct TestSpec {
    spec: ffi::IndexSpec,
    _keys_dict: OwnedDict<KeysDictType>,
}

impl TestSpec {
    // Each term gets a matching `DocIdsOnly` inverted index with docs
    // `1..=doc_count`, registered under the term in the keys dict.
    fn create(terms: impl IntoIterator<Item = (&'static [u8], u64)>) -> Self {
        let keys_dict = OwnedDict::<KeysDictType>::create();

        // SAFETY: a zeroed IndexSpec is valid for the fields this fixture sets below;
        // every other field is left zero/null and unused by the functions under test.
        let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
        spec.flags = IndexFlags_Index_DocIdsOnly;
        // SAFETY: `NewHiddenString` copies the static string because `takeOwnership` is true.
        spec.specName =
            unsafe { ffi::NewHiddenString(c"idx".as_ptr(), c"idx".count_bytes(), true) };
        spec.obfuscatedName = CString::new("Index@test").unwrap().into_raw();
        // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie; a terms trie stores no
        // payload, so a null free callback is correct.
        spec.terms = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
        spec.keysDict = keys_dict.as_mut_ptr();

        let mut test_spec = Self {
            spec,
            _keys_dict: keys_dict,
        };
        for (term, doc_count) in terms {
            test_spec.add_term(term, doc_count);
        }

        test_spec
    }

    fn add_term(&mut self, term: &[u8], doc_count: u64) {
        self.insert_trie_term(term, doc_count);

        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=doc_count {
            ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                .unwrap();
        }
        self._keys_dict
            .try_insert(term, Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)))
            .unwrap_or_else(|_| panic!("term already present in keysDict"));
    }

    fn delete_trie_term(&mut self, term: &[u8]) {
        // SAFETY: `self.spec.terms` is live and `term` points to `term.len()` valid bytes.
        let deleted = unsafe {
            ffi::Trie_Delete(self.spec.terms, term.as_ptr().cast::<c_char>(), term.len())
        };
        assert_eq!(deleted, 1, "the term was in the trie");
    }

    fn insert_trie_term(&mut self, term: &[u8], doc_count: u64) {
        // SAFETY: `self.spec.terms` is live; `term` points to `term.len()` valid bytes;
        // a null payload is accepted by the terms trie.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                self.spec.terms,
                term.as_ptr().cast::<c_char>(),
                term.len(),
                1.0,
                0,
                ptr::null_mut(),
                doc_count as usize,
            );
        }
    }

    /// The unique id of `term`'s inverted index, which an apply must quote.
    fn unique_id(&self, term: &[u8]) -> u32 {
        u32::from(
            self._keys_dict
                .fetch(term)
                .expect("the term is indexed")
                .unique_id(),
        )
    }

    fn read_guard(&self) -> mem::ManuallyDrop<IndexSpecReadGuard<'_>> {
        // SAFETY: the fixture owns `spec` for the duration of the guard and does not
        // mutate it while the guard is held.
        unsafe { IndexSpecReadGuard::from_locked(&self.spec) }
    }

    fn collect(&self) -> Vec<u8> {
        let guard = self.read_guard();
        let mut buf = Vec::new();
        collect_terms(&mut buf, &guard, &DocExistence::snapshot(&guard)).unwrap();
        buf
    }

    fn add_suffix_trie(&mut self, terms: impl IntoIterator<Item = &'static [u8]>) {
        // SAFETY: `NewTrie` returns a fresh, valid trie; `suffixTrie_freeCallback` is
        // the matching free callback for the payloads `addSuffixTrie` inserts.
        self.spec.suffix = unsafe {
            ffi::NewTrie(
                Some(ffi::suffixTrie_freeCallback),
                ffi::TrieSortMode_Trie_Sort_Lex,
            )
        };
        for term in terms {
            assert!(!term.is_empty(), "addSuffixTrie rejects empty strings");
            // SAFETY: `self.spec.suffix` is live, and `term` points to `term.len()`
            // valid, non-empty bytes.
            unsafe {
                ffi::addSuffixTrie(
                    self.spec.suffix,
                    term.as_ptr().cast::<c_char>(),
                    term.len() as u32,
                )
            };
        }
    }

    fn suffix_matches(&self, pattern: &str) -> Vec<String> {
        // SAFETY: `self.spec.suffix` is a live suffix trie owned by this fixture and
        // is not mutated for the duration of this borrow.
        let trie = unsafe { SuffixTrie::from_raw(self.spec.suffix) };

        let mut runes = vec![0 as ffi::rune; pattern.len() + 1];
        // SAFETY: `pattern` is valid UTF-8 of `pattern.len()` bytes, so the decode
        // stays within the slice, and `runes` has sufficient capacity.
        let rlen = unsafe {
            ffi::strToRunes(
                pattern.as_ptr().cast::<c_char>(),
                pattern.len(),
                runes.as_mut_ptr(),
                runes.len(),
            )
        };
        runes.truncate(rlen);

        let mut found = Vec::new();
        trie.iterate_contains(&runes, SuffixMode::Suffix, |term| {
            found.push(String::from_utf8_lossy(term).into_owned());
            ControlFlow::Continue(())
        });
        found.sort();
        found
    }

    const fn write(&mut self) -> mem::ManuallyDrop<IndexSpecWriteGuard<'_>> {
        // SAFETY: each test owns the fixture exclusively and accesses it only through
        // the returned guard until that guard goes out of scope.
        unsafe { IndexSpecWriteGuard::from_locked_mut(&mut self.spec) }
    }
}

impl Drop for TestSpec {
    fn drop(&mut self) {
        // SAFETY: this fixture owns the terms trie allocated by `create`.
        unsafe { ffi::TrieType_Free(self.spec.terms.cast()) };
        if !self.spec.suffix.is_null() {
            // SAFETY: this fixture owns the trie allocated by `add_suffix_trie`.
            unsafe { ffi::TrieType_Free(self.spec.suffix.cast()) };
        }
        // SAFETY: `create` allocated this hidden string with the Redis allocator.
        unsafe { ffi::HiddenString_Free(self.spec.specName, true) };
        // SAFETY: `create` transferred this CString's ownership with `into_raw`.
        drop(unsafe { CString::from_raw(self.spec.obfuscatedName) });
    }
}

fn assert_only_terminator(buf: &[u8]) {
    let mut cursor = Cursor::new(buf);
    assert!(
        deserialize::<Option<TermEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn empty_trie_writes_only_terminator() {
    let spec = TestSpec::create([]);
    assert_only_terminator(&spec.collect());
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn term_with_empty_inverted_index_is_skipped() {
    let spec = TestSpec::create([(&b"empty"[..], 0)]);
    assert_only_terminator(&spec.collect());
}

/// A term present in the trie but with no registered inverted index (never opened for
/// write) is skipped: `Redis_OpenInvertedIndex` returns null for it.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn term_without_inverted_index_is_skipped() {
    let mut spec = TestSpec::create([]);
    spec.insert_trie_term(b"orphan", 1);
    assert_only_terminator(&spec.collect());
}

/// Multiple terms each produce their own serialised entry, followed by `None`.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn multiple_terms_write_multiple_delta_frames() {
    let spec = TestSpec::create([(&b"apple"[..], 1), (&b"grape"[..], 1)]);
    let buf = spec.collect();

    let mut cursor = Cursor::new(&buf);

    let first = deserialize::<Option<TermEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();
    let second = deserialize::<Option<TermEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();

    let mut terms = [first.term.into_vec(), second.term.into_vec()];
    terms.sort(); // Entries can come in any order because trie iteration order isn't asserted here.

    assert_eq!(terms, [b"apple".to_vec(), b"grape".to_vec()]);
    assert!(
        deserialize::<Option<TermEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_none_returns_none() {
    let mut buf = Vec::new();
    serialize(&mut buf, Option::<TermEntry<&[u8]>>::None).unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(receive_terms(&mut cursor).unwrap().is_none());
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_malformed_entry_returns_codec_error() {
    let mut empty_frame = Vec::new();
    index_gc::Frame::Empty.encode(&mut empty_frame).unwrap();
    let mut terminator = Vec::new();
    index_gc::Frame::Terminator.encode(&mut terminator).unwrap();
    for bytes in [b"garbage".to_vec(), empty_frame, terminator] {
        assert!(matches!(
            receive_terms(&mut Cursor::new(bytes)),
            Err(GcError::Codec {
                msg: "decoding terms entry",
                ..
            })
        ));
    }
}

#[test]
fn receive_entry_returns_term_and_delta() {
    let mut buf = Vec::new();
    serialize(
        &mut buf,
        Some(TermEntry {
            term: b"apple".as_slice(),
            unique_id: 7,
            delta: GcScanDelta::empty_for_testing(),
        }),
    )
    .unwrap();

    let mut cursor = Cursor::new(&buf);
    let entry = receive_terms(&mut cursor).unwrap().unwrap();
    assert_eq!(&*entry.term, b"apple");
    assert_eq!(entry.unique_id, 7);
    assert_eq!(entry.delta, GcScanDelta::empty_for_testing());
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_returns_err_when_term_not_found() {
    let mut spec = TestSpec::create([]);
    let delta = GcScanDelta::empty_for_testing();

    {
        let mut write_guard = spec.write();
        assert!(matches!(
            apply_terms(&trie_term(b"nonexistent"), 0, delta, &mut write_guard),
            Err(GcError::ApplyError(
                "the term's inverted index was removed before the delta could be applied"
            ))
        ));
    }
}

/// A delta scanned from an index the term no longer holds, because the term was
/// dropped and re-added in between, is not applied to the replacement.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_returns_err_when_the_index_was_replaced() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)]);
    let scanned = spec.unique_id(b"apple") + 1;
    let delta = GcScanDelta::empty_for_testing();

    {
        let mut write_guard = spec.write();
        assert!(matches!(
            apply_terms(&trie_term(b"apple"), scanned, delta, &mut write_guard),
            Err(GcError::ApplyError(
                "the term's inverted index is not the one scanned"
            ))
        ));
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_some());
    }
}

/// A successful apply with a no-op delta leaves the term's entry in place.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_succeeds_and_keeps_entry_when_docs_remain() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)]);
    let unique_id = spec.unique_id(b"apple");
    let delta = GcScanDelta::empty_for_testing();

    {
        let mut write_guard = spec.write();
        let stats = apply_terms(&trie_term(b"apple"), unique_id, delta, &mut write_guard).unwrap();

        assert_eq!(stats.records_removed, 0);
        assert_eq!(stats.terms_removed, 0);
        assert_eq!(stats.terms_size_removed, 0);
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_some());
    }
}

/// Full child-to-parent roundtrip: when all docs are deleted, the term's entry is
/// removed from the keys dict and trie, and its statistics are returned to the caller.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn roundtrip_all_docs_deleted_removes_term() {
    let mut spec = TestSpec::create([(&b"apple"[..], 2)]);

    // Child side: collect.
    let buf = spec.collect();

    // Parent side: receive.
    let mut cursor = Cursor::new(&buf);
    let entry = receive_terms(&mut cursor).unwrap().unwrap();
    assert_eq!(&*entry.term, b"apple");

    // Parent side: apply.
    {
        let mut write_guard = spec.write();
        let stats =
            apply_terms(&entry.term, entry.unique_id, entry.delta, &mut write_guard).unwrap();

        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
        assert!(stats.bytes_collected > 0);
        assert_eq!(
            write_guard.terms_mut().num_docs(b"apple"),
            0,
            "the term is gone from the terms trie too"
        );
        assert_eq!(stats.terms_removed, 1);
        assert_eq!(stats.terms_size_removed, entry.term.len());
    }
}

/// A collected term is removed from the suffix trie along with the terms trie, but
/// only the terms that were actually collected.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_removes_the_collected_term_from_the_suffix_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0), (&b"maple"[..], 1)]);
    spec.add_suffix_trie([&b"apple"[..], &b"maple"[..]]);

    assert_eq!(spec.suffix_matches("ple"), ["apple", "maple"]);

    let unique_id = spec.unique_id(b"apple");
    {
        let mut write_guard = spec.write();
        apply_terms(
            &trie_term(b"apple"),
            unique_id,
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
    }

    assert_eq!(
        spec.suffix_matches("ple"),
        ["maple"],
        "only the removed term is dropped from the suffix trie"
    );
}

/// A spec with no `WITHSUFFIXTRIE` field has a null suffix trie; removal must skip it
/// rather than dereference it.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_removes_an_empty_term_without_a_suffix_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0)]);
    assert!(
        spec.spec.suffix.is_null(),
        "no field opted into a suffix trie"
    );

    let unique_id = spec.unique_id(b"apple");
    {
        let mut write_guard = spec.write();
        apply_terms(
            &trie_term(b"apple"),
            unique_id,
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
    }
}

/// The trie and the keys dict can disagree if a term was already dropped from the
/// trie. Removal logs a warning and still completes the rest of the bookkeeping.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_survives_an_empty_term_missing_from_the_trie() {
    let mut spec = TestSpec::create([(&b"apple"[..], 0)]);

    let unique_id = spec.unique_id(b"apple");
    {
        let mut write_guard = spec.write();
        let removed = write_guard.terms_mut().delete(&trie_term(b"apple"));
        assert!(
            removed,
            "drop the term from the trie only, leaving it in the keys dict"
        );

        let stats = apply_terms(
            &trie_term(b"apple"),
            unique_id,
            GcScanDelta::empty_for_testing(),
            &mut write_guard,
        )
        .unwrap();
        assert_eq!(stats.terms_removed, 1);
        assert_eq!(stats.terms_size_removed, b"apple".len());
        assert!(write_guard.keys_dict_mut().fetch_mut(b"apple").is_none());
    }
}

#[test]
fn receive_empty_term_returns_codec_error() {
    let mut bytes = Vec::new();
    serialize(
        &mut bytes,
        Some(TermEntry {
            term: b"".as_slice(),
            unique_id: 0,
            delta: GcScanDelta::empty_for_testing(),
        }),
    )
    .unwrap();

    assert!(matches!(
        receive_terms(&mut Cursor::new(bytes)),
        Err(GcError::Codec { .. })
    ));
}

/// Reports every doc id the fixtures use as present, whatever the spec's
/// document table says, as a snapshot taken before those docs were deleted.
fn all_live<'a>(guard: &'a IndexSpecReadGuard<'a>) -> DocExistence<'a> {
    const WORDS: usize = 1 << 14;
    DocExistence::with_bitmap(
        guard,
        vec![u64::MAX; WORDS].into_boxed_slice(),
        (WORDS * 64 - 1) as u64,
    )
}

/// The scanner must answer existence from `docs`: the fixture's document table
/// is empty, so a scanner that looked docs up itself would collect them all.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn docs_live_in_the_snapshot_are_kept() {
    let spec = TestSpec::create([(&b"apple"[..], 3)]);
    let guard = spec.read_guard();
    let mut buf = Vec::new();
    collect_terms(&mut buf, &guard, &all_live(&guard)).unwrap();
    assert_only_terminator(&buf);
}

/// `scan_term` finds the same delta `collect_terms` sends for that term.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn scan_term_matches_the_collected_entry() {
    let spec = TestSpec::create([(&b"apple"[..], 3)]);
    let buf = spec.collect();
    let collected = deserialize::<Option<TermEntry>>(&mut Cursor::new(&buf), "test message")
        .unwrap()
        .expect("the term has docs to collect");

    let guard = spec.read_guard();
    assert_eq!(
        scan_term(&guard, &trie_term(b"apple"), |id| guard.doc_exists(id)),
        Some((collected.unique_id, collected.delta))
    );
}

/// A term with no inverted index has nothing to scan.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn scan_term_returns_none_without_an_inverted_index() {
    let mut spec = TestSpec::create([]);
    spec.insert_trie_term(b"orphan", 1);
    let guard = spec.read_guard();
    assert_eq!(
        scan_term(&guard, &trie_term(b"orphan"), |id| guard.doc_exists(id)),
        None
    );
}

/// An empty inverted index has nothing to collect.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn scan_term_returns_none_for_an_empty_inverted_index() {
    let spec = TestSpec::create([(&b"empty"[..], 0)]);
    let guard = spec.read_guard();
    assert_eq!(
        scan_term(&guard, &trie_term(b"empty"), |id| guard.doc_exists(id)),
        None
    );
}

/// A resumable terms scan that stops after every term finds what the collector
/// sends, one term per call, and reports terms left until the last one.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn a_terms_scan_resumes_after_the_last_term_visited() {
    let spec = TestSpec::create([(&b"grape"[..], 1), (&b"apple"[..], 2), (&b"empty"[..], 0)]);
    let buf = spec.collect();
    let mut cursor_in = Cursor::new(&buf);
    let mut collected = Vec::new();
    while let Some(entry) =
        deserialize::<Option<TermEntry>>(&mut cursor_in, "test message").unwrap()
    {
        collected.push((entry.term.into_vec(), entry.delta));
    }

    let guard = spec.read_guard();
    let mut cursor = TermsCursor::default();
    let mut found = Vec::new();
    let mut calls = 0;
    while scan_terms_after(
        &guard,
        &mut cursor,
        |id| guard.doc_exists(id),
        || true,
        &mut found,
    ) {
        calls += 1;
        assert!(calls <= 3, "the scan must make progress");
    }

    let found: Vec<_> = found
        .into_iter()
        .filter_map(|(term, snapshot)| {
            let delta = snapshot.scan_gc(|id| guard.doc_exists(id)).ok().flatten()?;
            Some((term.into_bytes().into_vec(), delta))
        })
        .collect();
    assert_eq!(found, collected);
}

/// A trie that changed between two chunks: the scan resumes strictly after the
/// last term visited, so that term is not visited again even if it was
/// deleted and reinserted, a term added before it waits for the next cycle,
/// and every term after it, old or new, is visited once.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn a_terms_scan_resumes_after_its_cursor_in_a_changed_trie() {
    let mut spec = TestSpec::create([(&b"bee"[..], 1), (&b"dog"[..], 1), (&b"fox"[..], 1)]);
    let mut cursor = TermsCursor::default();
    let mut found = Vec::new();
    // The fixture's doc table is empty: every term's docs are gone.
    let gone = |_: DocId| false;
    assert!(scan_terms_after(
        &spec.read_guard(),
        &mut cursor,
        gone,
        || true,
        &mut found
    ));

    spec.delete_trie_term(b"bee");
    spec.insert_trie_term(b"bee", 1);
    spec.add_term(b"ant", 1);
    spec.add_term(b"cat", 1);
    spec.add_term(b"eel", 1);
    while scan_terms_after(&spec.read_guard(), &mut cursor, gone, || false, &mut found) {}

    let found: Vec<_> = found
        .into_iter()
        .map(|(term, _)| term.into_bytes().into_vec())
        .collect();
    assert_eq!(
        found,
        [&b"bee"[..], b"cat", b"dog", b"eel", b"fox"].map(<[u8]>::to_vec)
    );
}

/// Append a live record to `term`'s index, as a writer would between a scan and its apply.
fn append_doc(spec: &mut TestSpec, term: &[u8], doc_id: DocId) {
    let Some(OpaqueInvertedIndex::DocIdsOnly(ii)) = spec._keys_dict.fetch_mut(term) else {
        panic!("the term must be indexed as a doc-ids-only index");
    };
    ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
        .unwrap();
}

/// A delta whose last block grew since the scan is rescanned when the apply has a
/// liveness source, and denied when it has none.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn apply_with_a_liveness_source_rescans_a_grown_last_block() {
    // Docs 1 and 2 are gone; the one the writer appends is live.
    let exists = |id: DocId| id > 2;
    for (live, denied, removed) in [(Some(&exists as &dyn DocLiveness), 0, 2), (None, 1, 0)] {
        let mut spec = TestSpec::create([(&b"apple"[..], 2)]);
        let (unique_id, delta) = scan_term(&spec.read_guard(), &trie_term(b"apple"), exists)
            .expect("both docs are gone");
        append_doc(&mut spec, b"apple", 3);

        let mut guard = spec.write();
        let stats =
            apply_terms_with(&trie_term(b"apple"), unique_id, delta, live, &mut guard).unwrap();

        assert_eq!(stats.blocks_denied, denied);
        assert_eq!(stats.records_removed, removed);
        assert!(
            guard.keys_dict_mut().fetch_mut(b"apple").is_some(),
            "doc 3 is live, so the term stays"
        );
    }
}

/// A liveness source that rules whole ID ranges clean, as the in-process GC's does.
#[derive(Clone, Copy)]
struct Gone(&'static [DocId]);

impl DocLiveness for Gone {
    fn exists(&self, id: DocId) -> bool {
        !self.0.contains(&id)
    }

    fn any_gone(&self, first: DocId, last: DocId) -> bool {
        self.0.iter().any(|id| (first..=last).contains(id))
    }
}

/// A snapshot taken with a range-aware source, which copies only the blocks that
/// source finds dirty, scans to the same delta as the live index.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `invIdxDictType`")]
fn a_snapshot_with_a_range_aware_source_scans_like_the_live_index() {
    let spec = TestSpec::create([(&b"apple"[..], 300)]);
    let guard = spec.read_guard();
    let term = trie_term(b"apple");
    let gone = Gone(&[250]);

    let snapshot = snapshot_term(&guard, &term, &gone).expect("doc 250 is gone");
    let (_, live_delta) = scan_term(&guard, &term, gone).expect("doc 250 is gone");

    assert_eq!(snapshot.scan_gc(gone).unwrap(), Some(live_delta));
    assert!(snapshot_term(&guard, &term, &Gone(&[])).is_none());
}
