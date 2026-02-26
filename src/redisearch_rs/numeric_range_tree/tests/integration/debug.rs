/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot tests for `numeric_range_tree::debug` module.

use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::debug;
use redis_mock::reply::{ReplyValue, capture_replies};

fn mock_ctx() -> *mut redis_reply::RedisModuleCtx {
    redis_mock::init_redis_module_mock();
    std::ptr::dangling_mut::<redis_reply::RedisModuleCtx>()
}

fn capture_single_reply(f: impl FnOnce()) -> ReplyValue {
    let mut replies = capture_replies(f);
    assert_eq!(
        replies.len(),
        1,
        "expected single reply, got {}",
        replies.len()
    );
    replies.pop().unwrap()
}

/// Run a snapshot assertion with nondeterministic fields redacted.
///
/// `uniqueId` comes from a global `AtomicU32` counter and varies across runs.
fn with_redactions(f: impl FnOnce()) {
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(r#""uniqueId": \d+"#, r#""uniqueId": "[redacted]""#);
    settings.bind(f);
}

/// Helper: create a tree and add `count` entries with doc_ids 1..=count and
/// values `start`, `start + step`, `start + 2*step`, ...
fn populated_tree(count: u64, start: f64, step: f64, compress_floats: bool) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=count {
        tree.add(i, start + (i - 1) as f64 * step, false, 0);
    }
    tree
}

/// Helper: create a tree with multi-valued entries.
fn populated_multivalued_tree(count: u64) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=count {
        for j in 0..3 {
            tree.add(i, (i + j) as f64, true, 0);
        }
    }
    tree
}

// ── debug_summary ──────────────────────────────────────────────────────

#[test]
fn test_debug_summary_empty() {
    let tree = NumericRangeTree::new(false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_summary(ctx, Some(&tree)) });
    insta::assert_debug_snapshot!(reply, @r###"
    [
      "numRanges",
      1,
      "numLeaves",
      1,
      "numEntries",
      0,
      "lastDocId",
      0,
      "revisionId",
      0,
      "emptyLeaves",
      1,
      "RootMaxDepth",
      0,
      "MemoryUsage",
      848
    ]
    "###);
}

#[test]
fn test_debug_summary_populated() {
    let tree = populated_tree(50, 0.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_summary(ctx, Some(&tree)) });
    insta::assert_debug_snapshot!(reply, @r###"
    [
      "numRanges",
      2,
      "numLeaves",
      2,
      "numEntries",
      50,
      "lastDocId",
      50,
      "revisionId",
      1,
      "emptyLeaves",
      0,
      "RootMaxDepth",
      1,
      "MemoryUsage",
      1155
    ]
    "###);
}

// ── debug_dump_index ───────────────────────────────────────────────────

#[test]
fn test_debug_dump_index_no_headers() {
    let tree = populated_tree(10, 1.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply =
        capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), false) });
    insta::assert_debug_snapshot!(reply, @"[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]");
}

#[test]
fn test_debug_dump_index_with_headers() {
    let tree = populated_tree(10, 1.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), true) });
    insta::assert_debug_snapshot!(reply, @r#"
    [
      [
        [
          "numDocs",
          10,
          "numEntries",
          10,
          "lastId",
          10,
          "flags",
          32,
          "numberOfBlocks",
          1,
          "blocks_efficiency (numEntries/numberOfBlocks)",
          10
        ],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      ]
    ]
    "#);
}

#[test]
fn test_debug_dump_index_with_multivalued_tree() {
    let tree = populated_multivalued_tree(5);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply =
        capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), false) });
    insta::assert_debug_snapshot!(reply, @"[[1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5]]");
}

#[test]
fn test_debug_dump_index_no_headers_compressed() {
    let tree = populated_tree(10, 1.0, 1.0, true);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply =
        capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), false) });
    insta::assert_debug_snapshot!(reply, @"[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]");
}

#[test]
fn test_debug_dump_index_with_headers_compressed() {
    let tree = populated_tree(10, 1.0, 1.0, true);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), true) });
    insta::assert_debug_snapshot!(reply);
}

// ── debug_dump_tree ────────────────────────────────────────────────────

#[test]
fn test_debug_dump_tree_full() {
    let tree = populated_tree(10, 1.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), false) });
    with_redactions(|| insta::assert_debug_snapshot!(reply));
}

#[test]
fn test_debug_dump_tree_minimal() {
    let tree = populated_tree(10, 1.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), true) });
    with_redactions(|| {
        insta::assert_debug_snapshot!(reply, @r###"
        {
          "numRanges": 1,
          "numEntries": 10,
          "lastDocId": 10,
          "revisionId": 0,
          "uniqueId": "[redacted]",
          "emptyLeaves": 0,
          "root": {"range": []},
          "Tree stats": {"Average memory efficiency (numEntries/size)/numRanges": 0}
        }
        "###);
    });
}

#[test]
fn test_debug_dump_tree_with_children() {
    // Insert 100 distinct values to force splits and create internal nodes
    // with left/right children, covering the full recursive reply_node_debug path.
    let tree = populated_tree(100, 0.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), false) });
    with_redactions(|| insta::assert_debug_snapshot!(reply));
}

#[test]
fn test_debug_dump_tree_full_compressed() {
    let tree = populated_tree(10, 1.0, 1.0, true);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), false) });
    with_redactions(|| insta::assert_debug_snapshot!(reply));
}

#[test]
fn test_debug_dump_tree_with_children_compressed() {
    let tree = populated_tree(100, 0.0, 1.0, true);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), false) });
    with_redactions(|| insta::assert_debug_snapshot!(reply));
}

// ── debug_dump_index with internal nodes (splits) ──────────────────────

#[test]
fn test_debug_dump_index_with_splits() {
    // Insert 100 distinct values to force splits, creating internal nodes.
    // Internal nodes without ranges are skipped during dump_index.
    let tree = populated_tree(100, 0.0, 1.0, false);
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, Some(&tree), true) });
    with_redactions(|| insta::assert_debug_snapshot!(reply));
}

// ── None (empty index) snapshots ────────────────────────────────────────

#[test]
fn test_debug_summary_none() {
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_summary(ctx, None) });
    insta::assert_debug_snapshot!(reply, @r###"
    [
      "numRanges",
      0,
      "numLeaves",
      0,
      "numEntries",
      0,
      "lastDocId",
      0,
      "revisionId",
      0,
      "emptyLeaves",
      0,
      "RootMaxDepth",
      0,
      "MemoryUsage",
      0
    ]
    "###);
}

#[test]
fn test_debug_dump_index_none() {
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_index(ctx, None, false) });
    insta::assert_debug_snapshot!(reply, @"[]");
}

#[test]
fn test_debug_dump_tree_none() {
    let ctx = mock_ctx();
    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, None, true) });
    insta::assert_debug_snapshot!(reply, @r###"
    {
      "numRanges": 0,
      "numEntries": 0,
      "lastDocId": 0,
      "revisionId": 0,
      "uniqueId": 0,
      "emptyLeaves": 0,
      "root": {},
      "Tree stats": {"Average memory efficiency (numEntries/size)/numRanges": 0}
    }
    "###);
}

// ── Structural consistency: None has same keys as default tree ──────────

#[test]
fn test_debug_summary_none_has_same_keys_as_default() {
    let ctx = mock_ctx();
    let tree = NumericRangeTree::new(false);

    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply_default = capture_single_reply(|| unsafe { debug::debug_summary(ctx, Some(&tree)) });
    let reply_none = capture_single_reply(|| unsafe { debug::debug_summary(ctx, None) });

    // Extract keys (string elements at even indices in the flat array).
    fn keys(reply: &ReplyValue) -> Vec<&str> {
        match reply {
            ReplyValue::Array(arr) => arr
                .iter()
                .step_by(2)
                .map(|v| match v {
                    ReplyValue::SimpleString(s) => s.as_str(),
                    other => panic!("expected string key, got {other:?}"),
                })
                .collect(),
            other => panic!("expected array, got {other:?}"),
        }
    }
    assert_eq!(keys(&reply_default), keys(&reply_none));
}

#[test]
fn test_debug_dump_tree_none_has_same_keys_as_default() {
    let ctx = mock_ctx();
    let tree = NumericRangeTree::new(false);

    // SAFETY: `ctx` is a mock pointer — `redis_mock` intercepts all Redis module API calls.
    let reply_default =
        capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, Some(&tree), true) });
    let reply_none = capture_single_reply(|| unsafe { debug::debug_dump_tree(ctx, None, true) });

    // Extract top-level map keys.
    fn keys(reply: &ReplyValue) -> Vec<&str> {
        match reply {
            ReplyValue::Map(pairs) => pairs
                .iter()
                .map(|(k, _)| match k {
                    ReplyValue::SimpleString(s) => s.as_str(),
                    other => panic!("expected string key, got {other:?}"),
                })
                .collect(),
            other => panic!("expected map, got {other:?}"),
        }
    }
    assert_eq!(keys(&reply_default), keys(&reply_none));
}
