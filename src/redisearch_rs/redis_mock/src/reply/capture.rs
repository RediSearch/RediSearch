/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Infrastructure for capturing Redis replies during testing.

use std::cell::RefCell;

use super::value::ReplyValue;

/// Represents a container being built (array or map with postponed or fixed length).
pub(super) enum ContainerBuilder {
    /// Array builder with accumulated elements.
    /// `expected_len` is `None` for postponed-length arrays, or `Some(n)` for fixed-length.
    Array {
        elements: Vec<ReplyValue>,
        expected_len: Option<usize>,
    },
    /// Map builder with accumulated key-value pairs.
    /// The Option holds a pending key waiting for its value.
    /// `expected_len` is `None` for postponed-length maps, or `Some(n)` for fixed-length.
    Map {
        pairs: Vec<(ReplyValue, ReplyValue)>,
        pending_key: Option<ReplyValue>,
        expected_len: Option<usize>,
    },
}

/// Thread-local state for capturing replies.
pub(super) struct CaptureState {
    /// Stack of container builders for nested structures.
    /// When empty, replies go directly to `completed`.
    builder_stack: Vec<ContainerBuilder>,
    /// Completed top-level reply values.
    completed: Vec<ReplyValue>,
}

impl CaptureState {
    pub(super) const fn new() -> Self {
        Self {
            builder_stack: Vec::new(),
            completed: Vec::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        self.builder_stack.clear();
        self.completed.clear();
    }

    /// Push a value to the current context (either a builder or completed list).
    pub(super) fn push_value(&mut self, value: ReplyValue) {
        let Some(builder) = self.builder_stack.last_mut() else {
            self.completed.push(value);
            return;
        };
        match builder {
            ContainerBuilder::Array { elements, .. } => {
                elements.push(value);
            }
            ContainerBuilder::Map {
                pairs, pending_key, ..
            } => {
                if let Some(key) = pending_key.take() {
                    pairs.push((key, value));
                } else {
                    *pending_key = Some(value);
                }
            }
        }

        self.finalize_current_if_needed();
    }

    /// Finalize the current builder (for fixed-length containers).
    fn finalize_current_if_needed(&mut self) {
        let Some(builder) = self.builder_stack.last() else {
            return;
        };
        // Check if the builder is ready to be finalized
        let is_complete = match builder {
            ContainerBuilder::Array {
                elements,
                expected_len,
            } => expected_len.is_some_and(|len| elements.len() >= len),
            ContainerBuilder::Map {
                pairs,
                expected_len,
                ..
            } => expected_len.is_some_and(|len| pairs.len() >= len),
        };
        if !is_complete {
            return;
        }
        // Now pop and finalize
        let builder = self.builder_stack.pop().unwrap();
        let finalized_value = match builder {
            ContainerBuilder::Array { elements, .. } => ReplyValue::Array(elements),
            ContainerBuilder::Map {
                pairs, pending_key, ..
            } => {
                if pending_key.is_some() {
                    panic!(
                        "Map is being finalized, but the last key doesn't have a matching value"
                    );
                }
                ReplyValue::Map(pairs)
            }
        };
        self.push_value(finalized_value);
    }

    /// Start a new array with optional expected length.
    pub(super) fn start_array(&mut self, expected_len: Option<usize>) {
        self.builder_stack.push(ContainerBuilder::Array {
            elements: Vec::new(),
            expected_len,
        });
    }

    /// Start a new map with optional expected length.
    pub(super) fn start_map(&mut self, expected_len: Option<usize>) {
        self.builder_stack.push(ContainerBuilder::Map {
            pairs: Vec::new(),
            pending_key: None,
            expected_len,
        });
    }

    /// Finalize the current array builder.
    pub(super) fn finalize_array(&mut self, _len: i64) {
        if let Some(ContainerBuilder::Array { elements, .. }) = self.builder_stack.pop() {
            self.push_value(ReplyValue::Array(elements));
        }
    }

    /// Finalize the current map builder.
    pub(super) fn finalize_map(&mut self, _len: i64) {
        if let Some(ContainerBuilder::Map {
            pairs, pending_key, ..
        }) = self.builder_stack.pop()
        {
            if pending_key.is_some() {
                panic!("Map is being finalized, but the last key doesn't have a matching value");
            }
            self.push_value(ReplyValue::Map(pairs));
        }
    }

    /// Take the completed replies.
    pub(super) fn take_completed(&mut self) -> Vec<ReplyValue> {
        std::mem::take(&mut self.completed)
    }
}

thread_local! {
    pub(super) static CAPTURE_STATE: RefCell<CaptureState> = const { RefCell::new(CaptureState::new()) };
}

/// Execute a closure and capture all Redis replies made within it.
///
/// # Example
///
/// ```ignore
/// use redis_mock::reply::{capture_replies, ReplyValue};
///
/// let replies = capture_replies(|| {
///     let replier = unsafe { Replier::new(ctx) };
///     replier.long_long(42);
/// });
///
/// assert_eq!(replies, vec![ReplyValue::LongLong(42)]);
/// ```
pub fn capture_replies<F, R>(f: F) -> Vec<ReplyValue>
where
    F: FnOnce() -> R,
{
    CAPTURE_STATE.with(|state| {
        state.borrow_mut().clear();
    });

    let _ = f();

    CAPTURE_STATE.with(|state| state.borrow_mut().take_completed())
}
