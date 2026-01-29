/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the redis_reply crate using mocked Redis reply functions.

#[cfg(not(miri))]
mod array;
#[cfg(not(miri))]
mod edge_cases;
#[cfg(not(miri))]
mod fixed;
#[cfg(not(miri))]
mod map;
#[cfg(not(miri))]
mod replier;

use redis_mock::reply::{ReplyValue, capture_replies};
use redis_reply::{RedisModuleCtx, Replier};

// Define the required C symbols for linking
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Initialize mock and return a Replier ready for use.
pub fn init() -> Replier {
    redis_mock::init_redis_module_mock();
    // SAFETY: Context is ignored in mock mode, using non-null dummy address
    unsafe { Replier::new(std::ptr::dangling_mut::<RedisModuleCtx>()) }
}

/// Extract a single reply, panicking if there are zero or multiple replies.
pub fn expect_single_reply(mut replies: Vec<ReplyValue>) -> ReplyValue {
    assert_eq!(
        replies.len(),
        1,
        "expected single reply, got {}",
        replies.len()
    );
    replies.pop().unwrap()
}

/// Capture a single reply from a closure.
pub fn capture_single_reply(f: impl FnOnce()) -> ReplyValue {
    expect_single_reply(capture_replies(f))
}
