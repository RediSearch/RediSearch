/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Capture the `FT.PROFILE` reply an iterator prints, against the reply mock.

use redis_mock::reply::{ReplyValue, capture_replies};
use redis_reply::{MapBuilder, RedisModuleCtx, Replier};
use rqe_iterators::profile_print::{ProfilePrint, ProfilePrintCtx};
use std::ptr::NonNull;

/// Run `print` against a fresh reply map and return the single reply it writes.
///
/// # Panics
///
/// Panics if `print` writes anything other than exactly one reply.
pub fn capture_profile(
    print: impl FnOnce(&mut MapBuilder<'_>, &mut ProfilePrintCtx<'_>),
) -> ReplyValue {
    redis_mock::init_redis_module_mock();
    // SAFETY: `init_redis_module_mock` replaces every `RedisModule_Reply*` function with one that
    // records the reply without dereferencing the context, so a dangling non-null context is
    // never read.
    let mut replier = unsafe { Replier::new(NonNull::<RedisModuleCtx>::dangling()) };
    let mut replies = capture_replies(|| {
        let mut ctx = ProfilePrintCtx::new(false, false);
        print(&mut replier.map(), &mut ctx);
    });
    assert_eq!(replies.len(), 1, "a profile is a single map reply");
    replies.pop().expect("one reply was captured")
}

/// Print `it`'s profile through [`capture_profile`].
pub fn profile_of(it: &impl ProfilePrint) -> ReplyValue {
    capture_profile(|map, ctx| it.print_profile(map, ctx))
}
