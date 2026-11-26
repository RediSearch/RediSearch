/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`AliveToken`]: how an iterator learns, before touching its index again, that the index's
//! owner freed it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A flag an index's owner shares with the iterators reading the index: the owner
/// [revokes](AliveToken::revoke) it before freeing the index, and an iterator checks it before
/// reading the index again.
///
/// An iterator's own revalidation reads its index to find out whether the index changed, which
/// needs the index to still exist. When an owner frees whole indexes, as a NESTED field drops
/// its emptied terminals, the iterators of each index hold a clone of that index's token, and
/// abort once it is revoked, without reading the index. Each clone keeps the flag alive, so it
/// can be read after the index is gone.
#[derive(Debug, Clone)]
pub struct AliveToken(Arc<AtomicBool>);

impl AliveToken {
    /// A token for an index that exists.
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    /// Whether the index still exists.
    pub fn is_alive(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Mark the index gone, before freeing it.
    pub fn revoke(&self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Default for AliveToken {
    fn default() -> Self {
        Self::new()
    }
}
