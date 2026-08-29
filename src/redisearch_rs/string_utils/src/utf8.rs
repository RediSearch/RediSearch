/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Well-formedness checking for byte strings that RediSearch treats as UTF-8.
//!
//! The engine's text path accepts arbitrary bytes: nothing on the way in rejects a value that
//! is not valid UTF-8, and the decoders it later runs over that value ([`crate::libnu`]) do not
//! validate either — they re-interpret the bytes and produce replacement output. This module is
//! the single definition of what "valid UTF-8" means for the callers that do want to refuse
//! such input.

/// Returns whether `bytes` is well-formed UTF-8.
///
/// This is [`str::from_utf8`]'s notion of well-formed, so it rejects the encodings that decoders
/// tend to accept silently: overlong forms, unpaired surrogate code points, sequences above
/// `U+10FFFF`, and truncated trailing sequences. The empty slice is valid.
pub const fn is_valid(bytes: &[u8]) -> bool {
    str::from_utf8(bytes).is_ok()
}
