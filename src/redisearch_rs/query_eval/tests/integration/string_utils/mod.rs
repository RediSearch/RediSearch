/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod str_to_lower_runes;
mod unicode_tolower;

/// Codepoints where libnu and Rust's [`str::to_lowercase`] disagree on
/// lowercasing despite both targeting Unicode 17.0. The only remaining
/// cause is context-dependent casing: Rust applies the Greek final
/// sigma rule (Σ→ς at word end) while libnu lowercases per-character
/// (Σ→σ always).
#[cfg(not(miri))]
const LIBNU_DIVERGENT: &[u32] = &[
    0x03A3, // Greek capital sigma: Rust uses context-dependent final sigma (ς vs σ)
];
