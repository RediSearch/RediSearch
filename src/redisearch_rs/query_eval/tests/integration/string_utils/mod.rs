/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod str_to_lower_runes;
mod tag_strtolower;
mod unicode_tolower;
mod wildcard_remove_escape;

/// Codepoints where libnu (Unicode 9.0) and Rust (Unicode 17.0)
/// disagree on lowercasing. Two causes:
/// 1. Characters that gained lowercase mappings after Unicode 9.0.
/// 2. Context-dependent casing rules (Σ→ς/σ) that libnu doesn't
///    implement.
#[cfg(not(miri))]
const LIBNU_DIVERGENT: &[u32] = &[
    0x03A3, // Greek capital sigma: Rust uses context-dependent final sigma (ς vs σ)
    0x1C89, // Cyrillic small letter TJE (Unicode 11.0)
    // Georgian Mtavruli U+1C90..=U+1CBF (Unicode 11.0)
    0x1C90, 0x1C91, 0x1C92, 0x1C93, 0x1C94, 0x1C95, 0x1C96, 0x1C97, 0x1C98, 0x1C99, 0x1C9A, 0x1C9B,
    0x1C9C, 0x1C9D, 0x1C9E, 0x1C9F, 0x1CA0, 0x1CA1, 0x1CA2, 0x1CA3, 0x1CA4, 0x1CA5, 0x1CA6, 0x1CA7,
    0x1CA8, 0x1CA9, 0x1CAA, 0x1CAB, 0x1CAC, 0x1CAD, 0x1CAE, 0x1CAF, 0x1CB0, 0x1CB1, 0x1CB2, 0x1CB3,
    0x1CB4, 0x1CB5, 0x1CB6, 0x1CB7, 0x1CB8, 0x1CB9, 0x1CBA, 0x1CBD, 0x1CBE, 0x1CBF,
    0x2C2F, // Glagolitic capital letter CAUDATE CHRIVI (Unicode 14.0)
    // Latin Extended U+A7xx (Unicode 11.0–15.0)
    0xA7B8, 0xA7BA, 0xA7BC, 0xA7BE, 0xA7C0, 0xA7C2, 0xA7C4, 0xA7C5, 0xA7C6, 0xA7C7, 0xA7C9, 0xA7CB,
    0xA7CC, 0xA7CE, 0xA7D0, 0xA7D2, 0xA7D4, 0xA7D6, 0xA7D8, 0xA7DA, 0xA7DC, 0xA7F5,
];
