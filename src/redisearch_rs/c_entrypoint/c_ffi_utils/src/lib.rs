/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Provides [`crate::canary::CanaryProtected`] and [`crate::canary::CanaryGuarded`] types for canary-based memory initialization.
pub mod canary;

/// Helpers for implementing opaque sized types.
///
/// In order for types defined in Rust to be placed on the
/// stack in C, they need to be sized and FFI-safe. Annotating
/// these types with `#[repr(C)]` makes the type's layout well-defined,
/// but comes with two downsides:
///
/// 1. (Private) fields are exposed to C code, allowing C code
///    to rely on the internals, and even break invariants.
/// 2. All fields or variants of the type need to be FFI-safe. Therefore,
///    types like `String` and `Arc` cannot be used.
///
/// This module instead allows for defining an opaque sized type,
///  which is validated to have the same size and alignment
/// of the original type, but hides its internals and is FFI-safe even if
/// original is not.
pub mod opaque;

/// Provides the [`expect_unchecked`](crate::expect_unchecked!) macro.
pub mod expect_unchecked;
