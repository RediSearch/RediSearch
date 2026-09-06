/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Debug-only knobs over the iterator tree, driven by `FT.DEBUG` subcommands.

use rqe_iterators::interop::{mock_revalidate_timeout, set_mock_revalidate_timeout};

/// Make every subsequent iterator revalidation report `VALIDATE_TIMEOUT`, as if the query had run
/// out of time while re-seeking the index, until this is called again with `false`.
///
/// Backs `FT.DEBUG MOCK_REVALIDATE_TIMEOUT enable|disable`. The switch is process-wide, like the
/// `VECSIM_MOCK_TIMEOUT` one it mirrors, so a test that enables it must disable it again.
#[unsafe(no_mangle)]
pub extern "C" fn RQEIterators_SetMockRevalidateTimeout(enabled: bool) {
    set_mock_revalidate_timeout(enabled);
}

/// Report whether [`RQEIterators_SetMockRevalidateTimeout`] is currently on.
///
/// Backs `FT.DEBUG MOCK_REVALIDATE_TIMEOUT status`. A server left with the switch on reports a
/// timeout on every cursor resume, which is worth being able to see directly: a test that dies
/// between `enable` and its teardown would otherwise turn every later query in the file into a
/// confusing failure.
#[unsafe(no_mangle)]
pub extern "C" fn RQEIterators_GetMockRevalidateTimeout() -> bool {
    mock_revalidate_timeout()
}
