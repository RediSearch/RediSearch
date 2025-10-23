/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Contains global configuration accessors for Redis, i.e.
//! Functions to access global Redis configuration parameters.

/// Returns true if the Redis server is running in CRDT mode.
pub fn is_crdt() -> bool {
    // Safety: `isCrdt` is written at module startup and never changed afterwards, therefore it is safe to read it here.
    unsafe { ffi::isCrdt }
}

/// Returns the Redis server version as an integer.
pub fn get_server_version() -> i32 {
    // Safety: We access the global config, which is setup during module initialization, we readonly access the serverVersion field here.
    // which is safe as it is never changed after initialization.
    unsafe { ffi::RSGlobalConfig.serverVersion }
}

/// Returns true if the Redis server has the Scan Key API feature.
pub fn has_scan_key_feature() -> bool {
    let server_version = get_server_version();
    let feature = ffi::RM_SCAN_KEY_API_FIX as i32;
    feature <= server_version
}
