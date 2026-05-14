/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// `sort_asc_map` with bit 0 set: single ASC sort key.
pub const SORT_ASC: u64 = 0b1;
/// `sort_asc_map` with bit 0 clear: single DESC sort key.
pub const SORT_DESC: u64 = 0b0;
