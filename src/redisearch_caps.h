/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Rolling-upgrade capability predicates for internal coordinator<->shard protocol
// features. A single source of truth, shared by the coordinator (which decides
// whether to ask a shard for a feature) and, potentially, a shard-side consumer,
// so the version-to-capability mapping cannot drift between the two sides.
//
// Every predicate here must fail closed: an unrecognized, unparseable, or absent
// version resolves to `false`. Treating an incapable shard as capable is the only
// failure mode that breaks a query outright (the shard hard-fails on an argument it
// does not recognize); treating a capable shard as incapable only costs the
// performance the feature would have bought.

// Returns whether a shard advertising `moduleVersion` — encoded the same way as
// REDISEARCH_MODULE_VERSION in version.h (major*10000 + minor*100 + patch) — is
// known to support the row-block reply format (src/aggregate/row_block.h) for
// internal coordinator<->shard aggregation.
//
// Ranges are expressed per minor line rather than as a single `>=` threshold: a
// single threshold cannot express a later backport of the format to an older minor
// line without wrongly excluding it, or without wrongly widening every version
// above it. Add a new line's range instead of extending an existing one.
bool RediSearchCaps_HasRowBlock(int moduleVersion);

#ifdef __cplusplus
}
#endif
