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
// The version-to-capability mapping is the only thing that varies per feature:
// every capability is decided from the same single piece of state (the `search`
// module version a node advertises over HELLO, see MRConnPool's moduleVersion in
// conn.h), so the enum plus RediSearchCaps_Supports is the whole per-feature
// surface. Everything else (learning the version, invalidating it, sticky
// demotion) is generic and lives once in conn.c/conn.h.

// One entry per internal coordinator<->shard protocol feature gated by rolling
// upgrade. RS_CAP__COUNT is not a real capability; it is the table size and the
// bound for `demoted` bitmask shifts (see MRConnPool in conn.h).
typedef enum {
  RS_CAP_ROW_BLOCK = 0,  // compact binary aggregation rows, RESP2 reply path
  RS_CAP__COUNT
} RSCapability;

// Human-readable name for `cap`, used only for diagnostics (FT.DEBUG
// SHARD_CONNECTION_STATES). Returns a placeholder for an out-of-range value so a
// missing table entry shows up in the debug output instead of crashing it.
const char *RSCapability_Name(RSCapability cap);

// Returns whether a shard advertising `moduleVersion` — encoded the same way as
// REDISEARCH_MODULE_VERSION in version.h (major*10000 + minor*100 + patch) — is
// known to support `cap`.
//
// Every capability must fail closed: an unrecognized, unparseable, or absent
// version resolves to `false`, and so does a capability with no registered
// predicate (see redisearch_caps.c). Treating an incapable shard as capable is the
// only failure mode that breaks a query outright (the shard hard-fails on an
// argument it does not recognize); treating a capable shard as incapable only
// costs the performance the feature would have bought.
bool RediSearchCaps_Supports(RSCapability cap, int moduleVersion);

#ifdef __cplusplus
}
#endif
