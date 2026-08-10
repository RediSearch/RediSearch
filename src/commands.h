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

// Forward declaration to keep this header self-contained.
// `IsEnterprise()` is defined in module.c (declared in module.h).
bool IsEnterprise();

// Write commands - define both public (FT) and internal (_FT) variants.
// The appropriate variant is selected at runtime via `CMD_FOR_ENV(...)` based
// on `IsEnterprise()`:
//   - Enterprise: uses public "FT" prefix (DMC handles routing)
//   - OSS:        uses internal "_FT" prefix (coordinator registers public FT
//                 commands separately)
//
// Each pair is defined so that the INTERNAL variant is derived from the PUBLIC
// one by prepending "_". This guarantees the two strings always agree.

// RS_CREATE_CMD
#define RS_CREATE_CMD_PUBLIC   "FT.CREATE"
#define RS_CREATE_CMD_INTERNAL "_" RS_CREATE_CMD_PUBLIC

// RS_CREATE_IF_NX_CMD (for replica of support)
#define RS_CREATE_IF_NX_CMD_PUBLIC   "FT._CREATEIFNX"
#define RS_CREATE_IF_NX_CMD_INTERNAL "_" RS_CREATE_IF_NX_CMD_PUBLIC

// RS_SETPAYLOAD_CMD
#define RS_SETPAYLOAD_CMD_PUBLIC   "FT.SETPAYLOAD"
#define RS_SETPAYLOAD_CMD_INTERNAL "_" RS_SETPAYLOAD_CMD_PUBLIC

// RS_DROP_CMD
#define RS_DROP_CMD_PUBLIC   "FT.DROP"
#define RS_DROP_CMD_INTERNAL "_" RS_DROP_CMD_PUBLIC

// RS_DROP_INDEX_CMD
#define RS_DROP_INDEX_CMD_PUBLIC   "FT.DROPINDEX"
#define RS_DROP_INDEX_CMD_INTERNAL "_" RS_DROP_INDEX_CMD_PUBLIC

// RS_DROP_IF_X_CMD (for replica of support)
#define RS_DROP_IF_X_CMD_PUBLIC   "FT._DROPIFX"
#define RS_DROP_IF_X_CMD_INTERNAL "_" RS_DROP_IF_X_CMD_PUBLIC

// RS_DROP_INDEX_IF_X_CMD (for replica of support)
#define RS_DROP_INDEX_IF_X_CMD_PUBLIC   "FT._DROPINDEXIFX"
#define RS_DROP_INDEX_IF_X_CMD_INTERNAL "_" RS_DROP_INDEX_IF_X_CMD_PUBLIC

// RS_SYNUPDATE_CMD
#define RS_SYNUPDATE_CMD_PUBLIC   "FT.SYNUPDATE"
#define RS_SYNUPDATE_CMD_INTERNAL "_" RS_SYNUPDATE_CMD_PUBLIC

// RS_ALTER_CMD
#define RS_ALTER_CMD_PUBLIC   "FT.ALTER"
#define RS_ALTER_CMD_INTERNAL "_" RS_ALTER_CMD_PUBLIC

// RS_ALTER_IF_NX_CMD (for replica of support)
#define RS_ALTER_IF_NX_CMD_PUBLIC   "FT._ALTERIFNX"
#define RS_ALTER_IF_NX_CMD_INTERNAL "_" RS_ALTER_IF_NX_CMD_PUBLIC

// RS_DICT_ADD
#define RS_DICT_ADD_PUBLIC   "FT.DICTADD"
#define RS_DICT_ADD_INTERNAL "_" RS_DICT_ADD_PUBLIC

// RS_DICT_DEL
#define RS_DICT_DEL_PUBLIC   "FT.DICTDEL"
#define RS_DICT_DEL_INTERNAL "_" RS_DICT_DEL_PUBLIC

// RS_ALIASADD
#define RS_ALIASADD_PUBLIC   "FT.ALIASADD"
#define RS_ALIASADD_INTERNAL "_" RS_ALIASADD_PUBLIC

// RS_ALIASADD_IF_NX (for replica of support)
#define RS_ALIASADD_IF_NX_PUBLIC   "FT._ALIASADDIFNX"
#define RS_ALIASADD_IF_NX_INTERNAL "_" RS_ALIASADD_IF_NX_PUBLIC

// RS_ALIASDEL
#define RS_ALIASDEL_PUBLIC   "FT.ALIASDEL"
#define RS_ALIASDEL_INTERNAL "_" RS_ALIASDEL_PUBLIC

// RS_ALIASDEL_IF_X (for replica of support)
#define RS_ALIASDEL_IF_X_PUBLIC   "FT._ALIASDELIFX"
#define RS_ALIASDEL_IF_X_INTERNAL "_" RS_ALIASDEL_IF_X_PUBLIC

// RS_ALIASUPDATE
#define RS_ALIASUPDATE_PUBLIC   "FT.ALIASUPDATE"
#define RS_ALIASUPDATE_INTERNAL "_" RS_ALIASUPDATE_PUBLIC

// RS_RESTORE_IF_NX (for replica of support - Currently there is no FT.RESTORE command)
#define RS_RESTORE_IF_NX_PUBLIC   "FT._RESTOREIFNX"
#define RS_RESTORE_IF_NX_INTERNAL "_" RS_RESTORE_IF_NX_PUBLIC

// Selects the runtime-appropriate variant of a write command name. `cmd` must
// be the bare RS_*_CMD identifier; the macro appends `_PUBLIC` or `_INTERNAL`
// via token concatenation. Example: `CMD_FOR_ENV(RS_CREATE_CMD)`.
#define CMD_FOR_ENV(cmd) (IsEnterprise() ? cmd##_PUBLIC : cmd##_INTERNAL)

// Legacy write commands that are key-bounded (+ extra legacy commands that have to be registered for enterprise)
#define RS_ADD_CMD "FT.ADD"
#define RS_DEL_CMD "FT.DEL"
#define RS_GET_CMD "FT.GET"
#define RS_SAFEADD_CMD "FT.SAFEADD"
#define LEGACY_RS_SAFEADD_CMD "_FT.SAFEADD"
#define LEGACY_RS_DEL_CMD "_FT.DEL"

// Suggestion commands are key-bounded, so they are already directed to the correct shard
#define RS_SUGADD_CMD "FT.SUGADD"
#define RS_SUGGET_CMD "FT.SUGGET"
#define RS_SUGDEL_CMD "FT.SUGDEL"
#define RS_SUGLEN_CMD "FT.SUGLEN"

// read commands that are always performed locally
#define RS_EXPLAIN_CMD "FT.EXPLAIN"
#define RS_EXPLAINCLI_CMD "FT.EXPLAINCLI"
#define RS_DICT_DUMP "FT.DICTDUMP"
#define RS_SYNDUMP_CMD "FT.SYNDUMP"
#define RS_INDEX_LIST_CMD "FT._LIST"
#define RS_ALIASLIST_CMD "FT.ALIASLIST"
#define RS_SYNADD_CMD "FT.SYNADD" // Deprecated, always returns an error

// Read commands always use the internal "_FT" prefix
#define RS_CMD_READ_PREFIX "_FT"
#define RS_INFO_CMD RS_CMD_READ_PREFIX ".INFO"
#define RS_SEARCH_CMD RS_CMD_READ_PREFIX ".SEARCH"
#define RS_HYBRID_CMD RS_CMD_READ_PREFIX ".HYBRID"
#define RS_AGGREGATE_CMD RS_CMD_READ_PREFIX ".AGGREGATE"
#define RS_PROFILE_CMD RS_CMD_READ_PREFIX ".PROFILE"
#define RS_MGET_CMD RS_CMD_READ_PREFIX ".MGET"
#define RS_TAGVALS_CMD RS_CMD_READ_PREFIX ".TAGVALS"
#define RS_CURSOR_CMD RS_CMD_READ_PREFIX ".CURSOR"
#define RS_DEBUG RS_CMD_READ_PREFIX ".DEBUG"
#define RS_SPELL_CHECK RS_CMD_READ_PREFIX ".SPELLCHECK"
#define RS_CONFIG RS_CMD_READ_PREFIX ".CONFIG"
