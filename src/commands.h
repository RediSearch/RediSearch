/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once


// Write commands - define both internal (_FT) and public (FT) variants
// The appropriate variant is selected at runtime based on IsEnterprise()
// - Enterprise: uses public "FT" prefix (DMC handles routing)
// - OSS: uses internal "_FT" prefix (coordinator registers public FT commands separately)

// RS_CREATE_CMD
#define RS_CREATE_CMD_INTERNAL "_FT.CREATE"
#define RS_CREATE_CMD_PUBLIC "FT.CREATE"

// RS_CREATE_IF_NX_CMD (for replica of support)
#define RS_CREATE_IF_NX_CMD_INTERNAL "_FT._CREATEIFNX"
#define RS_CREATE_IF_NX_CMD_PUBLIC "FT._CREATEIFNX"

// RS_SETPAYLOAD_CMD
#define RS_SETPAYLOAD_CMD_INTERNAL "_FT.SETPAYLOAD"
#define RS_SETPAYLOAD_CMD_PUBLIC "FT.SETPAYLOAD"

// RS_DROP_CMD
#define RS_DROP_CMD_INTERNAL "_FT.DROP"
#define RS_DROP_CMD_PUBLIC "FT.DROP"

// RS_DROP_INDEX_CMD
#define RS_DROP_INDEX_CMD_INTERNAL "_FT.DROPINDEX"
#define RS_DROP_INDEX_CMD_PUBLIC "FT.DROPINDEX"

// RS_DROP_IF_X_CMD (for replica of support)
#define RS_DROP_IF_X_CMD_INTERNAL "_FT._DROPIFX"
#define RS_DROP_IF_X_CMD_PUBLIC "FT._DROPIFX"

// RS_DROP_INDEX_IF_X_CMD (for replica of support)
#define RS_DROP_INDEX_IF_X_CMD_INTERNAL "_FT._DROPINDEXIFX"
#define RS_DROP_INDEX_IF_X_CMD_PUBLIC "FT._DROPINDEXIFX"

// RS_SYNUPDATE_CMD
#define RS_SYNUPDATE_CMD_INTERNAL "_FT.SYNUPDATE"
#define RS_SYNUPDATE_CMD_PUBLIC "FT.SYNUPDATE"

// RS_ALTER_CMD
#define RS_ALTER_CMD_INTERNAL "_FT.ALTER"
#define RS_ALTER_CMD_PUBLIC "FT.ALTER"

// RS_ALTER_IF_NX_CMD (for replica of support)
#define RS_ALTER_IF_NX_CMD_INTERNAL "_FT._ALTERIFNX"
#define RS_ALTER_IF_NX_CMD_PUBLIC "FT._ALTERIFNX"

// RS_DICT_ADD
#define RS_DICT_ADD_INTERNAL "_FT.DICTADD"
#define RS_DICT_ADD_PUBLIC "FT.DICTADD"

// RS_DICT_DEL
#define RS_DICT_DEL_INTERNAL "_FT.DICTDEL"
#define RS_DICT_DEL_PUBLIC "FT.DICTDEL"

// RS_ALIASADD
#define RS_ALIASADD_INTERNAL "_FT.ALIASADD"
#define RS_ALIASADD_PUBLIC "FT.ALIASADD"

// RS_ALIASADD_IF_NX (for replica of support)
#define RS_ALIASADD_IF_NX_INTERNAL "_FT._ALIASADDIFNX"
#define RS_ALIASADD_IF_NX_PUBLIC "FT._ALIASADDIFNX"

// RS_ALIASDEL
#define RS_ALIASDEL_INTERNAL "_FT.ALIASDEL"
#define RS_ALIASDEL_PUBLIC "FT.ALIASDEL"

// RS_ALIASDEL_IF_EX (for replica of support)
#define RS_ALIASDEL_IF_EX_INTERNAL "_FT._ALIASDELIFX"
#define RS_ALIASDEL_IF_EX_PUBLIC "FT._ALIASDELIFX"

// RS_ALIASUPDATE
#define RS_ALIASUPDATE_INTERNAL "_FT.ALIASUPDATE"
#define RS_ALIASUPDATE_PUBLIC "FT.ALIASUPDATE"

// RS_RESTORE_IF_NX (for replica of support - Currently there is no FT.RESTORE command)
#define RS_RESTORE_IF_NX_INTERNAL "_FT._RESTOREIFNX"
#define RS_RESTORE_IF_NX_PUBLIC "FT._RESTOREIFNX"

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
