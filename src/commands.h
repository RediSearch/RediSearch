/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

/** RS_CMD_PREFIX can be defined with -D from the Makefile */
#ifdef RS_CLUSTER_ENTERPRISE
#define RS_CMD_WRITE_PREFIX "FT"
#define RS_CMD_READ_PREFIX "_FT"
#else  // OSS Cluster
#define RS_CMD_WRITE_PREFIX "_FT"
#define RS_CMD_READ_PREFIX "_FT"
#endif

// write commands
#define RS_CREATE_CMD RS_CMD_WRITE_PREFIX ".CREATE"
#define RS_CREATE_IF_NX_CMD RS_CMD_WRITE_PREFIX "._CREATEIFNX"        // for replica of support
#define RS_ADD_CMD RS_CMD_WRITE_PREFIX ".ADD"
#define RS_SAFEADD_CMD RS_CMD_WRITE_PREFIX ".SAFEADD"
#define LEGACY_RS_SAFEADD_CMD "_FT.SAFEADD"
#define RS_SETPAYLOAD_CMD RS_CMD_WRITE_PREFIX ".SETPAYLOAD"
#define RS_DEL_CMD RS_CMD_WRITE_PREFIX ".DEL"
#define LEGACY_RS_DEL_CMD "_FT.DEL"
#define RS_DROP_CMD RS_CMD_WRITE_PREFIX ".DROP"
#define RS_DROP_INDEX_CMD RS_CMD_WRITE_PREFIX ".DROPINDEX"
#define RS_DROP_IF_X_CMD RS_CMD_WRITE_PREFIX "._DROPIFX"             // for replica of support
#define RS_DROP_INDEX_IF_X_CMD RS_CMD_WRITE_PREFIX "._DROPINDEXIFX"  // for replica of support
#define RS_SYNUPDATE_CMD RS_CMD_WRITE_PREFIX ".SYNUPDATE"
#define RS_ALTER_CMD RS_CMD_WRITE_PREFIX ".ALTER"
#define RS_ALTER_IF_NX_CMD RS_CMD_WRITE_PREFIX "._ALTERIFNX"         // for replica of support
#define RS_DICT_ADD RS_CMD_WRITE_PREFIX ".DICTADD"
#define RS_DICT_DEL RS_CMD_WRITE_PREFIX ".DICTDEL"
#define RS_ALIASADD RS_CMD_WRITE_PREFIX ".ALIASADD"
#define RS_ALIASADD_IF_NX RS_CMD_WRITE_PREFIX "._ALIASADDIFNX"       // for replica of support
#define RS_ALIASDEL RS_CMD_WRITE_PREFIX ".ALIASDEL"
#define RS_ALIASDEL_IF_EX RS_CMD_WRITE_PREFIX "._ALIASDELIFX"        // for replica of support
#define RS_ALIASUPDATE RS_CMD_WRITE_PREFIX ".ALIASUPDATE"
#define RS_GET_CMD RS_CMD_WRITE_PREFIX ".GET"                        // "write" so it won't be redirected on enterprise cluster

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

// read commands
#define RS_INFO_CMD RS_CMD_READ_PREFIX ".INFO"
#define RS_SEARCH_CMD RS_CMD_READ_PREFIX ".SEARCH"
#define RS_AGGREGATE_CMD RS_CMD_READ_PREFIX ".AGGREGATE"
#define RS_PROFILE_CMD RS_CMD_READ_PREFIX ".PROFILE"
#define RS_MGET_CMD RS_CMD_READ_PREFIX ".MGET"
#define RS_TAGVALS_CMD RS_CMD_READ_PREFIX ".TAGVALS"
#define RS_CURSOR_CMD RS_CMD_READ_PREFIX ".CURSOR"
#define RS_DEBUG RS_CMD_READ_PREFIX ".DEBUG"
#define RS_SPELL_CHECK RS_CMD_READ_PREFIX ".SPELLCHECK"
#define RS_CONFIG RS_CMD_READ_PREFIX ".CONFIG"
