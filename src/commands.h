/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
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

#define PROXY_FILTERED "_proxy-filtered"

// write commands
#define RS_CREATE_CMD RS_CMD_WRITE_PREFIX ".CREATE"
#define RS_CREATE_IF_NX_CMD RS_CMD_WRITE_PREFIX "._CREATEIFNX"  // for replica of support
#define RS_DROP_CMD RS_CMD_WRITE_PREFIX ".DROP"
#define RS_DROP_INDEX_CMD RS_CMD_WRITE_PREFIX ".DROPINDEX"
#define RS_DROP_IF_X_CMD RS_CMD_WRITE_PREFIX "._DROPIFX"             // for replica of support
#define RS_DROP_INDEX_IF_X_CMD RS_CMD_WRITE_PREFIX "._DROPINDEXIFX"  // for replica of support
#define RS_SYNUPDATE_CMD RS_CMD_WRITE_PREFIX ".SYNUPDATE"
#define RS_ALTER_CMD RS_CMD_WRITE_PREFIX ".ALTER"
#define RS_ALTER_IF_NX_CMD RS_CMD_WRITE_PREFIX "._ALTERIFNX"  // for replica of support
#define RS_DICT_ADD RS_CMD_WRITE_PREFIX ".DICTADD"
#define RS_DICT_DEL RS_CMD_WRITE_PREFIX ".DICTDEL"
#define RS_ALIASADD RS_CMD_WRITE_PREFIX ".ALIASADD"
#define RS_ALIASADD_IF_NX RS_CMD_WRITE_PREFIX "._ALIASADDIFNX"  // for replica of support
#define RS_ALIASDEL RS_CMD_WRITE_PREFIX ".ALIASDEL"
#define RS_ALIASDEL_IF_EX RS_CMD_WRITE_PREFIX "._ALIASDELIFX"  // for replica of support
#define RS_ALIASUPDATE RS_CMD_WRITE_PREFIX ".ALIASUPDATE"

// document write commands
#define RS_ADD_CMD RS_CMD_WRITE_PREFIX ".ADD"
#define RS_SAFEADD_CMD RS_CMD_WRITE_PREFIX ".SAFEADD"
#define LEGACY_RS_SAFEADD_CMD "_FT.SAFEADD"
#define RS_DEL_CMD RS_CMD_WRITE_PREFIX ".DEL"
#define LEGACY_RS_DEL_CMD "_FT.DEL"


#define RS_WRITE_FLAGS_DEFAULT(flags) IsEnterprise() ? flags " " PROXY_FILTERED : flags

// RM_TRY(RMCreateSearchCommand(ctx, LEGACY_RS_SAFEADD_CMD, RSAddDocumentCommand, IsEnterprise() ? "write deny-oom " PROXY_FILTERED : "write deny-oom", INDEX_DOC_CMD_ARGS, "write admin"))
// RM_TRY(RMCreateSearchCommand(ctx, LEGACY_RS_DEL_CMD, DeleteCommand, IsEnterprise() ? "write " PROXY_FILTERED : "write", INDEX_DOC_CMD_ARGS, "write admin"))
#ifdef RS_CLUSTER_ENTERPRISE
  // on enterprise cluster we need to keep the _ft.safeadd/_ft.del command
  // to be able to replicate from an old RediSearch version.
  // If this is the light version then the _ft.safeadd/_ft.del does not exist
  // and we will get the normal ft.safeadd/ft.del command.
  #define SAFE_LEGACY_RS_COMMAND(OP, ...) \
    OP(LEGACY_RS_SAFEADD_CMD, RSAddDocumentCommand, RS_WRITE_FLAGS_DEFAULT("write deny-oom"), NULL, "write admin", __VA_ARGS__) \
    OP(LEGACY_RS_DEL_CMD, DeleteCommand, RS_WRITE_FLAGS_DEFAULT("write"), NULL, "write admin", __VA_ARGS__)
#else
  #define SAFE_LEGACY_RS_COMMAND(OP, ...)
#endif


// RM_TRY(RMCreateSearchCommand(ctx, RS_ADD_CMD, RSAddDocumentCommand, "write deny-oom", INDEX_DOC_CMD_ARGS, "write admin"))
// Safe legacy commands
// RM_TRY(RMCreateSearchCommand(ctx, RS_DEL_CMD, DeleteCommand, IsEnterprise() ? "write " PROXY_FILTERED : "write", INDEX_DOC_CMD_ARGS, "write admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SAFEADD_CMD, RSAddDocumentCommand, "write deny-oom", INDEX_DOC_CMD_ARGS, "write admin"))
#define RS_WRITE_DOC_COMMANDS(OP, ...) \
  OP(RS_ADD_CMD,     RSAddDocumentCommand, "write deny-oom",                NULL, "write admin", __VA_ARGS__) \
  SAFE_LEGACY_RS_COMMAND(OP, __VA_ARGS__)                                                                     \
  OP(RS_DEL_CMD,     DeleteCommand,        RS_WRITE_FLAGS_DEFAULT("write"), NULL, "write admin", __VA_ARGS__) \
  OP(RS_SAFEADD_CMD, RSAddDocumentCommand, "write deny-oom",                NULL, "write admin", __VA_ARGS__)



// RM_TRY(RMCreateSearchCommand(ctx, RS_CREATE_CMD, CreateIndexCommand, "write deny-oom", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_CREATE_IF_NX_CMD, CreateIndexIfNotExistsCommand, "write deny-oom", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_CMD, DropIndexCommand, "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_INDEX_CMD, DropIndexCommand, "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_IF_X_CMD, DropIfExistsIndexCommand, "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DROP_INDEX_IF_X_CMD, DropIfExistsIndexCommand, "write", INDEX_ONLY_CMD_ARGS, "write slow dangerous"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SYNUPDATE_CMD, SynUpdateCommand, "write", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALTER_CMD, AlterIndexCommand, "write", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALTER_IF_NX_CMD, AlterIndexIfNXCommand, "write", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_ADD, DictAddCommand, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_DEL, DictDelCommand, "readonly", 0, 0, 0, ""))
// Alias is a special case, we can not use the INDEX_ONLY_CMD_ARGS/INDEX_DOC_CMD_ARGS macros
// Cluster is managed outside of module lets trust it and not raise cross slot error.
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASADD, AliasAddCommand, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASADD_IF_NX, AliasAddCommandIfNX, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASUPDATE, AliasUpdateCommand, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASDEL, AliasDelCommand, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_ALIASDEL_IF_EX, AliasDelIfExCommand, "readonly", 0, 0, 0, ""))
#define RS_WRITE_COMMANDS(OP, ...)                                                                                                           \
  OP(RS_CREATE_CMD,          CreateIndexCommand,            "write deny-oom", SetFtCreateInfo,    "",                           __VA_ARGS__) \
  OP(RS_CREATE_IF_NX_CMD,    CreateIndexIfNotExistsCommand, "write deny-oom", NULL,               "write",                      __VA_ARGS__) \
  OP(RS_DROP_CMD,            DropIndexCommand,              "write",          SetFtDropindexInfo, "write slow dangerous admin", __VA_ARGS__) \
  OP(RS_DROP_INDEX_CMD,      DropIndexCommand,              "write",          SetFtDropindexInfo, "write slow dangerous",       __VA_ARGS__) \
  OP(RS_DROP_IF_X_CMD,       DropIfExistsIndexCommand,      "write",          SetFtDropindexInfo, "write slow dangerous admin", __VA_ARGS__) \
  OP(RS_DROP_INDEX_IF_X_CMD, DropIfExistsIndexCommand,      "write",          SetFtDropindexInfo, "write slow dangerous",       __VA_ARGS__) \
  OP(RS_SYNUPDATE_CMD,       SynUpdateCommand,              "write",          SetFtSynupdateInfo, "",                           __VA_ARGS__) \
  OP(RS_ALTER_CMD,           AlterIndexCommand,             "write",          SetFtAlterInfo,     "",                           __VA_ARGS__) \
  OP(RS_ALTER_IF_NX_CMD,     AlterIndexIfNXCommand,         "write",          NULL,               "",                           __VA_ARGS__) \
  OP(RS_DICT_ADD,            DictAddCommand,                "readonly",       NULL,               "",                           __VA_ARGS__) \
  OP(RS_DICT_DEL,            DictDelCommand,                "readonly",       NULL,               "",                           __VA_ARGS__) \
  OP(RS_ALIASADD,            AliasAddCommand,               "readonly",       NULL,               "",                           __VA_ARGS__) \
  OP(RS_ALIASADD_IF_NX,      AliasAddCommandIfNX,           "readonly",       NULL,               "",                           __VA_ARGS__) \
  OP(RS_ALIASUPDATE,         AliasUpdateCommand,            "readonly",       NULL,               "",                           __VA_ARGS__) \
  OP(RS_ALIASDEL,            AliasDelCommand,               "readonly",       NULL,               "",                           __VA_ARGS__)\
  OP(RS_ALIASDEL_IF_EX,      AliasDelIfExCommand,           "readonly",       NULL,               "",                           __VA_ARGS__)

// Suggestion commands are key-bounded, so they are already directed to the correct shard
#define RS_SUGADD_CMD "FT.SUGADD"
#define RS_SUGGET_CMD "FT.SUGGET"
#define RS_SUGDEL_CMD "FT.SUGDEL"
#define RS_SUGLEN_CMD "FT.SUGLEN"

// RM_TRY(RMCreateSearchCommand(ctx, RS_SUGADD_CMD, RSSuggestAddCommand, "write deny-oom", 1, 1, 1, "write"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SUGGET_CMD, RSSuggestGetCommand, "readonly", 1, 1, 1, "read"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SUGDEL_CMD, RSSuggestDelCommand, "write", 1, 1, 1, "write"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SUGLEN_CMD, RSSuggestLenCommand, "readonly", 1, 1, 1, "read"))
// Suggestion commands key specs should be 1, 1, 1
#define RS_SUG_COMMANDS(OP, ...) \
    OP(RS_SUGADD_CMD, RSSuggestAddCommand, "write deny-oom", SetFtSugaddInfo, "write", __VA_ARGS__) \
    OP(RS_SUGGET_CMD, RSSuggestGetCommand, "readonly",       SetFtSuggetInfo, "read",  __VA_ARGS__) \
    OP(RS_SUGDEL_CMD, RSSuggestDelCommand, "write",          SetFtSugdelInfo, "write", __VA_ARGS__) \
    OP(RS_SUGLEN_CMD, RSSuggestLenCommand, "readonly",       SetFtSuglenInfo, "read",  __VA_ARGS__)


// read commands that are always performed locally
#define RS_EXPLAIN_CMD "FT.EXPLAIN"
#define RS_EXPLAINCLI_CMD "FT.EXPLAINCLI"
#define RS_DICT_DUMP "FT.DICTDUMP"
#define RS_SYNDUMP_CMD "FT.SYNDUMP"
#define RS_INDEX_LIST_CMD "FT._LIST"
#define RS_SYNADD_CMD "FT.SYNADD" // Deprecated, always returns an error

// RM_TRY(RMCreateSearchCommand(ctx, RS_EXPLAIN_CMD, QueryExplainCommand, "readonly", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_EXPLAINCLI_CMD, QueryExplainCLICommand, "readonly", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_DICT_DUMP, DictDumpCommand, "readonly", 0, 0, 0, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SYNDUMP_CMD, SynDumpCommand, "readonly", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_INDEX_LIST_CMD, IndexList, "readonly", 0, 0, 0, "slow admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SYNADD_CMD, SynAddCommand, "write", INDEX_ONLY_CMD_ARGS, "admin"))
#define RS_LOCAL_COMMANDS(OP, ...) \
	OP(RS_EXPLAIN_CMD,    QueryExplainCommand,    "readonly", SetFtExplainInfo,    "",           __VA_ARGS__) \
	OP(RS_EXPLAINCLI_CMD, QueryExplainCLICommand, "readonly", SetFtExplaincliInfo, "",           __VA_ARGS__) \
    OP(RS_DICT_DUMP,      DictDumpCommand,        "readonly", NULL,                "",           __VA_ARGS__) \
    OP(RS_SYNDUMP_CMD,    SynDumpCommand,         "readonly", NULL,                "",           __VA_ARGS__) \
    OP(RS_INDEX_LIST_CMD, IndexList,              "readonly", NULL,                "slow admin", __VA_ARGS__) \
    OP(RS_SYNADD_CMD,     SynAddCommand,          "write",    NULL,                "admin",      __VA_ARGS__)


// read commands
#define RS_INFO_CMD RS_CMD_READ_PREFIX ".INFO"
#define RS_SEARCH_CMD RS_CMD_READ_PREFIX ".SEARCH"
#define RS_AGGREGATE_CMD RS_CMD_READ_PREFIX ".AGGREGATE"
#define RS_PROFILE_CMD RS_CMD_READ_PREFIX ".PROFILE"
#define RS_GET_CMD RS_CMD_WRITE_PREFIX ".GET" // "write" so it won't be redirected on enterprise cluster
#define RS_MGET_CMD RS_CMD_READ_PREFIX ".MGET"
#define RS_TAGVALS_CMD RS_CMD_READ_PREFIX ".TAGVALS"
#define RS_CURSOR_CMD RS_CMD_READ_PREFIX ".CURSOR"
#define RS_DEBUG RS_CMD_READ_PREFIX ".DEBUG"
#define RS_SPELL_CHECK RS_CMD_READ_PREFIX ".SPELLCHECK"
#define RS_CONFIG RS_CMD_READ_PREFIX ".CONFIG"

// RM_TRY(RMCreateSearchCommand(ctx, RS_INFO_CMD, IndexInfoCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SEARCH_CMD, RSSearchCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, "read"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_AGGREGATE_CMD, RSAggregateCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, "read"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_PROFILE_CMD, RSProfileCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, "read"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_GET_CMD, GetSingleDocumentCommand, "readonly", INDEX_DOC_CMD_ARGS, "read admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_MGET_CMD, GetDocumentsCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", 0, 0, 0, "read admin"))
// RM_TRY(RMCreateSearchCommand(ctx, RS_TAGVALS_CMD, TagValsCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, "read admin dangerous"))
// Do not force cross slot validation since coordinator will handle it.
// RM_TRY(RMCreateSearchCommand(ctx, RS_CURSOR_CMD, RSCursorCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", 0, 0, 0, "read"));
// RM_TRY(RMCreateSearchCommand(ctx, RS_DEBUG, NULL, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", RS_DEBUG_FLAGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_SPELL_CHECK, SpellCheckCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", INDEX_ONLY_CMD_ARGS, ""))
// RM_TRY(RMCreateSearchCommand(ctx, RS_CONFIG, ConfigCommand, IsEnterprise() ? "readonly " PROXY_FILTERED : "readonly", 0, 0, 0, "admin"))
// RM_TRY_F(RegisterDebugCommands, RedisModule_GetCommand(ctx, RS_DEBUG))
#define RS_READ_ONLY_FLAGS_DEFAULT IsEnterprise() ? "readonly" PROXY_FILTERED : "readonly"
#define RS_READ_ONLY_COMMANDS(OP, ...)                                                                                                               \
  OP(RS_INFO_CMD,      IndexInfoCommand,         RS_READ_ONLY_FLAGS_DEFAULT, SetFtInfoInfo,             "",                     __VA_ARGS__) \
  OP(RS_SEARCH_CMD,    RSSearchCommand,          RS_READ_ONLY_FLAGS_DEFAULT, SetFtSearchInfo,           "read",                 __VA_ARGS__) \
  OP(RS_AGGREGATE_CMD, RSAggregateCommand,       RS_READ_ONLY_FLAGS_DEFAULT, SetFtAggregateInfo,        "read",                 __VA_ARGS__) \
  OP(RS_PROFILE_CMD,   RSProfileCommand,         RS_READ_ONLY_FLAGS_DEFAULT, SetFtProfileInfo,          "read",                 __VA_ARGS__) \
  OP(RS_GET_CMD,       GetSingleDocumentCommand, "readonly",                 NULL,                      "read admin",           __VA_ARGS__) \
  OP(RS_MGET_CMD,      GetDocumentsCommand,      RS_READ_ONLY_FLAGS_DEFAULT, NULL,                      "read admin",           __VA_ARGS__) \
  OP(RS_TAGVALS_CMD,   TagValsCommand,           RS_READ_ONLY_FLAGS_DEFAULT, SetFtTagvalsInfo,          "read admin dangerous", __VA_ARGS__) \
  OP(RS_CURSOR_CMD,    NULL,                     RS_READ_ONLY_FLAGS_DEFAULT, RegisterCursorCommands,    "read",                 __VA_ARGS__) \
  OP(RS_DEBUG,         NULL,                     RS_READ_ONLY_FLAGS_DEFAULT, RegisterAllDebugCommands,  "",                     __VA_ARGS__) \
  OP(RS_SPELL_CHECK,   SpellCheckCommand,        RS_READ_ONLY_FLAGS_DEFAULT, SetFtSpellcheckInfo,       "",                     __VA_ARGS__) \
  OP(RS_CONFIG,        NULL,                     RS_READ_ONLY_FLAGS_DEFAULT, RegisterConfigSubCommands, "admin",                __VA_ARGS__)

#ifdef RS_CLUSTER_ENTERPRISE
#define RS_OSS_WRITE_COMMANDS(OP, ...)
#else
// write commands (on enterprise we do not define them, the dmc take care of them)
// search write slow dangerous
#define RS_OSS_WRITE_COMMANDS(OP, ...)                                                                                                    \
    OP("FT.CREATE",         SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtCreateInfo,      "",                     __VA_ARGS__) \
    OP("FT._CREATEIFNX",    SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "",                     __VA_ARGS__) \
    OP("FT.ALTER",          SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtAlterInfo,       "",                     __VA_ARGS__) \
    OP("FT._ALTERIFNX",     SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "",                     __VA_ARGS__) \
    OP("FT.DROPINDEX",      SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtDropindexInfo,   "write slow dangerous", __VA_ARGS__) \
    OP("FT._DROPINDEXIFX",  SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "write slow dangerous", __VA_ARGS__) \
    OP("FT.DICTADD",        SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtDictaddInfo,     "",                     __VA_ARGS__) \
    OP("FT.DICTDEL",        SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtDictdelInfo,     "",                     __VA_ARGS__) \
    OP("FT.ALIASADD",       SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtAliasaddInfo,    "",                     __VA_ARGS__) \
    OP("FT._ALIASADDIFNX",  SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "",                     __VA_ARGS__) \
    OP("FT.ALIASDEL",       SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtAliasdelInfo,    "",                     __VA_ARGS__) \
    OP("FT._ALIASDELIFX",   SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "",                     __VA_ARGS__) \
    OP("FT.ALIASUPDATE",    SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtAliasupdateInfo, "",                     __VA_ARGS__) \
    OP("FT.SYNUPDATE",      SafeCmd(MastersFanoutCommandHandler), "readonly", SetFtSynupdateInfo,   "",                     __VA_ARGS__) \
    OP("FT.SYNFORCEUPDATE", SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "",                     __VA_ARGS__) \
    OP("FT.GET",            SafeCmd(SingleShardCommandHandler),   "readonly", NULL,                 "read admin",           __VA_ARGS__) \
    OP("FT.ADD",            SafeCmd(SingleShardCommandHandler),   "readonly", NULL,                 "write admin",          __VA_ARGS__) \
    OP("FT.DEL",            SafeCmd(SingleShardCommandHandler),   "readonly", NULL,                 "write admin",          __VA_ARGS__) \
    OP("FT.DROP",           SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "write admin",          __VA_ARGS__) \
    OP("FT._DROPIFX",       SafeCmd(MastersFanoutCommandHandler), "readonly", NULL,                 "write admin",          __VA_ARGS__)

#endif

// With coordinator we do not want to raise a move error for index commands so we do not specify
// any key.
#define INDEX_ONLY_CMD_ARGS 0, 0, 0
#define INDEX_SUG_CMD_ARGS 1, 1, 1
#define INDEX_DOC_CMD_ARGS 2, 2, 1
#define COORD_ARGS 0, 0, -1

#define RS_COMMANDS(OP)                          \
  RS_WRITE_DOC_COMMANDS(OP, INDEX_DOC_CMD_ARGS)  \
  RS_WRITE_COMMANDS(OP, INDEX_ONLY_CMD_ARGS)     \
  RS_SUG_COMMANDS(OP, INDEX_SUG_CMD_ARGS)        \
  RS_LOCAL_COMMANDS(OP, INDEX_ONLY_CMD_ARGS)     \
  RS_READ_ONLY_COMMANDS(OP, INDEX_ONLY_CMD_ARGS)

#define RS_OSS_COMMANDS(OP) RS_OSS_WRITE_COMMANDS(OP, COORD_ARGS)
