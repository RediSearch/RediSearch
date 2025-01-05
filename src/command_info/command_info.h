/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

// This file is generated by gen_command_info.py
#pragma once
#include "redismodule.h"

int SetFtCreateInfo(RedisModuleCommand *cmd);
int SetFtInfoInfo(RedisModuleCommand *cmd);
int SetFtExplainInfo(RedisModuleCommand *cmd);
int SetFtExplaincliInfo(RedisModuleCommand *cmd);
int SetFtAlterInfo(RedisModuleCommand *cmd);
int SetFtDropindexInfo(RedisModuleCommand *cmd);
int SetFtAliasaddInfo(RedisModuleCommand *cmd);
int SetFtAliasupdateInfo(RedisModuleCommand *cmd);
int SetFtAliasdelInfo(RedisModuleCommand *cmd);
int SetFtTagvalsInfo(RedisModuleCommand *cmd);
int SetFtSugaddInfo(RedisModuleCommand *cmd);
int SetFtSuggetInfo(RedisModuleCommand *cmd);
int SetFtSugdelInfo(RedisModuleCommand *cmd);
int SetFtSuglenInfo(RedisModuleCommand *cmd);
int SetFtSynupdateInfo(RedisModuleCommand *cmd);
int SetFtSyndumpInfo(RedisModuleCommand *cmd);
int SetFtSpellcheckInfo(RedisModuleCommand *cmd);
int SetFtDictaddInfo(RedisModuleCommand *cmd);
int SetFtDictdelInfo(RedisModuleCommand *cmd);
int SetFtDictdumpInfo(RedisModuleCommand *cmd);
int SetFt_ListInfo(RedisModuleCommand *cmd);
int SetFtConfigSetInfo(RedisModuleCommand *cmd);
int SetFtConfigGetInfo(RedisModuleCommand *cmd);
int SetFtConfigHelpInfo(RedisModuleCommand *cmd);
int SetFtSearchInfo(RedisModuleCommand *cmd);
int SetFtAggregateInfo(RedisModuleCommand *cmd);
int SetFtProfileInfo(RedisModuleCommand *cmd);
int SetFtCursorReadInfo(RedisModuleCommand *cmd);
int SetFtCursorDelInfo(RedisModuleCommand *cmd);
