/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef REDISMOCK_H
#define REDISMOCK_H
#include "redismodule.h"

// Forward declarations for C++
#ifdef __cplusplus
#include <vector>
#include <cstdint>

struct RedisModuleIO {
  std::vector<uint8_t> buffer;
  size_t read_pos = 0;
  bool error_flag = false;
};

extern "C" {
#else
struct RedisModuleIO;
#endif

typedef int (*RMCKModuleLoadFunction)(RedisModuleCtx *, RedisModuleString **, int);

void RMCK_Bootstrap(RMCKModuleLoadFunction fn, const char **s, size_t n);

void RMCK_Notify(const char *action, int events, const char *key);

// Destroy all globals
void RMCK_Shutdown(void);

// Create a new RDB IO context for testing
RedisModuleIO *RMCK_CreateRdbIO(void);

// Free an RDB IO context
void RMCK_FreeRdbIO(RedisModuleIO *io);

// Reset RDB IO context for reuse
void RMCK_ResetRdbIO(RedisModuleIO *io);

// RDB save/load functions
void RMCK_SaveUnsigned(RedisModuleIO *io, uint64_t value);
uint64_t RMCK_LoadUnsigned(RedisModuleIO *io);
void RMCK_SaveSigned(RedisModuleIO *io, int64_t value);
int64_t RMCK_LoadSigned(RedisModuleIO *io);
void RMCK_SaveDouble(RedisModuleIO *io, double value);
double RMCK_LoadDouble(RedisModuleIO *io);
void RMCK_SaveString(RedisModuleIO *io, RedisModuleString *s);
void RMCK_SaveStringBuffer(RedisModuleIO *io, const char *str, size_t len);
RedisModuleString *RMCK_LoadString(RedisModuleIO *io);
char *RMCK_LoadStringBuffer(RedisModuleIO *io, size_t *lenptr);
int RMCK_IsIOError(RedisModuleIO *io);
RedisModuleCtx *RMCK_GetContextFromIO(RedisModuleIO *io);

#ifdef __cplusplus
}
#endif
#endif
