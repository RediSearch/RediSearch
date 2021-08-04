#pragma once
#include "redismodule.h"

void Backup_Globals();
void Restore_Globals();
void Discard_Globals_Backup();

// For rdb short read

#define LoadStringBufferAlloc_IOErrors(rdb, ptr, len, cleanup_exp)  \
do {                                                                \
  size_t tmp_len;                                                   \
  size_t *tmp_len_ptr = len ? len : &tmp_len;                       \
  char *oldbuf = RedisModule_LoadStringBuffer(rdb, tmp_len_ptr);    \
  if (RedisModule_IsIOError(rdb)) {                                 \
    cleanup_exp;                                                    \
  }                                                                 \
  RedisModule_Assert(oldbuf);                                       \
  ptr = rm_malloc(*tmp_len_ptr);                                    \
  memcpy(ptr, oldbuf, *tmp_len_ptr);                                \
  RedisModule_Free(oldbuf);                                         \
} while (0)

#define LoadStringBuffer_IOError(rdb, len_ptr, cleanup_exp)     \
  ({                                                            \
    char *res = RedisModule_LoadStringBuffer((rdb), (len_ptr)); \
    if (RedisModule_IsIOError(rdb)) {                           \
      cleanup_exp;                                              \
    }                                                           \
    (res);                                                      \
  })

#define LoadUnsigned_IOError(rdb, cleanup_exp)      \
  ({                                                \
    uint64_t res = RedisModule_LoadUnsigned((rdb)); \
    if (RedisModule_IsIOError(rdb)) {               \
      cleanup_exp;                                  \
    }                                               \
    (res);                                          \
  })

#define LoadSigned_IOError(rdb, cleanup_exp)     \
  ({                                             \
    int64_t res = RedisModule_LoadSigned((rdb)); \
    if (RedisModule_IsIOError(rdb)) {            \
      cleanup_exp;                               \
    }                                            \
    (res);                                       \
  })

#define LoadDouble_IOError(rdb, cleanup_exp)    \
  ({                                            \
    double res = RedisModule_LoadDouble((rdb)); \
    if (RedisModule_IsIOError(rdb)) {           \
      cleanup_exp;                              \
    }                                           \
    (res);                                      \
  })
