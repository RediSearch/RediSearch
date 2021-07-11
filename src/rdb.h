#ifndef REDISEARCH_RDB_H
#define REDISEARCH_RDB_H

#define RedisModule_LoadStringBufferAlloc(rdb, ptr, len)          \
do {                                                              \
  size_t tmp_len;                                                 \
  size_t *tmp_len_ptr = len ? len : &tmp_len;                     \
  char *oldbuf = RedisModule_LoadStringBuffer(rdb, tmp_len_ptr);  \
  ptr = rm_malloc(*tmp_len_ptr);                                  \
  memcpy(ptr, oldbuf, *tmp_len_ptr);                              \
  RedisModule_Free(oldbuf);                                       \
} while (0)

// For rdb short read

#define LoadStringBufferAlloc_IOErrors(rdb, ptr, len, cleanup_exp)  \
do {                                                                \
  size_t tmp_len;                                                   \
  size_t *tmp_len_ptr = len ? len : &tmp_len;                       \
  char *oldbuf = RedisModule_LoadStringBuffer(rdb, tmp_len_ptr);    \
  if (RedisModule_IsIOError(rdb)) {                                 \
    cleanup_exp;                                                    \
  }                                                                 \
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

#endif  // REDISEARCH_RDB_H
