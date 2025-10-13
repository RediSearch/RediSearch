
#pragma once

// clang-format off

#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------------------------------------------------------------------
// bigredis extensions

#define REDISMODULE_METADATA_NOT_ON_SWAP 0x80

/* Notification that a key's value is added to ram (from swap or otherwise).
 * swap_key_flags has 4 bits that the module can write / read.
 * when swap_key_metadata is NOT_ON_SWAP, it means the key is not loaded from swap. */
typedef void (*RedisModuleTypeKeyAddedToDbDictFunc)(RedisModuleCtx *ctx, RedisModuleString *key, void *value, int swap_key_metadata);

/* Notification that a key's value is removed from ram (may still exist on swap).
 * when swap_key_metadata is NOT_ON_SWAP it means the key does not exist on swap.
 * return swap_key_metadata or NOT_ON_SWAP if key is to be deleted (and not to be written). */
typedef int (*RedisModuleTypeRemovingKeyFromDbDictFunc)(RedisModuleCtx *ctx, RedisModuleString *key, void *value, int swap_key_metadata, int writing_to_swap);

/* return swap_key_metadata, 0 indicates nothing to write. when out_min_expire is -1 it indicates nothing to write. */
typedef int (*RedisModuleTypeGetKeyMetadataForRdbFunc)(RedisModuleCtx *ctx, RedisModuleString *key, void *value, long long *out_min_expire, long long *out_max_expire);

#define REDISMODULE_TYPE_EXT_METHOD_VERSION 1
typedef struct RedisModuleTypeExtMethods {
    uint64_t version;
    RedisModuleTypeKeyAddedToDbDictFunc key_added_to_db_dict;
    RedisModuleTypeRemovingKeyFromDbDictFunc removing_key_from_db_dict;
    RedisModuleTypeGetKeyMetadataForRdbFunc get_key_metadata_for_rdb;
} RedisModuleTypeExtMethods;

typedef void (*RedisModuleSwapPrefetchCB)(RedisModuleCtx *ctx, RedisModuleString *key, void* user_data);

/* APIs */

REDISMODULE_API int (*RedisModule_SetDataTypeExtensions)(RedisModuleCtx *ctx, RedisModuleType *mt, RedisModuleTypeExtMethods *typemethods) REDISMODULE_ATTR;
REDISMODULE_API int (*RedisModule_SwapPrefetchKey)(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleSwapPrefetchCB fn, void *user_data, int flags) REDISMODULE_ATTR;
REDISMODULE_API int (*RedisModule_GetSwapKeyMetadata)(RedisModuleCtx *ctx, RedisModuleString *key) REDISMODULE_ATTR;
REDISMODULE_API int (*RedisModule_SetSwapKeyMetadata)(RedisModuleCtx *ctx, RedisModuleString *key, int module_metadata) REDISMODULE_ATTR;
REDISMODULE_API int (*RedisModule_IsKeyInRam)(RedisModuleCtx *ctx, RedisModuleString *key) REDISMODULE_ATTR;

//---------------------------------------------------------------------------------------------

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes.
 * NOTE: These have to be in sync with NOTIFY_* in server.h */

#define REDISMODULE_NOTIFY_TRIMMED (1<<30)     /* trimmed by reshard trimming enterprise only event */

/* Server events definitions.
 * Those flags should not be used directly by the module, instead
 * the module should use RedisModuleEvent_* variables */

#define REDISMODULE_EVENT_SHARDING 1000

static const RedisModuleEvent
    RedisModuleEvent_Sharding = {
        REDISMODULE_EVENT_SHARDING,
        1
    };

/* Those are values that are used for the 'subevent' callback argument. */

#define REDISMODULE_SUBEVENT_SHARDING_SLOT_RANGE_CHANGED 0
#define REDISMODULE_SUBEVENT_SHARDING_TRIMMING_STARTED 1
#define REDISMODULE_SUBEVENT_SHARDING_TRIMMING_ENDED 2

/* APIs */

REDISMODULE_API int (*RedisModule_ShardingGetKeySlot)(RedisModuleString *keyname) REDISMODULE_ATTR;
REDISMODULE_API void (*RedisModule_ShardingGetSlotRange)(int *first_slot, int *last_slot) REDISMODULE_ATTR;

//---------------------------------------------------------------------------------------------

#define REDISMODULE_RLEC_API_DEFS \
    REDISMODULE_GET_API(ShardingGetKeySlot); \
    REDISMODULE_GET_API(ShardingGetSlotRange); \
    REDISMODULE_GET_API(SetDataTypeExtensions); \
    REDISMODULE_GET_API(SwapPrefetchKey); \
    REDISMODULE_GET_API(GetSwapKeyMetadata); \
    REDISMODULE_GET_API(SetSwapKeyMetadata); \
    REDISMODULE_GET_API(IsKeyInRam); \
    /**/

//---------------------------------------------------------------------------------------------

#ifdef __cplusplus
}
#endif
