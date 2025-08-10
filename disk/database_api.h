/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "redismodule.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Opaque type representing a disk database
 *
 * This type is used in the C API to represent a disk database instance.
 * It is implemented as a C++ class in the C++ code.
 */
typedef struct DiskDatabase DiskDatabase;

/**
 * @brief Deletes a disk database
 *
 * Removes all files associated with the database at the specified path.
 *
 * @param ctx Redis module context for logging
 * @param db_path Path to the database directory
 */
void DiskDatabase_Delete(RedisModuleCtx *ctx, const char *db_path);

/**
 * @brief Creates or opens a disk database
 *
 * @param ctx Redis module context for logging
 * @param db_path Path to the database directory
 * @return Pointer to the created database, or NULL if creation failed
 */
DiskDatabase *DiskDatabase_Create(RedisModuleCtx *ctx, const char *db_path);

/**
 * @brief Destroys a disk database instance
 *
 * Closes the database and frees all associated resources.
 *
 * @param db Pointer to the database to destroy
 */
void DiskDatabase_Destroy(DiskDatabase *db);

/**
 * @brief Callback function type for listing keys
 *
 * This function is called for each key in the database when listing keys.
 *
 * @param key The key string
 * @param ctx User-provided context pointer
 */
typedef void (*DiskDatabase_ListKeysCallback)(const char *key, void *ctx);

typedef struct DiskIndex DiskIndex;
/**
 * @brief Opens an index
 *
 * @param db Pointer to the database
 * @param indexName Name of the index
 * @param docType Document type
 * @return Pointer to the index, or NULL if creation failed
 */
DiskIndex* DiskDatabase_OpenIndex(DiskDatabase *db, const char *indexName, DocumentType docType);

/**
 * @brief Lists all keys in the database
 *
 * Iterates through all keys in the database and calls the provided callback
 * function for each key.
 *
 * @param db Pointer to the database
 * @param callback Function to call for each key
 * @param ctx User-provided context pointer passed to the callback
 */
void DiskDatabase_ListKeys(DiskIndex *db, DiskDatabase_ListKeysCallback callback, void *ctx);

/**
 * @brief Compacts an index
 *
 * @param idx Pointer to the index
 */
void DiskDatabase_CompactIndex(DiskIndex *idx);

#ifdef __cplusplus
}
#endif