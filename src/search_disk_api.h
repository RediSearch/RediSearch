/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redismodule.h"
#include "redisearch.h"
#include "index_iterator.h"

#ifdef __cplusplus
extern "C" {
#endif

// Helper opaque types for the disk API
typedef const void* RedisSearchDisk;
typedef const void* RedisSearchDiskIndexSpec;
typedef const void* RedisSearchDiskInvertedIndex;
typedef const void* RedisSearchDiskIterator;

// Callback function to allocate memory for the key in the scope of the search module memory
typedef char* (*AllocateKeyCallback)(const void*, size_t len);

typedef struct BasicDiskAPI {
  RedisSearchDisk *(*open)(RedisModuleCtx *ctx, const char *path);
  RedisSearchDiskIndexSpec *(*openIndexSpec)(RedisSearchDisk *disk, const char *indexName, DocumentType type);
  void (*closeIndexSpec)(RedisSearchDiskIndexSpec *index);
  void (*close)(RedisSearchDisk *disk);
} BasicDiskAPI;

typedef struct IndexDiskAPI {
  /**
   * @brief Indexes a document in the disk database
   */
  bool (*indexDocument)(RedisSearchDiskIndexSpec *index, const char *term, t_docId docId, t_fieldMask fieldMask);

  /**
   * @brief Creates a new iterator for the inverted index
   */
  IndexIterator *(*newTermIterator)(RedisSearchDiskIndexSpec* index, const char* term, t_fieldMask fieldMask);

  /**
   * @brief Returns an iterator over all docs
   */
  IndexIterator* (*newWildcardIterator)(RedisSearchDiskIndexSpec *index);
} IndexDiskAPI;

typedef struct DocTableDiskAPI {
  /**
   * @brief Adds a new document to the table
   */
  t_docId (*putDocument)(RedisSearchDiskIndexSpec* handle, const char* key, double score, uint32_t flags, uint32_t maxFreq);

  /**
   * @brief Returns whether the docId is in the deleted set
   */
  bool (*isDocIdDeleted)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  /**
   * @brief Synchronous metadata fetch (legacy)
   */
  bool (*getDocumentMetadata)(RedisSearchDiskIndexSpec* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocateKey);

  /**
   * @brief Schedule an async document metadata read for a given docId
   *
   * Initiates an asynchronous read operation for document metadata from disk.
   * The operation is queued and will be processed in the background.
   */
  void (*loadDmdAsync)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  /**
   * @brief Wait for an async document metadata read to complete
   *
   * Blocks until a previously scheduled async read operation completes or timeout expires.
   * On success, populates the provided RSDocumentMetadata structure with the document's
   * metadata including key, score, flags, and other attributes.
   *
   * @param handle Handle to the document table
   * @param dmd Output parameter - RSDocumentMetadata structure to populate
   * @param timeout_ms Maximum time to wait in milliseconds
   * @param allocateKey Callback function to allocate memory for the document key
   * @return 1 on success, 0 on timeout, -1 on error/failed read
   */
  bool (*waitDmd)(RedisSearchDiskIndexSpec* handle,
                 RSDocumentMetadata* dmd,
                 long long timeout_ms,
                 AllocateKeyCallback allocateKey);

} DocTableDiskAPI;

/**
 * @brief Wait for an async document metadata read to complete
 *
 * Blocks until a previously scheduled async read operation completes or the timeout expires.
 * On success, populates the provided RSDocumentMetadata structure with the document's
 * metadata including key, score, flags, and other attributes.
 *
 * @param handle Handle to the document table
 * @param dmd Output parameter - RSDocumentMetadata structure to populate
 * @param timeout_ms Maximum time to wait in milliseconds
 * @param allocateKey Callback function to allocate memory for the document key
 *
 * @note On timeout or error, dmd->keyPtr will be set to NULL
 * @note The document key is allocated via the provided AllocateKeyCallback and
 *       should be freed by the caller when no longer needed
 */
void SearchDisk_WaitDmd(RedisSearchDiskIndexSpec* handle,
                                        RSDocumentMetadata* dmd,
                                        long long timeout_ms,
                                        AllocateKeyCallback allocateKey);

typedef struct RedisSearchDiskAPI {
  BasicDiskAPI basic;
  IndexDiskAPI index;
  DocTableDiskAPI docTable;
} RedisSearchDiskAPI;

#define RedisSearchDiskAPI_LATEST_API_VER 1
#ifdef __cplusplus
}
#endif
