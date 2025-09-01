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
   * @brief Schedule an async dmd read for a given docId
   */
  void (*loadDmdAsync)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  /**
   * @brief Wait until a completion is available (or timeout_ms) and extract fields
   *
   * On success, returns 1 and outputs the dmd fields:
   *  - keyOut: pointer allocated via AllocateKeyCallback
   *  - keyLenOut: length of keyOut
   *  - scoreOut, flagsOut, maxFreqOut: basic metadata values
   * Returns 0 on timeout, -1 on error/failed read
   */
  int (*waitDmd)(RedisSearchDiskIndexSpec* handle,
                 RSDocumentMetadata* dmd,
                 long long timeout_ms,
                 AllocateKeyCallback allocateKey);

} DocTableDiskAPI;

/**
 * @brief Wait until a completion is available (or timeout_ms) and return its metadata
 *
 * On success, returns a DiskDocumentMetadata with a non-NULL key allocated via AllocateKeyCallback.
 * On timeout or failure, returns a struct with key == NULL.
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
