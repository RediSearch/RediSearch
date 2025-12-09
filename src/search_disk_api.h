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
#include "iterators/iterator_api.h"

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
  RedisSearchDiskIndexSpec *(*openIndexSpec)(RedisSearchDisk *disk, const char *indexName, size_t indexNameLen, DocumentType type);
  void (*closeIndexSpec)(RedisSearchDiskIndexSpec *index);
  void (*close)(RedisSearchDisk *disk);
} BasicDiskAPI;

typedef struct IndexDiskAPI {
  /**
   * @brief Indexes a document in the disk database
   *
   * Adds a document to the inverted index for the specified index name and term.
   *
   * @param index Pointer to the index
   * @param term Term to associate the document with
   * @param termLen Length of the term
   * @param docId Document ID to index
   * @param fieldMask Field mask indicating which fields are present in the document
   * @return true if the write was successful, false otherwise
   */
  bool (*indexDocument)(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask);

   /**
   * @brief Creates a new iterator for the inverted index
   *
   * @param index Pointer to the index
   * @param term Term to associate the document with
   * @param termLen Length of the term
   * @param fieldMask Field mask indicating which fields are present in the document
   * @param weight Weight for the iterator (used in scoring)
   * @return Pointer to the created iterator, or NULL if creation failed
   */
  QueryIterator *(*newTermIterator)(RedisSearchDiskIndexSpec* index, const char* term, size_t termLen, t_fieldMask fieldMask, double weight);

  /**
   * @brief Returns the number of documents in the index
   *
   * @param index Pointer to the index
   * @return Number of documents in the index
   */
  QueryIterator* (*newWildcardIterator)(RedisSearchDiskIndexSpec *index, double weight);
} IndexDiskAPI;

typedef struct DocTableDiskAPI {
  /**
   * @brief Adds a new document to the table
   *
   * Assigns a new document ID and stores the document metadata.
   * If the document key already exists, returns 0.
   *
   * @param handle Handle to the document table
   * @param key Document key
   * @param keyLen Length of the document key
   * @param score Document score (for ranking)
   * @param flags Document flags
   * @param maxFreq Maximum term frequency in the document
   * @return New document ID, or 0 on error/duplicate
   */
  t_docId (*putDocument)(RedisSearchDiskIndexSpec* handle, const char* key, size_t keyLen, float score, uint32_t flags, uint32_t maxFreq);

  /**
   * @brief Returns whether the docId is in the deleted set
   *
   * @param handle Handle to the document table
   * @param docId Document ID to check
   * @return true if deleted, false if not deleted or on error
   */
  bool (*isDocIdDeleted)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  bool (*getDocumentMetadata)(RedisSearchDiskIndexSpec* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocateKey);
} DocTableDiskAPI;

typedef struct RedisSearchDiskAPI {
  BasicDiskAPI basic;
  IndexDiskAPI index;
  DocTableDiskAPI docTable;
} RedisSearchDiskAPI;

#ifdef __cplusplus
}
#endif
