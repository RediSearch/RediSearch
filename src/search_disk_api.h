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
#include "VecSim/vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

// Forward declarations to avoid circular dependencies
typedef struct QueryIterator QueryIterator;

// Helper opaque types for the disk API
typedef const void* RedisSearchDisk;
typedef const void* RedisSearchDiskIndexSpec;
typedef const void* RedisSearchDiskInvertedIndex;
typedef const void* RedisSearchDiskIterator;

// Callback function to allocate memory for the key in the scope of the search module memory
typedef char* (*AllocateKeyCallback)(const void*, size_t len);

typedef struct BasicDiskAPI {
  RedisSearchDisk *(*open)(RedisModuleCtx *ctx, const char *path);
  void (*close)(RedisSearchDisk *disk);
  RedisSearchDiskIndexSpec *(*openIndexSpec)(RedisSearchDisk *disk, const char *indexName, size_t indexNameLen, DocumentType type);
  void (*closeIndexSpec)(RedisSearchDiskIndexSpec *index);
  void (*indexSpecRdbSave)(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);
  u_int32_t (*indexSpecRdbLoad)(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);
} BasicDiskAPI;

typedef struct IndexDiskAPI {
  /**
   * @brief Request the index to be deleted, once closeIndexSpec is called the index will be deleted from the disk.
   *
   * @param index Pointer to the index
   */
  void (*markToBeDeleted)(RedisSearchDiskIndexSpec *index);

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
   * @brief Deletes a document by key, looking up its doc ID, removing it from the doc table and marking its ID as deleted
   *
   * @param handle Handle to the document table
   * @param key Document key
   * @param keyLen Length of the document key
   * @param oldLen Optional pointer to receive the old document length (can be NULL)
   * @param id Optional pointer to receive the deleted document ID (can be NULL)
   */
  void (*deleteDocument)(RedisSearchDiskIndexSpec* handle, const char* key, size_t keyLen, uint32_t *oldLen, t_docId *id);

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
   * @param maxTermFreq Maximum frequency of any single term in the document
   * @param docLen Sum of the frequencies of all terms in the document
   * @param oldLen Pointer to an integer to store the length of the deleted document
   * @param ttl Document expiration time (ignored if !(flags & Document_HasExpiration))
   * @return New document ID, or 0 on error/duplicate
   */
  t_docId (*putDocument)(RedisSearchDiskIndexSpec* handle, const char* key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen, t_expirationTimePoint ttl);

  /**
   * @brief Returns whether the docId is in the deleted set
   *
   * @param handle Handle to the document table
   * @param docId Document ID to check
   * @return true if deleted, false if not deleted or on error
   */
  bool (*isDocIdDeleted)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  bool (*getDocumentMetadata)(RedisSearchDiskIndexSpec* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocateKey);

  /**
   * @brief Gets the maximum document ID assigned in the index
   *
   * @param handle Handle to the document table
   * @return The maximum document ID, or 0 if the index is empty
   */
  t_docId (*getMaxDocId)(RedisSearchDiskIndexSpec* handle);

  /**
   * @brief Gets the count of deleted document IDs
   *
   * @param handle Handle to the document table
   * @return The number of deleted document IDs
   */
  uint64_t (*getDeletedIdsCount)(RedisSearchDiskIndexSpec* handle);

  /**
   * @brief Gets all deleted document IDs (used for debugging)
   *
   * Fills the provided buffer with deleted document IDs. The caller must ensure
   * the buffer is large enough to hold all deleted IDs (use getDeletedIdsCount first).
   *
   * @param handle Handle to the document table
   * @param buffer Buffer to fill with deleted document IDs
   * @param buffer_size Size of the buffer (number of t_docId elements)
   * @return The number of IDs written to the buffer
   */
  size_t (*getDeletedIds)(RedisSearchDiskIndexSpec* handle, t_docId* buffer, size_t buffer_size);
} DocTableDiskAPI;

typedef struct VectorDiskAPI {
  /**
   * @brief Creates a disk-based vector index.
   *
   * The returned handle is a VecSimIndex* that can be used with all standard
   * VecSimIndex_* functions (AddVector, TopKQuery, etc.) due to polymorphism.
   *
   * @param index Pointer to the index spec (provides storage context)
   * @param params Vector index parameters
   * @return VecSimIndex* handle, or NULL on error
   */
  void* (*createVectorIndex)(RedisSearchDiskIndexSpec* index, const VecSimParamsDisk* params);

  /**
   * @brief Frees a disk-based vector index.
   *
   * @param vecIndex The vector index handle returned by createVectorIndex
   */
  void (*freeVectorIndex)(void* vecIndex);
} VectorDiskAPI;

/**
 * @brief Column family metrics for RocksDB/SpeeDB
 *
 * All metrics are non-string (integer) properties that can be queried efficiently.
 * These metrics are specific to individual column families (doc_table or inverted_index).
 */
typedef struct DiskColumnFamilyMetrics {
  // Memtable metrics
  uint64_t num_immutable_memtables;
  uint64_t num_immutable_memtables_flushed;
  uint64_t mem_table_flush_pending;
  uint64_t active_memtable_size;
  uint64_t size_all_mem_tables;
  uint64_t num_entries_active_memtable;
  uint64_t num_entries_imm_memtables;
  uint64_t num_deletes_active_memtable;
  uint64_t num_deletes_imm_memtables;

  // Compaction metrics
  uint64_t compaction_pending;
  uint64_t num_running_compactions;
  uint64_t num_running_flushes;
  uint64_t estimate_pending_compaction_bytes;

  // Data size estimates
  uint64_t estimate_num_keys;
  uint64_t estimate_live_data_size;
  uint64_t live_sst_files_size;

  // Version tracking
  uint64_t num_live_versions;

  // Memory usage
  uint64_t estimate_table_readers_mem;

  // TODO: Add field for deleted-ids.
} DiskColumnFamilyMetrics;

typedef struct MetricsDiskAPI {
  /**
   * @brief Collect metrics for the doc_table column family
   *
   * @param index Pointer to the index spec
   * @param metrics Pointer to the metrics structure to populate
   * @return true if successful, false on error
   */
  bool (*collectDocTableMetrics)(RedisSearchDiskIndexSpec* index, DiskColumnFamilyMetrics* metrics);

  /**
   * @brief Collect metrics for the inverted_index (fulltext) column family
   *
   * @param index Pointer to the index spec
   * @param metrics Pointer to the metrics structure to populate
   * @return true if successful, false on error
   */
  bool (*collectTextInvertedIndexMetrics)(RedisSearchDiskIndexSpec* index, DiskColumnFamilyMetrics* metrics);

  // TODO: Add db-level metrics exposure (num-snapshots etc..)
} MetricsDiskAPI;

typedef struct RedisSearchDiskAPI {
  BasicDiskAPI basic;
  IndexDiskAPI index;
  DocTableDiskAPI docTable;
  VectorDiskAPI vector;
  MetricsDiskAPI metrics;
} RedisSearchDiskAPI;

#ifdef __cplusplus
}
#endif
