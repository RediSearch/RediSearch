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

// Forward declaration for HiddenString
typedef struct HiddenString HiddenString;

// Helper opaque types for the disk API
typedef const void* RedisSearchDisk;
typedef const void* RedisSearchDiskIndexSpec;
typedef const void* RedisSearchDiskInvertedIndex;
typedef const void* RedisSearchDiskIterator;
typedef void* RedisSearchDiskAsyncReadPool;
typedef const void* RedisSearchDiskRdbState;

// Callback function to allocate memory for the key in the scope of the search module memory
typedef char* (*AllocateKeyCallback)(const void*, size_t len);

// Callback function to allocate a new RSDocumentMetadata with ref_count=1 and keyPtr set
typedef RSDocumentMetadata* (*AllocateDMDCallback)(const void* key_data, size_t key_len);

// Callback functions for applying text compaction delta updates.
// The C side owns private_data/update_ctx semantics; Rust treats them as opaque.
typedef struct SearchDiskCompactionCallbacks {
  // Opens an update session and returns opaque update context.
  // Implementations may acquire internal locks here.
  void *(*beginUpdate)(void *private_data);

  // Decrement term doc count in the serving trie.
  bool (*decrementTrieTermCount)(
      void *update_ctx,
      const char *term,
      size_t term_len,
      size_t doc_count_decrement);

  // Decrement numTerms in scoring stats.
  void (*decrementNumTerms)(void *update_ctx, uint64_t num_terms_removed);

  // Closes an update session.
  // Implementations may release internal locks here.
  void (*endUpdate)(void *update_ctx);
} SearchDiskCompactionCallbacks;

// Result of polling the async read pool
typedef struct AsyncPollResult {
  uint16_t ready_count;   // Number of successful reads in results buffer
  uint16_t failed_count;  // Number of failed reads in failed_user_data buffer
  uint16_t pending_count; // Number of reads still in flight
} AsyncPollResult;

// Result structure containing both DMD and user data (for successful reads only)
typedef struct AsyncReadResult {
  RSDocumentMetadata *dmd;  // Pointer to allocated DMD (caller must free with DMD_Return)
  uint64_t user_data;       // Generic user data passed to addAsyncRead (e.g., index, pointer, flags)
} AsyncReadResult;

typedef struct BasicDiskAPI {
  /**
   * @brief Open the disk storage context
   * @param ctx Redis module context
   * @param buffer_percentage Percentage of available memory to use for write buffer (0-100)
   * @param logObfuscation true to enable obfuscation, false to disable
   * @return Pointer to the disk context, or NULL on error
   */
  RedisSearchDisk *(*open)(RedisModuleCtx *ctx, int buffer_percentage, bool logObfuscation);
  void (*close)(RedisModuleCtx *ctx, RedisSearchDisk *disk);

  /**
   * @brief Enable or disable obfuscation of index names and field names in Disk log output
   * @param disk Pointer to the disk
   * @param enable true to enable obfuscation, false to disable
   */
  void (*setLogObfuscation)(RedisSearchDisk *disk, bool enable);

  /**
   * @brief Open an index spec
   * @param ctx Redis module context for BigModule APIs (required for getting DB path)
   * @param disk Pointer to the disk
   * @param indexName Name of the index
   * @param obfuscatedName Obfuscated name of the index (for logging)
   * @param obfuscatedNameLen Length of the obfuscated name
   * @param type Document type
   * @param deleteBeforeOpen If true, delete any existing data before opening
   * @return Pointer to the index spec, or NULL on error
   *
   * @note This opens the database but does NOT register it with Redis. Call registerIndex after this
   *       to register with BigModule APIs.
   */
  RedisSearchDiskIndexSpec *(*openIndexSpec)(RedisModuleCtx *ctx, RedisSearchDisk *disk, const HiddenString *indexName, const char *obfuscatedName, size_t obfuscatedNameLen, DocumentType type, bool deleteBeforeOpen);
  /**
   * @brief Close an index spec
   * @param disk Pointer to the disk context (for cleanup of index metrics)
   * @param index Pointer to the index spec
   *
   * @note This closes the database but does NOT unregister from Redis. Call unregisterIndex
   *       before this to unregister from BigModule APIs.
   */
  void (*closeIndexSpec)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);
  /**
   * @brief Register an index's database with Redis BigModule APIs
   * @param ctx Redis module context (required, must be valid)
   * @param index Pointer to the index spec
   *
   * @note Must be called from the main thread with a valid RedisModuleCtx.
   *       Call this after openIndexSpec to register the database with Redis.
   */
  void (*registerIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);
  /**
   * @brief Unregister an index's database from Redis BigModule APIs
   * @param ctx Redis module context (required, must be valid)
   * @param index Pointer to the index spec
   *
   * @note Must be called from the main thread with a valid RedisModuleCtx.
   *       Call this before closeIndexSpec to unregister the database from Redis.
   */
  void (*unregisterIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index);
  void (*indexSpecRdbSave)(RedisModuleIO *rdb, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Check if async I/O is supported by the underlying storage engine
   * @param disk Pointer to the disk
   * @return true if async I/O operations are available, false otherwise
   */
  bool (*isAsyncIOSupported)(RedisSearchDisk *disk);

  /**
   * @brief Set throttle callbacks for vector disk tiered indexes to pause/resume CMD_DENYOOM commands.
   * @param enable Callback to pause CMD_DENYOOM commands (wraps RedisModule_EnablePostponeClients)
   * @param disable Callback to resume CMD_DENYOOM commands (wraps RedisModule_DisablePostponeClients)
   */
  void (*setThrottleCallbacks)(ThrottleCB enable, ThrottleCB disable);

  /**
   * @brief Load disk-related RDB data into a temporary in-memory object.
   *
   * Called during RDB load when the IndexSpec cannot be created yet (e.g., during replication
   * before SST files arrive). The returned state must later be passed to openIndexSpecWithRdbState
   * or freed with freeRdbState.
   *
   * @param rdb The RedisModuleIO handle for RDB operations
   * @return Pointer to temporary RDB state, or NULL on error
   */
  RedisSearchDiskRdbState *(*loadRdbToTempObject)(RedisModuleIO *rdb);

  /**
   * @brief Create an IndexSpec and restore state from a previously loaded RDB state.
   *
   * Called after SST files are ready (e.g., after FULL_REPLICATION_FINISHED event).
   * Takes ownership of rdbState - it will be consumed and freed.
   *
   * @param disk Pointer to the disk context
   * @param indexName Name of the index
   * @param obfuscatedName Obfuscated name of the index (for logging)
   * @param obfuscatedNameLen Length of the obfuscated name
   * @param type Document type for this index
   * @param rdbState Temporary RDB state from loadRdbToTempObject (will be consumed)
   * @return Pointer to the created IndexSpec, or NULL on error
   */
  RedisSearchDiskIndexSpec *(*openIndexSpecWithRdbState)(RedisModuleCtx *ctx,
                                                          RedisSearchDisk *disk,
                                                          const HiddenString *indexName,
                                                          const char *obfuscatedName,
                                                          size_t obfuscatedNameLen,
                                                          DocumentType type,
                                                          RedisSearchDiskRdbState *rdbState);

  /**
   * @brief Free a temporary RDB state object without creating an IndexSpec.
   *
   * Use if index creation fails or is cancelled.
   *
   * @param rdbState The temporary RDB state to free (may be NULL)
   */
  void (*freeRdbState)(RedisSearchDiskRdbState *rdbState);


  /**
   * @brief Update the buffer budget and WBM in response to RAM configuration changes.
   *
   * This function requests a new buffer budget from Redis via BigWriteBufferBudgetInit
   * and updates the WriteBufferManager with the new size.
   *
   * @param ctx Redis module context
   * @param disk Pointer to the disk context
   * @param percentage Percentage of available memory to request (0-100)
   * @return The new buffer budget in bytes, or 0 on error. Use this value to update
   *         existing indexes via updateWriteBufferSize.
   */
  size_t (*updateBufferBudget)(RedisModuleCtx *ctx, RedisSearchDisk *disk, int percentage);
} BasicDiskAPI;

typedef struct IndexDiskAPI {
  /**
   * @brief Request the index to be deleted, once closeIndexSpec is called the index will be deleted from the disk.
   *
   * @param index Pointer to the index
   */
  void (*markToBeDeleted)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Indexes a term for fulltext search
   *
   * Adds a document to the inverted index for the specified term.
   * Used for fulltext field indexing.
   *
   * @param index Pointer to the index
   * @param term Term to associate the document with
   * @param termLen Length of the term
   * @param docId Document ID to index
   * @param fieldMask Field mask indicating which fields are present in the document
   * @param freq Frequency of the term in the document
   * @param offsets Pointer to varint-encoded term offset data (can be NULL)
   * @param offsetsLen Length of the offsets data in bytes
   * @return true if the write was successful, false otherwise
   */
  bool (*indexTerm)(RedisSearchDiskIndexSpec *index, const char *term, size_t termLen, t_docId docId, t_fieldMask fieldMask, uint32_t freq, const uint8_t *offsets, size_t offsetsLen);

  /**
   * @brief Indexes multiple tag values for a document
   *
   * Adds a document to the inverted index for each specified tag value.
   * Used for tag field indexing. Creates a new column family if this is the
   * first time indexing this tag field, and registers it with Redis BigModule.
   *
   * @param ctx Redis module context for BigModule APIs (used to register new CFs)
   * @param index Pointer to the index
   * @param values Array of tag values to associate the document with.
   *               NOTE: The array may contain NULL entries (e.g., from tokenization).
   *               Implementations must check for NULL before dereferencing each entry.
   * @param numValues Number of tag values in the array
   * @param docId Document ID to index
   * @param fieldIndex Field index for the tag field
   * @return true if the write was successful, false otherwise
   */
  bool (*indexTags)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index, const char **values, size_t numValues, t_docId docId, t_fieldIndex fieldIndex);

  /**
   * @brief Deletes a document by its doc ID directly, removing it from the doc table and marking its ID as deleted
   *
   * Used by the metadata unlink callback where the docId is already known
   * (no key-to-docId lookup needed).
   *
   * @param handle Handle to the document table
   * @param docId Document ID to delete
   * @param oldLen Optional pointer to receive the old document length (can be NULL)
   * @return true if the document was found and deleted, false if not found
   */
  bool (*deleteDocumentById)(RedisSearchDiskIndexSpec* handle, t_docId docId, uint32_t *oldLen);

   /**
   * @brief Creates a new iterator for the inverted index
   *
   * @param index Pointer to the index
   * @param term Pointer to the query term (contains term string, idf, bm25_idf)
   * @param fieldMask Field mask indicating which fields are present in the document
   * @param weight Weight for the iterator (used in scoring)
   * @param needsOffsets Whether the query needs term offset data (for scoring or phrase matching)
   * @return Pointer to the created iterator, or NULL if creation failed
   */
  QueryIterator *(*newTermIterator)(RedisSearchDiskIndexSpec* index, RSQueryTerm* term, t_fieldMask fieldMask, double weight, bool needsOffsets);

  /**
   * @brief Creates a new iterator for a tag index
   *
   * @param index Pointer to the index
   * @param tok Pointer to the token (contains tag string and length)
   * @param fieldIndex Field index for the tag field
   * @param weight Weight for the iterator (used in scoring)
   * @return Pointer to the created iterator, or NULL if creation failed
   */
  QueryIterator *(*newTagIterator)(RedisSearchDiskIndexSpec* index, const RSToken* tok, t_fieldIndex fieldIndex, double weight);

  /**
   * @brief Run a GC compaction cycle on the disk index.
   *
   * Synchronously runs a full compaction on the inverted index column family,
   * removing entries for deleted documents. Also applies the compaction delta
   * to update in-memory structures via the provided callback table.
   *
   * @param index Pointer to the disk index
   * @param callbacks Callback table for applying compaction delta updates
   * @param private_data Opaque pointer owned by caller and passed into beginUpdate
   *
   * @return Number of deletedIDs removed from the disk index
   */
  size_t (*runGC)(RedisSearchDiskIndexSpec *index,
                  const SearchDiskCompactionCallbacks *callbacks,
                  void *private_data);

  /**
   * @brief Get the total disk usage for this index.
   *
   * @param index Pointer to the disk index
   * @return Total disk usage in bytes
   */
  uint64_t (*getDiskUsage)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Flush all to disk
   *
   * Forces all memory to be flushed to disk
   *
   * @param index Pointer to the disk index
   */
  void (*flush)(RedisSearchDiskIndexSpec *index);

  /**
   * @brief Update the write buffer size for this index's database
   *
   * Dynamically changes the write_buffer_size option for all column families
   * in this index's database. Should be called after updateBufferBudget to
   * propagate the new per-index buffer size (budget / divisor).
   *
   * @param index Pointer to the disk index
   * @param new_budget New total buffer budget in bytes (will be divided internally)
   */
  void (*updateWriteBufferSize)(RedisSearchDiskIndexSpec *index, size_t new_budget);
} IndexDiskAPI;

typedef struct DocTableDiskAPI {
  /**
   * @brief Adds a new document to the table
   *
   * Assigns a new document ID and stores the document metadata.
   * If oldDocId is provided (non-zero), the old document is marked as deleted.
   *
   * @param handle Handle to the document table
   * @param key Document key
   * @param keyLen Length of the document key
   * @param score Document score (for ranking)
   * @param flags Document flags
   * @param maxTermFreq Maximum frequency of any single term in the document
   * @param docLen Sum of the frequencies of all terms in the document
   * @param oldLen Pointer to an integer to store the length of the deleted document
   * @param documentTtl Document expiration time (must be positive if Document_HasExpiration flag is set; must be 0 and is ignored if the flag is not set)
   * @param oldDocId Old document ID from DocIdMeta (0 if new document)
   * @return New document ID, or 0 on error
   */
  t_docId (*putDocument)(RedisSearchDiskIndexSpec* handle, const char* key, size_t keyLen, float score, uint32_t flags, uint32_t maxTermFreq, uint32_t docLen, uint32_t *oldLen, t_expirationTimePoint documentTtl, t_docId oldDocId);

  /**
   * @brief Returns whether the docId is in the deleted set
   *
   * @param handle Handle to the document table
   * @param docId Document ID to check
   * @return true if deleted, false if not deleted or on error
   */
  bool (*isDocIdDeleted)(RedisSearchDiskIndexSpec* handle, t_docId docId);

  /**
   * @brief Gets document metadata by document ID
   *
   * @param handle Handle to the document table
   * @param docId Document ID
   * @param dmd Pointer to the document metadata structure to populate
   * @param allocate_key Callback to allocate memory for the key
   * @param expiration_point Current time for expiration check, or NULL to skip expiration check.
   * @return true if found and not expired, false if not found, expired, or on error
   */
  bool (*getDocumentMetadata)(RedisSearchDiskIndexSpec* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocate_key, const t_expirationTimePoint* expiration_point);

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

  /**
   * @brief Creates an async read pool for batched document metadata reads
   *
   * The pool allows adding async read requests up to a maximum concurrency limit,
   * and polling for completed results. This enables I/O parallelism for query processing.
   *
   * @param handle Handle to the index
   * @param max_concurrent Maximum number of concurrent pending reads
   * @return Opaque handle to the pool, or NULL on error. Must be freed with freeAsyncReadPool.
   */
  RedisSearchDiskAsyncReadPool (*createAsyncReadPool)(RedisSearchDiskIndexSpec* handle, uint16_t max_concurrent);

  /**
   * @brief Adds an async read request to the pool for the given document ID
   *
   * @param pool Pool handle from createAsyncReadPool
   * @param docId Document ID to read
   * @param user_data Generic user data to associate with this read (returned in AsyncReadResult)
   * @return true if the request was added, false if the pool is at capacity
   */
  bool (*addAsyncRead)(RedisSearchDiskAsyncReadPool pool, t_docId docId, uint64_t user_data);

  /**
   * @brief Polls the pool for ready results
   *
   * Checks for completed async reads and fills two buffers:
   * - results: successful reads with valid DMDs
   * - failed_user_data: user_data pointers for reads that failed or found no document
   *
   * Both buffers are required and must have capacity > 0. Polling stops when either buffer
   * is full, so callers should size buffers appropriately for their use case.
   *
   * @param pool Pool handle from createAsyncReadPool
   * @param timeout_ms 0 for non-blocking, >0 to wait up to that many milliseconds
   * @param results Buffer to fill with successful AsyncReadResult structures (DMD + user_data)
   * @param results_capacity Size of the results buffer (must be > 0)
   * @param failed_user_data Buffer to fill with user_data from failed reads (not found/error)
   * @param failed_capacity Size of the failed_user_data buffer (must be > 0)
   * @param expiration_point Current time for expiration check.
   * @param allocate_dmd Callback to allocate a new RSDocumentMetadata with ref_count=1 and keyPtr
   * @return AsyncPollResult with counts of ready, failed, and pending reads
   */
  AsyncPollResult (*pollAsyncReads)(RedisSearchDiskAsyncReadPool pool, uint32_t timeout_ms,
                                    AsyncReadResult* results, uint16_t results_capacity,
                                    uint64_t* failed_user_data, uint16_t failed_capacity,
                                    t_expirationTimePoint expiration_point,
                                    AllocateDMDCallback allocate_dmd);

  /**
   * @brief Frees the async read pool and cancels any pending reads
   *
   * @param pool Pool handle from createAsyncReadPool
   */
  void (*freeAsyncReadPool)(RedisSearchDiskAsyncReadPool pool);

  /**
   * @brief Replaces the key name in document metadata for a given document ID
   *
   * Used when a Redis key is renamed - updates the document metadata to reflect
   * the new key name while keeping the same document ID and all other metadata
   * (score, flags, expiration, etc.) unchanged.
   *
   * @param handle Handle to the document table
   * @param docId Document ID whose key should be replaced
   * @param newKey New key name
   * @param newKeyLen Length of the new key
   * @return true if the document was found and updated, false if not found or on error
   */
  bool (*replaceKey)(RedisSearchDiskIndexSpec* handle, t_docId docId, const char* newKey, size_t newKeyLen);
} DocTableDiskAPI;

typedef struct VectorDiskAPI {
  /**
   * @brief Creates a disk-based vector index.
   *
   * The returned handle is a VecSimIndex* that can be used with all standard
   * VecSimIndex_* functions (AddVector, TopKQuery, etc.) due to polymorphism.
   *
   * @param ctx Redis module context for BigModule APIs
   * @param index Pointer to the index spec (provides storage context)
   * @param params Vector index parameters
   * @return VecSimIndex* handle, or NULL on error
   */
  void* (*createVectorIndex)(RedisModuleCtx *ctx, RedisSearchDiskIndexSpec* index, const VecSimParamsDisk* params);

  /**
   * @brief Frees a disk-based vector index.
   *
   * @param vecIndex The vector index handle returned by createVectorIndex
   */
  void (*freeVectorIndex)(void* vecIndex);
} VectorDiskAPI;

typedef struct MetricsDiskAPI {
  /**
   * @brief Collect metrics for an index and store them in the disk context
   *
   * Collects metrics for both doc_table and inverted_index column families
   * and stores them in an internal map keyed by the index pointer.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return The total memory used by this index's disk components (for accumulation into total_mem)
   */
  uint64_t (*collectIndexMetrics)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total doc table memory for a specific index
   *
   * Returns disk-side doc table memory in bytes from the latest collected snapshot.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Doc table memory in bytes
   */
  uint64_t (*getDocTableTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total inverted index memory for a specific index
   *
   * Returns disk-side inverted index memory in bytes from the latest collected snapshot.
   * This value includes both text and tag inverted indexes.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Inverted index memory in bytes
   */
  uint64_t (*getInvertedIndexTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get total vector index memory for a specific index
   *
   * Returns disk-side vector index memory in bytes from the latest collected snapshot.
   * Does not include RAM-only accounting from non-disk paths.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Vector index memory in bytes
   */
  uint64_t (*getVectorIndexTotalMemory)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Get the disk-owned total number of records for a specific index
   *
   * Returns the disk-side num_records counter used by FT.INFO.
   *
   * @param disk Pointer to the disk context
   * @param index Pointer to the index spec
   * @return Number of records in the index
   */
  uint64_t (*getNumRecords)(RedisSearchDisk *disk, RedisSearchDiskIndexSpec *index);

  /**
   * @brief Output aggregated disk metrics to Redis INFO
   *
   * Iterates over all collected index metrics, aggregates them, and outputs
   * to the Redis INFO context using RedisModule_Info* functions.
   *
   * @param disk Pointer to the disk context
   * @param ctx Redis module info context
   */
  void (*outputInfoMetrics)(RedisSearchDisk *disk, RedisModuleInfoCtx *ctx);
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
