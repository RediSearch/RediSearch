/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * @file doc_table_disk_c.h
 * @brief C interface for the disk-based document table
 *
 * This file provides a C API for the disk-based document table implementation.
 * It allows C code to interact with the C++ DocTableDisk class, which provides
 * persistent storage for document metadata using a key-value store.
 *
 * The document table maps document keys to document IDs and stores document
 * metadata for each document ID. This implementation uses disk storage for
 * persistence.
 */

#pragma once
#include <stdint.h>
#include "redisearch.h"
#include "disk/database_api.h"
#include "iterators/iterator_api.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Initialize the CRoaring library to use Redis allocators
 *
 * This function sets up the CRoaring library to use Redis allocators
 * for all memory operations. It should be called before any Roaring
 * bitmap operations are performed.
 */
void initRoaringWithRedisAllocators();

/**
 * @brief Retrieves the document ID for a key
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param docIdOut Output parameter for the document ID
 * @return 1 if found, 0 if not found or on error
 */
int DocTableDisk_GetDocId(DiskDatabase* handle, const char* key, t_docId* docIdOut);

/**
 * @brief Adds a new document to the table
 *
 * Assigns a new document ID and stores the document metadata.
 * If the document key already exists, returns 0.
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param score Document score (for ranking)
 * @param flags Document flags
 * @param maxFreq Maximum term frequency in the document
 * @return New document ID, or 0 on error/duplicate
 */
t_docId DocTableDisk_Put(DiskIndex* handle, const char* key, double score, uint32_t flags, uint32_t maxFreq);

/**
 * @brief Gets the document ID for a key
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @return Document ID, or 0 if not found or on error
 */
t_docId DocTableDisk_GetId(DiskIndex* handle, const char* key);

/**
 * @brief Returns whether the docId is in the deleted set
 *
 * @param handle Handle to the document table
 * @param docId Document ID to check
 * @return true if deleted, false if not deleted or on error
 */
bool DocTableDisk_DocIdDeleted(DiskIndex* handle, t_docId docId);


/**
 * @brief Deletes a document by key
 *
 * Removes both the key->docId mapping and the docId->metadata mapping.
 * Also adds the docId to the deleted set.
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @return 1 if deleted, 0 if not found or on error
 */
int DocTableDisk_Del(DiskIndex* handle, const char* key);

/**
 * @brief Gets the key for a document ID
 *
 * @param handle Handle to the document table
 * @param docId Document ID
 * @param keyOut Output parameter for the key (caller must free with DocTableDisk_FreeString)
 * @return 1 if found, 0 if not found or on error
 */
int DocTableDisk_GetKey(DiskIndex* handle, t_docId docId, char** keyOut);

typedef char* (*AllocateKeyCallback)(const void*, size_t len);

/**
 * @brief Fills in the document metadata for a document ID
 *
 * @param handle Handle to the document table
 * @param dmd Document metadata to fill in
 * @return 1 if found, 0 if not found or on error
 */
int DocTableDisk_GetDmd(DiskIndex* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocateKey);

/**
 * @brief Frees memory allocated for a string returned by the API
 *
 * Use this function to free strings returned by functions like DocTableDisk_GetKey.
 *
 * @param str String to free
 */
void DocTableDisk_FreeString(char* str);

typedef struct DocTableIterator DocTableIterator;

/**
 * @brief Creates a new iterator for the document table
 *
 * @param handle Handle to disk index
 * @param weight Weight for the iterator (used in scoring)
 * @return Iterator for the document table, or NULL on error
 */
QueryIterator* DocTableDisk_NewQueryIterator(DiskIndex* handle, double weight);


#ifdef __cplusplus
}
#endif
