/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "disk/database_api.h"
#include "iterators/iterator_api.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Indexes a document in the disk database
 *
 * Adds a document to the inverted index for the specified index name and term.
 *
 * @param db Pointer to the disk database
 * @param indexName Name of the index to add the document to
 * @param term Term to associate the document with
 * @param docId Document ID to index
 * @param fieldMask Field mask indicating which fields are present in the document
 * @return true if the write was successful, false otherwise
 */
bool DiskDatabase_IndexDocument(DiskIndex *index, const char *term,
                                t_docId docId, t_fieldMask fieldMask);

/**
 * @brief Opaque type representing an iterator over the inverted index
 *
 * This type is used in the C API to represent an iterator instance.
 * It is implemented as a C++ class in the C++ code.
 */
typedef struct DiskIterator DiskIterator;

/**
 * @brief Creates a new iterator for the inverted index
 *
 * @param db Pointer to the disk database
 * @param indexName Name of the index to iterate
 * @param term Term within the index to iterate
 * @return Pointer to the created iterator, or NULL if creation failed
 */
DiskIterator *DiskDatabase_NewInvertedIndexIterator(DiskIndex* index, const char* term);

/**
 * @brief Advances the iterator to the next document
 *
 * Retrieves the next document from the iterator and populates the result
 * structure with its information.
 *
 * @param iter Pointer to the iterator
 * @param result Pointer to the result structure to populate
 * @return true if a document was retrieved, false if there are no more documents
 */
bool InvertedIndexIterator_Next(DiskIterator *iter, RSIndexResult* result);

/**
 *  @brief Skips the iterator to a specific document ID
 *
 * Skips the iterator to the first document with an ID greater than or equal to
 * the specified ID.
 *
 * @param iter Pointer to the iterator
 * @param docId Document ID to skip to
 * @param result Pointer to the result structure to populate
 * @return true if a document was found, false if there are no more documents
 */
bool InvertedIndexIterator_SkipTo(DiskIterator *iter, t_docId docId, RSIndexResult* result);

/**
 * @brief Rewinds the iterator to the beginning
 *
 * Resets the iterator to the first document in the inverted index.
 *
 * @param iter Pointer to the iterator to rewind
 */
void InvertedIndexIterator_Rewind(DiskIterator *iter);


/**
 * @brief Aborts the iterator
 *
 * Marks the iterator as aborted and prevents further iteration.
 *
 * @param iter Pointer to the iterator to abort
 */
void InvertedIndexIterator_Abort(DiskIterator *iter);

/**
 * @brief Returns the estimated number of documents in the iterator
 *
 * @param iter Pointer to the iterator
 * @return Estimated number of documents in the iterator
 */
size_t InvertedIndexIterator_NumEstimated(DiskIterator *iter);

/**
 * @brief Returns the number of documents in the iterator
 *
 * @param iter Pointer to the iterator
 * @return Number of documents in the iterator
 */
size_t InvertedIndexIterator_Len(DiskIterator *iter);

/**
 * @brief Returns the ID of the last document read by the iterator
 *
 * @param iter Pointer to the iterator
 * @return ID of the last document in the iterator
 */
t_docId InvertedIndexIterator_LastDocId(DiskIterator *iter);

/**
 * @brief Checks if the iterator has a next document
 *
 * @param ctx Pointer to the iterator
 * @return 1 if there is a next document, 0 otherwise
 */
int InvertedIndexIterator_HasNext(DiskIterator *iter);

/**
 * @brief Frees an iterator
 *
 * Destroys the iterator and frees all associated resources.
 *
 * @param iter Pointer to the iterator to free
 */
void InvertedIndexIterator_Free(DiskIterator *iter);

QueryIterator *NewDiskInvertedIndexIterator(DiskIndex *index, const char *term, t_fieldMask fieldMask, double weight);

#ifdef __cplusplus
}
#endif
