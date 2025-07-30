/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * @file doc_table_disk_c.cpp
 * @brief Implementation of the C interface for the disk-based document table
 *
 * This file implements the C API for the disk-based document table.
 * It provides a bridge between the C API defined in doc_table_disk_c.h
 * and the C++ implementation in doc_table_disk.hpp.
 */

#include "disk/doc_table/doc_table_disk_c.h"
#include "disk/doc_table/doc_table_disk.hpp"
#include "disk/index_iterator_adapter.h"
#include "disk/database.h"
#include "disk/document_metadata/document_metadata.hpp"
#include "disk/database_api.h"
#include <boost/endian/conversion.hpp>
#include <cstring>
#include <string>
#include <cstdlib>
#include <memory>

/**
 * @brief Retrieves the document ID for a key
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @param docIdOut Output parameter for the document ID
 * @return 1 if found, 0 if not found or on error
 */
int DocTableDisk_GetDocId(DiskDatabase* handle, const char* key, t_docId* docIdOut) {
  search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
  if (!index) return 0;
  auto docId = index->GetDocTable().getDocId(key);
  if (!docId) return 0;
  *docIdOut = docId->id;
  return 1;
}

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
t_docId DocTableDisk_Put(DiskIndex* handle, const char* key, double score, uint32_t flags, uint32_t maxFreq) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
    return index->GetDocTable().put(key, score, flags, maxFreq).id;
}

/**
 * @brief Gets the document ID for a key
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @return Document ID, or 0 if not found or on error
 */
t_docId DocTableDisk_GetId(DiskIndex* handle, const char* key) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
    auto docId = index->GetDocTable().getDocId(key);
    return docId ? docId->id : 0;
}

/**
 * @brief Returns whether the docId is in the deleted set
 *
 * @param handle Handle to the document table
 * @param docId Document ID to check
 * @return true if deleted, false if not deleted or on error
 */
bool DocTableDisk_DocIdDeleted(DiskIndex* handle, t_docId docId) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
    return index->GetDocTable().docIdDeleted(search::disk::DocumentID{docId}) ? true : false;
}

/**
 * @brief Checks if a document ID exists
 *
 * @param handle Handle to the document table
 * @param docId Document ID to check
 * @return 1 if exists, 0 if not exists or on error
 */
int DocTableDisk_Exists(DiskIndex* handle, t_docId docId) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
    return index->GetDocTable().docIdExists(search::disk::DocumentID{docId}) ? 1 : 0;
}

/**
 * @brief Checks if a document key exists
 *
 * @param handle Handle to the document table
 * @param key Document key to check
 * @return 1 if exists, 0 if not exists or on error
 */
int DocTableDisk_ExistsKey(DiskIndex* handle, const char* key) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
  return index->GetDocTable().keyExists(key) ? 1 : 0;
}

/**
 * @brief Deletes a document by key. Both the key->docId and docId->dmd entries
 * are deleted.
 *
 * @param handle Handle to the document table
 * @param key Document key
 * @return 1 if deleted, 0 if not found or on error
 */
int DocTableDisk_Del(DiskIndex* handle, const char* key) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return 0;
    return index->GetDocTable().del(key);
}

/**
 * @brief Gets the key for a document ID
 *
 * Retrieves the key associated with a document ID and allocates memory for it.
 * The caller is responsible for freeing this memory using DocTableDisk_FreeString.
 *
 * @param handle Handle to the document table
 * @param docId Document ID
 * @param keyOut Output parameter for the key (caller must free with DocTableDisk_FreeString)
 * @return 1 if found, 0 if not found or on error
 */
int DocTableDisk_GetKey(DiskIndex* handle, t_docId docId, char** keyOut) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index || !keyOut) return 0;

    auto keyOpt = index->GetDocTable().getKey(search::disk::DocumentID{docId});
    if (!keyOpt) return 0;

    const std::string& key = *keyOpt;
    *keyOut = (char*)malloc(key.size() + 1);
    if (!*keyOut) return 0;
    std::memcpy(*keyOut, key.c_str(), key.size() + 1);
    return 1;
}

int DocTableDisk_GetDmd(DiskIndex* handle, t_docId docId, RSDocumentMetadata* dmd, AllocateKeyCallback allocateKey) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index || !dmd) return 0;

    auto dmdOpt = index->GetDocTable().getDmd(search::disk::DocumentID{docId});
    if (!dmdOpt || dmdOpt->keyPtr.empty()) return 0;

    const search::disk::DocumentMetadata& dmdDisk = *dmdOpt;
    dmd->keyPtr = allocateKey(dmdDisk.keyPtr.c_str(), dmdDisk.keyPtr.size());
    dmd->id = docId;
    dmd->score = dmdDisk.score;
    dmd->maxFreq = dmdDisk.maxFreq;
    dmd->type = index->GetDocTable().getDocumentType();
    // dmd->flags = dmdDisk.flags;
    return 1;
}

/**
 * @brief Frees memory allocated for a string returned by the API
 *
 * Use this function to free strings returned by functions like DocTableDisk_GetKey.
 *
 * @param str String to free
 */
void DocTableDisk_FreeString(char* str) {
    free(str);
}

IndexIterator* DocTableDisk_NewIndexIterator(DiskIndex* handle) {
    search::disk::Database::Index* index = reinterpret_cast<search::disk::Database::Index*>(handle);
    if (!index) return nullptr;


    auto iter = index->GetDocTable().Iterate();
    if (!iter) {
        return nullptr;
    }

    auto adapter = new search::disk::IndexIteratorAdapter<search::disk::DocTableColumn::Iterator>(std::move(iter), RS_FIELDMASK_ALL);
    return &adapter->base;
}

