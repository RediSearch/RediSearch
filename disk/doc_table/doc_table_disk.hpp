/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <string>
#include <cstdint>
#include <memory>
#include <optional>
#include <atomic>
#include <rocksdb/db.h>
#include "disk/column.h"
#include "disk/document.h"
#include "disk/document_id.h"
#include "disk/document_metadata/document_metadata.hpp"
#include "redisearch.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"

namespace search::disk {

/**
 * @brief Document table implementation using disk storage
 *
 * This class provides a persistent document table that maps document keys
 * to document IDs and stores document metadata. It uses the DiskDatabase
 * API for storage.
 */
class DocTableColumn {
public:
    /**
     * @brief Constructs a document table using the specified database
     *
     * @param db Pointer to the disk database
     */
    DocTableColumn(Column column, DocumentType docType, std::shared_ptr<DeletedIds> deletedIds);

    DocTableColumn(DocTableColumn&& other) noexcept;

    /**
     * @brief Destructor
     */
    ~DocTableColumn();

    /**
     * @brief Adds a new document to the table
     *
     * Assigns a new document ID and stores the document metadata.
     *
     * @param key Document key
     * @param score Document score
     * @param flags Document flags
     * @param maxFreq Maximum term frequency
     * @return New document ID, or 0 on error/duplicate
     */
    DocumentID put(const ::std::string& key, double score, uint32_t flags, uint32_t maxFreq);

    /**
     * @brief Checks if a document ID exists in the disk table
     *
     * @param docId Document ID to check
     * @return true if exists, false otherwise
     */
    bool docIdExists(DocumentID docId);

    /**
     * @brief Checks if a document ID is in the deleted set
     *
     * @param docId Document ID to check
     * @return true if deleted, false otherwise
     */
    bool docIdDeleted(DocumentID docId);

    /**
     * @brief Checks if a document key exists
     *
     * @param key Document key to check
     * @return true if exists, false otherwise
     */
    bool keyExists(const ::std::string& key);

    /**
     * @brief Deletes a document by key
     *
     * Removes the key->docId mapping and the docId->metadata mapping.
     * Also adds the docId to the deleted set.
     *
     * @param key Document key
     * @return true if deleted, false otherwise
     */
    bool del(const std::string& key);

    /**
     * @brief Retrieves the document ID for a key
     *
     * @param key Document key
     * @return Document ID as optional, or std::nullopt if not found
     */
    std::optional<DocumentID> getDocId(const ::std::string& key);

    /**
     * @brief Retrieves document metadata for a document ID
     *
     * @param docId Document ID
     * @return Document metadata as optional, or std::nullopt if not found
     */
    std::optional<DocumentMetadata> getDmd(DocumentID docId);

    /**
     * @brief Gets the key for a document ID
     *
     * @param docId Document ID
     * @return Document key as optional, or std::nullopt if not found
     */
    std::optional<::std::string> getKey(DocumentID docId);

    DocumentType getDocumentType() const { return docType_; }

    struct Iterator {
        std::optional<Document> Next();

        bool HasNext() const;

        std::optional<DocumentID> LastDocId();

        size_t EstimateNumResults();

        std::optional<Document> SkipTo(DocumentID docId);

        void Abort();

        void Rewind();

        static Iterator* Create(rocksdb::Iterator* iter);
    private:
        Iterator(std::unique_ptr<search::disk::Iterator> it, size_t countEstimation);

        DocumentID lastDocId_{0};
        size_t countEstimation_;
        std::unique_ptr<search::disk::Iterator> iter_;
    };

    std::unique_ptr<Iterator> Iterate() {
        return std::unique_ptr<Iterator>(column_.template CreateIterator<Iterator>());
    }

private:
    /**
     * @brief Maps a key to a document ID
     *
     * @param key Document key
     * @param docId Document ID to associate with the key
     * @return true if successful, false otherwise
     */
    bool putKeyToDocId(const ::std::string& key, DocumentID docId);

    /**
     * @brief Stores document metadata for a document ID
     *
     * @param docId Document ID
     * @param dmd Document metadata to store
     * @return true if successful, false otherwise
     */
    bool putDocIdToDmd(DocumentID docId, const DocumentMetadata& dmd);

    /** Key type, either json or hash, based on DocumentType enum*/
    DocumentType docType_;

    /**
     * @brief Creates a database key for storing document metadata
     *
     * @param docId Document ID
     * @return Database key with "d:" prefix
     */
    static std::string makeDocIdKey(DocumentID docId);

    /**
     * @brief Creates a database key for storing key-to-docId mapping
     *
     * @param key Document key
     * @return Database key with "k:" prefix
     */
    static std::string makeKeyToDocIdKey(const std::string& key);

    /** Pointer to the disk database */
    search::disk::Column column_;

    /** The deleted IDs container */
    std::shared_ptr<DeletedIds> deletedIds_;

    /**
     * Track maximum document ID assigned
     * Using atomic to ensure thread safety when multiple threads access this variable
     */
    std::atomic<t_docId> maxDocId_{0};
};

} // namespace search::disk
