/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

// Implementation for DocTableDisk using the database_api
#include "disk/doc_table/doc_table_disk.hpp"
#include "disk/document_metadata/document_metadata.hpp"
#include "disk/database.h"
#include "redisearch.h"
#include "rmutil/rm_assert.h"
#include <boost/endian/conversion.hpp>
#include <cstring>
#include <cstdint>
#include <cassert>
#include <sstream>

namespace search::disk {

/**
 * @brief Constructs a document table using the specified database
 *
 * @param db Pointer to the disk database
 */
DocTableColumn::DocTableColumn(Column column, DocumentType docType, std::shared_ptr<DeletedIds> deletedIds)
    : column_(std::move(column)), docType_(docType), deletedIds_(deletedIds) {
    // TODO: Recover the max_docId once persistance is supported.
}

DocTableColumn::DocTableColumn(DocTableColumn&& other) noexcept
    : column_(std::move(other.column_)), docType_(other.docType_), deletedIds_(std::move(other.deletedIds_)) {
    maxDocId_ = other.maxDocId_.load();
}

/**
 * @brief Destructor
 */
DocTableColumn::~DocTableColumn() = default;

/**
 * @brief Adds a new document to the table
 *
 * Assigns a new document ID and stores the document metadata.
 * If the document key already exists, returns 0.
 *
 * @param key Document key
 * @param score Document score
 * @param flags Document flags
 * @param maxFreq Maximum term frequency
 * @param replace Whether to replace an existing document with the same key
 * @return New document ID, or 0 on error/duplicate
 */
DocumentID DocTableColumn::put(const std::string& key, double score, uint32_t flags, uint32_t maxFreq) {
    auto existingDocId = getDocId(key);

    // Add the deleted docId to the deleted set (if it exists), so that readers
    // will not get this docId in the read pipeline.
    if (existingDocId) {
        deletedIds_->add(existingDocId->id);
    }

    DocumentID docId{++maxDocId_};
    DocumentMetadata dmd{key, score, maxFreq, flags};

    // Prepare the key-value pairs for the batch operation
    const std::string keyToDocIdKey = makeKeyToDocIdKey(key);
    std::ostringstream keyToDocIdValueStream;
    docId.SerializeAsValue(keyToDocIdValueStream);
    const std::string keyToDocIdValue = keyToDocIdValueStream.str();

    const std::string docIdToDmdKey = makeDocIdKey(docId);
    const std::string docIdToDmdValue = dmd.serialize();

    // Execute the batch operation using WriteBatchT to avoid heap allocations
    bool success = column_.WriteBatchT([&](rocksdb::WriteBatch& batch, rocksdb::ColumnFamilyHandle& handle) {
        // If replacing an existing document, delete its metadata entry
        if (existingDocId) {
            batch.Delete(&handle, makeDocIdKey(*existingDocId));
        }

        // Add the new document metadata entry
        batch.Put(&handle, docIdToDmdKey, docIdToDmdValue);

        // Add or update the key to docId mapping
        batch.Put(&handle, keyToDocIdKey, keyToDocIdValue);
    });

    if (!success) {
        --maxDocId_;
        return DocumentID{0};
    }

    return docId;
}

/**
 * @brief Checks if a document ID exists in the disk table
 *
 * @param docId Document ID to check
 * @return true if exists, false otherwise
 */
bool DocTableColumn::docIdExists(DocumentID docId) {
    return getDmd(docId).has_value();
}

/**
 * @brief Checks if a document ID is in the deleted set
 *
 * @param docId Document ID to check
 * @return true if deleted, false otherwise
 */
bool DocTableColumn::docIdDeleted(DocumentID docId) {
    return deletedIds_->contains(docId.id);
}

/**
 * @brief Checks if a document key exists
 *
 * @param key Document key to check
 * @return true if exists, false otherwise
 */
bool DocTableColumn::keyExists(const std::string& key) {
    return getDocId(key).has_value();
}

/**
 * @brief Deletes a document by key
 *
 * Removes the key->docId mapping and the docId->metadata mapping.
 * Also adds the docId to the deleted set.
 *
 * @param key Document key
 * @return true if deleted, false otherwise
 */
bool DocTableColumn::del(const std::string& key) {
    auto docIdOpt = getDocId(key);
    if (!docIdOpt) return false;  // Key doesn't exist

    DocumentID docId = *docIdOpt;
    // Execute the batch operation using WriteBatchT to avoid heap allocations
    bool success = column_.WriteBatchT([&](rocksdb::WriteBatch& batch, rocksdb::ColumnFamilyHandle& handle) {
        // Delete the key->docId mapping
        batch.Delete(&handle, makeKeyToDocIdKey(key));

        // Delete the docId->metadata mapping
        batch.Delete(&handle, makeDocIdKey(docId));
    });

    // If the operation was successful, add the docId to the deleted set
    if (success) {
        if (!deletedIds_->add(docId.id)) {
            RedisModule_Log(RSDummyContext, "warning", "Failed to add docId %lu to deleted set", docId.id);
            return false;
        }
    }

    return success;
}

/**
 * @brief Retrieves the document ID for a key
 *
 * Looks up a document key and retrieves its associated document ID.
 *
 * @param key Document key
 * @return Document ID, or std::nullopt if not found
 */
std::optional<DocumentID> DocTableColumn::getDocId(const std::string& key) {
    // Use the doc_table column family
    std::optional<std::string> value = column_.Read(makeKeyToDocIdKey(key));
    if (!value || value->size() != sizeof(t_docId)) return std::nullopt;

    // Read the big-endian docId and convert to native format
    return DocumentID::DeserializeFromValue<false>(*value);
}

/**
 * @brief Retrieves document metadata for a document ID
 *
 * Looks up a document ID and retrieves its associated metadata.
 *
 * @param docId Document ID
 * @return Document metadata as optional, or std::nullopt if not found
 */
std::optional<DocumentMetadata> DocTableColumn::getDmd(DocumentID docId) {
    // Use the doc_table column family
    std::optional<std::string> value = column_.Read(makeDocIdKey(docId));
    if (!value) return std::nullopt;

    return DocumentMetadata::deserialize(*value);
}

/**
 * @brief Gets the key for a document ID
 *
 * @param docId Document ID
 * @return Document key as optional, or std::nullopt if not found
 */
std::optional<std::string> DocTableColumn::getKey(DocumentID docId) {
    auto dmdOpt = getDmd(docId);
    if (!dmdOpt) return std::nullopt;
    return dmdOpt->keyPtr;
}

std::optional<Document> DocTableColumn::Iterator::Next() {
    if (!iter_->Valid()) {
        return std::nullopt;
    }

    static constexpr std::array<char, 2> prefixArray = {'d', ':'};
    std::string_view prefix{prefixArray.data(), prefixArray.size()};
    auto id = DocumentID::DeserializeFromKey(iter_->GetCurrentKey().value(), prefix);
    if (!id) {
        return std::nullopt;
    }
    lastDocId_ = *id;
    iter_->Next();
    return Document{lastDocId_, RS_FIELDMASK_ALL};
}

bool DocTableColumn::Iterator::HasNext() const {
    return iter_->Valid() && iter_->GetCurrentKey().value().starts_with("d:");
}

std::optional<DocumentID> DocTableColumn::Iterator::LastDocId() {
    return lastDocId_;
}
size_t DocTableColumn::Iterator::EstimateNumResults() {
    return countEstimation_;
}
std::optional<Document> DocTableColumn::Iterator::SkipTo(DocumentID docId) {
    if (!iter_->Valid()) {
        return std::nullopt;
    }
    std::stringstream stream;
    stream << "d:";
    docId.SerializeAsKey(stream);
    const std::string key = stream.str();
    iter_->Seek(key);
    return Next();
}

void DocTableColumn::Iterator::Abort() {
    iter_->SeekToLast();
    if (iter_->Valid()) {
        iter_->Next();
    }
}

void DocTableColumn::Iterator::Rewind() {
    lastDocId_ = DocumentID{0};
    iter_->Seek("d:");
}

DocTableColumn::Iterator* DocTableColumn::Iterator::Create(rocksdb::Iterator* iter) {
    std::unique_ptr<rocksdb::Iterator> iterPtr(iter);
    if (iter == nullptr) {
        return nullptr;
    }
    iter->Seek("d:");
    if (!iter->Valid()) {
        return nullptr;
    }

    std::string_view startKeyView = iter->key().ToStringView();
    // we want to get an estimation for the number of documents the doc table has
    // we take the start id and then will take the last do id
    // we will return the difference between the two + 1.
    std::optional<DocumentID> startId = DocumentID::DeserializeFromKey(startKeyView, "d:");
    if (!startId) {
        return nullptr;
    }

    // Seek to last element which matches the given prefix
    std::stringstream stream;
    stream << "d" << char(':' + 1);  // ';' is the next char after ':'
    const auto end = stream.str();
    iter->Seek(end);
    if (!iter->Valid()) {
        iter->SeekToLast();
    } else {
        iter->Prev();
    }

    // get the last id
    std::string_view endKeyView = iter->key().ToStringView();
    std::optional<DocumentID> endId = DocumentID::DeserializeFromKey(endKeyView, "d:");
    if (!endId) {
        return nullptr;
    }

    iter->Seek("d:");
    const size_t countEstimation = endId->id - startId->id + 1;
    return new Iterator(std::unique_ptr<::search::disk::Iterator>(::search::disk::Iterator::Create(iterPtr.release())), countEstimation);
}

DocTableColumn::Iterator::Iterator(std::unique_ptr<search::disk::Iterator> it, size_t countEstimation)
    : iter_(std::move(it)), countEstimation_(countEstimation) {
}

/**
 * @brief Maps a key to a document ID
 *
 * Stores a mapping from a document key to its document ID in the database.
 *
 * @param key Document key
 * @param docId Document ID to associate with the key
 * @return true if successful, false otherwise
 */
bool DocTableColumn::putKeyToDocId(const std::string& key, DocumentID docId) {
    std::stringstream value;
    docId.SerializeAsValue(value);
    return column_.Write(makeKeyToDocIdKey(key), value.str());
}

/**
 * @brief Stores document metadata for a document ID
 *
 * Serializes and stores document metadata for a specific document ID.
 *
 * @param docId Document ID
 * @param dmd Document metadata to store
 * @return true if successful, false otherwise
 */
bool DocTableColumn::putDocIdToDmd(DocumentID docId, const DocumentMetadata& dmd) {
    // Use the doc_table column family
    std::string value = dmd.serialize();
    return column_.Write(makeDocIdKey(docId), value);
}

/**
 * @brief Creates a database key for storing document metadata
 *
 * @param docId Document ID
 * @return Database key with "d:" prefix
 */
std::string DocTableColumn::makeDocIdKey(DocumentID docId) {
    std::stringstream stream;
    stream << "d:";
    docId.SerializeAsKey(stream);
    return stream.str();
}

/**
 * @brief Creates a database key for storing key-to-docId mapping
 *
 * @param key Document key
 * @return Database key with "k:" prefix
 */
std::string DocTableColumn::makeKeyToDocIdKey(const std::string& key) {
    return "k:" + key;
}

} // namespace search::disk
