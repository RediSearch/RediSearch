/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <cstdint>
#include "redismodule.h"
#include "redisearch.h"
#include "disk/doc_table/deleted_ids/deleted_ids.hpp"

namespace search::disk {

/**
 * @brief Container for memory-resident data that needs RDB serialization
 *
 * This class holds all the in-memory structures that need to be persisted
 * to RDB and later combined with the disk database path during Database::Create.
 *
 * Currently supports:
 * - Index metadata (name, document type)
 * - Deleted document IDs (roaring bitmap)
 * - Maximum document ID per index
 */
class MemoryObject {
public:
    /**
     * @brief Information about an index that needs to be preserved
     */
    struct IndexInfo {
        std::string name;
        DocumentType docType;
        t_docId maxDocId;
        std::shared_ptr<DeletedIds> deletedIds;

        IndexInfo() : name(""), docType(DocumentType_Hash), maxDocId(0), deletedIds(nullptr) {}

        IndexInfo(const std::string& n, DocumentType dt, t_docId maxId, std::shared_ptr<DeletedIds> delIds)
            : name(n), docType(dt), maxDocId(maxId), deletedIds(delIds) {}

    private:
        /**
         * @brief Serialize a single IndexInfo instance to RDB format
         * @param rdb Redis module IO handle for RDB operations
         */
        void SerializeToRDB(RedisModuleIO* rdb) const;

        /**
         * @brief Deserialize a single IndexInfo instance from RDB format
         * @param rdb Redis module IO handle for RDB operations
         * @return true on success, false on error
         */
        bool DeserializeFromRDB(RedisModuleIO* rdb);

        friend class MemoryObject;
    };

    /**
     * @brief Default constructor creates empty memory object
     */
    MemoryObject() = default;

    /**
     * @brief Copy constructor
     */
    MemoryObject(const MemoryObject&) = default;

    /**
     * @brief Move constructor
     */
    MemoryObject(MemoryObject&&) = default;

    /**
     * @brief Copy assignment operator
     */
    MemoryObject& operator=(const MemoryObject&) = default;

    /**
     * @brief Move assignment operator
     */
    MemoryObject& operator=(MemoryObject&&) = default;

    /**
     * @brief Add index information to memory object
     * @param name Index name
     * @param docType Document type (hash or json)
     * @param maxDocId Maximum document ID for this index
     * @param deletedIds Shared pointer to deleted IDs container
     */
    void AddIndex(const std::string& name, DocumentType docType, t_docId maxDocId,
                  std::shared_ptr<DeletedIds> deletedIds);

    /**
     * @brief Get all stored indexes
     * @return Map of index name to index information
     */
    const std::unordered_map<std::string, IndexInfo>& GetIndexes() const { return indexes_; }

    /**
     * @brief Get index information by name
     * @param name Index name to look up
     * @return Pointer to IndexInfo if found, nullptr otherwise
     */
    const IndexInfo* GetIndex(const std::string& name) const;

    /**
     * @brief Check if memory object is empty
     * @return true if no indexes are stored, false otherwise
     */
    bool IsEmpty() const { return indexes_.empty(); }

    /**
     * @brief Serialize to RDB format
     * @param rdb Redis module IO handle for RDB operations
     */
    void SerializeToRDB(RedisModuleIO* rdb) const;

    /**
     * @brief Deserialize from RDB format
     * @param rdb Redis module IO handle for RDB operations
     * @return Unique pointer to deserialized MemoryObject, or nullptr on error
     */
    static std::unique_ptr<MemoryObject> DeserializeFromRDB(RedisModuleIO* rdb);

    /**
     * @brief Get the number of stored indexes
     * @return Number of indexes in this memory object
     */
    size_t GetIndexCount() const { return indexes_.size(); }

private:
    std::unordered_map<std::string, IndexInfo> indexes_;
};

} // namespace search::disk
