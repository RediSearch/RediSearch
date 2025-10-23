/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "redismodule.h"
#include <iostream>
#include <string>
#include <optional>
#include <vector>
#include <memory>
#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include "disk/column.h"
#include "disk/doc_table/doc_table_disk.hpp"
#include "disk/inverted_index/deleted_ids_compaction_filter.h"
#include "disk/memory_object.h"

// Forward declarations for RocksDB classes
namespace rocksdb {
class ColumnFamilyHandle;
class Iterator;
} // namespace rocksdb

namespace search::disk {

/**
 * @brief Main database class for disk-based storage
 *
 * This class manages a RocksDB database with multiple column families for
 * storing search index data. It provides access to the inverted index and
 * document table column families.
 */
class Database {
public:
    struct Index {
         /**
         * @brief Gets the index name
         * @return Index name
         */
        const std::string& Name() const {
            return name_;
        }

        /**
         * @brief Gets the inverted index column family
         * @return Reference to the inverted index column
         */
        Column& GetInvertedIndex() {
            return invertedIndex_;
        }

        /**
         * @brief Gets the document table column family
         * @return Reference to the document table column
         */
        DocTableColumn& GetDocTable() {
            return docTable_;
        }

        void Compact() {
            invertedIndex_.Compact();
        }

        /**
         * @brief Creates a new index
         * @param name Index name
         * @param db Reference to the RocksDB database
         * @param docType Document type - hash or json
         * @return Pointer to the created index, or nullptr if creation failed
         */
        static Index* Create(std::string name, rocksdb::DB& db, DocumentType docType);

        /**
         * @brief Creates a new index with memory object data
         * @param name Index name
         * @param db Reference to the RocksDB database
         * @param docType Document type - hash or json
         * @param maxDocId Maximum document ID from memory object
         * @param deletedIds Deleted IDs from memory object
         * @return Pointer to the created index, or nullptr if creation failed
         */
        static Index* Create(std::string name, rocksdb::DB& db, DocumentType docType,
                           t_docId maxDocId, std::shared_ptr<DeletedIds> deletedIds);
    private:
        Index(std::string name, DocTableColumn&& docTable, Column&& invertedIndex, DocumentType docType)
            : name_(name)
            , docTable_(std::move(docTable))
            , invertedIndex_(std::move(invertedIndex))
        {}

        /** Index name */
        std::string name_;

        /** Column for the inverted index */
        Column invertedIndex_;

        /** Column for the document table */
        DocTableColumn docTable_;
    };

    /*
     * @brief Creates a new database instance
     * @param ctx Redis module context for logging
     * @param db_path Path to the database directory
     * @param memory_obj Memory object containing serialized data (default: empty)
     * @return Pointer to the created database, or nullptr if creation failed
     * */
    static Database* Create(RedisModuleCtx* ctx, const std::string& db_path,
                           const MemoryObject& memory_obj = MemoryObject{});

    /**
     * @brief Destructor
     *
     * Closes the database and cleans up resources
     */
    ~Database();

    /**
     * @brief Opens an index
     * @param indexName Name of the index
     * @param docType Document type
     * @return Pointer to the index, or nullptr if creation failed
     */
    Index* OpenIndex(const std::string& indexName, DocumentType docType) {
        auto it = indexes_.find(indexName);
        if (it != indexes_.end()) {
            return it->second.get();
        }
        std::unique_ptr<Index> index(Index::Create(indexName, *db_, docType));
        if (!index) {
            return nullptr;
        }
        auto result = indexes_.emplace(indexName, std::move(index));
        return result.first->second.get();
    }

    /**
     * @brief Opens an index with memory object data
     * @param indexName Name of the index
     * @param docType Document type
     * @param maxDocId Maximum document ID from memory object
     * @param deletedIds Deleted IDs from memory object
     * @return Pointer to the index, or nullptr if creation failed
     */
    Index* OpenIndex(const std::string& indexName, DocumentType docType,
                     t_docId maxDocId, std::shared_ptr<DeletedIds> deletedIds) {
        auto it = indexes_.find(indexName);
        if (it != indexes_.end()) {
            return it->second.get();
        }
        std::unique_ptr<Index> index(Index::Create(indexName, *db_, docType, maxDocId, deletedIds));
        if (!index) {
            return nullptr;
        }
        auto result = indexes_.emplace(indexName, std::move(index));
        return result.first->second.get();
    }

    /**
     * @brief Get all indexes in the database
     * @return Const reference to the indexes map
     */
    const std::unordered_map<std::string, std::unique_ptr<Index>>& GetIndexes() const {
        return indexes_;
    }
private:
    /**
     * @brief Constructs a database with the specified path
     * @param ctx Redis module context for logging
     * @param db Pointer to the RocksDB database
     */
    Database(RedisModuleCtx* ctx, std::unique_ptr<rocksdb::DB> db);

    /** Redis module context for logging */
    RedisModuleCtx* ctx_;

    /** Pointer to the RocksDB database */
    std::unique_ptr<rocksdb::DB> db_;

    /** Map of index name to index */
    std::unordered_map<std::string, std::unique_ptr<Index>> indexes_;
};

/**
 * @brief Create a MemoryObject from an existing Database
 * @param database Database to extract memory state from
 * @return MemoryObject containing the database's memory state, should be moved
 */
MemoryObject CreateMemoryObjectFromDatabase(const Database& database);

} // namespace search::disk
