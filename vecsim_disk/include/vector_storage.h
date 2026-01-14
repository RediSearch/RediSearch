/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

/**
 * @file vector_storage.h
 * @brief Storage interface for disk-based vector indexes.
 *
 * ============================================================================
 * NOTE: SKELETON API - SUBJECT TO CHANGE
 * This interface exists to decouple HNSWDiskIndex from storage implementation,
 * enabling mock injection for unit testing. The API will evolve as MOD-13164
 * progresses.
 * ============================================================================
 */

#include <cstring>
#include <memory>
#include <string>

using labelType = size_t;

/**
 * @brief Abstract interface for vector storage.
 *
 * Allows HNSWDiskIndex to work with any storage backend (SpeedB, mocks, etc.)
 */
class VectorStore {
public:
    virtual ~VectorStore() = default;

    virtual bool put(labelType label, const void* data, size_t size) = 0;
    virtual bool get(labelType label, void* data, size_t size) const = 0;
    virtual bool del(labelType label) = 0;
};

// Forward declarations for SpeedB/RocksDB C API
typedef struct rocksdb_t rocksdb_t;
typedef struct rocksdb_column_family_handle_t rocksdb_column_family_handle_t;
typedef struct rocksdb_iterator_t rocksdb_iterator_t;
typedef struct rocksdb_readoptions_t rocksdb_readoptions_t;
typedef struct rocksdb_writeoptions_t rocksdb_writeoptions_t;

// RocksDB C API functions (linked at runtime)
extern "C" {
extern char* rocksdb_get_cf(rocksdb_t* db, const rocksdb_readoptions_t* options, rocksdb_column_family_handle_t* cf,
                            const char* key, size_t keylen, size_t* vallen, char** errptr);
extern void rocksdb_put_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* cf,
                           const char* key, size_t keylen, const char* val, size_t vallen, char** errptr);
extern void rocksdb_delete_cf(rocksdb_t* db, const rocksdb_writeoptions_t* options, rocksdb_column_family_handle_t* cf,
                              const char* key, size_t keylen, char** errptr);
extern rocksdb_iterator_t* rocksdb_create_iterator_cf(rocksdb_t* db, const rocksdb_readoptions_t* options,
                                                      rocksdb_column_family_handle_t* cf);
extern void rocksdb_iter_destroy(rocksdb_iterator_t* iter);
extern void rocksdb_iter_seek_to_first(rocksdb_iterator_t* iter);
extern unsigned char rocksdb_iter_valid(const rocksdb_iterator_t* iter);
extern void rocksdb_iter_next(rocksdb_iterator_t* iter);
extern const char* rocksdb_iter_key(const rocksdb_iterator_t* iter, size_t* klen);
extern const char* rocksdb_iter_value(const rocksdb_iterator_t* iter, size_t* vlen);
extern rocksdb_readoptions_t* rocksdb_readoptions_create(void);
extern void rocksdb_readoptions_destroy(rocksdb_readoptions_t* opt);
extern rocksdb_writeoptions_t* rocksdb_writeoptions_create(void);
extern void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t* opt);
extern void rocksdb_free(void* ptr);
}

/**
 * @brief SpeedB/RocksDB implementation of VectorStore.
 */
class SpeeDBStore : public VectorStore {
public:
    SpeeDBStore(rocksdb_t* db, rocksdb_column_family_handle_t* cf)
        : db_(db), cf_(cf), readOpts_(rocksdb_readoptions_create()), writeOpts_(rocksdb_writeoptions_create()) {}

    ~SpeeDBStore() override {
        if (readOpts_)
            rocksdb_readoptions_destroy(readOpts_);
        if (writeOpts_)
            rocksdb_writeoptions_destroy(writeOpts_);
    }

    bool put(labelType label, const void* data, size_t size) override {
        char* err = nullptr;
        std::string key = labelToKey(label);
        rocksdb_put_cf(db_, writeOpts_, cf_, key.data(), key.size(), static_cast<const char*>(data), size, &err);
        if (err) {
            rocksdb_free(err);
            return false;
        }
        return true;
    }

    bool get(labelType label, void* data, size_t size) const override {
        char* err = nullptr;
        size_t vallen = 0;
        std::string key = labelToKey(label);
        char* val = rocksdb_get_cf(db_, readOpts_, cf_, key.data(), key.size(), &vallen, &err);
        if (err) {
            rocksdb_free(err);
            return false;
        }
        if (!val)
            return false;
        if (vallen != size) {
            rocksdb_free(val);
            return false;
        }
        std::memcpy(data, val, size);
        rocksdb_free(val);
        return true;
    }

    bool del(labelType label) override {
        char* err = nullptr;
        std::string key = labelToKey(label);
        rocksdb_delete_cf(db_, writeOpts_, cf_, key.data(), key.size(), &err);
        if (err) {
            rocksdb_free(err);
            return false;
        }
        return true;
    }

private:
    rocksdb_t* db_;
    rocksdb_column_family_handle_t* cf_;
    rocksdb_readoptions_t* readOpts_;
    rocksdb_writeoptions_t* writeOpts_;

    static std::string labelToKey(labelType label) {
        return std::string(reinterpret_cast<const char*>(&label), sizeof(labelType));
    }
};

// Forward declaration for SpeeDBHandles (defined in vecsim_disk_api.h)
struct SpeeDBHandles;

// Creates a SpeeDBStore from handles. Defined in speedb_store.cpp.
// This function requires linking against SpeedB/RocksDB.
// For unit tests, a stub in speedb_store_stub.cpp returns nullptr.
std::unique_ptr<VectorStore> CreateSpeeDBStore(const SpeeDBHandles* handles);
