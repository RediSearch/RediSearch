#include "DocTable.hpp"
#include <stdexcept>
#include <rocksdb/slice.h>
#include <string>    // Include for std::to_string
#include <iostream>  // Include for std::cout
#include <memory>  // Include for std::unique_ptr

/**
 * @brief Constructor for DDocTable.
 * @param db Pointer to the RocksDB database.
 * @param cf Pointer to the ColumnFamilyHandle.
 */
DDocTable::DDocTable(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf) : db(db), cf(cf), curr_id(INVALID_DOC_ID) {}

/**
 * @brief Destructor for DDocTable.
 */
DDocTable::~DDocTable() {
    // No need to delete db or cf, as they are managed externally
}

/**
 * @brief Remove the document with the given key.
 * @param key The key of the document to remove.
 */
void DDocTable::Remove(const std::string& key) {
    rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), cf, key);
    if (!status.ok()) {
        throw std::runtime_error("Failed to delete key: " + status.ToString());
    }
}

/**
 * @brief Create a new document with the given key.
 * @param key The key for the new document.
 */
docId_t DDocTable::CreateDoc(const std::string& key) {
    uint64_t docId = ++curr_id;  // Increment the current document ID
    std::string docIdStr = std::to_string(docId);  // Convert docId to string

    // Debug-log
    // std::cout << "Creating document with key: " << key << " and ID: " << docIdStr << std::endl;

    // Store the key-docId pair in the database
    rocksdb::Status status = db->Put(rocksdb::WriteOptions(), cf, key, docIdStr);
    if (!status.ok()) {
        throw std::runtime_error("Failed to put key-docId pair: " + status.ToString());
    }

    // Store the docId-key pair in the database
    status = db->Put(rocksdb::WriteOptions(), cf, docIdStr, key);
    if (!status.ok()) {
        throw std::runtime_error("Failed to put docId-key pair: " + status.ToString());
    }
    return docId;
}

/**
 * @brief Get the document ID associated with the given key.
 * @param key The key to look up.
 * @param snapshot The snapshot to use for the read operation. Default is
 * `nullptr`, in which case no snapshot is used.
 * @return The document ID associated with the key, or INVALID_DOC_ID if not found.
 */
docId_t DDocTable::Get(const std::string& key, const rocksdb::Snapshot* snapshot) {
    rocksdb::ReadOptions read_options;
    if (snapshot != nullptr) {
        read_options.snapshot = snapshot;
    }

    std::string value;
    rocksdb::Status status = db->Get(read_options, this->cf, key, &value);
    if (!status.ok()) {
        return INVALID_DOC_ID;
    }
    return std::stoull(value);
}

/**
 * @brief Get the document ID associated with the given key using a snapshot.
 * @param key The key to look up.
 * @param shot The snapshot to use for the read operation. Default is
 * `nullptr`, in which case a snapshot is made within the function.
 * @return The document ID associated with the key, or INVALID_DOC_ID if not found.
 */
docId_t DDocTable::GetWithSnapshot(const std::string& key, const rocksdb::Snapshot* shot) {
    const rocksdb::Snapshot* snapshot = shot ? shot : db->GetSnapshot();
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot;

    std::string value;
    rocksdb::Status status = db->Get(read_options, cf, key, &value);
    if (!shot) {
        db->ReleaseSnapshot(snapshot);
    }

    if (!status.ok()) {
        return INVALID_DOC_ID;
    }
    return std::stoull(value);
}

/**
 * @brief Get the key associated with the given document ID.
 * @param docId The document ID to look up.
 * @param shot The snapshot to use for the read operation. Default is
 * `nullptr`, in which case a snapshot is made within the function.
 * @return The key associated with the document ID, or an empty string if not found.
 */
const std::string DDocTable::GetKey(docId_t docId, const rocksdb::Snapshot* shot) {
    const rocksdb::Snapshot* snapshot = shot ? shot : db->GetSnapshot();
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot;

    std::string docIdStr = std::to_string(docId);
    std::string value;
    rocksdb::Status status = db->Get(read_options, cf, docIdStr, &value);
    if (!shot) {
        db->ReleaseSnapshot(snapshot);
    }

    if (!status.ok()) {
        return "";
    }
    return value;
}

/**
 * @brief Get the key associated with a given document ID if the ID is valid.
 * A key is valid if the key maps to the given document ID. Otherwise, this
 * document ID is no longer valid.
 * @param docId The document ID to look up.
 * @param key[out] The key associated with the document ID if the ID is valid,
 * or an empty string if not found.
 * @param shot The snapshot to use for the read operation (default is
 * nullptr, in which case a snapshot is made within the function).
 * @return True if the document ID is valid, false otherwise.
 */
bool DDocTable::GetKeyIfValid(docId_t docId, std::string& key, const rocksdb::Snapshot* shot) {
    const rocksdb::Snapshot* snapshot = shot ? shot : db->GetSnapshot();
    rocksdb::ReadOptions read_options;
    read_options.snapshot = snapshot;

    const std::string keyStr = GetKey(docId, snapshot);
    if (Get(keyStr, snapshot) != docId) {
        key = "";
        return false;
    }
    key = keyStr;
    return true;
}

/**
 * @brief Print all documents in the DDocTable.
 */
void DDocTable::Print() {
    std::cout << "\n--------------------------" << std::endl;
    std::cout << "Printing DDocTable:" << std::endl;
    std::cout << "--------------------------" << std::endl;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions(), cf));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::cout << "Key: " << it->key().ToString() << " Value: " << it->value().ToString() << std::endl;
    }
    std::cout << "--------------------------" << std::endl;
    std::cout << "Printing DDocTable finished" << std::endl;
    std::cout << "--------------------------\n" << std::endl;
}
