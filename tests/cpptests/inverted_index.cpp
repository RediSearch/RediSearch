#include "inverted_index.hpp"
#include "rocksdb/merge_operator.h"
#include <unordered_map>

using Logger = rocksdb::Logger;

class IIAssociativeMergeOperator : public rocksdb::AssociativeMergeOperator {
public:
    bool Merge(const Slice &key, const Slice *existing_value,
               const Slice &value, std::string *new_value,
               Logger *logger) const override {
        if (existing_value == nullptr) {
            *new_value = value.ToString();
        } else {
            // Concatenate the existing value with the new value
            *new_value = existing_value->ToString() + " " + value.ToString();
        }
        return true; // Return true on success (cannot fail)
    }

    const char *Name() const override {
        return "IIAssociativeMergeOperator";
    }
};

InvertedIndices *InvertedIndices::instance = nullptr; // Initialize the singleton instance

// Constructors for the class

InvertedIndices::InvertedIndices(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *cf) : db(db), cf(cf) {
    std::unordered_map<std::string, std::string> options = {{"merge_operator", "IIAssociativeMergeOperator"}};
    db->SetOptions(cf, options);
}

InvertedIndices::InvertedIndices(rocksdb::DB *db, const char *cf_name) : db(db) {
    // Create a column family
    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.merge_operator.reset(new IIAssociativeMergeOperator());
    db->CreateColumnFamily(cf_options, cf_name, &cf);
}

InvertedIndices::InvertedIndices(const char *db_path, const char *cf_name) {
    // Open the database
    rocksdb::Options options;
    options.create_if_missing = true;
    options.error_if_exists = false;
    rocksdb::DB::Open(options, db_path, &db);
    // Create a column family
    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.merge_operator.reset(new IIAssociativeMergeOperator());
    db->CreateColumnFamily(cf_options, cf_name, &cf);
}

rocksdb::AssociativeMergeOperator *getIIMergeOperator() {
    return new IIAssociativeMergeOperator();
}

// API methods for the class

Status InvertedIndices::Add(Slice &term, uint64_t doc_id) {
    // Serialize the document ID
    Slice value{std::to_string(doc_id)};
    // Merge the document ID with the existing value
    return db->Merge(rocksdb::WriteOptions(), cf, term, value);
}

InvertedIndices::Iterator InvertedIndices::Iterate(Slice &term) const {
    // Get the key-value pair from the database
    std::string value;
    Status s = db->Get(rocksdb::ReadOptions(), cf, term, &value);

    if (s.IsNotFound()) {
        // Return an iterator with an empty string
        auto it = Iterator("");
        ++it;
        return it;
    } else if (s.ok()) {
        // Return an iterator with the value
        return Iterator(value);
    } else {
        throw std::runtime_error(s.ToString());
    }
}

void InvertedIndices::Iterator::operator++() {
    // Move the pointer to the next document ID
    ptr = std::strchr(ptr, ' ');
    if (ptr != nullptr) {
        ptr++; // Skip the space character
    }
}

uint64_t InvertedIndices::Iterator::operator*() const {
    // Parse the document ID
    return std::strtoull(ptr, nullptr, 10);
}

bool InvertedIndices::Iterator::atEnd() const {
    // Check if the pointer is at the end of the string
    return ptr == nullptr;
}
