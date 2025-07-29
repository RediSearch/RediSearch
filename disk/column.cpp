#include "disk/column.h"
#include <functional>
#include <rocksdb/write_batch.h>

namespace search::disk {

// Iterator class implementation
Iterator::Iterator(rocksdb::Iterator* iter)
    : iter_(iter) {
}

Iterator::~Iterator() {
}

bool Iterator::Seek(const std::string& prefix) {
    iter_->Seek(prefix);
    return iter_->Valid();
}

bool Iterator::SeekToFirst() {
    iter_->SeekToFirst();
    return iter_->Valid();
}

bool Iterator::SeekToLast() {
    iter_->SeekToLast();
    return iter_->Valid();
}

bool Iterator::Next() {
    iter_->Next();
    return iter_->Valid();
}

std::optional<std::string> Iterator::GetCurrentKey() {
    if (iter_->Valid()) {
        return std::string(iter_->key().data(), iter_->key().size());
    }
    return std::nullopt; // No valid key
}

std::optional<std::string> Iterator::GetCurrentValue() {
    if (iter_->Valid()) {
        return std::string(iter_->value().data(), iter_->value().size());
    }
    return std::nullopt;
}

Column::Column(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& handle)
    : handle_(&handle), db_(&db) {
}

Column::~Column() {
    if (handle_) {
        db_->DestroyColumnFamilyHandle(handle_);
    }
}

bool Column::Write(const std::string& key, const std::string& value) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), handle_, key, value);
    return status.ok();
}

std::optional<std::string> Column::Read(const std::string& key) {
    std::string value;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), handle_, key, &value);
    if (!status.ok()) {
        return std::nullopt; // Key not found or error
    }
    return value;
}

bool Column::Delete(const std::string& key) {
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), handle_, key);
    return status.ok();
}

}
