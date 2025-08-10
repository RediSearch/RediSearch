#pragma once
#include <rocksdb/db.h>
#include <optional>
#include "rmutil/rm_assert.h"

namespace search::disk {
/**
 * @brief Wrapper around RocksDB iterator for traversing database entries
 *
 * This class provides a simplified interface for iterating through RocksDB
 * entries, with methods for seeking to specific keys and retrieving
 * key-value pairs.
 */
class Iterator {
public:
    /**
     * @brief Constructs an iterator from a RocksDB iterator
     * @param iter Pointer to a RocksDB iterator (ownership is transferred)
     */
    Iterator(rocksdb::Iterator* iter);

    /**
     * @brief Destructor
     */
    ~Iterator();

    /**
     * @brief Positions the iterator at the first key that is >= the specified prefix
     * @param prefix The key prefix to seek to
     * @return true if a valid position was found, false otherwise
     */
    bool Seek(const std::string& prefix);

    /**
     * @brief Positions the iterator at the first key in the database
     * @return true if a valid position was found, false otherwise
     */
    bool SeekToFirst();

    /**
     * @brief Positions the iterator at the last key in the database
     * @return true if a valid position was found, false otherwise
     */
    bool SeekToLast();

    /**
     * @brief Advances the iterator to the next key
     * @return true if the iterator is still valid after advancing, false otherwise
     */
    bool Next();

    /**
     * @brief Checks if the iterator is valid
     * @return true if the iterator is valid, false otherwise
     */
    bool Valid() const {
        RS_ASSERT(iter_);
        return iter_->Valid();
    }

    /**
     * @brief Gets the current key at the iterator position
     * @return The current key, or nullopt if the iterator is invalid
     */
    std::optional<std::string> GetCurrentKey();

    /**
     * @brief Gets the current value at the iterator position
     * @return The current value, or nullopt if the iterator is invalid
     */
    std::optional<std::string> GetCurrentValue();

    /**
     * @brief Factory method to create a new Iterator
     * @param iter Pointer to a RocksDB iterator (ownership is transferred)
     * @return A new Iterator instance, or nullptr if iter is null
     */
    static Iterator* Create(rocksdb::Iterator* iter) {
        RS_ASSERT(iter);
        return new Iterator(iter);
    }
private:
    /** The underlying RocksDB iterator */
    std::unique_ptr<rocksdb::Iterator> iter_;
};

/**
 * @brief Represents a column family in the RocksDB database
 *
 * This class provides a simplified interface for reading from and writing to
 * a specific column family in RocksDB, as well as creating iterators for
 * traversing the column family.
 */
class Column {
public:
    /**
     * @brief Constructs a column with references to the database and column family handle
     * @param db Reference to the RocksDB database
     * @param handle Reference to the column family handle
     */
    Column(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& handle);

    /**
     * @brief Destructor
     */
    ~Column();

    Column(Column&& other) noexcept
        : handle_(other.handle_), db_(other.db_) {
        other.handle_ = nullptr;
    }

    Column& operator=(Column&& other) noexcept {
        if (this != &other) {
            std::swap(handle_, other.handle_);
            std::swap(db_, other.db_);
        }
        return *this;
    }
    Column(const Column&) = delete;
    Column& operator=(const Column&) = delete;

    /**
     * @brief Writes a key-value pair to the column family
     * @param key The key to write
     * @param value The value to write
     * @return true if the write was successful, false otherwise
     */
    bool Write(const std::string& key, const std::string& value);

    /**
     * @brief Reads a value from the column family
     * @param key The key to read
     * @return The value associated with the key, or nullopt if not found
     */
    std::optional<std::string> Read(const std::string& key);

    /**
     * @brief Executes a batch of operations atomically using a template function
     *
     * This method allows multiple write operations (put, delete) to be executed
     * as a single atomic transaction without heap allocations.
     *
     * @tparam Func Type of the function object
     * @param operations A function object that takes a rocksdb::WriteBatch& and the column family handle
     * @return true if the batch was successfully committed, false otherwise
     */
    template <typename Func>
    bool WriteBatchT(Func&& operations) {
        rocksdb::WriteBatch batch;
        operations(batch, *handle_);
        rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
        return status.ok();
    }

    /**
     * @brief Deletes a key from the column family
     * @param key The key to delete
     * @return true if the delete was successful, false otherwise
     */
    bool Delete(const std::string& key);

    /**
     * @brief Creates an iterator for traversing the column family
     *
     * This template method creates an iterator of the specified type for
     * traversing the column family, with optional additional arguments
     * passed to the iterator's Create method.
     *
     * @tparam IteratorType The type of iterator to create
     * @tparam Args Types of additional arguments to pass to the iterator's Create method
     * @param args Additional arguments to pass to the iterator's Create method
     * @return A new iterator instance
     */
    template <typename IteratorType, typename... Args>
    IteratorType* CreateIterator(Args&&... args) {
        return IteratorType::Create(db_->NewIterator(rocksdb::ReadOptions(), handle_), std::forward<Args>(args)...);
    }

    // Cause a compaction of the column family
    // Will eventually cause the filter and aggregator compaction to be called
    void Compact() {
        db_->CompactRange(rocksdb::CompactRangeOptions(), handle_, nullptr, nullptr);
    }
private:
    /** Reference to the column family handle */
    rocksdb::ColumnFamilyHandle* handle_;

    /** Reference to the RocksDB database */
    rocksdb::DB* db_;
};

}
