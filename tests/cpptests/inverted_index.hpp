#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/merge_operator.h"

using Slice = rocksdb::Slice;
using Status = rocksdb::Status;

class InvertedIndices {
private:
    // Database and column family handles of the class
    rocksdb::DB *db;
    rocksdb::ColumnFamilyHandle *cf;

protected:
    // The singleton instance of the class
    static InvertedIndices *instance;
    // The constructor of the class
    InvertedIndices(const char *db_path = "inverted_indexs.db", const char *cf_name = "inverted_indexs");
    InvertedIndices(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf);
    InvertedIndices(rocksdb::DB* db, const char *cf_name = "inverted_indexs");
public:
    InvertedIndices(InvertedIndices &) = delete;
    void operator=(const InvertedIndices &) = delete;

    // Get the singleton instance of the class
    template <typename... Args>
    static InvertedIndices *getInvertedIndices(Args... args) {
        if (instance == nullptr) {
            instance = new InvertedIndices(args...);
        }
        return instance;
    }

    Status Add(Slice &term, uint64_t doc_id);

    class Iterator {
    // private:
    public:
        // Iterator instance members
        std::string ids;
        const char *ptr;
    public:
        Iterator(std::string ids_) : ids(std::move(ids_)), ptr(ids.c_str()) {};
        ~Iterator() = default;

        void operator++();
        uint64_t operator*() const;
        bool atEnd() const;
    };

    Iterator Iterate(Slice &term) const;
};

rocksdb::AssociativeMergeOperator *getIIMergeOperator();
