#include <rocksdb/db.h>
#include <string>
#include <atomic>

typedef uint64_t docId_t;

#define INVALID_DOC_ID 0
#define FIRST_VALID_DOC_ID 1

class DDocTable {
public:
    /**
     * @brief Constructor for DocTable.
     * @param db Pointer to the RocksDB database.
     * @param cf Pointer to the ColumnFamilyHandle.
     */
    DDocTable(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf);

    /**
     * @brief Destructor for DocTable.
     */
    virtual ~DDocTable();

    /**
     * @brief Create a new document with the given key.
     * @param key The key for the new document.
     */
    docId_t CreateDoc(const std::string& key);
    // TODO: Try adding a merge-operator that will delete the previous docId->key
    // pair that may exist in the database.

    /**
     * @brief Get the document ID associated with the given key.
     * @param key The key to look up.
     * @param snapshot The snapshot to use for the read operation. Default is
     * `nullptr`, in which case no snapshot is used.
     * @return The document ID associated with the key, or INVALID_DOC_ID if not found.
     */
    docId_t Get(const std::string& key, const rocksdb::Snapshot* snapshot = nullptr);

    /**
     * @brief Get the document ID associated with the given key using a snapshot.
     * @param key The key to look up.
     * @param shot The snapshot to use for the read operation. Default is
     * `nullptr`, in which case a snapshot is made within the function.
     * @return The document ID associated with the key, or INVALID_DOC_ID if not found.
     */
    docId_t GetWithSnapshot(const std::string& key, const rocksdb::Snapshot* shot = nullptr);

    /**
     * @brief Get the key associated with the given document ID.
     * @param docId The document ID to look up.
     * @param shot The snapshot to use for the read operation (default is
     * `nullptr`, in which case a snapshot is made within the function).
     * @return The key associated with the document ID, or an empty string if not found.
     */
    const std::string GetKey(docId_t docId, const rocksdb::Snapshot* shot = nullptr);

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
    bool GetKeyIfValid(docId_t docId, std::string& key, const rocksdb::Snapshot* shot = nullptr);

    /**
     * @brief Remove the document with the given key.
     * @param key The key of the document to remove.
     */
    void Remove(const std::string& key);

    /**
     * @brief Print all documents in the DocTable.
     */
    void Print();

private:
    rocksdb::DB* db;
    rocksdb::ColumnFamilyHandle* cf;
    std::atomic<uint64_t> curr_id{0};
};
