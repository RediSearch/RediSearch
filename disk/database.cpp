#include <unordered_set>
#include <filesystem>
#include <sstream>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "disk/database.h"
#include "disk/inverted_index/merge_operator.h"

namespace search::disk {

static rocksdb::ColumnFamilyOptions CreateInvertedIndexOptions(std::shared_ptr<DeletedIds> deletedIds, size_t cacheSize) {
  rocksdb::ColumnFamilyOptions options;
  options.merge_values = true;
  options.merge_operator.reset(new InvertedIndexMergeOperator(deletedIds));
  rocksdb::BlockBasedTableOptions blockBasedOptions;
  blockBasedOptions.block_cache = rocksdb::NewLRUCache(cacheSize); 
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blockBasedOptions));
  return options;
}

static rocksdb::ColumnFamilyOptions CreateDocTableOptions(size_t cacheSize) {
  rocksdb::ColumnFamilyOptions options;
  rocksdb::BlockBasedTableOptions blockBasedOptions;
  blockBasedOptions.block_cache = rocksdb::NewLRUCache(cacheSize);
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blockBasedOptions));
  return options;
}

Database::Index* Database::Index::Create(std::string name, rocksdb::DB& db, DocumentType docType) {
  auto createColumnName = [&name, docType](const std::string& columnName) {
    std::ostringstream ss;
    ss << name;
    if (docType == DocumentType_Hash) {
      ss << ":hash";
    } else if (docType == DocumentType_Json) {
      ss << ":json";
    }
    // we want to be able to extract the type so we need constant delimiters and don't want column name to have them
    assert(columnName.find(':') == std::string::npos);
    ss << ":" << columnName;
    return ss.str();
  };

  static constexpr size_t InvertedIndexCacheSize = 10 * 1024 * 1024; // 10MB
  static constexpr size_t DocTableCacheSize = 30 * 1024 * 1024; // 30MB

  auto deletedIds = std::make_shared<DeletedIds>();
  std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;

  const size_t docTableColumnIndex = 0;
  columnFamilies.push_back(rocksdb::ColumnFamilyDescriptor(createColumnName("doc_table"), CreateDocTableOptions(DocTableCacheSize)));
  const size_t invertedIndexColumnIndex = 1;
  columnFamilies.push_back(rocksdb::ColumnFamilyDescriptor(createColumnName("inverted_indices"), CreateInvertedIndexOptions(deletedIds, InvertedIndexCacheSize)));

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  const rocksdb::Status status = db.CreateColumnFamilies(columnFamilies, &handles);
  if (!status.ok()) {
    return nullptr;
  }
  return new Index(name,
      DocTableColumn(Column(db, *handles[docTableColumnIndex]), docType, deletedIds),
      Column(db, *handles[invertedIndexColumnIndex]),
      docType);
}

Database* Database::Create(RedisModuleCtx* ctx, const std::string& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    // this probably needs to be changed when doing benchmarks
    options.drop_read_cache = false;
    options.write_buffer_size = 5 * 1024 * 1024; // 5MB

    std::vector<std::string> columnFamilies;
    if (std::filesystem::exists(db_path)) {
        rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, db_path, &columnFamilies);
        if (!status.ok()) {
            RedisModule_Log(ctx, "error", "Failed to list column families: %s", status.ToString().c_str());
            return nullptr;
        }
    }

    std::unordered_map<std::string, DocumentType> indexes;
    for (const std::string& columnName : columnFamilies) {
        if (columnName == rocksdb::kDefaultColumnFamilyName) {
            continue;
        }
        std::string_view view = columnName;
        auto pos = view.find_last_of(':');
        if (pos == std::string::npos) {
            RedisModule_Log(ctx, "error", "Invalid column family name: %s", columnName.c_str());
            return nullptr;
        }

        view.remove_suffix(view.size() - pos);
        pos = view.find_last_of(':');
        if (pos == std::string::npos) {
            RedisModule_Log(ctx, "error", "Invalid column family name: %s", columnName.c_str());
            return nullptr;
        }

        std::string_view docType = view.substr(pos + 1);
        std::string_view indexName = view.substr(0, pos);
        if (indexes.find(std::string(indexName)) != indexes.end()) {
            // Already added this index. This is now ok since we have a separate
            // column-family for the doc-table and inverted index.
            continue;
        }
        if (docType == "hash") {
            indexes.emplace(std::string(indexName), DocumentType_Hash);
        } else if (docType == "json") {
            indexes.emplace(std::string(indexName), DocumentType_Json);
        } else {
            RedisModule_Log(ctx, "error", "Invalid column family document type: %s", columnName.c_str());
            return nullptr;
        }
    }

    // Open the database with the existing column families
    rocksdb::DB *db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        RedisModule_Log(ctx, "error", "Failed to open database: %s", status.ToString().c_str());
        return nullptr;
    }

    Database* database = new Database(ctx, std::unique_ptr<rocksdb::DB>(db));
    for (auto& [name, docType] : indexes) {
        database->OpenIndex(name, docType);
    }

    return database;
}

Database::Database(RedisModuleCtx* ctx, std::unique_ptr<rocksdb::DB> db)
    : ctx_(ctx), db_(std::move(db)) {
    RedisModule_Log(ctx_, "notice", "Database opened successfully");
}

Database::~Database() {
    if (db_) {
        indexes_.clear();
        rocksdb::Status status = db_->Close();
        status.ok() ?
            RedisModule_Log(ctx_, "notice", "Database closed successfully") :
            RedisModule_Log(ctx_, "error", "Failed to close database: %s", status.ToString().c_str());
    }
}

} // namespace search::disk {
