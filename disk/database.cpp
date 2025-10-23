#include <unordered_set>
#include <filesystem>
#include <sstream>
#include "disk/database.h"
#include "disk/inverted_index/merge_operator.h"
#include "disk/inverted_index/inverted_index.h"
#include "disk/memory_object.h"
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/filter_policy.h"

namespace search::disk {

static rocksdb::SliceTransform* CreateInvertedIndexPrefixExtractor() {
  class InvertedIndexPrefixExtractor : public rocksdb::SliceTransform {
  public:
    const char* Name() const override { return "InvertedIndexPrefixExtractor"; }

    rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
      auto view = src.ToStringView();
      const auto pos = view.find_last_of(SingleDocument::KEY_DELIMITER);
      return rocksdb::Slice(src.data(), pos + 1);
    }

    bool InDomain(const rocksdb::Slice& src) const override {
        return src.ToStringView().find_last_of(SingleDocument::KEY_DELIMITER) != std::string::npos;
    }
  };
  return new InvertedIndexPrefixExtractor();
}

static rocksdb::ColumnFamilyOptions CreateInvertedIndexOptions(std::shared_ptr<DeletedIds> deletedIds) {
  rocksdb::ColumnFamilyOptions options;
  options.merge_values = true;
  options.merge_operator.reset(new InvertedIndexMergeOperator(deletedIds));
  options.prefix_extractor.reset(CreateInvertedIndexPrefixExtractor());
  rocksdb::BlockBasedTableOptions blockBasedOptions;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blockBasedOptions));
  return options;
}

static rocksdb::ColumnFamilyOptions CreateDocTableOptions(size_t cacheSize) {
  rocksdb::ColumnFamilyOptions options;
  rocksdb::BlockBasedTableOptions blockBasedOptions;
  blockBasedOptions.block_cache = rocksdb::NewLRUCache(cacheSize);

  blockBasedOptions.block_align = true;
  blockBasedOptions.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  blockBasedOptions.cache_index_and_filter_blocks = false;
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blockBasedOptions));
  return options;
}

namespace {
  // Helper function to create column name with document type and column suffix
  std::string CreateColumnName(const std::string& indexName, DocumentType docType, const std::string& columnName) {
    std::ostringstream ss;
    ss << indexName;
    if (docType == DocumentType_Hash) {
      ss << ":hash";
    } else if (docType == DocumentType_Json) {
      ss << ":json";
    }
    // we want to be able to extract the type so we need constant delimiters and don't want column name to have them
    RS_ASSERT(columnName.find(':') == std::string::npos);
    ss << ":" << columnName;
    return ss.str();
  }

  // Helper function to create column families and handles
  bool CreateColumnFamiliesForIndex(
      const std::string& indexName,
      DocumentType docType,
      rocksdb::DB& db,
      std::shared_ptr<DeletedIds> deletedIds,
      std::vector<rocksdb::ColumnFamilyHandle*>& outHandles) {
    static constexpr size_t DocTableCacheSize = 30 * 1024 * 1024; // 30MB

    std::vector<rocksdb::ColumnFamilyDescriptor> columnFamilies;
    columnFamilies.push_back(
      rocksdb::ColumnFamilyDescriptor(
        CreateColumnName(indexName, docType, "doc_table"), CreateDocTableOptions(DocTableCacheSize)
      )
    );
    columnFamilies.push_back(
      rocksdb::ColumnFamilyDescriptor(
        CreateColumnName(indexName, docType, "inverted_indices"), CreateInvertedIndexOptions(deletedIds)
      )
    );

    const rocksdb::Status status = db.CreateColumnFamilies(columnFamilies, &outHandles);
    return status.ok();
  }
}

Database::Index* Database::Index::Create(std::string name, rocksdb::DB& db, DocumentType docType) {
  auto deletedIds = std::make_shared<DeletedIds>();

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  if (!CreateColumnFamiliesForIndex(name, docType, db, deletedIds, handles)) {
    return nullptr;
  }

  const size_t docTableColumnIndex = 0;
  const size_t invertedIndexColumnIndex = 1;

  return new Index(name,
      DocTableColumn(Column(db, *handles[docTableColumnIndex]), docType, deletedIds, 0),
      Column(db, *handles[invertedIndexColumnIndex]),
      docType);
}

Database::Index* Database::Index::Create(std::string name, rocksdb::DB& db, DocumentType docType,
                                        t_docId maxDocId, std::shared_ptr<DeletedIds> deletedIds) {
  // Use the provided deletedIds instead of creating a new one
  if (!deletedIds) {
    deletedIds = std::make_shared<DeletedIds>();
  }

  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  if (!CreateColumnFamiliesForIndex(name, docType, db, deletedIds, handles)) {
    return nullptr;
  }

  const size_t docTableColumnIndex = 0;
  const size_t invertedIndexColumnIndex = 1;

  // Create DocTableColumn with the provided maxDocId
  DocTableColumn docTable(Column(db, *handles[docTableColumnIndex]), docType, deletedIds, maxDocId);

  return new Index(name,
      std::move(docTable),
      Column(db, *handles[invertedIndexColumnIndex]),
      docType);
}

Database* Database::Create(RedisModuleCtx* ctx, const std::string& db_path, const MemoryObject& memory_obj) {
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

    // First, collect indexes from MemoryObject
    for (const auto& [name, memIndex] : memory_obj.GetIndexes()) {
      indexes[name] = memIndex.docType;
    }

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

            //TODO: Assert that the docType encountered in the column-family matches the one in the memory object
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
        // Check if we have memory object data for this index
        const auto* memIndex = memory_obj.GetIndex(name);
        t_docId maxDocId = 0;
        std::shared_ptr<DeletedIds> deletedIds = std::make_shared<DeletedIds>();
        if (memIndex) {
            maxDocId = memIndex->maxDocId;
            deletedIds = memIndex->deletedIds;
        }
        database->OpenIndex(name, docType, maxDocId, deletedIds);
    }

    return database;
}

Database::Database(RedisModuleCtx* ctx, std::unique_ptr<rocksdb::DB> db)
    : ctx_(ctx), db_(std::move(db)) {
    RedisModule_Log(ctx_, "warning", "RediSearch Disk Database opened successfully");
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

MemoryObject CreateMemoryObjectFromDatabase(const Database& database) {
    MemoryObject memObj;

    // Iterate through all indexes in the database
    for (const auto& [indexName, indexPtr] : database.GetIndexes()) {
        RS_ASSERT(indexPtr);

        const auto& docTable = indexPtr->GetDocTable();

        // Extract the memory state from the index
        t_docId maxDocId = docTable.GetMaxDocId();
        std::shared_ptr<DeletedIds> deletedIds = docTable.GetDeletedIds();
        DocumentType docType = docTable.getDocumentType();

        // Add to memory object
        memObj.AddIndex(indexName, docType, maxDocId, deletedIds);
    }

    return memObj;
}

} // namespace search::disk {
