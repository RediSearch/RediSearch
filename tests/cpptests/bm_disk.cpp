
#include <benchmark/benchmark.h>

#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <filesystem>
#include <chrono>
#include <iostream>

#include "src/redis_index.h"
#include "src/info/indexes_info.h"
#include "common.h"

#include "redisearch_api.h"

#include "DocTable.hpp"
#include "inverted_index.hpp"

class IndexAPI {
public:
    virtual void Insert(const std::string &doc, const std::string &terms) = 0;
    virtual std::vector<std::string> Search(const std::string &term) = 0;
    virtual void Delete(const std::string &doc) = 0;
};

class DiskIndex : public IndexAPI {
private:
    DDocTable *docTable;
    InvertedIndices *invertedIndices;

    rocksdb::DB *db;
    std::vector<rocksdb::ColumnFamilyHandle*> cfs;

    std::string db_path;

public:
    DiskIndex() {
        static std::atomic<int> db_counter{0};
        db_path = "test_db_" + std::to_string(db_counter++);

        rocksdb::DBOptions options;
        options.create_if_missing = true;
        options.error_if_exists = true;
        options.create_missing_column_families = true;

        options.db_write_buffer_size = 1024 * 10; // 10 MB global write buffer limit

        // Prepare the column families
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        // Default column family
        column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

        // DocTable column family
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("doc_table", rocksdb::ColumnFamilyOptions()));

        // Inverted indices column family
        rocksdb::ColumnFamilyOptions invertedIndicesOptions;
        invertedIndicesOptions.merge_operator.reset(getIIMergeOperator());
        column_families.push_back(rocksdb::ColumnFamilyDescriptor("inverted_indices", invertedIndicesOptions));

        rocksdb::Status status = rocksdb::DB::Open(options, db_path, column_families, &cfs, &db);
        if (!status.ok()) {
            throw std::runtime_error("Failed to open RocksDB: " + status.ToString());
        }

        docTable = new DDocTable(db, cfs[1]);
        invertedIndices = InvertedIndices::getInvertedIndices(db, cfs[2]);
    }

    ~DiskIndex() {
        delete docTable;
        for (auto cf : cfs) {
            delete cf;
        }
        db->Close();
        delete db; // last
        std::filesystem::remove_all(db_path);
    }

    void Insert(const std::string &doc, const std::string &terms) override {
        docId_t docId = docTable->CreateDoc(doc);
        std::istringstream iss(terms);
        std::string term;
        while (iss >> term) {
            Slice termSlice{term};
            invertedIndices->Add(termSlice, docId);
        }
    }

    std::vector<std::string> Search(const std::string &term) override {
        Slice termSlice{term};
        InvertedIndices::Iterator it = invertedIndices->Iterate(termSlice);
        std::vector<std::string> results;
        while (!it.atEnd()) {
            results.push_back(docTable->GetKey(*it));
            ++it;
        }
        return results;
    }

    void Delete(const std::string &doc) override {
        docTable->Remove(doc);
    }

};

class redisIndex : public IndexAPI {
private:
    RSIndex *index;

public:
    redisIndex() {
        RSIndexOptions opts = {.stopwords = nullptr, .stopwordsLen = 0};
        index = RediSearch_CreateIndex("idx", &opts);
        RediSearch_CreateTextField(index, "text");
    }

    ~redisIndex() {
        RediSearch_DropIndex(index);
    }

    void Insert(const std::string &docName, const std::string &terms) override {
        RSDoc *doc = RediSearch_CreateDocument(docName.c_str(), docName.size(), 1.0, NULL);
        RediSearch_DocumentAddFieldString(doc, "text", terms.c_str(), terms.size(), RSFLDTYPE_FULLTEXT);
        RediSearch_SpecAddDocument(index, doc); // consumes the document
    }

    std::vector<std::string> Search(const std::string &term) override {
        RSQNode *q = RediSearch_CreateTokenNode(index, "text", term.c_str());
        RSResultsIterator *iter = RediSearch_GetResultsIterator(q, index);
        std::vector<std::string> results;
        const char *cur;
        while ((cur = (const char *)RediSearch_ResultsIteratorNext(iter, index, NULL)) != NULL) {
            results.emplace_back(cur);
        }
        RediSearch_ResultsIteratorFree(iter);
        RediSearch_QueryNodeFree(q);
        return results;
    }

    void Delete(const std::string &doc) override {
        RediSearch_DeleteDocument(index, doc.c_str(), doc.size());
    }

};



static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}

bool initialized = false;
const char *raw_data = "data.txt";
std::vector<std::string> data;
std::vector<std::string> docs;
std::unordered_set<std::string> terms;

class BMDisk : public benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State &state) override {
        if (!initialized) {
            initialized = true;

            std::cout << "Initializing RediSearch..." << std::endl;
            const char *arguments[] = {"NOGC"};
            RMCK_Bootstrap(my_OnLoad, arguments, 1);
            RSGlobalConfig.freeResourcesThread = false;

            std::cout << "Loading data..." << std::endl;
            auto start = std::chrono::high_resolution_clock::now();
            std::ifstream file(raw_data);
            std::string line;
            size_t docID = 1;
            while (std::getline(file, line)) {
                // Add full line to data vector
                data.push_back(line);
                // Generate a doc name for each line
                docs.push_back("doc:" + std::to_string(docID++));
                // Add all terms to the terms set
                std::istringstream iss(line);
                std::string term;
                while (iss >> term) {
                    terms.insert(term);
                }
            }
            auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Data loaded in " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;
        }
    }

    // ~BMDisk() {
    //     RMCK_Shutdown();
    //     RediSearch_CleanupModule();
    // }
};

static void loadToIndex(IndexAPI *index, const std::vector<std::string> &docs, const std::vector<std::string> &data) {
    size_t n_docs = docs.size();
    for (size_t i = 0; i < n_docs; i++) {
        index->Insert(docs[i], data[i]);
    }
}

BENCHMARK_DEFINE_F(BMDisk, Load_RediSearch)(benchmark::State &state) {
    for (auto _ : state) {
        redisIndex index;
        loadToIndex(&index, docs, data);
    }
}

BENCHMARK_DEFINE_F(BMDisk, Load_Disk)(benchmark::State &state) {
    for (auto _ : state) {
        DiskIndex index;
        loadToIndex(&index, docs, data);
    }
}

BENCHMARK_DEFINE_F(BMDisk, Search_RediSearch)(benchmark::State &state) {
    redisIndex index;
    loadToIndex(&index, docs, data);
    auto terms_it = terms.begin();
    for (auto _ : state) {
        if (terms_it == terms.end()) {
            terms_it = terms.begin();
        }
        index.Search(*terms_it);
        ++terms_it;
    }
}

BENCHMARK_DEFINE_F(BMDisk, Search_Disk)(benchmark::State &state) {
    DiskIndex index;
    loadToIndex(&index, docs, data);
    auto terms_it = terms.begin();
    for (auto _ : state) {
        if (terms_it == terms.end()) {
            terms_it = terms.begin();
        }
        index.Search(*terms_it);
        ++terms_it;
    }
}

BENCHMARK_REGISTER_F(BMDisk, Load_RediSearch)->Unit(benchmark::kSecond);
BENCHMARK_REGISTER_F(BMDisk, Load_Disk)->Unit(benchmark::kSecond);;
BENCHMARK_REGISTER_F(BMDisk, Search_RediSearch)->Unit(benchmark::kMillisecond);;
BENCHMARK_REGISTER_F(BMDisk, Search_Disk)->Unit(benchmark::kMillisecond);;

BENCHMARK_MAIN();
