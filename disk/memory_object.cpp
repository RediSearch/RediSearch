/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "disk/memory_object.h"
#include "rmutil/rm_assert.h"
#include <algorithm>

namespace search::disk {

void MemoryObject::AddIndex(const std::string& name, DocumentType docType, t_docId maxDocId,
                           std::shared_ptr<DeletedIds> deletedIds) {
    // Insert or update the index (unordered_map will replace if key exists)
    indexes_[name] = IndexInfo(name, docType, maxDocId, deletedIds);
}

const MemoryObject::IndexInfo* MemoryObject::GetIndex(const std::string& name) const {
    auto it = indexes_.find(name);
    return (it != indexes_.end()) ? &(it->second) : nullptr;
}

void MemoryObject::IndexInfo::SerializeToRDB(RedisModuleIO* rdb) const {
    RS_ASSERT(rdb != nullptr);

    // Save index name
    RedisModule_SaveStringBuffer(rdb, name.c_str(), name.length());

    // Save document type
    RedisModule_SaveUnsigned(rdb, static_cast<uint64_t>(docType));

    // Save max document ID
    RedisModule_SaveUnsigned(rdb, maxDocId);

    // Save deleted IDs
    if (deletedIds) {
        RedisModule_SaveUnsigned(rdb, 1); // Has deleted IDs
        deletedIds->SerializeToRDB(rdb);
    } else {
        RedisModule_SaveUnsigned(rdb, 0); // No deleted IDs
    }
}

void MemoryObject::SerializeToRDB(RedisModuleIO* rdb) const {
    RS_ASSERT(rdb != nullptr);

    // Save the number of indexes
    RedisModule_SaveUnsigned(rdb, indexes_.size());

    // Save each index
    for (const auto& [name, index] : indexes_) {
        index.SerializeToRDB(rdb);
    }
}

bool MemoryObject::IndexInfo::DeserializeFromRDB(RedisModuleIO* rdb) {
    RS_ASSERT(rdb != nullptr);

    // Load index name
    size_t nameLen;
    char* namePtr = RedisModule_LoadStringBuffer(rdb, &nameLen);
    if (!namePtr) {
        return false; // Error loading name
    }
    name = std::string(namePtr, nameLen);
    RedisModule_Free(namePtr);

    // Load document type
    uint64_t docTypeValue = RedisModule_LoadUnsigned(rdb);
    docType = static_cast<DocumentType>(docTypeValue);

    // Load max document ID
    maxDocId = RedisModule_LoadUnsigned(rdb);

    // Load deleted IDs
    uint64_t hasDeletedIds = RedisModule_LoadUnsigned(rdb);
    if (hasDeletedIds) {
        deletedIds = std::make_shared<DeletedIds>();
        if (!deletedIds->DeserializeFromRDB(rdb)) {
            return false; // Error loading deleted IDs
        }
    } else {
        deletedIds = std::make_shared<DeletedIds>();
    }

    return true;
}

std::unique_ptr<MemoryObject> MemoryObject::DeserializeFromRDB(RedisModuleIO* rdb) {
    RS_ASSERT(rdb != nullptr);

    auto memObj = std::make_unique<MemoryObject>();

    // Load the number of indexes
    uint64_t numIndexes = RedisModule_LoadUnsigned(rdb);

    // Load each index
    for (uint64_t i = 0; i < numIndexes; ++i) {
        IndexInfo info;

        if (!info.DeserializeFromRDB(rdb)) {
            return nullptr; // Error loading index
        }

        memObj->indexes_[info.name] = std::move(info);
    }

    return memObj;
}

} // namespace search::disk
