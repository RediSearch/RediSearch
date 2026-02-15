/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

/**
 * @file hnsw_storage.h
 * @brief Storage interface for disk-based vector indexes.
 *
 * ============================================================================
 * NOTE: SKELETON API - SUBJECT TO CHANGE
 * This interface exists to decouple HNSWDiskIndex from storage implementation,
 * enabling mock injection for unit testing. The API will evolve as MOD-13164
 * progresses.
 * ============================================================================
 */

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdint>
#include "VecSim/vec_sim_common.h"
#include "VecSim/utils/vecsim_stl.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "storage/encoding.h"
#include "storage/edge_merge_operator.h"

typedef uint16_t levelType;
static_assert(sizeof(idType) == 4 && "IDType must be 4 bytes");

/**
 * @brief Edge operation type for batch merge operations.
 */
enum class EdgeOperation : uint8_t {
    Append, ///< Append an edge to the incoming edges list
    Delete  ///< Delete an edge from the incoming edges list
};

/**
 * @brief SpeedB/RocksDB storage for disk-based vector indexes.
 *
 * Key Format:
 * - Vectors:         "R" + idType (4 bytes)
 * - Outgoing edges:  "O" + level (2 bytes) + idType (4 bytes)
 * - Incoming edges:  "I" + level (2 bytes) + idType (4 bytes)
 */
template <typename DataType>
class HNSWStorage {
public:
    HNSWStorage(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf) : db_(db), cf_(cf) {}

    ~HNSWStorage() = default;

    void batch_merge_incoming_edges(
        const vecsim_stl::vector<std::tuple<idType, levelType, idType, EdgeOperation>>& operations) {
        // Each tuple: (node_id, level, edge_id, operation)
        rocksdb::WriteBatch batch;
        for (const auto& [id, level, edge, op] : operations) {
            char key_buf[kEdgeKeySize];
            incomingEdgesKey(id, level, key_buf);
            std::string_view key(key_buf, kEdgeKeySize);
            std::string operand = (op == EdgeOperation::Append)
                                      ? rocksdb::EdgeListMergeOperator::CreateAppendOperand(edge)
                                      : rocksdb::EdgeListMergeOperator::CreateDeleteOperand(edge);
            batch.Merge(cf_, key, operand);
        }
        rocksdb::Status status = db_->Write(writeOpts_, &batch);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    // Merge operations for incoming edges only (outgoing edges use GET + PUT per design)
    void append_incoming_edge(idType id, levelType level, idType edge) {
        char key_buf[kEdgeKeySize];
        incomingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        std::string operand = rocksdb::EdgeListMergeOperator::CreateAppendOperand(edge);
        rocksdb::Status status = db_->Merge(writeOpts_, cf_, key, operand);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void delete_edge_from_incoming(idType id, levelType level, idType edge_to_delete) {
        char key_buf[kEdgeKeySize];
        incomingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        std::string operand = rocksdb::EdgeListMergeOperator::CreateDeleteOperand(edge_to_delete);
        rocksdb::Status status = db_->Merge(writeOpts_, cf_, key, operand);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void put_vector(idType id, const void* data, size_t size) {
        assert(size % sizeof(DataType) == 0 && "Invalid vector size");
        char key_buf[kVectorKeySize];
        vectorKey(id, key_buf);
        std::string_view key(key_buf, kVectorKeySize);
        std::string value_buf;
        std::string_view value =
            serializeVector(static_cast<const DataType*>(data), value_buf, size / sizeof(DataType));
        rocksdb::Status status = db_->Put(writeOpts_, cf_, key, value);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void put_outgoing_edges(idType id, levelType level, const vecsim_stl::vector<idType>& edges) {
        char key_buf[kEdgeKeySize];
        outgoingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        std::string value_buf;
        std::string_view value = serializeEdges(edges, value_buf);
        rocksdb::Status status = db_->Put(writeOpts_, cf_, key, value);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void put_incoming_edges(idType id, levelType level, const vecsim_stl::vector<idType>& edges) {
        char key_buf[kEdgeKeySize];
        incomingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        std::string value_buf;
        std::string_view value = serializeEdges(edges, value_buf);
        rocksdb::Status status = db_->Put(writeOpts_, cf_, key, value);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void get_vector(idType id, void* data, size_t size) const {
        assert(size % sizeof(DataType) == 0 && "Invalid vector size");
        char key_buf[kVectorKeySize];
        vectorKey(id, key_buf);
        std::string_view key(key_buf, kVectorKeySize);
        rocksdb::PinnableSlice value;
        rocksdb::Status status = db_->Get(readOpts_, cf_, key, &value);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
        if (value.size() != size) {
            throw std::runtime_error("Vector size mismatch");
        }
        deserializeVector(value, static_cast<DataType*>(data));
    }

    void get_vectors_multi(const vecsim_stl::vector<idType>& ids, vecsim_stl::vector<void*>& data_buffers,
                           size_t vector_size) const {
        if (ids.empty()) {
            return;
        }

        const size_t num_keys = ids.size();
        assert(data_buffers.size() == num_keys && "data_buffers size must match ids size");
        assert(vector_size % sizeof(DataType) == 0 && "Invalid vector size");

        std::vector<char> key_storage(num_keys * kVectorKeySize);
        std::vector<rocksdb::Slice> keys(num_keys);
        for (size_t i = 0; i < num_keys; ++i) {
            char* key_buf = &key_storage[i * kVectorKeySize];
            vectorKey(ids[i], key_buf);
            keys[i] = rocksdb::Slice(key_buf, kVectorKeySize);
        }

        std::vector<rocksdb::PinnableSlice> values(num_keys);
        std::vector<rocksdb::Status> statuses(num_keys);

        db_->MultiGet(readOpts_, cf_, num_keys, keys.data(), values.data(), statuses.data(), /*sorted_input=*/false);

        for (size_t i = 0; i < num_keys; ++i) {
            if (!statuses[i].ok()) {
                throw std::runtime_error(statuses[i].ToString());
            }
            if (values[i].size() != vector_size) {
                throw std::runtime_error("Vector size mismatch");
            }
            deserializeVector(values[i], static_cast<DataType*>(data_buffers[i]));
        }
    }

    void get_outgoing_edges(idType id, levelType level, vecsim_stl::vector<idType>& edges) const {
        char key_buf[kEdgeKeySize];
        outgoingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        rocksdb::PinnableSlice value;
        rocksdb::Status status = db_->Get(readOpts_, cf_, key, &value);
        if (status.IsNotFound()) {
            edges.clear();
            return; // Not found is valid - empty edge list
        }
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
        deserializeEdges(value, edges);
    }

    void get_incoming_edges(idType id, levelType level, vecsim_stl::vector<idType>& edges) const {
        char key_buf[kEdgeKeySize];
        incomingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        rocksdb::PinnableSlice value;
        rocksdb::Status status = db_->Get(readOpts_, cf_, key, &value);
        if (status.IsNotFound()) {
            edges.clear();
            return; // Not found is valid - empty edge list
        }
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
        deserializeEdges(value, edges);
    }

    void del_vector(idType id) {
        char key_buf[kVectorKeySize];
        vectorKey(id, key_buf);
        std::string_view key(key_buf, kVectorKeySize);
        rocksdb::Status status = db_->Delete(writeOpts_, cf_, key);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void del_outgoing_edges(idType id, levelType level) {
        char key_buf[kEdgeKeySize];
        outgoingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        rocksdb::Status status = db_->Delete(writeOpts_, cf_, key);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

    void del_incoming_edges(idType id, levelType level) {
        char key_buf[kEdgeKeySize];
        incomingEdgesKey(id, level, key_buf);
        std::string_view key(key_buf, kEdgeKeySize);
        rocksdb::Status status = db_->Delete(writeOpts_, cf_, key);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
    }

private:
    rocksdb::DB* db_;
    rocksdb::ColumnFamilyHandle* cf_;
    rocksdb::ReadOptions readOpts_;
    rocksdb::WriteOptions writeOpts_;
    static constexpr size_t kVectorKeySize = 1 + sizeof(idType);
    static constexpr size_t kEdgeKeySize = 1 + sizeof(levelType) + sizeof(idType);

    // Key generation functions - uses little-endian encoding
    // Encodes directly into the string buffer to avoid intermediate copies
    // On little-endian systems (x86, ARM), this is just a memcpy with no byte swapping
    static void vectorKey(idType id, char* buf) noexcept {
        buf[0] = 'R';
        encoding::EncodeFixedBE<idType>(&buf[1], id);
    }

    static void edgesKey(idType id, levelType level, char* buf) noexcept {
        encoding::EncodeFixedBE<levelType>(&buf[1], level);
        encoding::EncodeFixedBE<idType>(&buf[1 + sizeof(levelType)], id);
    }

    static void outgoingEdgesKey(idType id, levelType level, char* buf) noexcept {
        buf[0] = 'O';
        edgesKey(id, level, buf);
    }

    static void incomingEdgesKey(idType id, levelType level, char* buf) noexcept {
        buf[0] = 'I';
        edgesKey(id, level, buf);
    }

    // Edge serialization/deserialization functions - uses little-endian encoding
    static std::string_view serializeEdges(const vecsim_stl::vector<idType>& edges, std::string& value_buf) noexcept {
        const size_t result_size = edges.size() * sizeof(idType);
        if constexpr (encoding::kIsLittleEndian) {
            return std::string_view(reinterpret_cast<const char*>(edges.data()), result_size);
        }
        if (value_buf.capacity() < result_size) {
            value_buf.reserve(result_size); // Exact size, no over-allocation
        }

        value_buf.resize(result_size);
        char* ptr = value_buf.data();
        for (const idType& edge : edges) {
            encoding::EncodeFixedLE<idType>(ptr, edge);
            ptr += sizeof(idType);
        }

        return std::string_view(value_buf.data(), result_size);
    }

    static void deserializeEdges(const rocksdb::PinnableSlice& data, vecsim_stl::vector<idType>& edges) noexcept {
        size_t size = data.size();
        assert(size % sizeof(idType) == 0 && "Invalid edge data size");

        size_t num_edges = size / sizeof(idType);
        edges.clear();
        edges.resize(num_edges);

        // Deserialize the edges from little-endian format
        if constexpr (encoding::kIsLittleEndian) {
            // Direct copy on little-endian systems
            std::memcpy(edges.data(), data.data(), size);
            return;
        }
        // Decode with byte swapping on big-endian systems
        const char* ptr = data.data();
        for (size_t i = 0; i < num_edges; ++i) {
            edges[i] = encoding::DecodeFixedLE<idType>(ptr);
            ptr += sizeof(idType);
        }
    }

    // Vector serialization - converts vector to little-endian format
    // Returns serialized data as a string
    static std::string_view serializeVector(const DataType* vector, std::string& value_buf,
                                            const size_t num_elements) noexcept {
        static_assert(sizeof(DataType) == 4 || sizeof(DataType) == 8, "DataType must be 4 or 8 bytes");

        if constexpr (encoding::kIsLittleEndian) {
            return std::string_view(reinterpret_cast<const char*>(vector), num_elements * sizeof(DataType));
        }
        value_buf.resize(num_elements * sizeof(DataType));

        char* ptr = value_buf.data();
        for (size_t i = 0; i < num_elements; ++i) {
            encoding::EncodeFixedLE<DataType>(ptr, vector[i]);
            ptr += sizeof(DataType);
        }

        return std::string_view(value_buf.data(), num_elements * sizeof(DataType));
    }

    // Vector deserialization - converts from little-endian format to native format
    static void deserializeVector(const rocksdb::PinnableSlice& data, DataType* vector) noexcept {
        static_assert(sizeof(DataType) == 4 || sizeof(DataType) == 8, "DataType must be 4 or 8 bytes");
        size_t size = data.size();
        assert(size % sizeof(DataType) == 0 && "Invalid vector data size");
        size_t num_elements = size / sizeof(DataType);

        if constexpr (encoding::kIsLittleEndian) {
            std::memcpy(vector, data.data(), size);
            return;
        }
        const char* ptr = data.data();
        for (size_t i = 0; i < num_elements; ++i) {
            vector[i] = encoding::DecodeFixedLE<DataType>(ptr);
            ptr += sizeof(DataType);
        }
    }
};

// Forward declaration for SpeeDBHandles (defined in vecsim_disk_api.h)
struct SpeeDBHandles;

// Creates HNSWStorage from handles. Defined in speedb_store.cpp.
// This function requires linking against SpeedB/RocksDB.
template <typename DataType>
std::unique_ptr<HNSWStorage<DataType>> CreateHNSWStorage(const SpeeDBHandles* handles);
