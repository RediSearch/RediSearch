/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redisearch.h"
#include <ostream>
#include <optional>
#include <boost/endian/conversion.hpp>
#include <rocksdb/iterator.h>

namespace search::disk {

/**
 * @brief Represents a unique document identifier in the search index
 *
 * DocumentID encapsulates the document ID used in RediSearch and provides
 * serialization/deserialization methods for disk storage. The serialization
 * ensures proper endianness for cross-platform compatibility.
 */
struct DocumentID {
    /** The numeric document identifier */
    t_docId id;

    auto operator<=>(const DocumentID& other) const {
        return id <=> other.id;
    };

    bool operator==(const DocumentID& other) const {
        return id == other.id;
    };

    /**
     * @brief Serializes the document ID to a binary stream
     *
     * Converts the ID to big-endian format for consistent cross-platform storage
     *
     * @param stream The output stream to write the serialized ID to
     */
    void SerializeAsValue(std::ostream& stream) const {
        const t_docId raw = boost::endian::native_to_big(id);
        stream.write(reinterpret_cast<const char*>(&raw), sizeof(raw));
    }

    /**
     * @brief Serializes the document ID to a key format
     *
     * Creates a key by appending the document ID to the provided stream,
     * with zero-padding to ensure correct lexicographic ordering.
     *
     * @param stream Output stream to write the key to
     */
    void SerializeAsKey(std::ostream& stream) const;

    /**
     * @brief Deserializes a document ID from binary data
     *
     * Converts from big-endian format back to native endianness
     *
     * @param buffer String view containing the serialized document ID
     * @return DocumentID The deserialized document ID
     */
    template <bool consume = true>
    static DocumentID DeserializeFromValue(std::conditional_t<consume, std::string_view&, const std::string_view&> buffer) {
        const t_docId* elements = reinterpret_cast<const t_docId*>(buffer.data());
        const t_docId single = boost::endian::big_to_native(elements[0]);
        if constexpr (consume) {
            buffer.remove_prefix(sizeof(t_docId));
        }
        return DocumentID{single};
    }

    /**
     * @brief Deserializes a document ID from a key format
     *
     * @param key String view containing the serialized document ID with a prefix
     * @param prefix String view containing the key prefix
     * @return DocumentID The deserialized document ID
     */
    static std::optional<DocumentID> DeserializeFromKey(std::string_view key, std::string_view prefix);

    /**
     * @brief Deserializes a document ID from a key format
     *
     * @param buffer String view containing the serialized document ID
     * @return DocumentID The deserialized document ID
     */
    static std::optional<DocumentID> DeserializeFromKey(std::string_view key);

    static size_t EstimateCount(rocksdb::Iterator& iter, const rocksdb::Slice& prefix);
};

} // namespace search::disk
