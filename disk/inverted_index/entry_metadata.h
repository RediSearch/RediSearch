/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "redisearch.h"
#include <string>
#include <ostream>
#include <boost/endian/conversion.hpp>

namespace search::disk {

/**
 * @brief Contains metadata associated with a document in the search index
 *
 * EntryMetadata stores additional information about a document, such as
 * which fields are indexed (represented by a field mask). It provides
 * serialization/deserialization methods for disk storage with proper
 * endianness handling.
 */
struct EntryMetadata {
    /**
     * Field mask for the document - a bit mask where each bit represents
     * whether a specific field is present in the document
     */
    t_fieldMask fieldMask;

    /**
     * @brief Serializes the document metadata to a binary stream
     *
     * Converts the field mask to big-endian format for consistent cross-platform storage
     *
     * @param stream The output stream to write the serialized metadata to
     */
    void Serialize(std::ostream& stream) const {
        const t_fieldMask rawFieldMask = boost::endian::native_to_big(fieldMask);
        stream.write(reinterpret_cast<const char*>(&rawFieldMask), sizeof(rawFieldMask));
    }

    /**
     * @brief Deserializes document metadata from binary data
     *
     * Converts from big-endian format back to native endianness
     *
     * @param metadata String view containing the serialized metadata
     * @return EntryMetadata The deserialized entry metadata
     */
    static EntryMetadata Deserialize(std::string_view& metadata) {
        const t_fieldMask* elements = reinterpret_cast<const t_fieldMask*>(metadata.data());
        const t_fieldMask single = boost::endian::big_to_native(elements[0]);
        metadata.remove_prefix(sizeof(t_fieldMask));
        return EntryMetadata{single};
    }
};

} // namespace search::disk