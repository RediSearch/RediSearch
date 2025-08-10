/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include <string>
#include <cstdint>
#include <ostream>
#include <optional>

namespace search::disk {

/**
 * @brief Minimal on-disk RSDocumentMetadata struct
 *
 * This structure stores document metadata for disk-based storage, including
 * the document key, score, maximum frequency, and flags.
 */
struct DocumentMetadata {
    /** The document key */
    ::std::string keyPtr;

    /** Document score (for ranking) */
    double score;

    /** Maximum term frequency in the document */
    uint32_t maxFreq;

    /** Document flags */
    uint32_t flags;

    /**
     * @brief Serializes the metadata to an output stream
     *
     * @param stream The output stream to write the serialized metadata to
     */
    void serialize(::std::ostream& stream) const;

    /**
     * @brief Serializes the metadata to a string
     *
     * This is a convenience method that creates a string stream,
     * calls the stream-based serialize method, and returns the resulting string.
     *
     * @return Serialized metadata as a string
     */
    ::std::string serialize() const;

    /**
     * @brief Deserializes metadata from a string
     *
     * @param data Serialized metadata string
     * @return Optional containing the deserialized DocumentMetadata object, or nullopt on failure
     */
    static ::std::optional<DocumentMetadata> deserialize(const ::std::string& data);
};

} // namespace search::disk
