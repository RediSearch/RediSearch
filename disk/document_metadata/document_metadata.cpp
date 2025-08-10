/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "disk/document_metadata/document_metadata.hpp"
#include <boost/endian/conversion.hpp>
#include <sstream>
#include <cstring>
#include <stdexcept>

namespace search::disk {

/**
 * @brief Serializes document metadata to an output stream
 *
 * This method converts the document metadata to a binary format suitable for
 * storage in the database. The format includes:
 * - Version (1 byte)
 * - Key length (8 bytes, big-endian)
 * - Key bytes (variable length)
 * - Score (8 bytes, big-endian)
 * - MaxFreq (4 bytes, big-endian)
 * - Flags (4 bytes, big-endian)
 *
 * @param stream The output stream to write the serialized metadata to
 */
void DocumentMetadata::serialize(std::ostream& stream) const {
    // Write version
    uint8_t version = 1;
    stream.write(reinterpret_cast<const char*>(&version), sizeof(version));

    // Write key length and key bytes
    uint64_t keyLen = boost::endian::native_to_big(static_cast<uint64_t>(keyPtr.size()));
    stream.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
    stream.write(keyPtr.data(), keyPtr.size());

    // Write score (double as uint64)
    uint64_t beScore;
    static_assert(sizeof(double) == sizeof(uint64_t), "Unexpected double size");
    std::memcpy(&beScore, &score, sizeof(double));
    beScore = boost::endian::native_to_big(beScore);
    stream.write(reinterpret_cast<const char*>(&beScore), sizeof(beScore));

    // Write maxFreq, flags (uint32 each)
    uint32_t beMaxFreq = boost::endian::native_to_big(maxFreq);
    uint32_t beFlags   = boost::endian::native_to_big(flags);
    stream.write(reinterpret_cast<const char*>(&beMaxFreq), sizeof(beMaxFreq));
    stream.write(reinterpret_cast<const char*>(&beFlags), sizeof(beFlags));
}

/**
 * @brief Serializes document metadata to a string
 *
 * This is a convenience method that creates a string stream,
 * calls the stream-based serialize method, and returns the resulting string.
 *
 * @return Serialized metadata as a string
 */
std::string DocumentMetadata::serialize() const {
    std::ostringstream oss;

    serialize(oss);
    if (oss.fail() || oss.bad()) {
        throw std::ios_base::failure("Serialization failed: stream error");
    }

    return oss.str();
}

/**
 * @brief Deserializes document metadata from a string
 *
 * This static method converts a binary string back to a document metadata object.
 * It expects the format created by the serialize() method.
 *
 * @param data Serialized metadata string
 * @return Optional containing the deserialized DocumentMetadata object, or nullopt on failure
 */
std::optional<DocumentMetadata> DocumentMetadata::deserialize(const std::string& data) {
    DocumentMetadata dmd;
    std::istringstream iss(data);

    // Read version
    uint8_t version;
    if (!iss.read(reinterpret_cast<char*>(&version), sizeof(version)) || version != 1) {
        return std::nullopt;
    }

    // Read key length and key
    uint64_t keyLen;
    if (!iss.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))) {
        return std::nullopt;
    }
    keyLen = boost::endian::big_to_native(keyLen);
    dmd.keyPtr.resize(keyLen);
    if (!iss.read(&dmd.keyPtr[0], keyLen)) {
        return std::nullopt;
    }

    // Read score
    uint64_t beScore;
    if (!iss.read(reinterpret_cast<char*>(&beScore), sizeof(beScore))) {
        return std::nullopt;
    }
    beScore = boost::endian::big_to_native(beScore);
    std::memcpy(&dmd.score, &beScore, sizeof(double));

    // Read maxFreq, flags
    uint32_t beMaxFreq, beFlags;
    if (!iss.read(reinterpret_cast<char*>(&beMaxFreq), sizeof(beMaxFreq)) ||
        !iss.read(reinterpret_cast<char*>(&beFlags), sizeof(beFlags))) {
        return std::nullopt;
    }
    dmd.maxFreq = boost::endian::big_to_native(beMaxFreq);
    dmd.flags   = boost::endian::big_to_native(beFlags);
    return dmd;
}

} // namespace search::disk
