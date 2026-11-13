/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <sstream>
#include <fstream>
#include <string>
#include "VecSim/utils/serializer.h"
#include <filesystem>

typedef struct {
    bool valid_state;
    long memory_usage; // in bytes
    size_t index_size;
    size_t storage_size;
    size_t label_count;
    size_t capacity;
    size_t changes_count;
    bool is_compressed;
    bool is_multi;
} SVSIndexMetaData;

// Middle layer for SVS serialization
// Abstract functions should be implemented by the templated SVS index

class SVSSerializer : public Serializer {
public:
    enum class EncodingVersion { V0, INVALID };

    explicit SVSSerializer(EncodingVersion version = EncodingVersion::V0);

    static EncodingVersion ReadVersion(std::ifstream &input);

    void saveIndex(const std::string &location) override;

    virtual void loadIndex(const std::string &location) = 0;

    EncodingVersion getVersion() const;

    virtual bool checkIntegrity() const = 0;

protected:
    EncodingVersion m_version;

    virtual void impl_save(const std::string &location) = 0;

    // Helper function to compare the svs index fields with the metadata file
    template <typename T>
    static void compareField(std::istream &in, const T &expected, const std::string &fieldName);

private:
    virtual bool compareMetadataFile(const std::string &metadataFilePath) const = 0;
};

// Implement << operator for enum class
inline std::ostream &operator<<(std::ostream &os, SVSSerializer::EncodingVersion version) {
    return os << static_cast<int>(version);
}

template <typename T>
void SVSSerializer::compareField(std::istream &in, const T &expected,
                                 const std::string &fieldName) {
    T actual;
    Serializer::readBinaryPOD(in, actual);
    if (!in.good()) {
        throw std::runtime_error("Failed to read field: " + fieldName);
    }
    if (actual != expected) {
        std::ostringstream msg;
        msg << "Field mismatch in \"" << fieldName << "\": expected " << expected << ", got "
            << actual;
        throw std::runtime_error(msg.str());
    }
}
