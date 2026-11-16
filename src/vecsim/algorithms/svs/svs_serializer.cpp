/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "svs_serializer.h"

namespace fs = std::filesystem;

SVSSerializer::SVSSerializer(EncodingVersion version) : m_version(version) {}

SVSSerializer::EncodingVersion SVSSerializer::ReadVersion(std::ifstream &input) {
    input.seekg(0, std::ifstream::beg);

    EncodingVersion version = EncodingVersion::INVALID;
    readBinaryPOD(input, version);

    if (version >= EncodingVersion::INVALID) {
        input.close();
        throw std::runtime_error("Cannot load index: bad encoding version: " +
                                 std::to_string(static_cast<int>(version)));
    }
    return version;
}

void SVSSerializer::saveIndex(const std::string &location) {
    EncodingVersion version = EncodingVersion::V0;
    auto metadata_path = fs::path(location) / "metadata";
    std::ofstream output(metadata_path, std::ios::binary);
    writeBinaryPOD(output, version);
    saveIndexIMP(output);
    output.close();
    impl_save(location);
}

SVSSerializer::EncodingVersion SVSSerializer::getVersion() const { return m_version; }
