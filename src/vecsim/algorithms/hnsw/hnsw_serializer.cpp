/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "hnsw_serializer.h"

HNSWSerializer::HNSWSerializer(EncodingVersion version) : m_version(version) {}

HNSWSerializer::EncodingVersion HNSWSerializer::ReadVersion(std::ifstream &input) {
    input.seekg(0, std::ifstream::beg);

    EncodingVersion version = EncodingVersion::INVALID;
    readBinaryPOD(input, version);

    if (version <= EncodingVersion::DEPRECATED) {
        input.close();
        throw std::runtime_error("Cannot load index: deprecated encoding version: " +
                                 std::to_string(static_cast<int>(version)));
    } else if (version >= EncodingVersion::INVALID) {
        input.close();
        throw std::runtime_error("Cannot load index: bad encoding version: " +
                                 std::to_string(static_cast<int>(version)));
    }
    return version;
}

void HNSWSerializer::saveIndex(const std::string &location) {
    EncodingVersion version = getWriteVersion();
    std::ofstream output(location, std::ios::binary);
    writeBinaryPOD(output, version);
    saveIndexIMP(output);
    output.close();
}

HNSWSerializer::EncodingVersion HNSWSerializer::getVersion() const { return m_version; }
