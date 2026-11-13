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

#pragma once

#include <fstream>
#include <string>
#include "VecSim/utils/serializer.h"

// Middle layer for HNSW serialization
// Abstract functions should be implemented by the templated HNSW index

class HNSWSerializer : public Serializer {
public:
    enum class EncodingVersion {
        DEPRECATED = 2, // Last deprecated version
        V3,
        V4,
        V5, // SQ8 quantization fields (quantType + mean vector)
        INVALID
    };

    explicit HNSWSerializer(EncodingVersion version = EncodingVersion::V4);

    static EncodingVersion ReadVersion(std::ifstream &input);

    void saveIndex(const std::string &location);

    EncodingVersion getVersion() const;

protected:
    EncodingVersion m_version;

private:
    virtual EncodingVersion getWriteVersion() const = 0;

    void saveIndexFields(std::ofstream &output) const = 0;
};
