/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <ostream>
#include <istream>

/*
 * Serializer Abstraction Layer for Vector Indexes
 * -----------------------------------------------
 * This header defines the base `Serializer` class, which provides a generic interface for
 * serializing vector indexes to disk. It is designed to be inherited
 * by algorithm-specific serializers (e.g., HNSWSerializer, SVSSerializer), and provides a
 * versioned, extensible mechanism for managing persistent representations of index state.
 * Each serializer subclass must define its own EncodingVersion enum.
 * How to Extend:
 * 1. Derive a new class from `Serializer`, e.g., `MyIndexSerializer`.
 * 2. Implement `saveIndex()` and `saveIndexIMP()`.
 * 3. Implement `saveIndexFields()` to write out relevant fields in a deterministic order.
 * 4. Optionally, add version-aware deserialization methods.
 *
 * Example Inheritance Tree:
 *   Serializer (abstract)
 *      ├── HNSWSerializer
 *      │     └── HNSWIndex<T, U>
 *      └── SVSSerializer
 *            └── SVSIndex<T...>
 */

class Serializer {
public:
    enum class EncodingVersion { INVALID };

    Serializer(EncodingVersion version = EncodingVersion::INVALID) : m_version(version) {}

    virtual void saveIndex(const std::string &location) = 0;

    EncodingVersion getVersion() const;

    static EncodingVersion ReadVersion(std::ifstream &input);

    // Helper functions for serializing the index.
    template <typename T>
    static inline void writeBinaryPOD(std::ostream &out, const T &podRef) {
        out.write((char *)&podRef, sizeof(T));
    }

    template <typename T>
    static inline void readBinaryPOD(std::istream &in, T &podRef) {
        in.read((char *)&podRef, sizeof(T));
    }

protected:
    EncodingVersion m_version;

    // Index memory size might be changed during index saving.
    virtual void saveIndexIMP(std::ofstream &output) = 0;

private:
    virtual void saveIndexFields(std::ofstream &output) const = 0;
};
