// Copyright (c) 2006-Present, Redis Ltd.
// All rights reserved.
//
// Licensed under your choice of the Redis Source Available License 2.0
// (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
// GNU Affero General Public License v3 (AGPLv3).

#include <gtest/gtest.h>
#include "storage/encoding.h"
#include <cmath>
#include <limits>

// Test that endianness detection works at compile time
TEST(EncodingTest, EndiannessDetection) {
    // This test verifies that endianness is detected at compile time
#if defined(FORCE_BIG_ENDIAN_TEST)
    EXPECT_FALSE(encoding::kIsLittleEndian) << "Should be in forced big-endian mode";
    std::cout << "Running in FORCED BIG-ENDIAN mode for testing" << std::endl;
#endif

    // Print the detected endianness for debugging
    if (encoding::kIsLittleEndian) {
        std::cout << "Detected: Little-endian system" << std::endl;
    } else {
        std::cout << "Detected: Big-endian system" << std::endl;
    }
}

TEST(EncodingTest, LittleEndianByteOrder) {
    {
        char buf[2];
        encoding::EncodeFixedLE<uint16_t>(buf, 0x1234);
#if defined(FORCE_BIG_ENDIAN_TEST)
        // In forced big-endian mode, we're simulating a big-endian system on x86
        // The EncodeFixedLE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
#else
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x12);
#endif
    }

    {
        char buf[4];
        encoding::EncodeFixedLE<uint32_t>(buf, 0x12345678);
#if defined(FORCE_BIG_ENDIAN_TEST)
        // In forced big-endian mode, we're simulating a big-endian system on x86
        // The EncodeFixedLE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x78);
#else
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x12);
#endif
    }

    {
        char buf[8];
        encoding::EncodeFixedLE<uint64_t>(buf, 0x123456789ABCDEF0ULL);

#if defined(FORCE_BIG_ENDIAN_TEST)
        // In forced big-endian mode, we're simulating a big-endian system on x86
        // The EncodeFixedLE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[4]), 0x9A);
        EXPECT_EQ(static_cast<unsigned char>(buf[5]), 0xBC);
        EXPECT_EQ(static_cast<unsigned char>(buf[6]), 0xDE);
        EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0xF0);
#else
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0xF0);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0xDE);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0xBC);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x9A);
        EXPECT_EQ(static_cast<unsigned char>(buf[4]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[5]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[6]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0x12);

#endif
    }
}

TEST(EncodingTest, BigEndianByteOrder) {
    {
        char buf[2];
        encoding::EncodeFixedBE<uint16_t>(buf, 0x1234);
#if defined(FORCE_BIG_ENDIAN_TEST)
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x12);

#else
        // In little-endian mode, the EncodeFixedBE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
#endif
    }

    {
        char buf[4];
        encoding::EncodeFixedBE<uint32_t>(buf, 0x12345678);
#if defined(FORCE_BIG_ENDIAN_TEST)

        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x12);
#else
        // In little-endian mode, the EncodeFixedBE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x78);
#endif
    }

    {
        char buf[8];
        encoding::EncodeFixedBE<uint64_t>(buf, 0x123456789ABCDEF0ULL);

#if defined(FORCE_BIG_ENDIAN_TEST)
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0xF0);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0xDE);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0xBC);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x9A);
        EXPECT_EQ(static_cast<unsigned char>(buf[4]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[5]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[6]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0x12);

#else
        // In little-endian mode, the EncodeFixedBE should do a byte-by-byte swap
        EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x12);
        EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x34);
        EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x56);
        EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x78);
        EXPECT_EQ(static_cast<unsigned char>(buf[4]), 0x9A);
        EXPECT_EQ(static_cast<unsigned char>(buf[5]), 0xBC);
        EXPECT_EQ(static_cast<unsigned char>(buf[6]), 0xDE);
        EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0xF0);

#endif
    }
}

// Test that LE and BE encodings are byte-swapped versions of each other
TEST(EncodingTest, LEandBEAreSwapped) {
    {
        uint16_t value = 0x1234;
        char buf_le[2], buf_be[2];

        encoding::EncodeFixedLE<uint16_t>(buf_le, value);
        encoding::EncodeFixedBE<uint16_t>(buf_be, value);

        EXPECT_EQ(buf_le[0], buf_be[1]);
        EXPECT_EQ(buf_le[1], buf_be[0]);
    }

    {
        uint32_t value = 0x12345678;
        char buf_le[4], buf_be[4];

        encoding::EncodeFixedLE<uint32_t>(buf_le, value);
        encoding::EncodeFixedBE<uint32_t>(buf_be, value);

        EXPECT_EQ(buf_le[0], buf_be[3]);
        EXPECT_EQ(buf_le[1], buf_be[2]);
        EXPECT_EQ(buf_le[2], buf_be[1]);
        EXPECT_EQ(buf_le[3], buf_be[0]);
    }

    {
        uint64_t value = 0x123456789ABCDEF0;
        char buf_le[8], buf_be[8];

        encoding::EncodeFixedLE<uint64_t>(buf_le, value);
        encoding::EncodeFixedBE<uint64_t>(buf_be, value);

        EXPECT_EQ(buf_le[0], buf_be[7]);
        EXPECT_EQ(buf_le[1], buf_be[6]);
        EXPECT_EQ(buf_le[2], buf_be[5]);
        EXPECT_EQ(buf_le[3], buf_be[4]);
        EXPECT_EQ(buf_le[4], buf_be[3]);
        EXPECT_EQ(buf_le[5], buf_be[2]);
        EXPECT_EQ(buf_le[6], buf_be[1]);
        EXPECT_EQ(buf_le[7], buf_be[0]);
    }
}

TEST(EncodingTest, EncodeDecodeLE) {
    {
        uint16_t values[] = {0, 1, 255, 256, 32767, 65535};

        for (uint16_t value : values) {
            char buf[2];
            encoding::EncodeFixedLE<uint16_t>(buf, value);
            uint16_t decoded = encoding::DecodeFixedLE<uint16_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }

    {
        uint32_t values[] = {0, 1, 255, 256, 65535, 65536, 0x12345678, 0xFFFFFFFF};

        for (uint32_t value : values) {
            char buf[4];
            encoding::EncodeFixedLE<uint32_t>(buf, value);
            uint32_t decoded = encoding::DecodeFixedLE<uint32_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }
    {
        uint64_t values[] = {0, 1, 255, 256, 65535, 65536, 0x123456789ABCDEF0ULL, 0xFFFFFFFFFFFFFFFFULL};

        for (uint64_t value : values) {
            char buf[8];
            encoding::EncodeFixedLE<uint64_t>(buf, value);
            uint64_t decoded = encoding::DecodeFixedLE<uint64_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }
}

TEST(EncodingTest, EncodeDecodeBE) {
    {
        uint16_t values[] = {0, 1, 255, 256, 32767, 65535};

        for (uint16_t value : values) {
            char buf[2];
            encoding::EncodeFixedBE<uint16_t>(buf, value);
            uint16_t decoded = encoding::DecodeFixedBE<uint16_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }

    {
        uint32_t values[] = {0, 1, 255, 256, 65535, 65536, 0x12345678, 0xFFFFFFFF};

        for (uint32_t value : values) {
            char buf[4];
            encoding::EncodeFixedBE<uint32_t>(buf, value);
            uint32_t decoded = encoding::DecodeFixedBE<uint32_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }

    {
        uint64_t values[] = {0, 1, 255, 256, 65535, 65536, 0x123456789ABCDEF0ULL, 0xFFFFFFFFFFFFFFFFULL};

        for (uint64_t value : values) {
            char buf[8];
            encoding::EncodeFixedBE<uint64_t>(buf, value);
            uint64_t decoded = encoding::DecodeFixedBE<uint64_t>(buf);
            EXPECT_EQ(value, decoded) << "Failed for value: 0x" << std::hex << value;
        }
    }
}

// =============================================================================
// Float/Double Serialization Tests
// =============================================================================

// Test that float/double serialization works correctly for little-endian
TEST(EncodingTest, FloatDoubleEncodingDecodingLE) {
    {
        float test_values[] = {0.0f,
                               -0.0f,
                               1.0f,
                               -1.0f,
                               3.14159f,
                               -2.71828f,
                               std::numeric_limits<float>::min(),
                               std::numeric_limits<float>::max(),
                               std::numeric_limits<float>::infinity(),
                               -std::numeric_limits<float>::infinity()};

        for (float f : test_values) {

            char buf[4];
            encoding::EncodeFixedLE<float>(buf, f);
            float decoded_f = encoding::DecodeFixedLE<float>(buf);

            EXPECT_FLOAT_EQ(decoded_f, f) << "Failed for value: " << f;
        }
    }

    {
        double test_values[] = {0.0,
                                -0.0,
                                1.0,
                                -1.0,
                                3.14159,
                                -2.71828,
                                std::numeric_limits<double>::min(),
                                std::numeric_limits<double>::max(),
                                std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity()};

        for (double d : test_values) {

            char buf[8];
            encoding::EncodeFixedLE<double>(buf, d);
            double decoded_d = encoding::DecodeFixedLE<double>(buf);

            EXPECT_DOUBLE_EQ(decoded_d, d) << "Failed for value: " << d;
        }
    }
#if defined(FORCE_BIG_ENDIAN_TEST)
    std::cout << "  [Verified float/double encoding in forced big-endian mode]" << std::endl;
#else
    std::cout << "  [Verified float/double encoding in native little-endian mode]" << std::endl;
#endif
}

// Test that float/double serialization works correctly for big-endian
TEST(EncodingTest, FloatDoubleEncodingDecodingBE) {
    {
        float test_values[] = {0.0f,
                               -0.0f,
                               1.0f,
                               -1.0f,
                               3.14159f,
                               -2.71828f,
                               std::numeric_limits<float>::min(),
                               std::numeric_limits<float>::max(),
                               std::numeric_limits<float>::infinity(),
                               -std::numeric_limits<float>::infinity()};

        for (float f : test_values) {

            char buf[4];
            encoding::EncodeFixedBE<float>(buf, f);
            float decoded_f = encoding::DecodeFixedBE<float>(buf);

            EXPECT_FLOAT_EQ(decoded_f, f) << "Failed for value: " << f;
        }
    }

    {
        double test_values[] = {0.0,
                                -0.0,
                                1.0,
                                -1.0,
                                3.14159,
                                -2.71828,
                                std::numeric_limits<double>::min(),
                                std::numeric_limits<double>::max(),
                                std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity()};

        for (double d : test_values) {

            char buf[8];
            encoding::EncodeFixedBE<double>(buf, d);
            double decoded_d = encoding::DecodeFixedBE<double>(buf);

            EXPECT_DOUBLE_EQ(decoded_d, d) << "Failed for value: " << d;
        }
    }
#if defined(FORCE_BIG_ENDIAN_TEST)
    std::cout << "  [Verified float/double encoding in forced big-endian mode]" << std::endl;
#else
    std::cout << "  [Verified float/double encoding in native little-endian mode]" << std::endl;
#endif
}
