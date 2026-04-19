/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

// The vector-ingestion helpers tested here are exposed only under ENABLE_ASSERT
// (i.e. Debug builds). In pure Release builds this translation unit compiles to
// nothing, so `rstest` still links cleanly.
#ifdef ENABLE_ASSERT

#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <vector>

#include "json_test_api.h"

namespace {

// Half-precision bit patterns for exact whole-number values used by tests.
constexpr uint16_t FP16_ZERO = 0x0000;
constexpr uint16_t FP16_ONE  = 0x3c00;
constexpr uint16_t FP16_TWO  = 0x4000;
constexpr uint16_t FP16_NEG1 = 0xbc00;
constexpr uint16_t BF16_ZERO = 0x0000;
constexpr uint16_t BF16_ONE  = 0x3f80;
constexpr uint16_t BF16_TWO  = 0x4000;
constexpr uint16_t BF16_NEG1 = 0xbf80;

}  // namespace

// -------- Accept matrix -----------------------------------------------------

TEST(JsonVecAccept, FloatTargetsAcceptAllHomogeneousTags) {
  const VecSimType float_targets[] = {
      VecSimType_FLOAT32,
      VecSimType_FLOAT64,
      VecSimType_FLOAT16,
      VecSimType_BFLOAT16,
    };
  const JSONArrayType homogeneous[] = {
      JSONArrayType_I8,  JSONArrayType_U8,  JSONArrayType_I16, JSONArrayType_U16,
      JSONArrayType_F16, JSONArrayType_BF16,JSONArrayType_I32, JSONArrayType_U32,
      JSONArrayType_I64, JSONArrayType_U64, JSONArrayType_F32, JSONArrayType_F64};
  for (VecSimType t : float_targets) {
    for (JSONArrayType s : homogeneous) {
      EXPECT_TRUE(JSONTest_AcceptsJSONArrayType(t, s)) << "target=" << t << " src=" << s;
    }
    EXPECT_FALSE(JSONTest_AcceptsJSONArrayType(t, JSONArrayType_Heterogeneous));
  }
}

TEST(JsonVecAccept, IntTargetsAcceptOnlyIntegerTags) {
  const VecSimType int_targets[] = {VecSimType_INT8, VecSimType_UINT8};
  const JSONArrayType int_tags[] = {
      JSONArrayType_I8,  JSONArrayType_U8,  JSONArrayType_I16, JSONArrayType_U16,
      JSONArrayType_I32, JSONArrayType_U32, JSONArrayType_I64, JSONArrayType_U64};
  const JSONArrayType float_tags[] = {
      JSONArrayType_F16, JSONArrayType_BF16, JSONArrayType_F32, JSONArrayType_F64};
  for (VecSimType t : int_targets) {
    for (JSONArrayType s : int_tags) {
      EXPECT_TRUE(JSONTest_AcceptsJSONArrayType(t, s)) << "target=" << t << " src=" << s;
    }
    for (JSONArrayType s : float_tags) {
      EXPECT_FALSE(JSONTest_AcceptsJSONArrayType(t, s)) << "target=" << t << " src=" << s;
    }
    EXPECT_FALSE(JSONTest_AcceptsJSONArrayType(t, JSONArrayType_Heterogeneous));
  }
}

TEST(JsonVecAccept, UnsupportedTargetsRejectAll) {
  const VecSimType unsupported[] = {VecSimType_INT32, VecSimType_INT64};
  const JSONArrayType all_tags[] = {
      JSONArrayType_Heterogeneous,
      JSONArrayType_I8,  JSONArrayType_U8,  JSONArrayType_I16, JSONArrayType_U16,
      JSONArrayType_F16, JSONArrayType_BF16,JSONArrayType_I32, JSONArrayType_U32,
      JSONArrayType_I64, JSONArrayType_U64, JSONArrayType_F32, JSONArrayType_F64};
  for (VecSimType t : unsupported) {
    for (JSONArrayType s : all_tags) {
      EXPECT_FALSE(JSONTest_AcceptsJSONArrayType(t, s)) << "target=" << t << " src=" << s;
    }
  }
}

// -------- Conversion matrix -------------------------------------------------
//
// Each row below is exercised by one `<Target>_Matrix` test; each cell marks
// how that (target, source-tag) pair is expected to flow through
// `VecSim_ConvertFromTypedBuffer`. Rejected cells are covered by the
// `JsonVecAccept.*` tests above.
//
//   Legend: M = memcpy fast path  C = per-element conversion loop  R = rejected
//
//                I8  U8  I16 U16 I32 U32 I64 U64 F16 BF16 F32 F64
//   FLOAT32  :   C   C   C   C   C   C   C   C   C   C    M   C
//   FLOAT64  :   C   C   C   C   C   C   C   C   C   C    C   M
//   FLOAT16  :   C   C   C   C   C   C   C   C   M   C    C   C
//   BFLOAT16 :   C   C   C   C   C   C   C   C   C   M    C   C
//   INT8     :   M   C   C   C   C   C   C   C   R   R    R   R
//   UINT8    :   C   M   C   C   C   C   C   C   R   R    R   R

namespace {

// Runs `JSONTest_ConvertFromTypedBuffer` on `src` and bit-compares the output
// to `expected`. `label` identifies the (target, source) pair for diagnostics.
// The test values throughout this file are small whole numbers (exactly
// representable in every involved type), so plain `EXPECT_EQ` on floats is safe.
template <typename Dst, typename Src>
void VerifyConvert(const char* label, VecSimType target, JSONArrayType jtype,
                   std::initializer_list<Src> src_il,
                   std::initializer_list<Dst> expected_il) {
  std::vector<Src> src(src_il);
  std::vector<Dst> expected(expected_il);
  ASSERT_EQ(src.size(), expected.size()) << label;
  std::vector<Dst> dst(src.size(), Dst{});
  JSONTest_ConvertFromTypedBuffer(target, jtype, src.data(), src.size(),
                                  reinterpret_cast<char*>(dst.data()));
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i], dst[i]) << label << " [i=" << i << "]";
  }
}

}  // namespace

// -------- Conversion: per-target matrix tests -------------------------------
// Each test walks one row of the conversion matrix above, exercising every
// accepted source tag (including the identity/memcpy case, marked "F32", etc.).

TEST(JsonVecConvert, FLOAT32_Matrix) {
  VerifyConvert<float, int8_t>  ("F32<-I8",   VecSimType_FLOAT32, JSONArrayType_I8,   {-3, 0, 7},                    {-3.0f, 0.0f, 7.0f});
  VerifyConvert<float, uint8_t> ("F32<-U8",   VecSimType_FLOAT32, JSONArrayType_U8,   {0, 1, 255},                   {0.0f, 1.0f, 255.0f});
  VerifyConvert<float, int16_t> ("F32<-I16",  VecSimType_FLOAT32, JSONArrayType_I16,  {-1000, 0, 1000},              {-1000.0f, 0.0f, 1000.0f});
  VerifyConvert<float, uint16_t>("F32<-U16",  VecSimType_FLOAT32, JSONArrayType_U16,  {0, 1, 65535},                 {0.0f, 1.0f, 65535.0f});
  VerifyConvert<float, int32_t> ("F32<-I32",  VecSimType_FLOAT32, JSONArrayType_I32,  {-100000, 0, 100000},          {-100000.0f, 0.0f, 100000.0f});
  VerifyConvert<float, uint32_t>("F32<-U32",  VecSimType_FLOAT32, JSONArrayType_U32,  {0u, 1u, 100000u},             {0.0f, 1.0f, 100000.0f});
  VerifyConvert<float, int64_t> ("F32<-I64",  VecSimType_FLOAT32, JSONArrayType_I64,  {-123456L, 0L, 123456L},       {-123456.0f, 0.0f, 123456.0f});
  VerifyConvert<float, uint64_t>("F32<-U64",  VecSimType_FLOAT32, JSONArrayType_U64,  {0uL, 1uL, 123456uL},          {0.0f, 1.0f, 123456.0f});
  VerifyConvert<float, uint16_t>("F32<-F16",  VecSimType_FLOAT32, JSONArrayType_F16,  {FP16_NEG1, FP16_ZERO, FP16_TWO},  {-1.0f, 0.0f, 2.0f});
  VerifyConvert<float, uint16_t>("F32<-BF16", VecSimType_FLOAT32, JSONArrayType_BF16, {BF16_NEG1, BF16_ZERO, BF16_TWO},  {-1.0f, 0.0f, 2.0f});
  VerifyConvert<float, float>   ("F32<-F32",  VecSimType_FLOAT32, JSONArrayType_F32,  {-2.5f, 0.0f, 2.5f},           {-2.5f, 0.0f, 2.5f}); // M
  VerifyConvert<float, double>  ("F32<-F64",  VecSimType_FLOAT32, JSONArrayType_F64,  {-2.5, 0.0, 2.5},              {-2.5f, 0.0f, 2.5f});
}

TEST(JsonVecConvert, FLOAT64_Matrix) {
  VerifyConvert<double, int8_t>  ("F64<-I8",   VecSimType_FLOAT64, JSONArrayType_I8,   {-3, 0, 7},                    {-3.0, 0.0, 7.0});
  VerifyConvert<double, uint8_t> ("F64<-U8",   VecSimType_FLOAT64, JSONArrayType_U8,   {0, 1, 255},                   {0.0, 1.0, 255.0});
  VerifyConvert<double, int16_t> ("F64<-I16",  VecSimType_FLOAT64, JSONArrayType_I16,  {-1000, 0, 1000},              {-1000.0, 0.0, 1000.0});
  VerifyConvert<double, uint16_t>("F64<-U16",  VecSimType_FLOAT64, JSONArrayType_U16,  {0, 1, 65535},                 {0.0, 1.0, 65535.0});
  VerifyConvert<double, int32_t> ("F64<-I32",  VecSimType_FLOAT64, JSONArrayType_I32,  {-100000, 0, 100000},          {-100000.0, 0.0, 100000.0});
  VerifyConvert<double, uint32_t>("F64<-U32",  VecSimType_FLOAT64, JSONArrayType_U32,  {0u, 1u, 100000u},             {0.0, 1.0, 100000.0});
  VerifyConvert<double, int64_t> ("F64<-I64",  VecSimType_FLOAT64, JSONArrayType_I64,  {-123456L, 0L, 123456L},       {-123456.0, 0.0, 123456.0});
  VerifyConvert<double, uint64_t>("F64<-U64",  VecSimType_FLOAT64, JSONArrayType_U64,  {0uL, 1uL, 123456uL},          {0.0, 1.0, 123456.0});
  VerifyConvert<double, uint16_t>("F64<-F16",  VecSimType_FLOAT64, JSONArrayType_F16,  {FP16_NEG1, FP16_ZERO, FP16_TWO},  {-1.0, 0.0, 2.0});
  VerifyConvert<double, uint16_t>("F64<-BF16", VecSimType_FLOAT64, JSONArrayType_BF16, {BF16_NEG1, BF16_ZERO, BF16_TWO},  {-1.0, 0.0, 2.0});
  VerifyConvert<double, float>   ("F64<-F32",  VecSimType_FLOAT64, JSONArrayType_F32,  {-2.5f, 0.0f, 2.5f},           {-2.5, 0.0, 2.5});
  VerifyConvert<double, double>  ("F64<-F64",  VecSimType_FLOAT64, JSONArrayType_F64,  {-2.5, 0.0, 2.5},              {-2.5, 0.0, 2.5}); // M
}

TEST(JsonVecConvert, FLOAT16_Matrix) {
  VerifyConvert<uint16_t, int8_t>  ("F16<-I8",   VecSimType_FLOAT16, JSONArrayType_I8,   {-1, 0, 2},                        {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, uint8_t> ("F16<-U8",   VecSimType_FLOAT16, JSONArrayType_U8,   {0, 1, 2},                         {FP16_ZERO, FP16_ONE, FP16_TWO});
  VerifyConvert<uint16_t, int16_t> ("F16<-I16",  VecSimType_FLOAT16, JSONArrayType_I16,  {-1, 0, 2},                        {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, uint16_t>("F16<-U16",  VecSimType_FLOAT16, JSONArrayType_U16,  {0, 1, 2},                         {FP16_ZERO, FP16_ONE, FP16_TWO});
  VerifyConvert<uint16_t, int32_t> ("F16<-I32",  VecSimType_FLOAT16, JSONArrayType_I32,  {-1, 0, 2},                        {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, uint32_t>("F16<-U32",  VecSimType_FLOAT16, JSONArrayType_U32,  {0u, 1u, 2u},                      {FP16_ZERO, FP16_ONE, FP16_TWO});
  VerifyConvert<uint16_t, int64_t> ("F16<-I64",  VecSimType_FLOAT16, JSONArrayType_I64,  {-1L, 0L, 2L},                     {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, uint64_t>("F16<-U64",  VecSimType_FLOAT16, JSONArrayType_U64,  {0uL, 1uL, 2uL},                   {FP16_ZERO, FP16_ONE, FP16_TWO});
  VerifyConvert<uint16_t, uint16_t>("F16<-F16",  VecSimType_FLOAT16, JSONArrayType_F16,  {FP16_NEG1, FP16_ZERO, FP16_TWO},  {FP16_NEG1, FP16_ZERO, FP16_TWO}); // M
  VerifyConvert<uint16_t, uint16_t>("F16<-BF16", VecSimType_FLOAT16, JSONArrayType_BF16, {BF16_NEG1, BF16_ZERO, BF16_TWO},  {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, float>   ("F16<-F32",  VecSimType_FLOAT16, JSONArrayType_F32,  {-1.0f, 0.0f, 2.0f},               {FP16_NEG1, FP16_ZERO, FP16_TWO});
  VerifyConvert<uint16_t, double>  ("F16<-F64",  VecSimType_FLOAT16, JSONArrayType_F64,  {-1.0, 0.0, 2.0},                  {FP16_NEG1, FP16_ZERO, FP16_TWO});
}

TEST(JsonVecConvert, BFLOAT16_Matrix) {
  VerifyConvert<uint16_t, int8_t>  ("BF16<-I8",   VecSimType_BFLOAT16, JSONArrayType_I8,   {-1, 0, 2},                        {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, uint8_t> ("BF16<-U8",   VecSimType_BFLOAT16, JSONArrayType_U8,   {0, 1, 2},                         {BF16_ZERO, BF16_ONE, BF16_TWO});
  VerifyConvert<uint16_t, int16_t> ("BF16<-I16",  VecSimType_BFLOAT16, JSONArrayType_I16,  {-1, 0, 2},                        {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, uint16_t>("BF16<-U16",  VecSimType_BFLOAT16, JSONArrayType_U16,  {0, 1, 2},                         {BF16_ZERO, BF16_ONE, BF16_TWO});
  VerifyConvert<uint16_t, int32_t> ("BF16<-I32",  VecSimType_BFLOAT16, JSONArrayType_I32,  {-1, 0, 2},                        {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, uint32_t>("BF16<-U32",  VecSimType_BFLOAT16, JSONArrayType_U32,  {0u, 1u, 2u},                      {BF16_ZERO, BF16_ONE, BF16_TWO});
  VerifyConvert<uint16_t, int64_t> ("BF16<-I64",  VecSimType_BFLOAT16, JSONArrayType_I64,  {-1L, 0L, 2L},                     {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, uint64_t>("BF16<-U64",  VecSimType_BFLOAT16, JSONArrayType_U64,  {0uL, 1uL, 2uL},                   {BF16_ZERO, BF16_ONE, BF16_TWO});
  VerifyConvert<uint16_t, uint16_t>("BF16<-F16",  VecSimType_BFLOAT16, JSONArrayType_F16,  {FP16_NEG1, FP16_ZERO, FP16_TWO},  {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, uint16_t>("BF16<-BF16", VecSimType_BFLOAT16, JSONArrayType_BF16, {BF16_NEG1, BF16_ZERO, BF16_TWO},  {BF16_NEG1, BF16_ZERO, BF16_TWO}); // M
  VerifyConvert<uint16_t, float>   ("BF16<-F32",  VecSimType_BFLOAT16, JSONArrayType_F32,  {-1.0f, 0.0f, 2.0f},               {BF16_NEG1, BF16_ZERO, BF16_TWO});
  VerifyConvert<uint16_t, double>  ("BF16<-F64",  VecSimType_BFLOAT16, JSONArrayType_F64,  {-1.0, 0.0, 2.0},                  {BF16_NEG1, BF16_ZERO, BF16_TWO});
}

TEST(JsonVecConvert, INT8_Matrix) {
  // Values stay within int8_t range so the final `(int8_t)` cast is identity.
  VerifyConvert<int8_t, int8_t>  ("I8<-I8",  VecSimType_INT8, JSONArrayType_I8,  {-128, 0, 127}, {-128, 0, 127}); // M
  VerifyConvert<int8_t, uint8_t> ("I8<-U8",  VecSimType_INT8, JSONArrayType_U8,  {0, 1, 127},    {0, 1, 127});
  VerifyConvert<int8_t, int16_t> ("I8<-I16", VecSimType_INT8, JSONArrayType_I16, {-128, 0, 127}, {-128, 0, 127});
  VerifyConvert<int8_t, uint16_t>("I8<-U16", VecSimType_INT8, JSONArrayType_U16, {0, 1, 127},    {0, 1, 127});
  VerifyConvert<int8_t, int32_t> ("I8<-I32", VecSimType_INT8, JSONArrayType_I32, {-128, 0, 127}, {-128, 0, 127});
  VerifyConvert<int8_t, uint32_t>("I8<-U32", VecSimType_INT8, JSONArrayType_U32, {0u, 1u, 127u}, {0, 1, 127});
  VerifyConvert<int8_t, int64_t> ("I8<-I64", VecSimType_INT8, JSONArrayType_I64, {-128L, 0L, 127L}, {-128, 0, 127});
  VerifyConvert<int8_t, uint64_t>("I8<-U64", VecSimType_INT8, JSONArrayType_U64, {0uL, 1uL, 127uL}, {0, 1, 127});
}

TEST(JsonVecConvert, UINT8_Matrix) {
  VerifyConvert<uint8_t, int8_t>  ("U8<-I8",  VecSimType_UINT8, JSONArrayType_I8,  {0, 1, 127},    {0, 1, 127});
  VerifyConvert<uint8_t, uint8_t> ("U8<-U8",  VecSimType_UINT8, JSONArrayType_U8,  {0, 1, 255},    {0, 1, 255}); // M
  VerifyConvert<uint8_t, int16_t> ("U8<-I16", VecSimType_UINT8, JSONArrayType_I16, {0, 1, 255},    {0, 1, 255});
  VerifyConvert<uint8_t, uint16_t>("U8<-U16", VecSimType_UINT8, JSONArrayType_U16, {0, 1, 255},    {0, 1, 255});
  VerifyConvert<uint8_t, int32_t> ("U8<-I32", VecSimType_UINT8, JSONArrayType_I32, {0, 1, 255},    {0, 1, 255});
  VerifyConvert<uint8_t, uint32_t>("U8<-U32", VecSimType_UINT8, JSONArrayType_U32, {0u, 1u, 255u}, {0, 1, 255});
  VerifyConvert<uint8_t, int64_t> ("U8<-I64", VecSimType_UINT8, JSONArrayType_I64, {0L, 1L, 255L}, {0, 1, 255});
  VerifyConvert<uint8_t, uint64_t>("U8<-U64", VecSimType_UINT8, JSONArrayType_U64, {0uL, 1uL, 255uL}, {0, 1, 255});
}

// -------- Edge cases --------------------------------------------------------

// Overflowing values on an integer target wrap like V6's `(int8_t)(long long)x`
// (raw two's-complement truncation), not saturate.
TEST(JsonVecConvert, INT8_TruncatesOverflow) {
  VerifyConvert<int8_t, int16_t>("I8<-I16 overflow", VecSimType_INT8, JSONArrayType_I16,
                                 {256, 257, 384}, {0, 1, -128});
}

#endif // ENABLE_ASSERT
