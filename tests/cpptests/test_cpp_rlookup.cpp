/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rlookup.h"
#include "gtest/gtest.h"

class RLookupTest : public ::testing::Test {};

TEST_F(RLookupTest, testInit) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testFlags) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  RLookupKey *fook = RLookup_GetKey_Read(&lk, "foo", RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(NULL, fook);
  // Try with M_WRITE
  fook = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(fook);
  // Try again with M_WRITE
  RLookupKey *tmpk = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(NULL, tmpk);
  // Try again with M_WRITE and OVERWRITE
  RLookupKey *tmpk2 = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_OVERRIDE);
  ASSERT_TRUE(tmpk2);

  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testRow) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  RLookupKey *fook = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  RLookupKey *bark = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = {0};
  RSValue *vfoo = RS_Int64Val(42);
  RSValue *vbar = RS_Int64Val(666);

  ASSERT_EQ(1, vfoo->refcount);
  RLookup_WriteKey(fook, &rr, vfoo);
  ASSERT_EQ(2, vfoo->refcount);

  RSValue *vtmp = RLookup_GetItem(fook, &rr);
  ASSERT_EQ(vfoo, vtmp);
  ASSERT_EQ(2, vfoo->refcount);
  ASSERT_EQ(1, rr.ndyn);

  // Write a NULL value
  RLookup_WriteKey(fook, &rr, RS_NullVal());
  ASSERT_EQ(1, vfoo->refcount);

  // Get the 'bar' key -- should be NULL
  ASSERT_TRUE(NULL == RLookup_GetItem(bark, &rr));

  // Clean up the row
  RLookupRow_Wipe(&rr);
  vtmp = RLookup_GetItem(fook, &rr);
  ASSERT_TRUE(NULL == RLookup_GetItem(fook, &rr));

  RSValue_Decref(vfoo);
  RSValue_Decref(vbar);
  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testCloneKey) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);

  // Create original key
  RLookupKey *original = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(original);

  // Clone the key
  RLookupKey *cloned = RLookupKey_Clone(original);
  ASSERT_TRUE(cloned);

  // Verify basic fields are copied
  ASSERT_EQ(original->dstidx, cloned->dstidx);
  ASSERT_EQ(original->svidx, cloned->svidx);
  ASSERT_EQ(original->name_len, cloned->name_len);

  // Cloned key should have NAMEALLOC flag since we always allocate strings
  ASSERT_TRUE(cloned->flags & RLOOKUP_F_NAMEALLOC);

  // Verify strings are the same content
  ASSERT_STREQ(original->name, cloned->name);
  ASSERT_STREQ(original->path, cloned->path);

  // Test writing with original and reading with cloned (same row, same lookup)
  RLookupRow rr = {0};
  RSValue *value = RS_Int64Val(123);

  RLookup_WriteKey(original, &rr, value);
  RSValue *retrieved = RLookup_GetItem(cloned, &rr);
  ASSERT_EQ(value, retrieved);

  // Test that the value can be read correctly
  double num_val = 0;
  int result = RSValue_ToNumber(retrieved, &num_val);
  ASSERT_EQ(1, result);  // Should succeed
  ASSERT_EQ(123.0, num_val);

    // Cleanup
  RSValue_Decref(value);
  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
  RLookupKey_Free(cloned);  // Properly free the cloned key
}

TEST_F(RLookupTest, testCloneLookup) {
  RLookup original_lk = {0};
  RLookup_Init(&original_lk, NULL);

  // Create some keys in original
  RLookupKey *key1 = RLookup_GetKey_Write(&original_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *key2 = RLookup_GetKey_Write(&original_lk, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(key1);
  ASSERT_TRUE(key2);

  // Clone the entire lookup
  RLookup *cloned_lk = RLookup_Clone(&original_lk);
  ASSERT_TRUE(cloned_lk);

  // Verify basic structure is copied
  ASSERT_EQ(original_lk.rowlen, cloned_lk->rowlen);
  ASSERT_EQ(original_lk.options, cloned_lk->options);
  ASSERT_EQ(original_lk.spcache, cloned_lk->spcache);

  // Test iterative read using both lookups on same row
  RLookupRow rr = {0};
  RSValue *value1 = RS_Int64Val(111);
  RSValue *value2 = RS_Int64Val(222);

  // Write using original
  RLookup_WriteKey(key1, &rr, value1);
  RLookup_WriteKey(key2, &rr, value2);

  // Read using both original and cloned - should get same results
  RLookupKey *cloned_key1 = RLookup_GetKey_Read(cloned_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *cloned_key2 = RLookup_GetKey_Read(cloned_lk, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(cloned_key1);
  ASSERT_TRUE(cloned_key2);

  RSValue *orig_val1 = RLookup_GetItem(key1, &rr);
  RSValue *orig_val2 = RLookup_GetItem(key2, &rr);
  RSValue *clone_val1 = RLookup_GetItem(cloned_key1, &rr);
  RSValue *clone_val2 = RLookup_GetItem(cloned_key2, &rr);

  ASSERT_EQ(orig_val1, clone_val1);
  ASSERT_EQ(orig_val2, clone_val2);

  // Test that values can be read correctly
  double num_val1 = 0, num_val2 = 0;
  int result1 = RSValue_ToNumber(clone_val1, &num_val1);
  int result2 = RSValue_ToNumber(clone_val2, &num_val2);
  ASSERT_EQ(1, result1);  // Should succeed
  ASSERT_EQ(1, result2);  // Should succeed
  ASSERT_EQ(111.0, num_val1);
  ASSERT_EQ(222.0, num_val2);

  // Cleanup
  RSValue_Decref(value1);
  RSValue_Decref(value2);
  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&original_lk);
  RLookup_Cleanup(cloned_lk);
  rm_free(cloned_lk);
}

TEST_F(RLookupTest, testCloneWithAdditionalFields) {
  RLookup source_lk = {0};
  RLookup_Init(&source_lk, NULL);

  // Create keys in source
  RLookupKey *src_key1 = RLookup_GetKey_Write(&source_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *src_key2 = RLookup_GetKey_Write(&source_lk, "field2", RLOOKUP_F_NOFLAGS);

  // Clone and add more fields to target
  RLookup *target_lk = RLookup_Clone(&source_lk);
  RLookupKey *tgt_key3 = RLookup_GetKey_Write(target_lk, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *tgt_key4 = RLookup_GetKey_Write(target_lk, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(tgt_key3);
  ASSERT_TRUE(tgt_key4);

  // Verify target has more keys than source
  ASSERT_GT(target_lk->rowlen, source_lk.rowlen);

  // Create row with source data
  RLookupRow rr = {0};
  RSValue *value1 = RS_Int64Val(100);
  RSValue *value2 = RS_Int64Val(200);

  RLookup_WriteKey(src_key1, &rr, value1);
  RLookup_WriteKey(src_key2, &rr, value2);

  // Target should still be able to read from row created by source
  RLookupKey *tgt_key1 = RLookup_GetKey_Read(target_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *tgt_key2 = RLookup_GetKey_Read(target_lk, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(tgt_key1);
  ASSERT_TRUE(tgt_key2);

  RSValue *read_val1 = RLookup_GetItem(tgt_key1, &rr);
  RSValue *read_val2 = RLookup_GetItem(tgt_key2, &rr);

  // Test that values can be read correctly
  double num_val1 = 0, num_val2 = 0;
  int result1 = RSValue_ToNumber(read_val1, &num_val1);
  int result2 = RSValue_ToNumber(read_val2, &num_val2);
  ASSERT_EQ(1, result1);  // Should succeed
  ASSERT_EQ(1, result2);  // Should succeed
  ASSERT_EQ(100.0, num_val1);
  ASSERT_EQ(200.0, num_val2);

  // Write and read to added fields
  RSValue *value3 = RS_Int64Val(300);
  RSValue *value4 = RS_Int64Val(400);

  RLookup_WriteKey(tgt_key3, &rr, value3);
  RLookup_WriteKey(tgt_key4, &rr, value4);

  RSValue *read_val3 = RLookup_GetItem(tgt_key3, &rr);
  RSValue *read_val4 = RLookup_GetItem(tgt_key4, &rr);

  // Test that additional field values can be read correctly
  double num_val3 = 0, num_val4 = 0;
  int result3 = RSValue_ToNumber(read_val3, &num_val3);
  int result4 = RSValue_ToNumber(read_val4, &num_val4);
  ASSERT_EQ(1, result3);  // Should succeed
  ASSERT_EQ(1, result4);  // Should succeed
  ASSERT_EQ(300.0, num_val3);
  ASSERT_EQ(400.0, num_val4);

  // Verify original lookup cannot read the added fields (keys don't exist)
  ASSERT_EQ(NULL, RLookup_GetKey_Read(&source_lk, "field3", RLOOKUP_F_NOFLAGS));
  ASSERT_EQ(NULL, RLookup_GetKey_Read(&source_lk, "field4", RLOOKUP_F_NOFLAGS));

  // Cleanup
  RSValue_Decref(value1);
  RSValue_Decref(value2);
  RSValue_Decref(value3);
  RSValue_Decref(value4);
  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&source_lk);
  RLookup_Cleanup(target_lk);
  rm_free(target_lk);
}

TEST_F(RLookupTest, testCloneNullHandling) {
  // Test NULL handling
  ASSERT_EQ(NULL, RLookupKey_Clone(NULL));
  ASSERT_EQ(NULL, RLookup_Clone(NULL));

  // Test with allocated strings by manually creating a key with NAMEALLOC flag
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);

  // Create key normally first
  RLookupKey *key = RLookup_GetKey_Write(&lk, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(key);

  // Save original values before modifying
  const char *orig_name = key->name;
  const char *orig_path = key->path;
  uint32_t orig_flags = key->flags;

  // Manually set up allocated strings to test the deep copy behavior
  key->flags |= RLOOKUP_F_NAMEALLOC;
  key->name = rm_strdup("allocated_field");
  key->path = key->name;  // Use same string for both
  key->name_len = strlen(key->name);

  // Clone should also have allocated strings
  RLookupKey *cloned = RLookupKey_Clone(key);
  ASSERT_TRUE(cloned);
  ASSERT_TRUE(cloned->flags & RLOOKUP_F_NAMEALLOC);
  ASSERT_STREQ(key->name, cloned->name);
  ASSERT_NE(key->name, cloned->name);  // Different pointers

        // Cleanup cloned key properly
  RLookupKey_Free(cloned);

  // Restore original values before cleanup to avoid double-free
  rm_free((void*)key->name);
  key->name = orig_name;
  key->path = orig_path;
  key->flags = orig_flags;

  // Cleanup
  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testCloneAlwaysAllocatesStrings) {
  // CRITICAL TEST: Verify that cloned keys ALWAYS allocate their own strings,
  // even when the source key just points to const strings (no RLOOKUP_F_NAMEALLOC)

  RLookup lk = {0};
  RLookup_Init(&lk, NULL);

  // Create a key that points to const strings (typical case with RLOOKUP_F_NOFLAGS)
  const char *const_name = "test_field";
  RLookupKey *original = RLookup_GetKey_Write(&lk, const_name, RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(original);

  // Verify original key points to the const string and has no NAMEALLOC flag
  ASSERT_EQ(const_name, original->name);  // Same pointer - points to const string
  ASSERT_FALSE(original->flags & RLOOKUP_F_NAMEALLOC);  // No allocation flag

  // Clone the key
  RLookupKey *cloned = RLookupKey_Clone(original);
  ASSERT_TRUE(cloned);

  // Verify clone has allocated its own strings
  ASSERT_TRUE(cloned->flags & RLOOKUP_F_NAMEALLOC);  // Must have allocation flag
  ASSERT_STREQ(original->name, cloned->name);  // Same content
  ASSERT_NE(original->name, cloned->name);     // Different pointers - cloned allocated its own

  // Verify path handling
  if (original->path == original->name) {
    ASSERT_EQ(cloned->name, cloned->path);  // Should point to cloned name
  }

  // Cleanup
  RLookup_Cleanup(&lk);
  RLookupKey_Free(cloned);
}

TEST_F(RLookupTest, testCloneReferenceCountingSpecCache) {
  // This test would require a mock IndexSpecCache to verify reference counting
  // For now, we test that spcache is properly copied
  RLookup lk1 = {0};
  RLookup lk2 = {0};
  RLookup_Init(&lk1, NULL);
  RLookup_Init(&lk2, NULL);

  // Test copying with NULL spcache
  RLookup *copy1 = RLookup_Clone(&lk1);
  ASSERT_TRUE(copy1);
  ASSERT_EQ(NULL, copy1->spcache);

  RLookup_Cleanup(&lk1);
  RLookup_Cleanup(&lk2);
  RLookup_Cleanup(copy1);
  rm_free(copy1);
}
