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

/*
 * Test RLookupKey_Clone functionality:
 * Verifies that a cloned key can access the same data as the original key
 * by writing with the original and reading with the clone.
 */
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

/*
 * Test RLookup_CloneInto basic functionality:
 * Verifies that cloning preserves lookup structure and enables cross-lookup data access.
 * Creates a source lookup with fields, clones it into a destination, then validates
 * that data written using original keys can be read using cloned keys.
 */
TEST_F(RLookupTest, testCloneLookup) {
  RLookup original_lk = {0};
  RLookup_Init(&original_lk, NULL);

  // Create some keys in original
  RLookupKey *key1 = RLookup_GetKey_Write(&original_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *key2 = RLookup_GetKey_Write(&original_lk, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(key1);
  ASSERT_TRUE(key2);

  // Initialize destination lookup (simulating hybrid_request.c pattern)
  RLookup cloned_lk = {0};
  RLookup_Init(&cloned_lk, NULL);

  // Clone into initialized destination
  RLookup_CloneInto(&cloned_lk, &original_lk);

  // Verify basic structure is copied but initialization state is preserved
  ASSERT_EQ(original_lk.rowlen, cloned_lk.rowlen);
  ASSERT_EQ(original_lk.options, cloned_lk.options);  // Should preserve destination's options
  ASSERT_EQ(NULL, cloned_lk.spcache);   // Should preserve destination's spcache

  // Test iterative read using both lookups on same row
  RLookupRow rr = {0};
  RSValue *value1 = RS_Int64Val(111);
  RSValue *value2 = RS_Int64Val(222);

  // Write using original
  RLookup_WriteKey(key1, &rr, value1);
  RLookup_WriteKey(key2, &rr, value2);

  // Read using both original and cloned - should get same results
  RLookupKey *cloned_key1 = RLookup_GetKey_Read(&cloned_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *cloned_key2 = RLookup_GetKey_Read(&cloned_lk, "field2", RLOOKUP_F_NOFLAGS);
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
  RLookup_Cleanup(&cloned_lk);
}

/*
 * Test cloned RLookup independence and extensibility:
 * Verifies that a cloned lookup remains functional even when the source is extended
 * with additional fields.
 * Create an RLookup, add some keys to it, clone it, add more keys to the source, and verify that the clone can read the original keys.
 */
TEST_F(RLookupTest, testCloneWithAdditionalFields) {
  RLookup source_lk = {0};
  RLookup_Init(&source_lk, NULL);

  // Create keys in source
  RLookupKey *src_key1 = RLookup_GetKey_Write(&source_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *src_key2 = RLookup_GetKey_Write(&source_lk, "field2", RLOOKUP_F_NOFLAGS);

  // Initialize target and clone source into it, then add more fields
  RLookup target_lk = {0};
  RLookup_Init(&target_lk, NULL);
  RLookup_CloneInto(&target_lk, &source_lk);

  RLookupKey *tgt_key3 = RLookup_GetKey_Write(&target_lk, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *tgt_key4 = RLookup_GetKey_Write(&target_lk, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(tgt_key3);
  ASSERT_TRUE(tgt_key4);

  // Verify target has more keys than source
  ASSERT_GT(target_lk.rowlen, source_lk.rowlen);

  // Create row with source data
  RLookupRow rr = {0};
  RSValue *value1 = RS_Int64Val(100);
  RSValue *value2 = RS_Int64Val(200);

  RLookup_WriteKey(src_key1, &rr, value1);
  RLookup_WriteKey(src_key2, &rr, value2);

  // Target should still be able to read from row created by source
  RLookupKey *tgt_key1 = RLookup_GetKey_Read(&target_lk, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *tgt_key2 = RLookup_GetKey_Read(&target_lk, "field2", RLOOKUP_F_NOFLAGS);
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
  RLookup_Cleanup(&target_lk);
}

/*
 * Test RLookupKey_Clone string allocation behavior:
 * Verifies that cloned keys always allocate their own string copies, even when
 * the source key references const strings without the NAMEALLOC flag.
 */
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

/*
 * Test RLookup_CloneInto initialization preservation:
 * Verifies that cloning preserves the destination's initialization state (like spcache)
 * while copying the source's structure. Creates a destination with specific initialization,
 * clones a source into it, and validates that initialization state is maintained.
 */
TEST_F(RLookupTest, testCloneIntoPreservesInitialization) {
  // Test that RLookup_CloneInto preserves destination's initialization
  RLookup source = {0};
  RLookup_Init(&source, NULL);

  // Add some keys to source
  RLookupKey *src_key = RLookup_GetKey_Write(&source, "test", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(src_key);

  // Initialize destination with different state
  IndexSpecCache spcache = {0};
  RLookup dest = {0};
  RLookup_Init(&dest, &spcache);

  // Clone into destination - should preserve dest's initialization
  RLookup_CloneInto(&dest, &source);

  // Verify dest's initialization is preserved but structure is copied
  ASSERT_EQ(&spcache, dest.spcache);   // Preserved
  ASSERT_EQ(source.rowlen, dest.rowlen); // Copied

  // Verify options are copied
  ASSERT_EQ(source.options, dest.options);

  // Verify key was copied
  RLookupKey *dest_key = RLookup_GetKey_Read(&dest, "test", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key);

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test RLookup_CloneInto with field override functionality:
 * Ensures the destination RLookup behaves correctly after a key in the source is overridden.
 * Creates a lookup, clones it, then overrides an existing field with the
 * RLOOKUP_F_OVERRIDE flag. Verifies that data is written using the override key
 * and read using the original key.
 */
TEST_F(RLookupTest, testCloneWithOverride) {
  // Create source lookup with initial field
  RLookup source = {0};
  RLookup_Init(&source, NULL);

  RLookupKey *src_key = RLookup_GetKey_Write(&source, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(src_key);

  // Initialize destination with spcache
  IndexSpecCache spcache = {0};
  RLookup dest = {0};
  RLookup_Init(&dest, &spcache);

  // Clone source into destination BEFORE override
  RLookup_CloneInto(&dest, &source);

  // Now override the same field in source with override flag
  RLookupKey *override_src_key = RLookup_GetKey_Write(&source, "test_field", RLOOKUP_F_OVERRIDE);
  ASSERT_TRUE(override_src_key);
  ASSERT_NE(src_key, override_src_key);  // Should be different keys

  // Verify the overridden field structure is cloned properly
  RLookupKey *dest_key = RLookup_GetKey_Read(&dest, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key);

  // Test functionality: write to cloned key and read back
  RLookupRow row = {0};
  RSValue *test_value = RS_Int64Val(42);

  RLookup_WriteKey(dest_key, &row, test_value);
  RSValue *read_value = RLookup_GetItem(dest_key, &row);
  ASSERT_TRUE(read_value);

  double num_val = 0;
  int result = RSValue_ToNumber(read_value, &num_val);
  ASSERT_EQ(1, result);
  ASSERT_EQ(42.0, num_val);

  // Verify spcache is preserved
  ASSERT_EQ(&spcache, dest.spcache);

  // Verify that we can still override in the destination after cloning
  RLookupKey *dest_override_key = RLookup_GetKey_Write(&dest, "test_field", RLOOKUP_F_OVERRIDE);
  ASSERT_TRUE(dest_override_key);
  ASSERT_NE(dest_key, dest_override_key);

  // Cleanup
  RSValue_Decref(test_value);
  RLookupRow_Cleanup(&row);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}
