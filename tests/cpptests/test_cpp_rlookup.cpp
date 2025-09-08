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

//
// NEW TESTS FOR HYBRID SEARCH RLOOKUP FUNCTIONS
//

// Helper functions for test setup and verification
struct TestKeySet {
  std::vector<RLookupKey*> keys;
  std::vector<const char*> names;

  TestKeySet(const std::vector<const char*>& fieldNames) : names(fieldNames) {}
};

// Helper: Initialize lookup with specified field names
TestKeySet init_keys(RLookup* lookup, const std::vector<const char*>& fieldNames, uint32_t flags = RLOOKUP_F_NOFLAGS) {
  TestKeySet keySet(fieldNames);

  for (const char* name : fieldNames) {
    RLookupKey* key = RLookup_GetKey_Write(lookup, name, flags);
    EXPECT_TRUE(key != nullptr) << "Failed to create key: " << name;
    keySet.keys.push_back(key);
  }

  return keySet;
}

// Helper: Create test values with distinct integers starting from baseValue
std::vector<RSValue*> create_test_values(const std::vector<int>& values) {
  std::vector<RSValue*> rsValues;
  for (int val : values) {
    rsValues.push_back(RS_Int64Val(val));
  }
  return rsValues;
}

// Helper: Write values to row using keys
void write_values_to_row(const TestKeySet& keySet, RLookupRow* row, const std::vector<RSValue*>& values) {
  EXPECT_EQ(keySet.keys.size(), values.size()) << "Key count must match value count";

  for (size_t i = 0; i < keySet.keys.size() && i < values.size(); i++) {
    RLookup_WriteKey(keySet.keys[i], row, values[i]);
  }
}

// Helper: Verify values in destination row by field names
void verify_values_by_names(RLookup* lookup, RLookupRow* row,
                           const std::vector<const char*>& fieldNames,
                           const std::vector<double>& expectedValues) {
  EXPECT_EQ(fieldNames.size(), expectedValues.size()) << "Field count must match expected value count";

  for (size_t i = 0; i < fieldNames.size() && i < expectedValues.size(); i++) {
    RLookupKey* key = RLookup_GetKey_Read(lookup, fieldNames[i], RLOOKUP_F_NOFLAGS);
    EXPECT_TRUE(key != nullptr) << "Field not found: " << fieldNames[i];

    RSValue* value = RLookup_GetItem(key, row);
    EXPECT_TRUE(value != nullptr) << "No value for field: " << fieldNames[i];

    double actualValue;
    int result = RSValue_ToNumber(value, &actualValue);
    EXPECT_EQ(1, result) << "Failed to convert value for field: " << fieldNames[i];
    EXPECT_EQ(expectedValues[i], actualValue) << "Wrong value for field: " << fieldNames[i];
  }
}

// Helper: Verify fields are null/empty
void verify_fields_empty(RLookup* lookup, RLookupRow* row, const std::vector<const char*>& fieldNames) {
  for (const char* fieldName : fieldNames) {
    RLookupKey* key = RLookup_GetKey_Read(lookup, fieldName, RLOOKUP_F_NOFLAGS);
    EXPECT_TRUE(key != nullptr) << "Field not found: " << fieldName;

    RSValue* value = RLookup_GetItem(key, row);
    EXPECT_EQ(nullptr, value) << "Field should be empty: " << fieldName;
  }
}

// Helper: Cleanup RSValue array
void cleanup_values(const std::vector<RSValue*>& values) {
  for (RSValue* val : values) {
    if (val) RSValue_Decref(val);
  }
}

/*
 * Test Group 1: RLookup_AddKeysFrom Basic Functionality
 */

/*
 * Test 1.1: Basic Key Addition
 * Verifies that keys from source are correctly added to destination
 */
TEST_F(RLookupTest, testAddKeysFromBasic) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Initial destination is empty
  ASSERT_EQ(0, dest.rowlen);

  // Add keys from source to destination
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Verify all keys from source exist in destination
  ASSERT_EQ(3, dest.rowlen);

  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2 && dest_key3);

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test 1.2: Empty Source Lookup
 * Verifies that adding from empty source doesn't change destination
 */
TEST_F(RLookupTest, testAddKeysFromEmptySource) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in destination
  TestKeySet destKeys = init_keys(&dest, {"existing1", "existing2"});

  uint32_t original_rowlen = dest.rowlen;
  ASSERT_EQ(2, original_rowlen);

  // Add keys from empty source
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Verify destination remains unchanged
  ASSERT_EQ(original_rowlen, dest.rowlen);

  // Verify original keys still exist
  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "existing1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "existing2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2);

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test 1.3: Key Name Conflicts - Default Behavior (First Wins)
 * Verifies that existing keys in destination are preserved by default
 */
TEST_F(RLookupTest, testAddKeysFromConflictsFirstWins) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source: "field1", "field2", "field3"
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Create keys in destination: "field2", "field4" (field2 conflicts)
  TestKeySet destKeys = init_keys(&dest, {"field2", "field4"});

  // Store original indices before adding (to verify override did NOT happen)
  uint32_t original_field2_idx = destKeys.keys[0]->dstidx;  // field2
  uint32_t original_field4_idx = destKeys.keys[1]->dstidx;  // field4

  // Add keys from source (default behavior - first wins)
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Verify destination has all unique keys: "field2" (original), "field4" (original), "field1" (new), "field3" (new)
  ASSERT_EQ(4, dest.rowlen);

  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2 && check_key3 && check_key4);

  // Verify override did NOT happen for existing keys (indices unchanged)
  ASSERT_EQ(original_field2_idx, check_key2->dstidx) << "field2 should NOT have been overridden";
  ASSERT_EQ(original_field4_idx, check_key4->dstidx) << "field4 should remain unchanged";

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test 1.4: Key Name Conflicts - Override Behavior
 * Verifies that RLOOKUP_F_OVERRIDE flag causes existing keys to be overridden
 */
TEST_F(RLookupTest, testAddKeysFromConflictsOverride) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source: "field1", "field2", "field3"
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Create keys in destination: "field2", "field4" (field2 conflicts with source)
  TestKeySet destKeys = init_keys(&dest, {"field2", "field4"});

  // Store references to original keys before override
  RLookupKey *original_field2_key = destKeys.keys[0];  // field2 (should be overridden)
  RLookupKey *original_field4_key = destKeys.keys[1];  // field4 (should remain unchanged)

  // Add keys with OVERRIDE flag
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_OVERRIDE);

  // Verify destination has all keys
  ASSERT_EQ(4, dest.rowlen);

  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2 && check_key3 && check_key4);

  // Verify override DID happen for conflicting key (original key name nullified)
  ASSERT_EQ(nullptr, original_field2_key->name) << "Original field2 key should have been nullified";
  ASSERT_NE(original_field2_key, check_key2) << "field2 should point to new key object";

  // Verify override did NOT happen for non-conflicting key (same key object)
  ASSERT_EQ(original_field4_key, check_key4) << "field4 should be the same key object (not overridden)";

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test Group 2: RLookup_AddKeysFrom Edge Cases
 */

/*
 * Test 2.1: Multiple Additions
 * Verifies sequential additions from multiple sources with conflict resolution
 */
TEST_F(RLookupTest, testAddKeysFromMultipleAdditions) {
  RLookup src1 = {0}, src2 = {0}, src3 = {0}, dest = {0};
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&src3, NULL);
  RLookup_Init(&dest, NULL);

  // Create overlapping keys in different sources
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2", "field3"});
  TestKeySet src2Keys = init_keys(&src2, {"field2", "field3", "field4"});  // field2,3 overlap with src1
  TestKeySet src3Keys = init_keys(&src3, {"field3", "field4", "field5"});  // field3,4 overlap

  // Add sources sequentially (first wins for conflicts)
  RLookup_AddKeysFrom(&dest, &src1, RLOOKUP_F_NOFLAGS);  // field1, field2, field3
  RLookup_AddKeysFrom(&dest, &src2, RLOOKUP_F_NOFLAGS);  // field4 (field2, field3 already exist)
  RLookup_AddKeysFrom(&dest, &src3, RLOOKUP_F_NOFLAGS);  // field5 (field3, field4 already exist)

  // Verify final result: all unique keys present (first wins for conflicts)
  ASSERT_EQ(5, dest.rowlen);  // field1, field2, field3, field4, field5

  RLookupKey *d_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key5 = RLookup_GetKey_Read(&dest, "field5", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(d_key1 && d_key2 && d_key3 && d_key4 && d_key5);

  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&src3);
  RLookup_Cleanup(&dest);
}

/*
 * Test Group 3: RLookupRow_TransferFields Functionality
 */

/*
 * Test 3.1: Basic Field Transfer
 * Verifies that data is correctly transferred and accessible by field names
 */
TEST_F(RLookupTest, testTransferFieldsBasic) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Setup: create source keys and add to destination
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2"});
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Create test data and write to source row
  RLookupRow srcRow = {0}, destRow = {0};
  std::vector<RSValue*> values = create_test_values({100, 200});
  write_values_to_row(srcKeys, &srcRow, values);

  // Store original pointers for ownership verification
  RSValue *original_ptr1 = values[0];
  RSValue *original_ptr2 = values[1];

  // Transfer fields from source to destination
  RLookupRow_TransferFields(&srcRow, &source, &destRow, &dest);

  // Verify transferred values are correct and accessible by field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2"}, {100.0, 200.0});

  // Verify ownership transfer (same pointers, not copies)
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(original_ptr1, RLookup_GetItem(dest_key1, &destRow));
  ASSERT_EQ(original_ptr2, RLookup_GetItem(dest_key2, &destRow));

  // Verify source row slots are nullified (ownership transferred)
  ASSERT_EQ(nullptr, RLookup_GetItem(srcKeys.keys[0], &srcRow));
  ASSERT_EQ(nullptr, RLookup_GetItem(srcKeys.keys[1], &srcRow));

  // Cleanup
  cleanup_values(values);
  RLookupRow_Cleanup(&srcRow);
  RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test 3.2: Empty Source Row
 * Verifies transfer behavior when source row has no data
 */
TEST_F(RLookupTest, testTransferFieldsEmptySource) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source
  RLookupKey *src_key1 = RLookup_GetKey_Write(&source, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *src_key2 = RLookup_GetKey_Write(&source, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(src_key1 && src_key2);

  // Add source keys to destination
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Create empty rows
  RLookupRow srcRow = {0}, destRow = {0};

  // Transfer from empty source
  RLookupRow_TransferFields(&srcRow, &source, &destRow, &dest);

  // Verify destination remains empty
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2);

  RSValue *dest_val1 = RLookup_GetItem(dest_key1, &destRow);
  RSValue *dest_val2 = RLookup_GetItem(dest_key2, &destRow);
  ASSERT_EQ(NULL, dest_val1);
  ASSERT_EQ(NULL, dest_val2);

  // Cleanup
  RLookupRow_Cleanup(&srcRow);
  RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test 3.3: Different Schema Mapping
 * Verifies transfer between schemas with different internal indices
 */
TEST_F(RLookupTest, testTransferFieldsDifferentMapping) {
  RLookup source = {0}, dest = {0};
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create source keys in specific order
  RLookupKey *src_key1 = RLookup_GetKey_Write(&source, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *src_key2 = RLookup_GetKey_Write(&source, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *src_key3 = RLookup_GetKey_Write(&source, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(src_key1 && src_key2 && src_key3);

  // Create some dest keys first to ensure different indices
  RLookup_GetKey_Write(&dest, "other_field", RLOOKUP_F_NOFLAGS);

  // Add source keys to destination (they'll have different dstidx values)
  RLookup_AddKeysFrom(&dest, &source, RLOOKUP_F_NOFLAGS);

  // Verify keys exist but may have different indices
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2 && dest_key3);

  // Create rows and add data with distinct values
  RLookupRow srcRow = {0}, destRow = {0};
  RSValue *value1 = RS_Int64Val(111);
  RSValue *value2 = RS_Int64Val(222);
  RSValue *value3 = RS_Int64Val(333);
  RLookup_WriteKey(src_key1, &srcRow, value1);  // field1=111
  RLookup_WriteKey(src_key2, &srcRow, value2);  // field2=222
  RLookup_WriteKey(src_key3, &srcRow, value3);  // field3=333

  // Transfer fields
  RLookupRow_TransferFields(&srcRow, &source, &destRow, &dest);

  // Verify data is readable by field names despite potentially different indices
  RSValue *dest_val1 = RLookup_GetItem(dest_key1, &destRow);
  RSValue *dest_val2 = RLookup_GetItem(dest_key2, &destRow);
  RSValue *dest_val3 = RLookup_GetItem(dest_key3, &destRow);
  ASSERT_TRUE(dest_val1 && dest_val2 && dest_val3);

  // Verify correct values
  double num_val1 = 0, num_val2 = 0, num_val3 = 0;
  int result1 = RSValue_ToNumber(dest_val1, &num_val1);
  int result2 = RSValue_ToNumber(dest_val2, &num_val2);
  int result3 = RSValue_ToNumber(dest_val3, &num_val3);
  ASSERT_EQ(1, result1);
  ASSERT_EQ(1, result2);
  ASSERT_EQ(1, result3);
  ASSERT_EQ(111.0, num_val1);
  ASSERT_EQ(222.0, num_val2);
  ASSERT_EQ(333.0, num_val3);

  // Verify ownership transfer (same pointers)
  ASSERT_EQ(value1, dest_val1);
  ASSERT_EQ(value2, dest_val2);
  ASSERT_EQ(value3, dest_val3);

  // Cleanup
  RSValue_Decref(value1);
  RSValue_Decref(value2);
  RSValue_Decref(value3);
  RLookupRow_Cleanup(&srcRow);
  RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

/*
 * Test Group 4: Multiple Upstream Integration Tests
 */

/*
 * Test 4.1: No Overlap - Distinct Field Sets
 * Simulates hybrid search with completely different field sets from each source
 */
TEST_F(RLookupTest, testMultipleUpstreamNoOverlap) {
  RLookup src1 = {0}, src2 = {0}, dest = {0};
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create distinct field sets: src1["field1", "field2"], src2["field3", "field4"]
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2"});
  TestKeySet src2Keys = init_keys(&src2, {"field3", "field4"});

  // Add keys from both sources to destination
  RLookup_AddKeysFrom(&dest, &src1, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&dest, &src2, RLOOKUP_F_NOFLAGS);

  // Create test data and populate source rows
  RLookupRow src1Row = {0}, src2Row = {0}, destRow = {0};
  std::vector<RSValue*> src1Values = create_test_values({10, 20});  // field1=10, field2=20
  std::vector<RSValue*> src2Values = create_test_values({30, 40});  // field3=30, field4=40

  write_values_to_row(src1Keys, &src1Row, src1Values);
  write_values_to_row(src2Keys, &src2Row, src2Values);

  // Transfer data from both sources to single destination row
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);
  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);

  // Verify all 4 fields are readable from destination using field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2", "field3", "field4"}, {10.0, 20.0, 30.0, 40.0});

  // Cleanup
  cleanup_values(src1Values);
  cleanup_values(src2Values);
  RLookupRow_Cleanup(&src1Row);
  RLookupRow_Cleanup(&src2Row);
  RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&dest);
}

/*
 * Test 4.2: Partial Overlap - Some Shared Fields
 * Simulates hybrid search with overlapping field names (first source wins)
 */
TEST_F(RLookupTest, testMultipleUpstreamPartialOverlap) {
  RLookup src1 = {0}, src2 = {0}, dest = {0};
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create overlapping field sets: src1["field1", "field2", "field3"], src2["field2", "field4", "field5"]
  RLookupKey *s1_key1 = RLookup_GetKey_Write(&src1, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *s1_key2 = RLookup_GetKey_Write(&src1, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *s1_key3 = RLookup_GetKey_Write(&src1, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *s2_key2 = RLookup_GetKey_Write(&src2, "field2", RLOOKUP_F_NOFLAGS); // Conflicts with src1
  RLookupKey *s2_key4 = RLookup_GetKey_Write(&src2, "field4", RLOOKUP_F_NOFLAGS);
  RLookupKey *s2_key5 = RLookup_GetKey_Write(&src2, "field5", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(s1_key1 && s1_key2 && s1_key3 && s2_key2 && s2_key4 && s2_key5);

  // Add keys (first source wins for key creation, but last transfer wins for data)
  RLookup_AddKeysFrom(&dest, &src1, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&dest, &src2, RLOOKUP_F_NOFLAGS);

  // Create rows with conflicting data for "field2"
  RLookupRow src1Row = {0}, src2Row = {0}, destRow = {0};
  RSValue *s1_val2 = RS_Int64Val(100);  // src1 field2 = 100 (will be overwritten)
  RSValue *s2_val2 = RS_Int64Val(999);  // src2 field2 = 999 (wins due to last transfer)
  RSValue *s1_val1 = RS_Int64Val(1);
  RSValue *s1_val3 = RS_Int64Val(3);
  RSValue *s2_val4 = RS_Int64Val(4);
  RSValue *s2_val5 = RS_Int64Val(5);

  RLookup_WriteKey(s1_key1, &src1Row, s1_val1);
  RLookup_WriteKey(s1_key2, &src1Row, s1_val2);  // field2=100 (first)
  RLookup_WriteKey(s1_key3, &src1Row, s1_val3);

  RLookup_WriteKey(s2_key2, &src2Row, s2_val2);  // field2=999 (should be ignored)
  RLookup_WriteKey(s2_key4, &src2Row, s2_val4);
  RLookup_WriteKey(s2_key5, &src2Row, s2_val5);

  // Transfer src1 first, then src2
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);
  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);

  // Verify field2 contains src2 data (last transfer wins)
  RLookupKey *dest_field2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field2);
  RSValue *field2_val = RLookup_GetItem(dest_field2, &destRow);
  ASSERT_TRUE(field2_val);

  double field2_num;
  ASSERT_EQ(1, RSValue_ToNumber(field2_val, &field2_num));
  ASSERT_EQ(999.0, field2_num);  // Should be 999 (src2), last transfer wins

  // Verify all unique fields transferred correctly
  RLookupKey *dest_field1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_field4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field1 && dest_field4);

  // Cleanup
  RSValue_Decref(s1_val1); RSValue_Decref(s1_val2); RSValue_Decref(s1_val3);
  RSValue_Decref(s2_val2); RSValue_Decref(s2_val4); RSValue_Decref(s2_val5);
  RLookupRow_Cleanup(&src1Row); RLookupRow_Cleanup(&src2Row); RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&src1); RLookup_Cleanup(&src2); RLookup_Cleanup(&dest);
}

/*
 * Test 4.3: Full Overlap - Identical Field Sets (Last Transfer Wins)
 * Simulates hybrid search where both sources have same field names.
 * Demonstrates that data transfer order matters: last transfer overwrites previous data.
 */
TEST_F(RLookupTest, testMultipleUpstreamFullOverlap) {
  RLookup src1 = {0}, src2 = {0}, dest = {0};
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Both sources have identical field names: ["field1", "field2", "field3"]
  RLookupKey *s1_key1 = RLookup_GetKey_Write(&src1, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *s1_key2 = RLookup_GetKey_Write(&src1, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *s1_key3 = RLookup_GetKey_Write(&src1, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *s2_key1 = RLookup_GetKey_Write(&src2, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *s2_key2 = RLookup_GetKey_Write(&src2, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *s2_key3 = RLookup_GetKey_Write(&src2, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(s1_key1 && s1_key2 && s1_key3 && s2_key1 && s2_key2 && s2_key3);

  // Add keys from both sources
  RLookup_AddKeysFrom(&dest, &src1, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&dest, &src2, RLOOKUP_F_NOFLAGS);

  // Create rows with different data for same field names
  RLookupRow src1Row = {0}, src2Row = {0}, destRow = {0};
  RSValue *s1_val1 = RS_Int64Val(100);
  RSValue *s1_val2 = RS_Int64Val(200);
  RSValue *s1_val3 = RS_Int64Val(300);
  RSValue *s2_val1 = RS_Int64Val(999);  // Will overwrite src1
  RSValue *s2_val2 = RS_Int64Val(888);  // Will overwrite src1
  RSValue *s2_val3 = RS_Int64Val(777);  // Will overwrite src1

  RLookup_WriteKey(s1_key1, &src1Row, s1_val1);
  RLookup_WriteKey(s1_key2, &src1Row, s1_val2);
  RLookup_WriteKey(s1_key3, &src1Row, s1_val3);

  RLookup_WriteKey(s2_key1, &src2Row, s2_val1);
  RLookup_WriteKey(s2_key2, &src2Row, s2_val2);
  RLookup_WriteKey(s2_key3, &src2Row, s2_val3);

  // Transfer src1 first, then src2
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);
  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);

  // NOTE: Since both sources have identical field names, src2 overwrites src1 data
  // This test demonstrates "last transfer wins" behavior, not "first source wins"
  RLookupKey *d_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(d_key1 && d_key2 && d_key3);

  RSValue *dest_val1 = RLookup_GetItem(d_key1, &destRow);
  RSValue *dest_val2 = RLookup_GetItem(d_key2, &destRow);
  RSValue *dest_val3 = RLookup_GetItem(d_key3, &destRow);
  ASSERT_TRUE(dest_val1 && dest_val2 && dest_val3);

  double num1, num2, num3;
  ASSERT_EQ(1, RSValue_ToNumber(dest_val1, &num1));
  ASSERT_EQ(1, RSValue_ToNumber(dest_val2, &num2));
  ASSERT_EQ(1, RSValue_ToNumber(dest_val3, &num3));
  ASSERT_EQ(999.0, num1);  // src2 overwrites src1
  ASSERT_EQ(888.0, num2);  // src2 overwrites src1
  ASSERT_EQ(777.0, num3);  // src2 overwrites src1

  // Cleanup
  RSValue_Decref(s1_val1); RSValue_Decref(s1_val2); RSValue_Decref(s1_val3);
  RSValue_Decref(s2_val1); RSValue_Decref(s2_val2); RSValue_Decref(s2_val3);
  RLookupRow_Cleanup(&src1Row); RLookupRow_Cleanup(&src2Row); RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&src1); RLookup_Cleanup(&src2); RLookup_Cleanup(&dest);
}

/*
 * Test 4.4: One Empty Source
 * Simulates hybrid search where one source has no data
 */
TEST_F(RLookupTest, testMultipleUpstreamOneEmpty) {
  RLookup src1 = {0}, src2 = {0}, dest = {0};
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create field sets: src1["field1", "field2"], src2["field3", "field4"]
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2"});
  TestKeySet src2Keys = init_keys(&src2, {"field3", "field4"});

  // Add keys from both sources
  RLookup_AddKeysFrom(&dest, &src1, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&dest, &src2, RLOOKUP_F_NOFLAGS);

  // Create test data - only populate src1 (src2 remains empty)
  RLookupRow src1Row = {0}, src2Row = {0}, destRow = {0};
  std::vector<RSValue*> src1Values = create_test_values({50, 60});  // field1=50, field2=60

  write_values_to_row(src1Keys, &src1Row, src1Values);
  // src2Row intentionally left empty

  // Transfer from both sources
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);
  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);  // Empty source

  // Verify src1 data is present and accessible by field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2"}, {50.0, 60.0});

  // Verify src2 fields remain empty
  verify_fields_empty(&dest, &destRow, {"field3", "field4"});

  // Cleanup
  cleanup_values(src1Values);
  RLookupRow_Cleanup(&src1Row);
  RLookupRow_Cleanup(&src2Row);
  RLookupRow_Cleanup(&destRow);
  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&dest);
}
