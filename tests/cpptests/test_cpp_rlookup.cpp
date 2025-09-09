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

  // Verify shared ownership (same pointers in both source and destination)
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(original_ptr1, RLookup_GetItem(dest_key1, &destRow));
  ASSERT_EQ(original_ptr2, RLookup_GetItem(dest_key2, &destRow));

  // Verify source row still contains the values (shared ownership, not transferred)
  ASSERT_EQ(original_ptr1, RLookup_GetItem(srcKeys.keys[0], &srcRow));
  ASSERT_EQ(original_ptr2, RLookup_GetItem(srcKeys.keys[1], &srcRow));

  // Verify refcounts increased due to sharing (now referenced by both rows)
  ASSERT_EQ(3, original_ptr1->refcount);  // 1 original + 1 source row + 1 dest row
  ASSERT_EQ(3, original_ptr2->refcount);  // 1 original + 1 source row + 1 dest row

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

  // Create test values with distinct data
  RSValue *values[] = {RS_Int64Val(111), RS_Int64Val(222), RS_Int64Val(333)};
  RSValue *value1 = values[0], *value2 = values[1], *value3 = values[2];
  RLookupKey *src_keys[] = {src_key1, src_key2, src_key3};

  // Write values to source row
  for (int i = 0; i < 3; i++) {
    RLookup_WriteKey(src_keys[i], &srcRow, values[i]);
  }

  // Transfer fields
  RLookupRow_TransferFields(&srcRow, &source, &destRow, &dest);

  // Verify data is readable by field names despite potentially different indices
  RLookupKey *dest_keys[] = {dest_key1, dest_key2, dest_key3};
  RSValue *dest_vals[3];
  double expected_nums[] = {111.0, 222.0, 333.0};

  for (int i = 0; i < 3; i++) {
    dest_vals[i] = RLookup_GetItem(dest_keys[i], &destRow);
    ASSERT_TRUE(dest_vals[i]) << "dest_vals[" << i << "] should exist";

    // Verify correct values
    double num_val;
    int result = RSValue_ToNumber(dest_vals[i], &num_val);
    ASSERT_EQ(1, result) << "Failed to convert dest_vals[" << i << "]";
    ASSERT_EQ(expected_nums[i], num_val) << "Wrong value for dest_vals[" << i << "]";

    // Verify ownership transfer (same pointers)
    ASSERT_EQ(values[i], dest_vals[i]) << "dest_vals[" << i << "] should point to values[" << i << "]";
  }

  // Cleanup
  for (int i = 0; i < 3; i++) {
    RSValue_Decref(values[i]);
  }
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

  // Create src1 values: field1=1, field2=100, field3=3
  RSValue *s1_vals[] = {RS_Int64Val(1), RS_Int64Val(100), RS_Int64Val(3)};
  RSValue *s1_val1 = s1_vals[0], *s1_val2 = s1_vals[1], *s1_val3 = s1_vals[2];

  // Create src2 values: field2=999 (conflict), field4=4, field5=5
  RSValue *s2_vals[] = {RS_Int64Val(999), RS_Int64Val(4), RS_Int64Val(5)};
  RSValue *s2_val2 = s2_vals[0], *s2_val4 = s2_vals[1], *s2_val5 = s2_vals[2];

  // Initially all values have refcount = 1 (just created, no rows reference them yet)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(1, s1_vals[i]->refcount) << "s1_vals[" << i << "] initial refcount";
    ASSERT_EQ(1, s2_vals[i]->refcount) << "s2_vals[" << i << "] initial refcount";
  }

  // Write values to rows - refcount should increase by 1 for each row that references the value
  RLookup_WriteKey(s1_key1, &src1Row, s1_val1);  // s1_val1 now in 1 row
  RLookup_WriteKey(s1_key2, &src1Row, s1_val2);  // s1_val2 now in 1 row
  RLookup_WriteKey(s1_key3, &src1Row, s1_val3);  // s1_val3 now in 1 row

  RLookup_WriteKey(s2_key2, &src2Row, s2_val2);  // s2_val2 now in 1 row
  RLookup_WriteKey(s2_key4, &src2Row, s2_val4);  // s2_val4 now in 1 row
  RLookup_WriteKey(s2_key5, &src2Row, s2_val5);  // s2_val5 now in 1 row

  // Verify refcounts after writing to rows - all should be 2 (original + row)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(2, s1_vals[i]->refcount) << "s1_vals[" << i << "] refcount after writing to src1Row";
    ASSERT_EQ(2, s2_vals[i]->refcount) << "s2_vals[" << i << "] refcount after writing to src2Row";
  }

  // Transfer src1 first, then src2
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);

  // After first transfer, s1_val2 should have refcount 3 (original + src1Row + destRow)
  ASSERT_EQ(3, s1_val2->refcount);  // Shared between source and destination
  ASSERT_EQ(2, s2_val2->refcount);  // s2_val2 unchanged yet

  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);

  // After second transfer, s1_val2 should be decremented (overwritten in dest), s2_val2 should be shared
  ASSERT_EQ(2, s1_val2->refcount);  // Back to original + src1Row (removed from destRow)
  ASSERT_EQ(3, s2_val2->refcount);  // Now shared: original + src2Row + destRow

  // Verify field2 contains src2 data (last transfer wins)
  RLookupKey *dest_field2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field2);
  RSValue *field2_val = RLookup_GetItem(dest_field2, &destRow);
  ASSERT_TRUE(field2_val);

  // Verify it's the same pointer (ownership transfer, not copy)
  ASSERT_EQ(s2_val2, field2_val);

  double field2_num;
  ASSERT_EQ(1, RSValue_ToNumber(field2_val, &field2_num));
  ASSERT_EQ(999.0, field2_num);  // Should be 999 (src2), last transfer wins

  // Verify all unique fields transferred correctly
  RLookupKey *dest_field1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_field4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field1 && dest_field4);

  // Cleanup
  for (int i = 0; i < 3; i++) {
    RSValue_Decref(s1_vals[i]);
    RSValue_Decref(s2_vals[i]);
  }
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

  // Create test values using arrays for less repetition
  RSValue *s1_vals[] = {RS_Int64Val(100), RS_Int64Val(200), RS_Int64Val(300)};
  RSValue *s2_vals[] = {RS_Int64Val(999), RS_Int64Val(888), RS_Int64Val(777)};  // Will overwrite src1
  RLookupKey *s1_keys[] = {s1_key1, s1_key2, s1_key3};
  RLookupKey *s2_keys[] = {s2_key1, s2_key2, s2_key3};

  // Write values to rows using loops
  for (int i = 0; i < 3; i++) {
    RLookup_WriteKey(s1_keys[i], &src1Row, s1_vals[i]);
    RLookup_WriteKey(s2_keys[i], &src2Row, s2_vals[i]);
  }

  // Verify initial refcounts before any transfers - all should be 2 (original + row)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(2, s1_vals[i]->refcount) << "s1_vals[" << i << "] refcount after writing to src1Row";
    ASSERT_EQ(2, s2_vals[i]->refcount) << "s2_vals[" << i << "] refcount after writing to src2Row";
  }

  // Transfer src1 first
  RLookupRow_TransferFields(&src1Row, &src1, &destRow, &dest);

  // After first transfer, src1 values should have refcount 3 (shared: original + src1Row + destRow)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(3, s1_vals[i]->refcount) << "s1_vals[" << i << "] should be shared between src1Row and destRow";
    ASSERT_EQ(2, s2_vals[i]->refcount) << "s2_vals[" << i << "] should be unchanged";
  }

  // Transfer src2 - this will overwrite all src1 values
  RLookupRow_TransferFields(&src2Row, &src2, &destRow, &dest);

  // After second transfer, all src1 values should be decremented (overwritten in destRow)
  // and all src2 values should have refcount 3 (shared: original + src2Row + destRow)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(2, s1_vals[i]->refcount) << "s1_vals[" << i << "] back to original + src1Row (removed from destRow)";
    ASSERT_EQ(3, s2_vals[i]->refcount) << "s2_vals[" << i << "] shared between src2Row and destRow";
  }

  // NOTE: Since both sources have identical field names, src2 overwrites src1 data in destination
  // This test demonstrates "last transfer wins" behavior for the destination row
  RLookupKey *d_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(d_key1 && d_key2 && d_key3);

  RSValue *dest_val1 = RLookup_GetItem(d_key1, &destRow);
  RSValue *dest_val2 = RLookup_GetItem(d_key2, &destRow);
  RSValue *dest_val3 = RLookup_GetItem(d_key3, &destRow);
  ASSERT_TRUE(dest_val1 && dest_val2 && dest_val3);

  // Verify pointers are from src2 (shared ownership, same pointers)
  RSValue *dest_vals[] = {dest_val1, dest_val2, dest_val3};
  double expected_nums[] = {999.0, 888.0, 777.0};

  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(s2_vals[i], dest_vals[i]) << "dest_val" << (i+1) << " should point to s2_vals[" << i << "]";

    double num_val;
    ASSERT_EQ(1, RSValue_ToNumber(dest_vals[i], &num_val)) << "Failed to convert dest_val" << (i+1);
    ASSERT_EQ(expected_nums[i], num_val) << "Wrong value for dest_val" << (i+1);
  }

  // Cleanup
  for (int i = 0; i < 3; i++) {
    RSValue_Decref(s1_vals[i]);
    RSValue_Decref(s2_vals[i]);
  }
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
