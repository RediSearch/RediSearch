/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rlookup.h"
#include "value.h"
#include "gtest/gtest.h"

class RLookupTest : public ::testing::Test {};

TEST_F(RLookupTest, testInit) {
  RLookup lk = RLookup_New();
  RLookup_Init(&lk, NULL);
  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testFlags) {
  RLookup lk = RLookup_New();
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
  RLookup lk = RLookup_New();
  RLookup_Init(&lk, NULL);
  RLookupKey *fook = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  RLookupKey *bark = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = RLookupRow_New();
  RSValue *vfoo = RSValue_NewNumberFromInt64(42);
  RSValue *vbar = RSValue_NewNumberFromInt64(666);

  ASSERT_EQ(1, RSValue_Refcount(vfoo));
  RLookup_WriteKey(fook, &rr, vfoo);
  ASSERT_EQ(2, RSValue_Refcount(vfoo));

  RSValue *vtmp = RLookup_GetItem(fook, &rr);
  ASSERT_EQ(vfoo, vtmp);
  ASSERT_EQ(2, RSValue_Refcount(vfoo));

  // Write a NULL value
  RLookup_WriteKey(fook, &rr, RSValue_NullStatic());
  ASSERT_EQ(1, RSValue_Refcount(vfoo));

  // Get the 'bar' key -- should be NULL
  ASSERT_TRUE(NULL == RLookup_GetItem(bark, &rr));

  // Clean up the row
  RLookupRow_Wipe(&rr);
  vtmp = RLookup_GetItem(fook, &rr);
  ASSERT_TRUE(NULL == RLookup_GetItem(fook, &rr));

  RSValue_DecrRef(vfoo);
  RSValue_DecrRef(vbar);
  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}


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
    rsValues.push_back(RSValue_NewNumberFromInt64(val));
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

  for (size_t i = 0; i < fieldNames.size(); i++) {
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
    if (val) RSValue_DecrRef(val);
  }
}


// Tests basic key addition from source to destination lookup
TEST_F(RLookupTest, testAddKeysFromBasic) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Initial destination is empty
  ASSERT_EQ(0, RLookup_GetRowLen(&dest));

  // Add keys from source to destination
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Verify all keys from source exist in destination
  ASSERT_EQ(3, RLookup_GetRowLen(&dest));

  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2 && dest_key3);

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests that adding keys from empty source doesn't change destination
TEST_F(RLookupTest, testAddKeysFromEmptySource) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in destination
  TestKeySet destKeys = init_keys(&dest, {"existing1", "existing2"});

  uint32_t original_rowlen = RLookup_GetRowLen(&dest);
  ASSERT_EQ(2, original_rowlen);

  // Add keys from empty source
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Verify destination remains unchanged
  ASSERT_EQ(original_rowlen, RLookup_GetRowLen(&dest));

  // Verify original keys still exist
  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "existing1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "existing2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2);

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests key name conflicts with default behavior (first wins)
TEST_F(RLookupTest, testAddKeysFromConflictsFirstWins) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source: "field1", "field2", "field3"
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Create keys in destination: "field2", "field4" (field2 conflicts)
  TestKeySet destKeys = init_keys(&dest, {"field2", "field4"});

  // Store original indices before adding (to verify override did NOT happen)
  uint32_t original_field2_idx = RLookupKey_GetDstIdx(destKeys.keys[0]);  // field2
  uint32_t original_field4_idx = RLookupKey_GetDstIdx(destKeys.keys[1]);  // field4

  // Add keys from source (default behavior - first wins)
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Verify destination has all unique keys: "field2" (original), "field4" (original), "field1" (new), "field3" (new)
  ASSERT_EQ(4, RLookup_GetRowLen(&dest));

  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2 && check_key3 && check_key4);

  // Verify override did NOT happen for existing keys (indices unchanged)
  ASSERT_EQ(original_field2_idx, RLookupKey_GetDstIdx(check_key2)) << "field2 should NOT have been overridden";
  ASSERT_EQ(original_field4_idx, RLookupKey_GetDstIdx(check_key4)) << "field4 should remain unchanged";

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests key name conflicts with override behavior
TEST_F(RLookupTest, testAddKeysFromConflictsOverride) {
  RLookup source = RLookup_New(), dest = RLookup_New();
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
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_OVERRIDE);

  // Verify destination has all keys
  ASSERT_EQ(4, RLookup_GetRowLen(&dest));

  RLookupKey *check_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  RLookupKey *check_key4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(check_key1 && check_key2 && check_key3 && check_key4);

  // Verify override DID happen for conflicting key (original key name nullified)
  ASSERT_EQ(nullptr, RLookupKey_GetName(original_field2_key)) << "Original field2 key should have been nullified";
  ASSERT_NE(original_field2_key, check_key2) << "field2 should point to new key object";

  // Verify override did NOT happen for non-conflicting key (same key object)
  ASSERT_EQ(original_field4_key, check_key4) << "field4 should be the same key object (not overridden)";

  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}


// Tests sequential additions from multiple sources with conflict resolution
TEST_F(RLookupTest, testAddKeysFromMultipleAdditions) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), src3 = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&src3, NULL);
  RLookup_Init(&dest, NULL);

  // Create overlapping keys in different sources
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2", "field3"});
  TestKeySet src2Keys = init_keys(&src2, {"field2", "field3", "field4"});  // field2,3 overlap with src1
  TestKeySet src3Keys = init_keys(&src3, {"field3", "field4", "field5"});  // field3,4 overlap

  // Add sources sequentially (first wins for conflicts)
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);  // field1, field2, field3
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_NOFLAGS);  // field4 (field2, field3 already exist)
  RLookup_AddKeysFrom(&src3, &dest, RLOOKUP_F_NOFLAGS);  // field5 (field3, field4 already exist)

  // Verify final result: all unique keys present (first wins for conflicts)
  ASSERT_EQ(5, RLookup_GetRowLen(&dest));  // field1, field2, field3, field4, field5

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


// Tests basic field writing between lookup rows
TEST_F(RLookupTest, testWriteFieldsBasic) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Setup: create source keys and add to destination
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2"});
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Create test data and write to source row
  RLookupRow srcRow = RLookupRow_New(), destRow = RLookupRow_New();
  std::vector<RSValue*> values = create_test_values({100, 200});
  write_values_to_row(srcKeys, &srcRow, values);

  // Store original pointers for ownership verification
  RSValue *original_ptr1 = values[0];
  RSValue *original_ptr2 = values[1];

  // Write fields from source to destination
  RLookupRow_WriteFieldsFrom(&srcRow, &source, &destRow, &dest, false);

  // Verify written values are correct and accessible by field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2"}, {100.0, 200.0});

  // Verify shared ownership (same pointers in both source and destination)
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(original_ptr1, RLookup_GetItem(dest_key1, &destRow));
  ASSERT_EQ(original_ptr2, RLookup_GetItem(dest_key2, &destRow));

  // Verify source row still contains the values (shared ownership, not moved)
  ASSERT_EQ(original_ptr1, RLookup_GetItem(srcKeys.keys[0], &srcRow));
  ASSERT_EQ(original_ptr2, RLookup_GetItem(srcKeys.keys[1], &srcRow));

  // Verify refcounts increased due to sharing (now referenced by both rows)
  ASSERT_EQ(3, RSValue_Refcount(original_ptr1));  // 1 original + 1 source row + 1 dest row
  ASSERT_EQ(3, RSValue_Refcount(original_ptr2));  // 1 original + 1 source row + 1 dest row

  // Cleanup
  cleanup_values(values);
  RLookupRow_Reset(&srcRow);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests field writing when source row has no data
TEST_F(RLookupTest, testWriteFieldsEmptySource) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2"});

  // Add source keys to destination
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Create empty rows
  RLookupRow srcRow = RLookupRow_New(), destRow = RLookupRow_New();

  // Write from empty source
  RLookupRow_WriteFieldsFrom(&srcRow, &source, &destRow, &dest, false);

  // Verify destination remains empty
  verify_fields_empty(&dest, &destRow, {"field1", "field2"});

  // Cleanup
  RLookupRow_Reset(&srcRow);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests field writing between schemas with different internal indices
TEST_F(RLookupTest, testWriteFieldsDifferentMapping) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create source keys in specific order
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Create some dest keys first to ensure different indices
  RLookup_GetKey_Write(&dest, "other_field", RLOOKUP_F_NOFLAGS);

  // Add source keys to destination (they'll have different dstidx values)
  RLookup_AddKeysFrom(&source, &dest, RLOOKUP_F_NOFLAGS);

  // Verify keys exist but may have different indices
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2 && dest_key3);

  // Create rows and add data with distinct values
  RLookupRow srcRow = RLookupRow_New(), destRow = RLookupRow_New();

  // Create test values with distinct data
  std::vector<RSValue*> values = create_test_values({111, 222, 333});

  // Write values to source row
  write_values_to_row(srcKeys, &srcRow, values);

  // Write fields
  RLookupRow_WriteFieldsFrom(&srcRow, &source, &destRow, &dest, false);

  // Verify data is readable by field names despite potentially different indices
  verify_values_by_names(&dest, &destRow, {"field1", "field2", "field3"}, {111.0, 222.0, 333.0});

  // Verify shared ownership (same pointers) - need to check individual values
  std::vector<RLookupKey*> dest_keys = {dest_key1, dest_key2, dest_key3};
  for (int i = 0; i < 3; i++) {
    RSValue *dest_val = RLookup_GetItem(dest_keys[i], &destRow);
    ASSERT_EQ(values[i], dest_val) << "dest_vals[" << i << "] should point to values[" << i << "]";
  }

  // Cleanup
  cleanup_values(values);
  RLookupRow_Reset(&srcRow);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}


// Tests RLookupRow_WriteFieldsFrom with distinct field sets from each source
TEST_F(RLookupTest, testMultipleSourcesNoOverlap) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create distinct field sets: src1["field1", "field2"], src2["field3", "field4"]
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2"});
  TestKeySet src2Keys = init_keys(&src2, {"field3", "field4"});

  // Add keys from both sources to destination
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_NOFLAGS);

  // Create test data and populate source rows
  RLookupRow src1Row = RLookupRow_New(), src2Row = RLookupRow_New(), destRow = RLookupRow_New();
  std::vector<RSValue*> src1Values = create_test_values({10, 20});  // field1=10, field2=20
  std::vector<RSValue*> src2Values = create_test_values({30, 40});  // field3=30, field4=40

  write_values_to_row(src1Keys, &src1Row, src1Values);
  write_values_to_row(src2Keys, &src2Row, src2Values);

  // Write data from both sources to single destination row
  RLookupRow_WriteFieldsFrom(&src1Row, &src1, &destRow, &dest, false);
  RLookupRow_WriteFieldsFrom(&src2Row, &src2, &destRow, &dest, false);

  // Verify all 4 fields are readable from destination using field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2", "field3", "field4"}, {10.0, 20.0, 30.0, 40.0});

  // Cleanup
  cleanup_values(src1Values);
  cleanup_values(src2Values);
  RLookupRow_Reset(&src1Row);
  RLookupRow_Reset(&src2Row);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&dest);
}

// Tests RLookupRow_WriteFieldsFrom with overlapping field names (last write wins)
TEST_F(RLookupTest, testMultipleSourcesPartialOverlap) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), dest = RLookup_New();
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

  // Add keys (first source wins for key creation, but last write wins for data)
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_NOFLAGS);

  // Create rows with conflicting data for "field2"
  RLookupRow src1Row = RLookupRow_New(), src2Row = RLookupRow_New(), destRow = RLookupRow_New();

  // Create src1 values: field1=1, field2=100, field3=3
  std::vector<RSValue*> s1_vals = create_test_values({1, 100, 3});
  RSValue *s1_val1 = s1_vals[0], *s1_val2 = s1_vals[1], *s1_val3 = s1_vals[2];

  // Create src2 values: field2=999 (conflict), field4=4, field5=5
  std::vector<RSValue*> s2_vals = create_test_values({999, 4, 5});
  RSValue *s2_val2 = s2_vals[0], *s2_val4 = s2_vals[1], *s2_val5 = s2_vals[2];

  // Initially all values have refcount = 1 (just created, no rows reference them yet)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(1, RSValue_Refcount(s1_vals[i])) << "s1_vals[" << i << "] initial refcount";
    ASSERT_EQ(1, RSValue_Refcount(s2_vals[i])) << "s2_vals[" << i << "] initial refcount";
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
    ASSERT_EQ(2, RSValue_Refcount(s1_vals[i])) << "s1_vals[" << i << "] refcount after writing to src1Row";
    ASSERT_EQ(2, RSValue_Refcount(s2_vals[i])) << "s2_vals[" << i << "] refcount after writing to src2Row";
  }

  // Write src1 first, then src2
  RLookupRow_WriteFieldsFrom(&src1Row, &src1, &destRow, &dest, false);

  // After first write, s1_val2 should have refcount 3 (original + src1Row + destRow)
  ASSERT_EQ(3, RSValue_Refcount(s1_val2));  // Shared between source and destination
  ASSERT_EQ(2, RSValue_Refcount(s2_val2));  // s2_val2 unchanged yet

  RLookupRow_WriteFieldsFrom(&src2Row, &src2, &destRow, &dest, false);

  // After second write, s1_val2 should be decremented (overwritten in dest), s2_val2 should be shared
  ASSERT_EQ(2, RSValue_Refcount(s1_val2));  // Back to original + src1Row (removed from destRow)
  ASSERT_EQ(3, RSValue_Refcount(s2_val2));  // Now shared: original + src2Row + destRow

  // Verify field2 contains src2 data (last write wins)
  RLookupKey *dest_field2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field2);
  RSValue *field2_val = RLookup_GetItem(dest_field2, &destRow);
  ASSERT_TRUE(field2_val);

  // Verify it's the same pointer (shared ownership, not copy)
  ASSERT_EQ(s2_val2, field2_val);

  double field2_num;
  ASSERT_EQ(1, RSValue_ToNumber(field2_val, &field2_num));
  ASSERT_EQ(999.0, field2_num);  // Should be 999 (src2), last write wins

  // Verify all unique fields written correctly
  RLookupKey *dest_field1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_field4 = RLookup_GetKey_Read(&dest, "field4", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_field1 && dest_field4);

  // Cleanup
  cleanup_values(s1_vals);
  cleanup_values(s2_vals);
  RLookupRow_Reset(&src1Row); RLookupRow_Reset(&src2Row); RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&src1); RLookup_Cleanup(&src2); RLookup_Cleanup(&dest);
}

// Tests RLookupRow_WriteFieldsFrom with identical field sets (last write wins)
TEST_F(RLookupTest, testMultipleSourcesFullOverlap) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), dest = RLookup_New();
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
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_NOFLAGS);

  // Create rows with different data for same field names
  RLookupRow src1Row = RLookupRow_New(), src2Row = RLookupRow_New(), destRow = RLookupRow_New();

  // Create test values using vectors for consistency
  std::vector<RSValue*> s1_vals = create_test_values({100, 200, 300});
  std::vector<RSValue*> s2_vals = create_test_values({999, 888, 777});  // Will overwrite src1
  std::vector<RLookupKey*> s1_keys = {s1_key1, s1_key2, s1_key3};
  std::vector<RLookupKey*> s2_keys = {s2_key1, s2_key2, s2_key3};

  // Write values to rows using loops
  for (int i = 0; i < 3; i++) {
    RLookup_WriteKey(s1_keys[i], &src1Row, s1_vals[i]);
    RLookup_WriteKey(s2_keys[i], &src2Row, s2_vals[i]);
  }

  // Verify initial refcounts before any writes - all should be 2 (original + row)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(2, RSValue_Refcount(s1_vals[i])) << "s1_vals[" << i << "] refcount after writing to src1Row";
    ASSERT_EQ(2, RSValue_Refcount(s2_vals[i])) << "s2_vals[" << i << "] refcount after writing to src2Row";
  }

  // Write src1 first
  RLookupRow_WriteFieldsFrom(&src1Row, &src1, &destRow, &dest, false);

  // After first write, src1 values should have refcount 3 (shared: original + src1Row + destRow)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(3, RSValue_Refcount(s1_vals[i])) << "s1_vals[" << i << "] should be shared between src1Row and destRow";
    ASSERT_EQ(2, RSValue_Refcount(s2_vals[i])) << "s2_vals[" << i << "] should be unchanged";
  }

  // Write src2 - this will overwrite all src1 values
  RLookupRow_WriteFieldsFrom(&src2Row, &src2, &destRow, &dest, false);

  // After second write, all src1 values should be decremented (overwritten in destRow)
  // and all src2 values should have refcount 3 (shared: original + src2Row + destRow)
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(2, RSValue_Refcount(s1_vals[i])) << "s1_vals[" << i << "] back to original + src1Row (removed from destRow)";
    ASSERT_EQ(3, RSValue_Refcount(s2_vals[i])) << "s2_vals[" << i << "] shared between src2Row and destRow";
  }

  // NOTE: Since both sources have identical field names, src2 overwrites src1 data in destination
  // This test demonstrates "last write wins" behavior for the destination row
  RLookupKey *d_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *d_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(d_key1 && d_key2 && d_key3);

  RSValue *dest_val1 = RLookup_GetItem(d_key1, &destRow);
  RSValue *dest_val2 = RLookup_GetItem(d_key2, &destRow);
  RSValue *dest_val3 = RLookup_GetItem(d_key3, &destRow);
  ASSERT_TRUE(dest_val1 && dest_val2 && dest_val3);

  // Verify pointers are from src2 (shared ownership, same pointers)
  std::vector<RSValue*> dest_vals = {dest_val1, dest_val2, dest_val3};
  std::vector<double> expected_nums = {999.0, 888.0, 777.0};

  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(s2_vals[i], dest_vals[i]) << "dest_val" << (i+1) << " should point to s2_vals[" << i << "]";

    double num_val;
    ASSERT_EQ(1, RSValue_ToNumber(dest_vals[i], &num_val)) << "Failed to convert dest_val" << (i+1);
    ASSERT_EQ(expected_nums[i], num_val) << "Wrong value for dest_val" << (i+1);
  }

  // Cleanup
  cleanup_values(s1_vals);
  cleanup_values(s2_vals);
  RLookupRow_Reset(&src1Row); RLookupRow_Reset(&src2Row); RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&src1); RLookup_Cleanup(&src2); RLookup_Cleanup(&dest);
}

// Tests hybrid search where one source has no data
TEST_F(RLookupTest, testMultipleSourcesOneEmpty) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create field sets: src1["field1", "field2"], src2["field3", "field4"]
  TestKeySet src1Keys = init_keys(&src1, {"field1", "field2"});
  TestKeySet src2Keys = init_keys(&src2, {"field3", "field4"});

  // Add keys from both sources
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_NOFLAGS);

  // Create test data - only populate src1 (src2 remains empty)
  RLookupRow src1Row = RLookupRow_New(), src2Row = RLookupRow_New(), destRow = RLookupRow_New();
  std::vector<RSValue*> src1Values = create_test_values({50, 60});  // field1=50, field2=60

  write_values_to_row(src1Keys, &src1Row, src1Values);
  // src2Row intentionally left empty

  // Write from both sources
  RLookupRow_WriteFieldsFrom(&src1Row, &src1, &destRow, &dest, false);
  RLookupRow_WriteFieldsFrom(&src2Row, &src2, &destRow, &dest, false);  // Empty source

  // Verify src1 data is present and accessible by field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2"}, {50.0, 60.0});

  // Verify src2 fields remain empty
  verify_fields_empty(&dest, &destRow, {"field3", "field4"});

  // Cleanup
  cleanup_values(src1Values);
  RLookupRow_Reset(&src1Row);
  RLookupRow_Reset(&src2Row);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&dest);
}

// Tests createMissingKeys=true: keys are created in destination on demand
TEST_F(RLookupTest, testWriteFieldsCreateMissingKeys) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source but NOT in destination
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Destination is empty - no keys added
  ASSERT_EQ(0, RLookup_GetRowLen(&dest));

  // Create test data and write to source row
  RLookupRow srcRow = RLookupRow_New(), destRow = RLookupRow_New();
  std::vector<RSValue*> values = create_test_values({100, 200, 300});
  write_values_to_row(srcKeys, &srcRow, values);

  // Write fields with createMissingKeys=true - should create keys on demand
  RLookupRow_WriteFieldsFrom(&srcRow, &source, &destRow, &dest, true);

  // Verify keys were created in destination
  ASSERT_EQ(3, RLookup_GetRowLen(&dest));

  // Verify values are accessible by field names
  verify_values_by_names(&dest, &destRow, {"field1", "field2", "field3"},
                        {100.0, 200.0, 300.0});

  // Verify shared ownership (same pointers)
  RLookupKey *dest_key1 = RLookup_GetKey_Read(&dest, "field1", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key2 = RLookup_GetKey_Read(&dest, "field2", RLOOKUP_F_NOFLAGS);
  RLookupKey *dest_key3 = RLookup_GetKey_Read(&dest, "field3", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key1 && dest_key2 && dest_key3);
  ASSERT_EQ(values[0], RLookup_GetItem(dest_key1, &destRow));
  ASSERT_EQ(values[1], RLookup_GetItem(dest_key2, &destRow));
  ASSERT_EQ(values[2], RLookup_GetItem(dest_key3, &destRow));

  // Cleanup
  cleanup_values(values);
  RLookupRow_Reset(&srcRow);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests createMissingKeys=true with partial overlap: some keys exist, some created
TEST_F(RLookupTest, testWriteFieldsCreateMissingKeysPartialOverlap) {
  RLookup source = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&source, NULL);
  RLookup_Init(&dest, NULL);

  // Create keys in source
  TestKeySet srcKeys = init_keys(&source, {"field1", "field2", "field3"});

  // Create only field2 in destination (field1 and field3 are missing)
  RLookupKey *existing_key = RLookup_GetKey_Write(&dest, "field2", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(existing_key);
  ASSERT_EQ(1, RLookup_GetRowLen(&dest));

  // Create test data and write to source row
  RLookupRow srcRow = RLookupRow_New(), destRow = RLookupRow_New();
  std::vector<RSValue*> values = create_test_values({100, 200, 300});
  write_values_to_row(srcKeys, &srcRow, values);

  // Write fields with createMissingKeys=true
  RLookupRow_WriteFieldsFrom(&srcRow, &source, &destRow, &dest, true);

  // Verify all 3 keys now exist (field1 and field3 were created)
  ASSERT_EQ(3, RLookup_GetRowLen(&dest));

  // Verify all values are correct
  verify_values_by_names(&dest, &destRow, {"field1", "field2", "field3"},
                        {100.0, 200.0, 300.0});

  // Cleanup
  cleanup_values(values);
  RLookupRow_Reset(&srcRow);
  RLookupRow_Reset(&destRow);
  RLookup_Cleanup(&source);
  RLookup_Cleanup(&dest);
}

// Tests F_HIDDEN flag handling: preservation and override behavior
TEST_F(RLookupTest, testAddKeysFromHiddenFlagHandling) {
  RLookup src1 = RLookup_New(), src2 = RLookup_New(), dest = RLookup_New();
  RLookup_Init(&src1, NULL);
  RLookup_Init(&src2, NULL);
  RLookup_Init(&dest, NULL);

  // Create key in src1 with F_HIDDEN flag
  RLookupKey *src1_key = RLookup_GetKey_Write(&src1, "test_field", RLOOKUP_F_HIDDEN);
  ASSERT_TRUE(src1_key);
  ASSERT_TRUE(RLookupKey_GetFlags(src1_key) & RLOOKUP_F_HIDDEN) << "src1 key should have F_HIDDEN flag";

  // Add src1 keys first - test flag preservation
  RLookup_AddKeysFrom(&src1, &dest, RLOOKUP_F_NOFLAGS);

  RLookupKey *dest_key_after_src1 = RLookup_GetKey_Read(&dest, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key_after_src1);
  ASSERT_TRUE(RLookupKey_GetFlags(dest_key_after_src1) & RLOOKUP_F_HIDDEN) << "Destination key should preserve F_HIDDEN flag";

  // Create same key name in src2 WITHOUT F_HIDDEN flag
  RLookupKey *src2_key = RLookup_GetKey_Write(&src2, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(src2_key);
  ASSERT_FALSE(RLookupKey_GetFlags(src2_key) & RLOOKUP_F_HIDDEN) << "src2 key should NOT have F_HIDDEN flag";

  // Store reference to check override behavior
  RLookupKey *original_dest_key = dest_key_after_src1;

  // Add src2 keys with OVERRIDE flag - test flag override behavior
  RLookup_AddKeysFrom(&src2, &dest, RLOOKUP_F_OVERRIDE);

  // Verify the key was overridden
  RLookupKey *dest_key_after_src2 = RLookup_GetKey_Read(&dest, "test_field", RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(dest_key_after_src2);

  // Verify override happened (original key should be nullified, new key created)
  ASSERT_EQ(nullptr, RLookupKey_GetName(original_dest_key)) << "Original key should have been nullified";
  ASSERT_NE(original_dest_key, dest_key_after_src2) << "Should point to new key object after override";

  // Verify F_HIDDEN flag is now gone (src2 overwrote src1's hidden status)
  ASSERT_FALSE(RLookupKey_GetFlags(dest_key_after_src2) & RLOOKUP_F_HIDDEN) << "Destination key should NOT be hidden after src2 override";

  RLookup_Cleanup(&src1);
  RLookup_Cleanup(&src2);
  RLookup_Cleanup(&dest);
}
