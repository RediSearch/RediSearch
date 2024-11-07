#include "rlookup.h"
#include "gtest/gtest.h"
#include "common.h"

class RLookupTest : public ::testing::Test {};

TEST_F(RLookupTest, testInit) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testFlags) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto foo = RS::MakeHiddenName("foo");
  RLookupKey *fook = RLookup_GetKey(&lk, foo.get(), RLOOKUP_M_READ, RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(NULL, fook);
  // Try with M_WRITE
  fook = RLookup_GetKey(&lk, foo.get(), RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  ASSERT_TRUE(fook);
  // Try again with M_WRITE
  RLookupKey *tmpk = RLookup_GetKey(&lk, foo.get(), RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  ASSERT_EQ(NULL, tmpk);
  // Try again with M_WRITE and OVERWRITE
  RLookupKey *tmpk2 = RLookup_GetKey(&lk, foo.get(), RLOOKUP_M_WRITE, RLOOKUP_F_OVERRIDE);
  ASSERT_TRUE(tmpk2);

  RLookup_Cleanup(&lk);
}

TEST_F(RLookupTest, testRow) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto foo = RS::MakeHiddenName("foo");
  auto bar = RS::MakeHiddenName("bar");
  RLookupKey *fook = RLookup_GetKey(&lk, foo.get(), RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupKey *bark = RLookup_GetKey(&lk, bar.get(), RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
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
