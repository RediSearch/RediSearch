
#include "gtest/gtest.h"
#include "cursor.h"
#include <algorithm>
#include <vector>
#define is_Idle(cur) ((cur)->pos != -1)

bool IdInArray(uint64_t id, const uint64_t *arr, int size) {
  return std::find(arr, arr + size, id) != arr + size;
}

class CursorsTest : public ::testing::Test {
  protected:
  const char *idx = "idx";
  IndexSpec *spec;

    void SetUp() override {
      static const char *args[] = {"SCHEMA", "title", "TEXT"};
      QueryError err = {QueryErrorCode(0)};
      const char *idx = "idx";
      spec = IndexSpec_Parse(idx, args, sizeof(args) / sizeof(const char *), &err);
      CursorList_AddSpec(&RSCursors, idx);
    }

    void TearDown() override {
      IndexSpec_Free(spec);
    }
};

TEST_F(CursorsTest, BasicAPI) {
  Cursor *cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  auto id = cur->id;

  ASSERT_EQ(Cursors_TakeForExecution(&RSCursors, id), nullptr) << "Cursor already in use";

  Cursor_Pause(cur);
  ASSERT_TRUE((cur));
  ASSERT_TRUE(is_Idle(cur));

  Cursor *cur2 = Cursors_TakeForExecution(&RSCursors, id);
  ASSERT_TRUE(cur2 != NULL);
  ASSERT_FALSE(is_Idle(cur2));
  ASSERT_FALSE(cur2->delete_mark);
  ASSERT_EQ(cur, cur2);
  ASSERT_EQ(cur->id, cur2->id);

  Cursor_Free(cur);

}

TEST_F(CursorsTest, OwnershipAPI) {
  // Case 1: Cursors_Purge marks non-idle cursor for deletion
  Cursor *cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));

  auto id = cur->id;
  ASSERT_EQ(Cursors_Purge(&RSCursors, id), REDISMODULE_OK) << "Should be able to mark for deletion";
  ASSERT_EQ(Cursors_TakeForExecution(&RSCursors, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark);

  ASSERT_EQ(Cursors_GetInfoStats().total, 1) << "Cursor should be alive";
  ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Pausing the cursor Should actually free it.";
  ASSERT_EQ(Cursors_GetInfoStats().total, 0) << "Cursor should be deleted";

  // Case 2: Cursors_Purge with explicit cursor free
  cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;
  ASSERT_EQ(Cursors_TakeForExecution(&RSCursors, id), nullptr) << "Cursor already in use";

  ASSERT_EQ(Cursors_Purge(&RSCursors, id), REDISMODULE_OK) << "Should be able to mark for deletion";
  ASSERT_EQ(Cursors_TakeForExecution(&RSCursors, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark);

  ASSERT_EQ(Cursors_GetInfoStats().total, 1) << "Cursor should be alive";
  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK) << "Cursor should be deleted";
  ASSERT_EQ(Cursors_GetInfoStats().total, 0) << "Cursor should be deleted";

  // Case 3: CursorList_Empty marks non-idle cursor for deletion
  cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;

  CursorList_Empty(&RSCursors);

  ASSERT_EQ(Cursors_GetInfoStats().total, 1) << "Cursor should still be alive";
  ASSERT_EQ(Cursors_TakeForExecution(&RSCursors, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark) << "Cursor should be marked for deletion";

  ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Pausing the cursor should actually free it";
  ASSERT_EQ(Cursors_GetInfoStats().total, 0) << "Cursor should be deleted";

  // Case 4: CursorList_Empty with explicit cursor free
  cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;

  CursorList_Empty(&RSCursors);

  ASSERT_TRUE(cur->delete_mark) << "Cursor should be marked for deletion";
  ASSERT_EQ(Cursors_GetInfoStats().total, 1) << "Cursor should still be alive";

  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK) << "Cursor should be deleted";
  ASSERT_EQ(Cursors_GetInfoStats().total, 0) << "Cursor should be deleted";

  // Case 5: CursorList_Empty on multiple cursors, some idle, some active
  constexpr int numCursors = 5;
  constexpr int numIdle = numCursors / 2 + numCursors % 2;
  std::vector<uint64_t> ids;

  for (int i = 0; i < numCursors; ++i) {
    cur = Cursors_Reserve(&RSCursors, idx, 1000, NULL);
    ASSERT_TRUE(cur != NULL);
    ASSERT_FALSE(cur->delete_mark);
    ASSERT_FALSE(is_Idle(cur));
    if (i % 2 == 0) {
      ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Cursor should be paused";
      ids.push_back(cur->id);
    }
  }

  ASSERT_EQ(Cursors_GetInfoStats().total, numCursors) << "All cursors should be alive";

  CursorList_Empty(&RSCursors);

  ASSERT_EQ(Cursors_GetInfoStats().total, numCursors - numIdle) << "Half of the cursors should be alive";

  for (khiter_t ii = 0; ii != kh_end(RSCursors.lookup); ++ii) {
    if (!kh_exist(RSCursors.lookup, ii)) {
      continue;
    }
    Cursor *c = kh_val(RSCursors.lookup, ii);
    ASSERT_TRUE(c->delete_mark) << "Cursor should be marked for deletion";
    ASSERT_FALSE(IdInArray(c->id, ids.data(), ids.size())) << "Cursor should not be in the deleted array";
    ASSERT_EQ(Cursor_Pause(c), REDISMODULE_OK) << "Cursor should be paused";
  }

  ASSERT_EQ(Cursors_GetInfoStats().total, 0) << "All cursors should be deleted";
}
