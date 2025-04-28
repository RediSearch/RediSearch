
#include "gtest/gtest.h"
#include "cursor.h"

#define is_Idle(cur) ((cur)->pos != -1)

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

  // Try again with explicitly deleting the cursor
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

}
