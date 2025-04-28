
#include "gtest/gtest.h"
#include "cursor.h"

#define is_Idle(cur) ((cur)->pos != -1)

class CursorsTest : public ::testing::Test {};

TEST_F(CursorsTest, BasicAPI) {
  StrongRef dummy = {0};
  Cursor *cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  auto id = cur->id;

  ASSERT_EQ(Cursors_TakeForExecution(&g_CursorsList, id), nullptr) << "Cursor already in use";

  Cursor_Pause(cur);
  ASSERT_TRUE((cur));
  ASSERT_TRUE(is_Idle(cur));

  Cursor *cur2 = Cursors_TakeForExecution(&g_CursorsList, id);
  ASSERT_TRUE(cur2 != NULL);
  ASSERT_FALSE(is_Idle(cur2));
  ASSERT_FALSE(cur2->delete_mark);
  ASSERT_EQ(cur, cur2);
  ASSERT_EQ(cur->id, cur2->id);

  Cursor_Free(cur);

}

TEST_F(CursorsTest, OwnershipAPI) {
  StrongRef dummy = {0};
  Cursor *cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));

  auto id = cur->id;
  ASSERT_EQ(Cursors_Purge(&g_CursorsList, id), REDISMODULE_OK) << "Should be able to mark for deletion";
  ASSERT_EQ(Cursors_TakeForExecution(&g_CursorsList, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark);

  ASSERT_EQ(Cursors_GetInfoStats().total_user, 1) << "Cursor should be alive";
  ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Pausing the cursor Should actually free it.";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 0) << "Cursor should be deleted";

  // Try again with explicitly deleting the cursor
  cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;
  ASSERT_EQ(Cursors_TakeForExecution(&g_CursorsList, id), nullptr) << "Cursor already in use";

  ASSERT_EQ(Cursors_Purge(&g_CursorsList, id), REDISMODULE_OK) << "Should be able to mark for deletion";
  ASSERT_EQ(Cursors_TakeForExecution(&g_CursorsList, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark);

  ASSERT_EQ(Cursors_GetInfoStats().total_user, 1) << "Cursor should be alive";
  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK) << "Cursor should be deleted";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 0) << "Cursor should be deleted";

}
