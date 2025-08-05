
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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

  // Case 1: Cursors_Purge marks non-idle cursor for deletion
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

  // Case 2: Cursors_Purge with explicit cursor free
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

  // Case 3: CursorList_Empty marks non-idle cursor for deletion
  cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;

  // Call CursorList_Empty while cursor is not idle (active)
  CursorList_Empty(&g_CursorsList);

  // Cursor should be marked for deletion, not immediately freed
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 1) << "Cursor should still be alive";
  ASSERT_EQ(Cursors_TakeForExecution(&g_CursorsList, id), nullptr) << "Cursor already deleted";
  ASSERT_TRUE(cur->delete_mark) << "Cursor should be marked for deletion";

  // When cursor is paused, it should actually be freed due to delete_mark
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 1) << "Cursor should be alive";
  ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Pausing the cursor should actually free it";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 0) << "Cursor should be deleted";

  // Case 4: CursorList_Empty with explicit cursor free
  cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  ASSERT_FALSE(cur->delete_mark);
  ASSERT_FALSE(is_Idle(cur));
  id = cur->id;

  // Call CursorList_Empty while cursor is not idle (active)
  CursorList_Empty(&g_CursorsList);

  // Cursor should be marked for deletion, not immediately freed
  ASSERT_TRUE(cur->delete_mark) << "Cursor should be marked for deletion";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 1) << "Cursor should still be alive";

  // When cursor is explicitly freed, it should be deleted
  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK) << "Cursor should be deleted";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 0) << "Cursor should be deleted";
}
