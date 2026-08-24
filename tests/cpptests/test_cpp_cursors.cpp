
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
#include "aggregate/aggregate.h"
#include "info/info_redis/block_client.h"
#include "info/info_redis/threads/main_thread.h"
#include "info/info_redis/types/blocked_queries.h"
#include <vector>
#include <algorithm>

#define is_Idle(cur) ((cur)->pos != -1)

class CursorsTest : public ::testing::Test {};

bool IdInArray(uint64_t id, const uint64_t *arr, int size) {
  return std::find(arr, arr + size, id) != arr + size;
}



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

  // Case 5: CursorList_Empty on multiple cursors, some idle, some active
  // Verify that the idle cursors are freed immediately, and the active ones are marked for deletion
  constexpr int numCursors = 5;
  constexpr int numIdle = numCursors / 2 + numCursors % 2;
  std::vector<uint64_t> ids;

  for (int i = 0; i < numCursors; ++i) {
    cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
    ASSERT_TRUE(cur != NULL);
    ASSERT_FALSE(cur->delete_mark);
    ASSERT_FALSE(is_Idle(cur));
    if (i % 2 == 0) {
      ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Cursor should be paused";
      ids.push_back(cur->id);
    }
  }



  ASSERT_EQ(Cursors_GetInfoStats().total_user, numCursors) << "All cursors should be alive";

  // Call CursorList_Empty
  CursorList_Empty(&g_CursorsList);

  // The Idle cursors should be freed immediately, the active ones should be marked for deletion
  ASSERT_EQ(Cursors_GetInfoStats().total_user, numCursors - numIdle) << "Half of the cursors should be alive";

  // Verify the Ids of the cursors alive
  for (khiter_t ii = 0; ii != kh_end(g_CursorsList.lookup); ++ii) {
    if (!kh_exist(g_CursorsList.lookup, ii)) {
      continue;
    }
    Cursor *cur = kh_val(g_CursorsList.lookup, ii);
    // Assert mark delete

    ASSERT_TRUE(cur->delete_mark) << "Cursor should be marked for deletion";
    ASSERT_FALSE(IdInArray(cur->id, ids.data(), ids.size())) << "Cursor should not be in the deleted array";
    // Pause the cursor
    ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Cursor should be paused";
  }

  // After the cursors are paused, they should be freed
  ASSERT_EQ(Cursors_GetInfoStats().total_user, 0) << "All cursors should be deleted";

}

// A cursor owns its carried request: every path that frees the cursor must
// free the request exactly once (leaks and double frees surface under ASAN).
TEST_F(CursorsTest, CarriedRequestOwnership) {
  StrongRef dummy = {0};
  const size_t base = Cursors_GetInfoStats().total_user;

  // Freeing a cursor frees its carried request.
  AREQ *r = AREQ_New(NULL, 0);
  Cursor *cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  cur->query = &r->base;
  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK);
  ASSERT_EQ(Cursors_GetInfoStats().total_user, base);

  // A delete-marked cursor (CURSOR DEL / list purge mid-cycle) converts its
  // park into a free, which must also free the carried request.
  r = AREQ_New(NULL, 0);
  cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  cur->query = &r->base;
  ASSERT_EQ(Cursors_Purge(&g_CursorsList, cur->id), REDISMODULE_OK);
  ASSERT_TRUE(cur->delete_mark);
  ASSERT_EQ(Cursor_Pause(cur), REDISMODULE_OK) << "Pause must convert to free";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, base);
}

// EndCycle executes the recorded disposition: PAUSE parks the cursor (which
// keeps owning its carried request); the FREE default tears down cursor and
// request.
TEST_F(CursorsTest, EndCycleExecutesRecordedDisposition) {
  StrongRef dummy = {0};
  const size_t base = Cursors_GetInfoStats().total_user;

  // PAUSE: the cursor is parked back into the idle list.
  AREQ *r = AREQ_New(NULL, 0);
  Cursor *cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  cur->query = &r->base;
  r->base.blockedClientCycleActive = true;
  r->base.cursorInfo.cursor = cur;
  r->base.cursorInfo.disposition = CURSOR_DISPOSITION_PAUSE;
  QueryRequest_EndCycle(&r->base);
  ASSERT_TRUE(is_Idle(cur));
  ASSERT_EQ(Cursors_GetInfoStats().total_user, base + 1);
  ASSERT_EQ(Cursor_Free(cur), REDISMODULE_OK) << "Owner free: cursor + request";
  ASSERT_EQ(Cursors_GetInfoStats().total_user, base);

  // FREE is the per-cycle default: a cycle that recorded no PAUSE (e.g. the
  // timeout replied without exposing a live cursor id) tears down the
  // published cursor and its carried request, never parks them.
  r = AREQ_New(NULL, 0);
  cur = Cursors_Reserve(&g_CursorsList, dummy, 1000, NULL);
  ASSERT_TRUE(cur != NULL);
  cur->query = &r->base;
  r->base.blockedClientCycleActive = true;
  r->base.cursorInfo.cursor = cur;
  ASSERT_EQ(r->base.cursorInfo.disposition, CURSOR_DISPOSITION_FREE);
  QueryRequest_EndCycle(&r->base);
  ASSERT_EQ(Cursors_GetInfoStats().total_user, base);
}

// Shutdown unwind: requests whose queued free-privdata callback never drains
// (module cleanup runs inside the SHUTDOWN event, before the event loop can
// run it) are unlinked by BlockedQueries_UnwindCycles — both registry lists
// end up empty while the requests themselves stay alive, since async
// borrowers (an MR iterator context) may still hold them at that point.
TEST_F(CursorsTest, UnwindCyclesUnlinksWithoutFreeing) {
  if (!MainThread_GetBlockedQueries()) {
    ASSERT_EQ(MainThread_InitBlockedQueries(), 0);
  }
  BlockedQueries *bq = MainThread_GetBlockedQueries();
  ASSERT_TRUE(bq != NULL);

  // Two lingering cycles, one per registry list. Mirrors BeginCycle's
  // registry effects without a blocked client (none exists in unit tests).
  AREQ *reqs[2] = {AREQ_New(NULL, 0), AREQ_New(NULL, 0)};
  DLLIST *lists[2] = {&bq->queries, &bq->cursors};
  for (int i = 0; i < 2; i++) {
    reqs[i]->base.blockedClientCycleActive = true;
    reqs[i]->base.registryInfo.cycle_start = time(NULL);
    dllist_prepend(lists[i], &reqs[i]->base.registryInfo.node);
  }

  BlockedQueries_UnwindCycles();

  ASSERT_TRUE(DLLIST_IS_EMPTY(&bq->queries));
  ASSERT_TRUE(DLLIST_IS_EMPTY(&bq->cursors));
  for (int i = 0; i < 2; i++) {
    // Still alive and fully unlinked: a borrower's late read would be valid.
    ASSERT_FALSE(RegistryInfo_IsLinked(&reqs[i]->base.registryInfo));
    ASSERT_EQ(reqs[i]->base.registryInfo.cycle_start, 0);
    // In production the unwound requests leak by design; the unit test frees
    // them to stay ASAN-clean.
    reqs[i]->base.blockedClientCycleActive = false;
    QueryRequest_Free(&reqs[i]->base);
  }
}