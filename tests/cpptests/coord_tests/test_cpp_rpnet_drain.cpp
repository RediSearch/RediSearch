/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "coord/rpnet.h"
#include "search_result.h"
#include "search_result_ffi.h"

TEST(RPNetDrainTest, constructorProvidesDrainWithoutChainInsertion) {
  MRCommand command = {};
  RPNet *network = RPNet_New(&command, nullptr);
  SearchResult result = SearchResult_New();
  ASSERT_NE(nullptr, network->base.Drain);
  EXPECT_EQ(RP_DRAIN_EOF, network->base.Drain(&network->base, &result));
  network->base.Free(&network->base);
  SearchResult_Destroy(&result);
}
