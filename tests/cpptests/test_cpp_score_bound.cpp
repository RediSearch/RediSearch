/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <cmath>
#include <limits>

#include "gtest/gtest.h"
#include "iterators/iterator_api.h"
#include "score_bound.h"
#include "iterator_util.h"

class ScoreBoundTest : public ::testing::Test {};

TEST_F(ScoreBoundTest, dropsOnlyClearlyBelowTheThreshold) {
  const double theta = 2.5;
  const double minCompetitive = ScoreBound_MinCompetitive(theta);
  const double edge = theta * (1 - SCORE_BOUND_REL_EPSILON);

  // A bound equal to the threshold may still enter the heap through the tie-break.
  EXPECT_FALSE(ScoreBound_CanDrop(theta, minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(edge, minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(std::nextafter(edge, INFINITY), minCompetitive));
  EXPECT_TRUE(ScoreBound_CanDrop(std::nextafter(edge, -INFINITY), minCompetitive));
  // Inside the margin, near ties are kept.
  EXPECT_FALSE(ScoreBound_CanDrop(theta * (1 - SCORE_BOUND_REL_EPSILON / 2), minCompetitive));
  EXPECT_TRUE(ScoreBound_CanDrop(theta * (1 - 2 * SCORE_BOUND_REL_EPSILON), minCompetitive));
  EXPECT_FALSE(ScoreBound_CanDrop(INFINITY, minCompetitive));
}

TEST_F(ScoreBoundTest, nonPositiveThresholdDropsNothing) {
  for (double theta : {0.0, -0.0, -1.0}) {
    const double minCompetitive = ScoreBound_MinCompetitive(theta);
    EXPECT_EQ(minCompetitive, -INFINITY);
    EXPECT_FALSE(ScoreBound_CanDrop(0, minCompetitive));
    EXPECT_FALSE(ScoreBound_CanDrop(-1e300, minCompetitive));
  }
}

TEST_F(ScoreBoundTest, supportedScorers) {
  for (const char *name : {"TFIDF", "TFIDF.DOCNORM", "BM25", "BM25STD", "DOCSCORE"}) {
    EXPECT_TRUE(ScoreBound_ScorerSupported(name)) << name;
  }
  for (const char *name : {"BM25STD.TANH", "BM25STD.NORM", "DISMAX", "HAMMING", "example_scorer"}) {
    EXPECT_FALSE(ScoreBound_ScorerSupported(name)) << name;
  }
}

TEST_F(ScoreBoundTest, filterLeavesAddNothingOnlyUnderBM25STD) {
  EXPECT_EQ(ScoreBound_FilterLeafBound("BM25STD"), 0);
  for (const char *name : {"TFIDF", "TFIDF.DOCNORM", "BM25", "DOCSCORE"}) {
    EXPECT_EQ(ScoreBound_FilterLeafBound(name), INFINITY) << name;
  }
}

// C iterators that leave the hooks unset report no bound and read as if there were no threshold.
TEST_F(ScoreBoundTest, unsetHooksAreSafeDefaults) {
  MockIterator *mock = new MockIterator(t_docId{1}, t_docId{2}, t_docId{3});
  QueryIterator *it = &mock->base;
  ASSERT_EQ(QueryIterator_ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 1);
  const ScoreBoundCtx ctx = {.filterLeafBound = 0};
  EXPECT_EQ(QueryIterator_CurrentMaxScore(it, &ctx), INFINITY);
  EXPECT_EQ(QueryIterator_CurrentBlockLastDocId(it), 0);
  ASSERT_EQ(QueryIterator_ReadWithThreshold(it, INFINITY), ITERATOR_OK);
  EXPECT_EQ(it->lastDocId, 2);
  it->Free(it);
}
