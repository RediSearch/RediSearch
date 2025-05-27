/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#include <algorithm>
#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/iterators/metric_iterator.h"

static bool cmp_docids(const t_docId& d1, const t_docId& d2) {
  return d1 < d2;
}

class IDMetricIteratorCommonTest : public ::testing::TestWithParam<<std::vector<t_docId>, std::vector<double>, Metric, bool>> {
protected:
  std::vector<t_docId> docIds;
  std::vector<double> scores;
  Metric metric_type; 
  bool yields_metric;
  QueryIterator *iterator_base;

  void SetUp() override {
    auto [docIds, scores, metric_type, yields_metric] = GetParam();
    std::vector<size_t> indices(docIds.size());
    std::vector<int> sorted_docIds;
    std::vector<double> sorted_scores;
    for (size_t i = 0; i < indices.size(); ++i) {
        indices[i] = i;
    }

    std::sort(indices.begin(), indices.end(), [&docIds](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });
    for (size_t i : indices) {
      sorted_docIds.push_back(docIds[i]);
      sorted_scores.push_back(scores[i]);
    }
    t_docId * docIdsArray = array_new(t_docId, docIds.size());
    array_ensure_append_n(docIdsArray, sorted_docIds.data(), sorted_docIds.size());
    double * scoresArray = array_new(double, scores.size());
    array_ensure_append_n(scoresArray, sorted_scores.data(), sorted_scores.size());
    iterator_base = IT_V2(NewMetricIterator)(docIdsArray, scoresArray, metric_type, yields_metric);
  }
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};


TEST_P(IDMetricIteratorCommonTest, ReadNotYield) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  MetricIterator *iterator = (MetricIterator *)iterator_base;
  IteratorStatus rc;
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), docIds.size());
}

TEST_P(IDMetricIteratorCommonTest, SkipToNotYield) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  MetricIterator *iterator = (MetricIterator *)iterator_base;
  IteratorStatus rc;
}

TEST_P(IDMetricIteratorCommonTest, Rewind) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  MetricIterator *iterator = (MetricIterator *)iterator_base;
  IteratorStatus rc;
}

TEST_P(IDListIteratorCommonTest, Rewind) {
  auto sorted_docIds = docIds;
  std::sort(sorted_docIds.begin(), sorted_docIds.end(), cmp_docids);
  IdListIterator *iterator = (IdListIterator *)iterator_base;
  IteratorStatus rc;
  for (t_docId id : sorted_docIds) {
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator->base.lastDocId, 0);
    ASSERT_FALSE(iterator->base.atEOF);
  }
  for (t_docId id : sorted_docIds) {
    rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator->base.lastDocId, id);
    ASSERT_EQ(iterator->base.current->docId, id);
  }
  // Rewind after EOF read
  rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator->base.atEOF);
  ASSERT_EQ(iterator->base.lastDocId, sorted_docIds.back());
  ASSERT_EQ(iterator->base.current->docId, sorted_docIds.back());
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator->base.lastDocId, 0);
  ASSERT_FALSE(iterator->base.atEOF);
}

// Parameters for the tests above. Some set of docIDs sorted and unsorted
INSTANTIATE_TEST_SUITE_P(IDListIteratorP, IDListIteratorCommonTest, ::testing::Values(
        std::vector<t_docId>{1, 2, 3, 40, 50},
        std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2}
    )
);