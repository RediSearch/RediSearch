/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#include <algorithm>
#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/iterators/idlist_iterator.h"

static bool cmp_docids(const t_docId& d1, const t_docId& d2) {
  return d1 < d2;
}

class MetricIteratorCommonTest : public ::testing::TestWithParam<std::tuple<std::vector<t_docId>, std::vector<double>, Metric>>  {
protected:
  std::vector<t_docId> docIds;
  std::vector<double> scores;
  std::vector<double> sortedScores;
  std::vector<t_docId> sortedDocIds;
  Metric metric_type;
  bool yields_metric;
  QueryIterator *iterator_base;

  void SetUp() override {
    std::tie(docIds, scores, metric_type) = GetParam();
    std::vector<size_t> indices(docIds.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      indices[i] = i;
    }

    std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });
    for (size_t i : indices) {
      sortedDocIds.push_back(docIds[i]);
      sortedScores.push_back(scores[i]);
    }
    t_docId* ids_array = (t_docId*)rm_malloc(sortedDocIds.size() * sizeof(t_docId));
    double *scores_array = (double*)rm_malloc(sortedScores.size() * sizeof(double));
    std::copy(sortedDocIds.begin(), sortedDocIds.end(), ids_array);
    std::copy(sortedScores.begin(), sortedScores.end(), scores_array);
    iterator_base = NewMetricIterator(ids_array, scores_array, indices.size(), metric_type);
  }
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_P(MetricIteratorCommonTest, Read) {
  IteratorStatus rc;
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), docIds.size());

  // Test reading until EOF
  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
    ASSERT_EQ(iterator_base->current->docId, sortedDocIds[i]);
    ASSERT_EQ(iterator_base->lastDocId, sortedDocIds[i]);
    ASSERT_FALSE(iterator_base->atEOF);

    // Check score value if yields_metric is true
    if (yields_metric) {
      ASSERT_EQ(iterator_base->current->data.tag, RSResultData_Metric);
      ASSERT_EQ(IndexResult_NumValue(iterator_base->current), sortedScores[i]);
      ASSERT_EQ(iterator_base->current->metrics[0].key, nullptr);
      ASSERT_EQ(RSValue_Type(iterator_base->current->metrics[0].value), RSValueType_Number);
      ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[i]);
    }
    i++;
  }

  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sortedDocIds[0]), ITERATOR_EOF); // SkipTo after EOF should return EOF
  ASSERT_EQ(i, sortedDocIds.size()) << "Expected to read " << sortedDocIds.size() << " documents";
}

TEST_P(MetricIteratorCommonTest, SkipTo) {
  IteratorStatus rc;

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, sortedDocIds[0]);
  ASSERT_EQ(iterator_base->lastDocId, sortedDocIds[0]);
  ASSERT_FALSE(iterator_base->atEOF);

  // Skip To to higher than last docID returns EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sortedDocIds.back() + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);

  t_docId i = 1;
  for (size_t index = 0; index < sortedDocIds.size(); index++) {
    t_docId id = sortedDocIds[index];
    while (i < id) {
      // Skip To from last sorted_id to the next one, should move the current iterator to id
      iterator_base->Rewind(iterator_base);
      rc = iterator_base->SkipTo(iterator_base, i);
      ASSERT_EQ(rc, ITERATOR_NOTFOUND);
      ASSERT_EQ(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, id);
      ASSERT_FALSE(iterator_base->atEOF); // EOF would be set in another iteration
      if (yields_metric) {
        ASSERT_EQ(IndexResult_NumValue(iterator_base->current), sortedScores[index]);
        ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[index]);
      }

      iterator_base->Rewind(iterator_base);
      i++;
    }
    iterator_base->Rewind(iterator_base);
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);
    ASSERT_FALSE(iterator_base->atEOF); // EOF would be set in another iteration

    if (yields_metric) {
      ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[index]);
    }

    i++;
  }
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);
  for (size_t index = 0; index < sortedDocIds.size(); index++) {
    t_docId id = sortedDocIds[index];
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);

    if (yields_metric) {
      ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[index]);
    }
  }
}

TEST_P(MetricIteratorCommonTest, Rewind) {
  IteratorStatus rc;

  for (size_t index = 0; index < sortedDocIds.size(); index++) {
    t_docId id = sortedDocIds[index];
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);

    if (yields_metric) {
      ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[index]);
    }

    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->lastDocId, 0);
    ASSERT_FALSE(iterator_base->atEOF);
  }

  for (size_t index = 0; index < sortedDocIds.size(); index++) {
    rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, sortedDocIds[index]);
    ASSERT_EQ(iterator_base->lastDocId, sortedDocIds[index]);

    if (yields_metric) {
      ASSERT_EQ(RSValue_Number_Get(iterator_base->current->metrics[0].value), sortedScores[index]);
    }
  }

  // Rewind after EOF read
  rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->current->docId, sortedDocIds.back());
  ASSERT_EQ(iterator_base->lastDocId, sortedDocIds.back());
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);
}

TEST_P(MetricIteratorCommonTest, Revalidate) {
  ASSERT_EQ(iterator_base->Revalidate(iterator_base), VALIDATE_OK);
}

INSTANTIATE_TEST_SUITE_P(MetricIteratorP, MetricIteratorCommonTest,
::testing::Values(
  std::make_tuple(
    std::vector<t_docId>{1, 2, 3, 40, 50},
    std::vector<double>{0.1, 0.2, 0.3, 0.4, 0.5},
    VECTOR_DISTANCE
  ),
  std::make_tuple(
    std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2},
    std::vector<double>{0.6, 0.5, 0.1, 0.98, 0.2, 1.0, 0.5, 0.3, 0.2},
    VECTOR_DISTANCE
  ),
  std::make_tuple(
    std::vector<t_docId>{10, 20, 30, 40, 50},
    std::vector<double>{0.9, 0.8, 0.7, 0.6, 0.5},
    VECTOR_DISTANCE
  ),
  std::make_tuple(
    std::vector<t_docId>{1000000, 2000000, 3000000},
    std::vector<double>{0.1, 0.5, 0.9},
    VECTOR_DISTANCE
  ),
  std::make_tuple(
    std::vector<t_docId>{42},
    std::vector<double>{1.0},
    VECTOR_DISTANCE
  )
 )
);
