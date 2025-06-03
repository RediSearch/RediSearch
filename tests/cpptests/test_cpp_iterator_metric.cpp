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

class MetricIteratorCommonTest : public ::testing::TestWithParam<std::tuple<std::vector<t_docId>, std::vector<double>, Metric, bool>>  {
protected:
  std::vector<t_docId> docIds;
  std::vector<double> scores;
  Metric metric_type;
  bool yields_metric;
  QueryIterator *iterator_base;

  void SetUp() override {
    std::tie(docIds, scores, metric_type, yields_metric) = GetParam();
    std::vector<size_t> indices(docIds.size());
    std::vector<t_docId> sortedDocIds;
    std::vector<double> sortedScores;
    for (size_t i = 0; i < indices.size(); ++i) {
      indices[i] = i;
    }

    std::sort(indices.begin(), indices.end(), [&](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });
    for (size_t i : indices) {
      sortedDocIds.push_back(docIds[i]);
      sortedScores.push_back(scores[i]);
    }
    t_docId* ids_array = (t_docId*)rm_malloc(sortedDocIds.size() * sizeof(t_docId));
    std::copy(sortedDocIds.begin(), sortedDocIds.end(), ids_array);

    double *scores_array = nullptr;

    if (yields_metric) {
      scores_array = (double*)rm_malloc(sortedScores.size() * sizeof(double));
      std::copy(sortedScores.begin(), sortedScores.end(), scores_array);
    }
    iterator_base = IT_V2(NewMetricIterator)(ids_array, scores_array, indices.size(), metric_type, yields_metric);
  }
  void TearDown() override {
    iterator_base->Free(iterator_base);
  }
};

TEST_P(MetricIteratorCommonTest, Read) {
  auto sorted_docIds = docIds;
  auto sorted_scores = scores;
  std::vector<size_t> indices(docIds.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = i;
  }
  std::sort(indices.begin(), indices.end(), [this](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });

  sorted_docIds.clear();
  sorted_scores.clear();
  for (size_t i : indices) {
    sorted_docIds.push_back(docIds[i]);
    sorted_scores.push_back(scores[i]);
  }

  IteratorStatus rc;
  ASSERT_EQ(iterator_base->NumEstimated(iterator_base), docIds.size());

  // Test reading until EOF
  size_t i = 0;
  while ((rc = iterator_base->Read(iterator_base)) == ITERATOR_OK) {
    ASSERT_EQ(iterator_base->current->docId, sorted_docIds[i]);
    ASSERT_EQ(iterator_base->lastDocId, sorted_docIds[i]);
    ASSERT_FALSE(iterator_base->atEOF);

    // Check score value if yields_metric is true
    if (yields_metric) {
      ASSERT_EQ(iterator_base->current->type, RSResultType_Metric);
      ASSERT_EQ(iterator_base->current->num.value, sorted_scores[i]);
      ASSERT_EQ(iterator_base->current->metrics[0].key, nullptr);
      ASSERT_EQ(iterator_base->current->metrics[0].value->t, RSValue_Number);
      ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[i]);
    }
    i++;
  }

  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF); // Reading after EOF should return EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sorted_docIds[0]), ITERATOR_EOF); // SkipTo after EOF should return EOF
  ASSERT_EQ(i, docIds.size()) << "Expected to read " << docIds.size() << " documents";
}

TEST_P(MetricIteratorCommonTest, SkipTo) {
  auto sorted_docIds = docIds;
  auto sorted_scores = scores;
  std::vector<size_t> indices(docIds.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = i;
  }
  std::sort(indices.begin(), indices.end(), [this](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });

  sorted_docIds.clear();
  sorted_scores.clear();
  for (size_t i : indices) {
    sorted_docIds.push_back(docIds[i]);
    sorted_scores.push_back(scores[i]);
  }

  IteratorStatus rc;

  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_OK);
  ASSERT_EQ(iterator_base->current->docId, sorted_docIds[0]);
  ASSERT_EQ(iterator_base->lastDocId, sorted_docIds[0]);
  ASSERT_FALSE(iterator_base->atEOF);

  // Skip To to higher than last docID returns EOF
  ASSERT_EQ(iterator_base->SkipTo(iterator_base, sorted_docIds.back() + 1), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);

  t_docId i = 1;
  for (size_t index = 0; index < sorted_docIds.size(); index++) {
    t_docId id = sorted_docIds[index];
    while (i < id) {
      // Skip To from last sorted_id to the next one, should move the current iterator to id
      iterator_base->Rewind(iterator_base);
      rc = iterator_base->SkipTo(iterator_base, i);
      ASSERT_EQ(rc, ITERATOR_NOTFOUND);
      ASSERT_EQ(iterator_base->lastDocId, id);
      ASSERT_EQ(iterator_base->current->docId, id);
      ASSERT_FALSE(iterator_base->atEOF); // EOF would be set in another iteration
      if (yields_metric) {
        ASSERT_EQ(iterator_base->current->num.value, sorted_scores[index]);
        ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[index]);
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
      ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[index]);
    }

    i++;
  }
  ASSERT_EQ(iterator_base->Read(iterator_base), ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);

  iterator_base->Rewind(iterator_base);
  for (size_t index = 0; index < sorted_docIds.size(); index++) {
    t_docId id = sorted_docIds[index];
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);

    if (yields_metric) {
      ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[index]);
    }
  }
}

TEST_P(MetricIteratorCommonTest, Rewind) {
  auto sorted_docIds = docIds;
  auto sorted_scores = scores;
  std::vector<size_t> indices(docIds.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = i;
  }
  std::sort(indices.begin(), indices.end(), [this](size_t i1, size_t i2) { return docIds[i1] < docIds[i2]; });

  sorted_docIds.clear();
  sorted_scores.clear();
  for (size_t i : indices) {
    sorted_docIds.push_back(docIds[i]);
    sorted_scores.push_back(scores[i]);
  }

  IteratorStatus rc;

  for (size_t index = 0; index < sorted_docIds.size(); index++) {
    t_docId id = sorted_docIds[index];
    rc = iterator_base->SkipTo(iterator_base, id);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, id);
    ASSERT_EQ(iterator_base->lastDocId, id);

    if (yields_metric) {
      ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[index]);
    }

    iterator_base->Rewind(iterator_base);
    ASSERT_EQ(iterator_base->lastDocId, 0);
    ASSERT_FALSE(iterator_base->atEOF);
  }

  for (size_t index = 0; index < sorted_docIds.size(); index++) {
    rc = iterator_base->Read(iterator_base);
    ASSERT_EQ(rc, ITERATOR_OK);
    ASSERT_EQ(iterator_base->current->docId, sorted_docIds[index]);
    ASSERT_EQ(iterator_base->lastDocId, sorted_docIds[index]);

    if (yields_metric) {
      ASSERT_EQ(iterator_base->current->metrics[0].value->numval, sorted_scores[index]);
    }
  }

  // Rewind after EOF read
  rc = iterator_base->Read(iterator_base);
  ASSERT_EQ(rc, ITERATOR_EOF);
  ASSERT_TRUE(iterator_base->atEOF);
  ASSERT_EQ(iterator_base->current->docId, sorted_docIds.back());
  ASSERT_EQ(iterator_base->lastDocId, sorted_docIds.back());
  iterator_base->Rewind(iterator_base);
  ASSERT_EQ(iterator_base->lastDocId, 0);
  ASSERT_FALSE(iterator_base->atEOF);
}

INSTANTIATE_TEST_SUITE_P(MetricIteratorP, MetricIteratorCommonTest,
::testing::Values(
  std::make_tuple(
    std::vector<t_docId>{1, 2, 3, 40, 50},
    std::vector<double>{0.1, 0.2, 0.3, 0.4, 0.5},
    VECTOR_DISTANCE,
    false
  ),
  std::make_tuple(
    std::vector<t_docId>{1, 2, 3, 40, 50},
    std::vector<double>{0.1, 0.2, 0.3, 0.4, 0.5},
    VECTOR_DISTANCE,
    true
  ),
  std::make_tuple(
    std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2},
    std::vector<double>{0.6, 0.5, 0.1, 0.98, 0.2, 1.0, 0.5, 0.3, 0.2},
    VECTOR_DISTANCE,
    false
  ),
  std::make_tuple(
    std::vector<t_docId>{6, 5, 1, 98, 20, 1000, 500, 3, 2},
    std::vector<double>{0.6, 0.5, 0.1, 0.98, 0.2, 1.0, 0.5, 0.3, 0.2},
    VECTOR_DISTANCE,
    true
  ),
  std::make_tuple(
    std::vector<t_docId>{10, 20, 30, 40, 50},
    std::vector<double>{0.9, 0.8, 0.7, 0.6, 0.5},
    VECTOR_DISTANCE,
    false
  ),
  std::make_tuple(
    std::vector<t_docId>{10, 20, 30, 40, 50},
    std::vector<double>{0.9, 0.8, 0.7, 0.6, 0.5},
    VECTOR_DISTANCE,
    true
  ),
  std::make_tuple(
    std::vector<t_docId>{1000000, 2000000, 3000000},
    std::vector<double>{0.1, 0.5, 0.9},
    VECTOR_DISTANCE,
    false
  ),
  std::make_tuple(
    std::vector<t_docId>{1000000, 2000000, 3000000},
    std::vector<double>{0.1, 0.5, 0.9},
    VECTOR_DISTANCE,
    true
  ),
  std::make_tuple(
    std::vector<t_docId>{42},
    std::vector<double>{1.0},
    VECTOR_DISTANCE,
    false
  ),
  std::make_tuple(
    std::vector<t_docId>{42},
    std::vector<double>{1.0},
    VECTOR_DISTANCE,
    true
  )
 )
);
