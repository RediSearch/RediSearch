/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/idlist_iterator.h"
#include "src/metric_iterator.h"

#include "src/index.h"

template <typename IteratorType, bool yield_metric>
class BM_MetricIterator : public benchmark::Fixture {
public:
  IteratorType *iterator_base;
  std::vector<t_docId> docIds;
  std::vector<double> scores;
  t_docId *docIdsArray;
  double *scoresArray;

  t_docId *old_docIds;
  double *old_metrics;
  size_t numDocuments;

  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    numDocuments = 1'000'000; // Target number of documents, before removing duplicates
    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, 2'000'000);
    std::uniform_real_distribution<double> score_dist(0.0, 1.0);

    std::vector<std::pair<t_docId, double>> pairs;
    pairs.reserve(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      pairs.emplace_back(dist(rng), score_dist(rng));
    }

    std::sort(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    auto new_end = std::unique(pairs.begin(), pairs.end(), [](const auto& a, const auto& b) { return a.first == b.first; });
    pairs.erase(new_end, pairs.end());
    numDocuments = pairs.size(); // Update numDocuments after removing duplicates

    docIds.resize(numDocuments);
    scores.resize(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      docIds[i] = pairs[i].first;
      scores[i] = pairs[i].second;
    }

    old_docIds = array_new(t_docId, numDocuments);
    old_metrics = array_new(double, numDocuments);

    for (int i = 0; i < numDocuments; ++i) {
      array_append(old_docIds, docIds[i]);
      array_append(old_metrics, scores[i]);
    }

    docIdsArray = (t_docId*)rm_malloc(numDocuments * sizeof(t_docId));
    scoresArray = (double*)rm_malloc(numDocuments * sizeof(double));

    // Copy data from vectors to arrays
    memcpy(docIdsArray, docIds.data(), numDocuments * sizeof(t_docId));
    memcpy(scoresArray, scores.data(), numDocuments * sizeof(double));

    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      if (yield_metric) {
        iterator_base = IT_V2(NewMetricIterator)(docIdsArray, scoresArray, numDocuments, VECTOR_DISTANCE);
      } else {
        iterator_base = IT_V2(NewIdListIterator)(docIdsArray, numDocuments, 1.0);
        rm_free(scoresArray);
      }
    } else if constexpr (std::is_same_v<IteratorType, IndexIterator>) {
      iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, yield_metric);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType, bool yield_metric>
bool BM_MetricIterator<IteratorType, yield_metric>::initialized = false;

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, Read_NotYield, QueryIterator, false)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, SkipTo_NotYield, QueryIterator, false)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, Read_Yield, QueryIterator, true)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, SkipTo_Yield, QueryIterator, true)(benchmark::State &state) {
  t_docId step = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, iterator_base->lastDocId + step);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Yield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Yield);


BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, Read_Old_NotYield, IndexIterator, false)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, SkipTo_Old_NotYield, IndexIterator, false)(benchmark::State &state) {
  RSIndexResult *hit = iterator_base->current;
  hit->docId = 0;
  t_docId step = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base, hit->docId + step, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
      hit = iterator_base->current;
      hit->docId = 0;
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, Read_Old_Yield, IndexIterator, true)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE2_DEFINE_F(BM_MetricIterator, SkipTo_Old_Yield, IndexIterator, true)(benchmark::State &state) {
  RSIndexResult *hit = iterator_base->current;
  hit->docId = 0;
  t_docId step = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base, hit->docId + step, &hit);
    if (rc == INDEXREAD_EOF) {
      iterator_base->Rewind(iterator_base);
      hit = iterator_base->current;
      hit->docId = 0;
    }
  }
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_Yield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_Yield);

BENCHMARK_MAIN();
