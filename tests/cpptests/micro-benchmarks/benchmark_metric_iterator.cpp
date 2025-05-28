/*
* Copyright Redis Ltd. 2016 - present
* Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
* the Server Side Public License v1 (SSPLv1).
*/

#define MICRO_BENCHMARKS

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/metric_iterator.h"

#include "src/index.h"

class BM_MetricIterator : public benchmark::Fixture {
public:
  std::vector<t_docId> docIds;
  std::vector<double> scores;
  t_docId *old_docIds;
  double *old_metrics;

  static bool initialized;

  void SetUp(::benchmark::State &state) {
    if (!initialized) {
      RMCK::init();
      initialized = true;
    }

    auto numDocuments = state.range(0);
    std::mt19937 rng(46);
    std::uniform_int_distribution<t_docId> dist(1, 2'000'000);
    std::uniform_real_distribution<double> score_dist(0.0, 1.0);

    docIds.resize(numDocuments);
    scores.resize(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      docIds[i] = dist(rng);
      scores[i] = score_dist(rng);
    }
    
    std::vector<std::pair<t_docId, double>> pairs(numDocuments);
    for (int i = 0; i < numDocuments; ++i) {
      pairs[i] = std::make_pair(docIds[i], scores[i]);
    }
    
    std::sort(pairs.begin(), pairs.end(), 
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
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
  }
};

bool BM_MetricIterator::initialized = false;

// Translation - exponential range from 2 to 20 (double each time), then 25, 50, 75, and 100.
// This is the number of docIds to iterate on in each scenario
#define METRIC_SCENARIOS() RangeMultiplier(2)->Range(2, 20)->DenseRange(25, 100, 25)

BENCHMARK_DEFINE_F(BM_MetricIterator, Read_NotYield)(benchmark::State &state) {
  QueryIterator * iterator_base = IT_V2(NewMetricIterator)(docIds.data(), scores.data(), scores.size(), VECTOR_DISTANCE, false);
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, SkipTo_NotYield)(benchmark::State &state) {
  QueryIterator * iterator_base = IT_V2(NewMetricIterator)(docIds.data(), scores.data(), scores.size(), VECTOR_DISTANCE, false);
  t_docId docId = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, Read_Yield)(benchmark::State &state) {
  QueryIterator * iterator_base = IT_V2(NewMetricIterator)(docIds.data(), scores.data(), scores.size(), VECTOR_DISTANCE, true);
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, SkipTo_Yield)(benchmark::State &state) {
  QueryIterator * iterator_base = IT_V2(NewMetricIterator)(docIds.data(), scores.data(), scores.size(), VECTOR_DISTANCE, true);
  t_docId docId = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_NotYield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Yield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_NotYield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Yield)->METRIC_SCENARIOS();


BENCHMARK_DEFINE_F(BM_MetricIterator, Read_Old_NotYield)(benchmark::State &state) {
  IndexIterator *iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, false);
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, SkipTo_Old_NotYield)(benchmark::State &state) {
  IndexIterator *iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, false);
  RSIndexResult *hit;

  t_docId docId = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base, docId, &hit);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, Read_Old_Yield)(benchmark::State &state) {
  IndexIterator *iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, true);
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
  iterator_base->Free(iterator_base);
}

BENCHMARK_DEFINE_F(BM_MetricIterator, SkipTo_Old_Yield)(benchmark::State &state) {
  IndexIterator *iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, true);
  RSIndexResult *hit;
  t_docId docId = 10;
  for (auto _ : state) {
    int rc = iterator_base->SkipTo(iterator_base, docId, &hit);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }

  iterator_base->Free(iterator_base);
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_NotYield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_Yield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_NotYield)->METRIC_SCENARIOS();
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_Yield)->METRIC_SCENARIOS();*/
