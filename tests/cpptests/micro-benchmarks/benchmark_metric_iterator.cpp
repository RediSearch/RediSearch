#define MICRO_BENCHMARKS

#include "benchmark/benchmark.h"
#include "redismock/util.h"

#include <random>
#include <vector>

#include "src/iterators/iterator_api.h"
#include "src/iterators/idlist_iterator.h"
#include "src/metric_iterator.h"

#include "src/index.h"

template <typename IteratorType>
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

    numDocuments = 10000;
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

    docIdsArray = (t_docId*)rm_malloc(numDocuments * sizeof(t_docId));
    scoresArray = (double*)rm_malloc(numDocuments * sizeof(double));

    // Copy data from vectors to arrays
    memcpy(docIdsArray, docIds.data(), numDocuments * sizeof(t_docId));
    memcpy(scoresArray, scores.data(), numDocuments * sizeof(double));

    if constexpr (std::is_same_v<IteratorType, QueryIterator>) {
      iterator_base = IT_V2(NewMetricIterator)(docIdsArray, scoresArray, numDocuments, VECTOR_DISTANCE, false);
    } else if constexpr (std::is_same_v<IteratorType, IndexIterator>) {
      iterator_base = NewMetricIterator(old_docIds, old_metrics, VECTOR_DISTANCE, false);
    }
  }

  void TearDown(::benchmark::State &state) {
    iterator_base->Free(iterator_base);
  }
};

template <typename IteratorType>
bool BM_MetricIterator<IteratorType>::initialized = false;

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, Read_NotYield, QueryIterator)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, SkipTo_NotYield, QueryIterator)(benchmark::State &state) {
  t_docId docId = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, Read_Yield, QueryIterator)(benchmark::State &state) {
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->Read(iterator_base);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, SkipTo_Yield, QueryIterator)(benchmark::State &state) {
  t_docId docId = 10;
  for (auto _ : state) {
    IteratorStatus rc = iterator_base->SkipTo(iterator_base, docId);
    docId += 10;
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
      docId = 10;
    }
  }
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Yield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Yield);


BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, Read_Old_NotYield, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, SkipTo_Old_NotYield, IndexIterator)(benchmark::State &state) {
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
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, Read_Old_Yield, IndexIterator)(benchmark::State &state) {
  RSIndexResult *hit;
  for (auto _ : state) {
    int rc = iterator_base->Read(iterator_base, &hit);
    if (rc == ITERATOR_EOF) {
      iterator_base->Rewind(iterator_base);
    }
  }
}

BENCHMARK_TEMPLATE1_DEFINE_F(BM_MetricIterator, SkipTo_Old_Yield, IndexIterator)(benchmark::State &state) {
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
}

BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, Read_Old_Yield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_NotYield);
BENCHMARK_REGISTER_F(BM_MetricIterator, SkipTo_Old_Yield);
