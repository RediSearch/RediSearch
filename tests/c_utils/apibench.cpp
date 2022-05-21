#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <cstring>
#include <cassert>

#include <redisearch.h>
#include <module.h>
#include <version.h>
#include <redisearch_api.h>
#include <cpptests/redismock/redismock.h>

#define NUM_DOCS 5000000UL  // 10M
#define NUM_ITER 100UL

REDISEARCH_API_INIT_SYMBOLS();

extern "C" {

static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "ft", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}
}

int main(int, char **) {
  const char *arguments[] = {"SAFEMODE", "NOGC"};
  RMCK_Bootstrap(my_OnLoad, arguments, 2);
  RediSearch_Initialize();
  auto options = RediSearch_CreateIndexOptions();
  RediSearch_IndexOptionsSetFlags(options, RSIDXOPT_DOCTBLSIZE_UNLIMITED);
  auto idx = RediSearch_CreateIndex("ix", options);
  RediSearch_FreeIndexOptions(options);

  RediSearch_CreateField(idx, "f1", RSFLDTYPE_FULLTEXT, RSFLDOPT_NONE);
  // Ok so far..
  for (size_t ii = 0; ii < NUM_DOCS; ++ii) {
    auto d = RediSearch_CreateDocument(&ii, sizeof ii, 1.0, NULL);
    RediSearch_DocumentAddFieldCString(d, "f1", "hello", RSFLDTYPE_DEFAULT);
    RediSearch_SpecAddDocument(idx, d);

    if ((ii + 1) % 10000 == 0) {
      printf("\r%lu/%lu done        ", ii + 1, NUM_DOCS);
      fflush(stdout);
    }
  }
  printf("\n");

  // so far so good?
  // now, execute the query
  using std::chrono::duration_cast;
  using std::chrono::microseconds;
  using std::chrono::system_clock;

  microseconds elapsed;
  for (size_t ii = 0; ii < NUM_ITER; ++ii) {
    auto qn = RediSearch_CreateTokenNode(idx, "f1", "hello");
    auto it = RediSearch_GetResultsIterator(qn, idx);
    assert(it);
    size_t ndummy = 0;

    auto begin = system_clock::now();
    size_t n = 0;
    while (RediSearch_ResultsIteratorNext(it, idx, &ndummy)) {
      n++;
    }
    elapsed += duration_cast<microseconds>(system_clock::now() - begin);
    // elapsed += system_clock::now() - begin;
    assert(n == NUM_DOCS);
    RediSearch_ResultsIteratorFree(it);
    if ((ii + 1) % 10 == 0) {
      printf("\r%lu/%lu queries performed", ii + 1, NUM_ITER);
      fflush(stdout);
    }
  }

  printf("\n");
  printf("µs/query: %llu\n", (unsigned long long)elapsed.count() / NUM_ITER);
  printf("ms/query: %llu\n",
         (unsigned long long)duration_cast<std::chrono::milliseconds>(elapsed).count() / NUM_ITER);
}
