
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h>

#define REDISMODULE_MAIN
#include "geometry.h"

static size_t rdtsc(void);
static void PrintStats(struct RTree const *rt);
static void DeleteRandom(struct RTree *rt, char const *path, size_t num);
static void Query(struct RTree const *rt, char const *wkt, enum QueryType query);

int main() {
  struct RTree *rt = RTree_New();
  assert(RTree_IsEmpty(rt));
  PrintStats(rt);

  rt = Load_WKT_File(rt, "geometry.in");
  assert(!RTree_IsEmpty(rt));
  PrintStats(rt);

  Query(rt, "POLYGON((1.25 1.25, 1.5 1.333, 1.333 1.5, 1.25 1.25))", CONTAINS);
  Query(rt, "POLYGON((0 0, 12.0000004 0, 0 12.0000004, 0 0))", WITHIN);

  DeleteRandom(rt, "geometry.in", 200000);
  PrintStats(rt);

  printf("loading 250k more unique polygons\n");
  rt = Load_WKT_File(rt, "geometry_more.in");
  PrintStats(rt);

  Query(rt, "POLYGON((1.25 1.25, 1.5 1.333, 1.333 1.5, 1.25 1.25))", CONTAINS);
  Query(rt, "POLYGON((0 0, 12.0000004 0, 0 12.0000004, 0 0))", WITHIN);

  RTree_Clear(rt);
  assert(RTree_IsEmpty(rt));
  PrintStats(rt);

  RTree_Free(rt);
  return 0;
}

__attribute__((__always_inline__)) static inline size_t rdtsc(void) {
  unsigned lo, hi;
  __asm__ __volatile__("lfence; rdtsc; lfence" : "=a"(lo), "=d"(hi));
  return (size_t)hi << 32 | lo;
}

static void PrintStats(struct RTree const *rt) {
  size_t size = RTree_Size(rt);
  size_t mem = RTree_MemUsage(rt);
  printf("num polygons in tree = %lu\n", size);
  printf("%lu bytes used\n", mem);
  printf("%f bytes used per indexed polygon\n", (double)mem / (double)size);
  puts("");
}

static void DeleteRandom(struct RTree *rt, char const *path, size_t num) {
  printf("deleting up to %ld random polygons\n", num);
  struct stat filestat;
  stat(path, &filestat);
  size_t len = (size_t)filestat.st_size;
  char *geos_in_buf = malloc(len + 1);
  FILE *geo_in = fopen(path, "r");
  len = fread(geos_in_buf, 1, len, geo_in);
  geos_in_buf[len] = 0;
  fclose(geo_in);

  char *wkts[250001] = {NULL};
  wkts[0] = strtok(geos_in_buf, "\n");
  for (int i = 1; (wkts[i] = strtok(NULL, "\n")); ++i)
    ;

  srand((unsigned)time(NULL));
  for (unsigned i = 0; i < num; ++i) {
    char const *wkt = wkts[rand() % (int)((sizeof wkts - 1) / sizeof *wkts)];
    RTree_Remove_WKT(rt, wkt, strlen(wkt), 0);
  }
  free(geos_in_buf);
}

char const *QueryType_ToString(enum QueryType query) {
  switch (query) {
    case CONTAINS:
      return "containing";
    case WITHIN:
      return "within";
    default:
      __builtin_unreachable();
  }
}

static void Query(struct RTree const *rt, char const *wkt, enum QueryType query) {
  printf("searching for polygons %s\n", QueryType_ToString(query));
  struct RTDoc *qdoc = From_WKT(wkt, strlen(wkt), 0);
  RTDoc_Print(qdoc);
  size_t start = rdtsc();
  struct GeometryQueryIterator *iter = RTree_Query(rt, qdoc, query);
  size_t end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc const *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);
}
