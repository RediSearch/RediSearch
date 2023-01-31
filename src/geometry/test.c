
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "geometry.h"

enum QueryType {
  CONTAINS,
  WITHIN,
};

static size_t rdtsc(void);
static void PrintStats(struct RTree const *rt);
static void DeleteRandom(struct RTree *rt, char const *path, size_t num);
static void Query(struct RTree const *rt, char const *wkt, enum QueryType query);

int main() {
  struct RTree *rt = RTree_New();
  assert(RTree_IsEmpty(rt));

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

  RTree_Free(rt);
  return 0;
}


__attribute__((always_inline))
static inline size_t rdtsc(void) {
	unsigned lo, hi;
	__asm__ __volatile__ (
		"lfence; rdtsc; lfence"
    : "=a" (lo), "=d" (hi)
	);
	return (size_t)hi << 32 | lo;
}

static void PrintStats(struct RTree const *rt) {
  size_t size = RTree_Size(rt);
  size_t mem = RTree_MemUsage(rt);
	printf("num polygons in tree = %ld\n", size);
  printf("%ld bytes used\n", mem);
  printf("%f bytes used per indexed polygon\n", (double)mem/size);
  puts("");
}

static void DeleteRandom(struct RTree *rt, char const *path, size_t num) {
  printf("deleting up to %ld random polygons\n", num);
  srand(time(NULL));
	FILE *geo_in = fopen(path, "r");
	fseek(geo_in, 0, SEEK_END);
	size_t len = ftell(geo_in);
	fseek(geo_in, 0, SEEK_SET);
	char *geos_in_buf = malloc(len);
	[[maybe_unused]] size_t _ = fread(geos_in_buf, 1, len, geo_in);

	char* wkts[250000] = {NULL};
	char** runner = wkts;
	*runner++ = strtok(geos_in_buf, "\n");
	while ((*runner++ = strtok(NULL, "\n")));
	for (int i = 0; i < 200000; ++i) {
		struct RTDoc *qdoc = From_WKT(wkts[rand() % (sizeof wkts / sizeof *wkts)], 0);
		RTree_Remove(rt, qdoc);
		RTDoc_Free(qdoc);
	}
	free(geos_in_buf);
}

char const *QueryType_ToString(enum QueryType query) {
  switch (query) {
    case CONTAINS: return "containing";
    case WITHIN:   return "within";
    default: __builtin_unreachable();
  }
}

static void Query(struct RTree const *rt, char const *wkt, enum QueryType query) {
  printf("searching for polygons %s\n", QueryType_ToString(query));
  struct RTDoc *qdoc = From_WKT(wkt, 0);
  RTDoc_Print(qdoc);
  struct QueryIterator *iter;
  size_t start = rdtsc();
  switch (query) {
    case CONTAINS: iter = RTree_Query_Contains(rt, qdoc); break;
    case WITHIN:   iter = RTree_Query_Within  (rt, qdoc); break;
    default: __builtin_unreachable();
  }
  size_t end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);
}
