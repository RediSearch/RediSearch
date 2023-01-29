
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "geometry.h"

static size_t rdtsc();

int main() {
  struct RTree *rt = RTree_New();
  assert(RTree_IsEmpty(rt));

  rt = Load_WKT_File(rt, "geometry.in");

  assert(!RTree_IsEmpty(rt));
  size_t size = RTree_Size(rt);
  size_t mem = RTree_MemUsage(rt);
	printf("num polygons in tree = %ld\n", size);
  printf("%ld bytes used\n", mem);
  printf("%f bytes used per indexed polygon\n", (double)mem/size);
  puts("");

  printf("searching for polygons containing\n");
  struct RTDoc *qdoc = From_WKT("POLYGON((1.25 1.25, 1.5 1.333, 1.333 1.5, 1.25 1.25))", 0);
  RTDoc_Print(qdoc);
  size_t start = rdtsc();
  struct QueryIterator *iter = RTree_Query_Contains(rt, qdoc);
  size_t end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);

  printf("searching for polygons within\n");
  qdoc = From_WKT("POLYGON((0 0, 12.0000004 0, 0 12.0000004, 0 0))", 0);
  RTDoc_Print(qdoc);
  start = rdtsc();
  iter = RTree_Query_Within(rt, qdoc);
  end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);

  srand(time(NULL));
	FILE *geo_in = fopen("geometry.in", "r");
	fseek(geo_in, 0, SEEK_END);
	size_t len = ftell(geo_in);
	fseek(geo_in, 0, SEEK_SET);
	char *geos_in_buf = malloc(len);
	[[maybe_unused]] size_t _ = fread(geos_in_buf, 1, len, geo_in);

	char* wkts[250000] = {NULL};
	char** runner = wkts;
	*runner++ = strtok(geos_in_buf, "\n");
	while ((*runner++ = strtok(NULL, "\n")));
	for (int i = 0; i < 100000; ++i) {
		qdoc = From_WKT(wkts[rand() % sizeof wkts / sizeof *wkts], 0);
		RTree_Remove(rt, qdoc);
		RTDoc_Free(qdoc);
	}
	free(geos_in_buf);

  size = RTree_Size(rt);
  mem = RTree_MemUsage(rt);
	printf("after deleting up to 100k random polygons, size = %ld\n", size);
  printf("%ld bytes used\n", mem);
  printf("%f bytes used per indexed polygon\n", (double)mem/size);
  puts("");

	rt = Load_WKT_File(rt, "geometry_more.in");
  size = RTree_Size(rt);
  mem = RTree_MemUsage(rt);
	printf("loading more polygons. new size = %ld\n", size);
  printf("%ld bytes used\n", mem);
  printf("%f bytes used per indexed polygon\n", (double)mem/size);
  puts("");

  printf("searching for polygons containing\n");
  qdoc = From_WKT("POLYGON((1.25 1.25, 1.5 1.333, 1.333 1.5, 1.25 1.25))", 0);
  RTDoc_Print(qdoc);
  start = rdtsc();
  iter = RTree_Query_Contains(rt, qdoc);
  end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);

  printf("searching for polygons within\n");
  qdoc = From_WKT("POLYGON((0 0, 12.0000004 0, 0 12.0000004, 0 0))", 0);
  RTDoc_Print(qdoc);
  start = rdtsc();
  iter = RTree_Query_Within(rt, qdoc);
  end = rdtsc();
  RTDoc_Free(qdoc);
  printf("num found results: %ld\n", QIter_Remaining(iter));
  printf("time taken: %ld clock cycles\n", end - start);
  for (struct RTDoc *result = QIter_Next(iter); NULL != result; result = QIter_Next(iter)) {
    // RTDoc_Print(result);
  }
  puts("");
  QIter_Free(iter);


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
