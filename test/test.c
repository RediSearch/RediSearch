#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "../triemap.h"
#include "time_sample.h"

uint16_t crc16(const char *buf, int len);

#define MB(x) x / (float)(1024 * 1024)

void formatKey(char *buf, int i) {
  sprintf(buf, "00key:%d", i);
  //*(uint16_t *)buf = crc16(&buf[2], strlen(buf) - 2);
  // printf("%s\n", buf);
  // sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
  // buf[4] = ':';
}

void testTrie(int N) {
  TrieMap *tm = NewTrieMap();
  int k = 32;

  unsigned char buf[k + 1];
  buf[k] = 0;

  TimeSample ts;
  TimeSampler_Reset(&ts);
  for (int i = 0; i < N; i++) {
    formatKey((char *)buf, i);
    TrieMapNode_Add(&tm, buf, strlen((char *)buf), NULL, NULL);
  }
  printf("created %d entries, memory size now %f\n", N,
         MB(TrieMapNode_MemUsage(tm)));
  formatKey((char *)buf, 35410);
  // buf[6] = 0;
  printf("searching for %.*s\n", 2, buf);
  TrieMapIterator *it = TrieMapNode_Iterate(tm, buf, 2);
  char *s;
  tm_len_t l;
  void *val;

  int matches = 0;
  TimeSampler_Reset(&ts);
  int rc;
  do {
    TIME_SAMPLE_BLOCK(ts, (rc = TrieMapIterator_Next(it, &s, &l, &val)));
    if (!rc) break;
    matches++;

    // printf("found %.*s\n", l, s);
  } while (1);
  printf("%d matches in %.03fsec (%.02fns/iter)\n", matches,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
}

int main(int argc, char **argv) {
  testTrie(5000000);
  return 0;
}