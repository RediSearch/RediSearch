#include "../triemap.h"
#include "time_sample.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

uint16_t crc16(const char *buf, int len);

#define MB(x) x / (float)(1024 * 1024)

const char alphabet[] =
    "  eeeeeeeeeeeeettttttttttaaaaaaaaaooooooooiiiiiiinnnnnnnsssss"
    "srrrrrrhhhhhllllddddcccuuummmffppggwwybbvkxjqz\0\0\0\0\0\0\0\0";

size_t formatRandomKey(char *buf, int i) {
  char *p = buf;
  size_t sz = 0;
  while (sz < i) {
    *p = alphabet[rand() % sizeof(alphabet)];
    sz++;
    if (*p == 0 || sz == i) {
      *p = 0;
      return sz;
    }
    p++;
  }
  return sz;
}

size_t formatKey(char *buf, int i) {
  return sprintf(buf, "key:%d", i);
  //  sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
  //  buf[4] = ':';
}
void populate(int N) {
  TrieMap *tm = NewTrieMap();
  int k = 32;

  char buf[k + 1];

  buf[k] = 0;
  size_t dataSize = 0;

  TimeSample ts;
  TimeSampler_Reset(&ts);
  for (int i = 0; i < N; i++) {
    size_t sz = formatKey(buf, i); // formatRandomKey((char *)buf, 12);
    if (!sz || buf[0] == 0)
      continue;
    // printf("%s\n", buf);
    dataSize += sz;
    TimeSampler_StartSection(&ts);
    TrieMap_Add(tm, buf, sz, NULL, NULL);
    TimeSampler_EndSection(&ts);
    TimeSampler_Tick(&ts);
    if (i % 1000000 == 999999) {
      printf("Insertion after %d items: %.03fsec (%.02fns/iteration), %.02fMB "
             "(%.02fMB raw data)\n",
             i + 1, TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts),
             MB(TrieMap_MemUsage(tm)), MB(dataSize));
      // TimeSampler_Reset(&ts);
    }
  }

  // memset(childrenHist, 0, sizeof(childrenHist));
  // size_t x = TrieMapNode_MemUsage(tm);
  // for (int i = 0; i < 256; i++) {
  //   if (childrenHist[i])
  //     printf("%d -> %d\n", i, childrenHist[i]);
  // }

  int L = N;

  TimeSampler_Reset(&ts);
  TimeSampler_Start(&ts);
  for (int i = 0; i < L; i++) {
    size_t sz = formatKey((char *)buf, i);
    TrieMap_Find(tm, buf, sz);
    TimeSampler_Tick(&ts);
  }
  TimeSampler_End(&ts);
  printf("Lookup of %d SEQUENTIAL items: %.03fsec (%.02fns/iteration)\n", L,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
  TimeSampler_Reset(&ts);
  TimeSampler_Start(&ts);
  for (int i = 0; i < L; i++) {
    size_t sz = formatKey((char *)buf, rand() % N);
    TrieMap_Find(tm, buf, sz);
    TimeSampler_Tick(&ts);
  }
  TimeSampler_End(&ts);

  printf("Lookup of %d RANDOM items: %.03fsec (%.02fns/iteration)\n", L,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));

  TimeSampler_Reset(&ts);

  for (int i = 0; i < N; i++) {
    formatKey((char *)buf, i);

    TimeSampler_StartSection(&ts);
    TrieMap_Delete(tm, buf, strlen((char *)buf), NULL);
    TimeSampler_EndSection(&ts);
    TimeSampler_Tick(&ts);

    if (i && i % 1000000 == 0) {
      printf("Deletion of %d items: %.03fsec (%.02fns/iteration)\n", i,
             TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));

      // TimeSampler_Reset(&ts);
    }
  }
  printf("Total Deletion of %d items: %.03fsec (%.02fns/iteration)\n", N,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
}

int main(int argc, char **argv) {
  populate(5000000);
  return 0;
}
