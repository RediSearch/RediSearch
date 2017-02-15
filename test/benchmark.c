#include "../triemap.h"
#include "time_sample.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

uint16_t crc16(const char *buf, int len);

int letterDist[] = {['E'] = 445, ['T'] = 330, ['A'] = 286, ['O'] = 272,
                    ['I'] = 269, ['N'] = 257, ['S'] = 232, ['R'] = 223,
                    ['H'] = 180, ['L'] = 145, ['D'] = 136, ['C'] = 119,
                    ['U'] = 97,  ['M'] = 89,  ['F'] = 85,  ['P'] = 76,
                    ['G'] = 66,  ['W'] = 59,  ['Y'] = 59,  ['B'] = 52,
                    ['V'] = 37,  ['K'] = 19,  ['X'] = 8,   ['J'] = 5,
                    ['Q'] = 4,   ['Z'] = 3};

#define MB(x) x / (float)(1024 * 1024)

const char alphabet[] =
    "  eeeeeeeeeeeetttttttttaaaaaaaaooooooooiiiiiiinnnnnnnsssss"
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
  // c return sprintf(buf, "key:%d", i);

  //  sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
  //  buf[4] = ':';
}
size_t formatKey(char *buf, int i) {

  return sprintf(buf, "key:%d", i);
  //  sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
  //  buf[4] = ':';
}
void populate(int N) {

  TrieMap *tm = NewTrieMap();
  int k = 32;

  unsigned char buf[k + 1];

  buf[k] = 0;
  size_t dataSize = 0;

  TimeSample ts;
  TimeSampler_Reset(&ts);
  int n = 0;
  srand(1337);
  for (int i = 0; n < N; i += 1 + rand() % 15) {

    size_t sz = formatKey((char *)buf, i);
    // if (!sz || buf[0] == 0)
    //   continue;
    // printf("%s\n", buf);
    dataSize += sz;
    TimeSampler_StartSection(&ts);
    n += TrieMapNode_Add(&tm, buf, sz, NULL, NULL);
    TimeSampler_EndSection(&ts);
    TimeSampler_Tick(&ts);
    if (i % 100000 == 99999) {

      printf(
          "Insertion after %d/%d items: %.03fsec (%.02fns/iteration), %.02fMB "
          "(%.02fMB raw data)\n",
          n, N, TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts),
          MB(TrieMapNode_MemUsage(tm)), MB(dataSize));
      // TimeSampler_Reset(&ts);
    }
  }

  int L = N;

  TimeSampler_Reset(&ts);
  TimeSampler_Start(&ts);
  for (int i = 0; i < L; i++) {

    size_t sz = formatKey((char *)buf, rand() % N);
    TrieMapNode_Find(tm, buf, sz);
    TimeSampler_Tick(&ts);
  }
  TimeSampler_End(&ts);

  printf("Lookup of %d RANDOM items: %.03fsec (%.02fns/iteration)\n", L,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));

  TimeSampler_Reset(&ts);
  TimeSampler_Start(&ts);
  n = 0;
  srand(1337);
  for (int i = 0; n < N; i += 1 + rand() % 15) {
    size_t sz = formatKey((char *)buf, i);
    TrieMapNode_Find(tm, buf, sz);
    ++n;
    TimeSampler_Tick(&ts);
  }
  TimeSampler_End(&ts);
  printf("Lookup of %d SEQUENTIAL items: %.03fsec (%.02fns/iteration)\n", n,
         TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));

  TimeSampler_Reset(&ts);
  TimeSampler_Start(&ts);
  for (int i = 0; i < N; i++) {

    formatKey((char *)buf, i);

    TimeSampler_StartSection(&ts);
    TrieMapNode_Delete(tm, buf, strlen((char *)buf), NULL);
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
