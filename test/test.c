#include <stdlib.h>
#include <stdio.h>
#include "../triemap.h"
#include <stdint.h>
#include "time_sample.h"

uint16_t crc16(const char *buf, int len);

#define MB(x) x / (float)(1024 * 1024)

 void formatKey(char *buf, int i)
{
    sprintf(buf, "0000:key:%d", i);
    // sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
    // buf[4] = ':';
}

void testTrie(int N)
{

    TrieMap *tm = NewTrieMap();
    int k = 32;

    unsigned char buf[k + 1];
    buf[k] = 0;

    TimeSample ts;
    TimeSampler_Reset(&ts);
    for (int i = 0; i < N; i++)
    {
        formatKey((char *)buf, i);
        TrieMapNode_Add(&tm, buf, strlen((char *)buf), NULL, NULL);
    }

    formatKey((char *)buf, 10);
    buf[6] = 0;
    printf("searching for %s\n", buf);
    TrieMapIterator *it = TrieMapNode_Iterate(tm, "0000:key:", 9);
    const char *s;
    tm_len_t l;
    void *val;

    int matches = 0;
    TimeSampler_Reset(&ts);
    int rc;
    do
    {
        TIME_SAMPLE_BLOCK(ts, (rc = TrieMapIterator_Next(it, &s, &l, &val)));
        
        if (!rc)
            break;
        matches++;
        
        //printf("found %.*s\n", l, s);
    } while (1);//matches < 1000);
    printf("%d matches in %.03fsec (%.02fns/iter)\n", matches, TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
}

int main(int argc, char **argv)
{
    testTrie(1000000);
    return 0;
}