#include <stdlib.h>
#include <stdio.h>
#include "../triemap.h"
#include <stdint.h>
#include "time_sample.h"

uint16_t crc16(const char *buf, int len);

#define MB(x) x / (float)(1024 * 1024)

inline void formatKey(char *buf, int i)
{
    sprintf(buf, "0000:key:%d", i);
    sprintf((char *)buf, "%04x", crc16(&buf[5], strlen(buf) - 5));
    buf[4] = ':';
}
void populate(int N)
{

    TrieMap *tm = NewTrieMap();
    int k = 32;

    unsigned char buf[k + 1];
    buf[k] = 0;
    size_t dataSize = 0;

    TimeSample ts;
    TimeSampler_Reset(&ts);
    for (int i = 0; i < N; i++)
    {
        formatKey((char *)buf, i);

        dataSize += strlen(buf);
        TimeSampler_StartSection(&ts);
        TrieMapNode_Add(&tm, buf, strlen((char *)buf), NULL, NULL);
        TimeSampler_EndSection(&ts);
        TimeSampler_Tick(&ts);
        if ( i % 1000000 == 999999)
        {

            printf("Insertion after %d items: %.03fsec (%.02fns/iteration), %.02fMB (%.02fMB raw data)\n",
                   i+1, TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts),
                   MB(TrieMapNode_MemUsage(tm)), MB(dataSize));
            //TimeSampler_Reset(&ts);
        }
    }

    int L = 1000000;
    
    TimeSampler_Reset(&ts);
    for (int i = 0; i < L; i++)
    {
        formatKey((char *)buf, rand() % N);
        TIME_SAMPLE_BLOCK(ts, TrieMapNode_Find(tm, buf, strlen((char *)buf)))
    }

    printf("Lookup of %d RANDOM items: %.03fsec (%.02fns/iteration)\n", L,
           TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));

    TimeSampler_Reset(&ts);
    for (int i = 0; i < L; i++)
    {
        formatKey((char *)buf, i);
        TIME_SAMPLE_BLOCK(ts, TrieMapNode_Find(tm, buf, strlen((char *)buf)))
    }
    printf("Lookup of %d SEQUENTIAL items: %.03fsec (%.02fns/iteration)\n", L,
           TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));



    TimeSampler_Reset(&ts);

    TimeSampler_Reset(&ts);
    for (int i = 0; i < N; i++)
    {

        formatKey((char *)buf, i);

        TimeSampler_StartSection(&ts);
        TrieMapNode_Delete(tm, buf, strlen((char *)buf), NULL);
        TimeSampler_EndSection(&ts);
        TimeSampler_Tick(&ts);

        if (i && i % 1000000 == 0)
        {
            printf("Deletion of %d items: %.03fsec (%.02fns/iteration)\n", i,
                   TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
            //TimeSampler_Reset(&ts);
        }
    }
    printf("Total Deletion of %d items: %.03fsec (%.02fns/iteration)\n", N,
           TimeSampler_DurationSec(&ts), TimeSampler_IterationNS(&ts));
}

int main(int argc, char **argv)
{
    populate(15000000);
    return 0;
}