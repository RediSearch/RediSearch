#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include "time_sample.h"

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#include <hiredis/adapters/libev.h>

#define PORT 6379
#define N 500000
void fill_first()
{
    redisContext* c = redisConnect("127.0.0.1", PORT);
    redisCommand(c, "FLUSHDB");

    for (int i = 0; i < N; i++)
    {
        int n = (rand() % 24) + 1; // some random size
        char tag[n], name[n];

        const char alphanum[] = { "abcdefghijklmnopqrstuvwxyz" };

        uint8_t rand_idx[n * 2];
        for (int xx = 0; xx < n*2; xx++) {
            rand_idx[xx] = rand() % 0xff;
        }
        
        for (int i = 0; i < n; i ++)
            tag[i] = alphanum[rand_idx[i] % sizeof(alphanum)]; // fill the array with alphanum chars

        for (int i = n; i < n * 2; i ++)
            name[i - n] = alphanum[rand_idx[i] % sizeof(alphanum)]; // fill the array with alphanum chars

        redisCommand(c, "FT.SUGADD userslex %b:%b %d", tag, n, name, n, i);
    }

    redisFree(c);
}

void add_delete(const char* variant)
{
    redisContext* c = redisConnect("127.0.0.1", PORT);

    for (int i = 0; i < N; i++)
        redisCommand(c, "FT.SUGADD userslex %s%d %d", variant, i, i);
    printf("Deleting!\n");
    for (int i = 0; i < N; i++)
        redisCommand(c, "FT.SUGDEL userslex %s%d", variant, i);

    redisFree(c);
}

void search(const char* str)
{
    redisContext* c = redisConnect("127.0.0.1", PORT);
    redisCommand(c, "FT.SUGGET userslex %b MAX 10 FUZZY", str, strlen(str));
    redisFree(c);
}

// typedef struct {
//     struct timespec start_time, end_time;
// } TimerSampler;

// void TimeSampler_Start(TimerSampler *ts) {
//     clock_gettime(CLOCK_REALTIME, &ts->start_time);
// }

// void TimeSampler_End(TimerSampler *ts) {

//    clock_gettime(CLOCK_REALTIME, &ts->end_time);
   
// }

// long long TimeSampler_DurationNS(TimerSampler *ts) {
    
//     long long diffInNanos = ((long long)1000000000 * ts->end_time.tv_sec + ts->end_time.tv_nsec) - 
//     ((long long)1000000000 * ts->start_time.tv_sec + ts->start_time.tv_nsec);
//     return diffInNanos;
// }

// #define TimeSampledBlock(ts, blk) { TimeSampler_Start(ts); { blk } ; TimeSampler_End(ts); }

int main (int argc, char** argv)
{

    printf("filling first!\n");


    TIME_SAMPLE_RUN(fill_first());
        
    
    
    for (int i  =0; i < 10; i++) {
    TIME_SAMPLE_RUN(search("asdfg"));
    }
    TIME_SAMPLE_RUN(add_delete("asdfg")); // now add 1000000 entries of a variant string and remove those entries
    
 for (int i  =0; i < 10; i++) {
    TIME_SAMPLE_RUN(search("asdfg")); // ended in 2 ms
    
 }
    //search("asdfg"); // ended in 66 ms, i.e. 33 times slower on the same key-set
    redisContext* c = redisConnect("127.0.0.1", PORT);
    redisCommand(c, "FLUSHDB");
}