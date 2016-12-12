#ifndef __RL_TIME_SAMPLE__
#define __RL_TIME_SAMPLE__

#include <sys/time.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

typedef struct {
  struct timespec startTime;
  struct timespec endTime;
  long long durationNS;
  int num;
} TimeSample;

static void TimeSampler_Start(TimeSample *ts) {
  clock_gettime(CLOCK_REALTIME, &ts->startTime);
  ts->num = 0;
}

static void TimeSampler_Tick(TimeSample *ts) { ++ts->num; }
static void TimeSampler_End(TimeSample *ts) {

  clock_gettime(CLOCK_REALTIME, &ts->endTime);

  ts->durationNS =
      ((long long)1000000000 * ts->endTime.tv_sec + ts->endTime.tv_nsec) -
      ((long long)1000000000 * ts->startTime.tv_sec + ts->startTime.tv_nsec);
}

static long long TimeSampler_DurationNS(TimeSample *ts) {

  return ts->durationNS;
}

static long long TimeSampler_DurationMS(TimeSample *ts) {

  return ts->durationNS / 1000000;
}

static double TimeSampler_DurationSec(TimeSample *ts) {

  return (double)ts->durationNS / 1000000000.0;
}

static double TimeSampler_IterationSec(TimeSample *ts) {

  return ((double)ts->durationNS / 1000000000.0) /
         (double)(ts->num ? ts->num : 1.0);
}

static double TimeSampler_IterationMS(TimeSample *ts) {

  return ((double)ts->durationNS / 1000000.0) /
         (double)(ts->num ? ts->num : 1.0);
}

#define TIME_SAMPLE_RUN(blk)                                                   \
  {                                                                            \
    TimeSample ts;                                                             \
    TimeSampler_Start(&ts);                                                    \
    { blk; };                                                                  \
    TimeSampler_End(&ts);                                                      \
    printf("Execution time for " #blk ": %f seconds\n",                        \
           TimeSampler_DurationSec(&ts));                                      \
  }

#define TIME_SAMPLE_RUN_LOOP(N, blk)                                           \
  {                                                                            \
    TimeSample ts;                                                             \
    TimeSampler_Start(&ts);                                                    \
    for (int __ts_loop = 0; __ts_loop < N; __ts_loop++) {                      \
      blk;                                                                     \
      TimeSampler_Tick(&ts);                                                   \
    };                                                                         \
    TimeSampler_End(&ts);                                                      \
    printf("Execution time for %d iterations of " #blk                         \
           ": %f msec/iteration\n",                                         \
           ts.num, TimeSampler_IterationMS(&ts));                             \
  }

#endif