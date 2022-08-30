#pragma once

#include <sys/time.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

struct TimeSample {
  struct timespec startTime;
  struct timespec endTime;
  long long durationNS;
  int num;

  void Start();
  void Tick();
  void End();

  long long DurationNS();
  long long DurationMS();
  double DurationSec();

  double IterationSec();
  double IterationMS();
};

inline void TimeSample::Start() {
  clock_gettime(CLOCK_REALTIME, &startTime);
  num = 0;
}

inline void TimeSample::Tick() {
  ++num;
}

inline void TimeSample::End() {
  clock_gettime(CLOCK_REALTIME, &endTime);

  durationNS =
    ((long long)1000000000 * endTime.tv_sec + endTime.tv_nsec) -
    ((long long)1000000000 * startTime.tv_sec + startTime.tv_nsec);
}

inline long long TimeSample::DurationNS() {
  return durationNS;
}

inline long long TimeSample::DurationMS() {
  return durationNS / 1000000;
}

inline double TimeSample::DurationSec() {
  return (double)durationNS / 1000000000.0;
}

inline double TimeSample::IterationSec() {
  return ((double)durationNS / 1000000000.0) /
         (double)(num ? num : 1.0);
}

inline double TimeSample::IterationMS() {
  return ((double)durationNS / 1000000.0) /
         (double)(num ? num : 1.0);
}
