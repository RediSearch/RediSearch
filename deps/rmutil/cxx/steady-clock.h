
#pragma once

#include <stdlib.h>

typedef size_t steady_clock_t;

void steady_clock_get(steady_clock_t *t);
double steady_clock_diff_msec(steady_clock_t *t1, steady_clock_t *t0);
double steady_clock_since_msec(steady_clock_t *t0);
long double steady_clock_since_usec(steady_clock_t *t0);
long double steady_clock_since_nsec(steady_clock_t *t0);
