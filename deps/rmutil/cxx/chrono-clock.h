/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include <stdlib.h>

typedef size_t steady_clock_t;

void steady_clock_get(steady_clock_t *t);
double steady_clock_diff_msec(steady_clock_t *t1, steady_clock_t *t0);
double steady_clock_since_msec(steady_clock_t *t0);
long double steady_clock_since_usec(steady_clock_t *t0);
long double steady_clock_since_nsec(steady_clock_t *t0);

typedef size_t hires_clock_t;

void hires_clock_get(hires_clock_t *t);
double hires_clock_diff_msec(hires_clock_t *t1, hires_clock_t *t0);
double hires_clock_since_msec(hires_clock_t *t0);
long double hires_clock_since_usec(hires_clock_t *t0);
long double hires_clock_since_nsec(hires_clock_t *t0);
