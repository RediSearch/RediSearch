/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef QUANTILE_H
#define QUANTILE_H

#include <stdlib.h>
#include <stdio.h>

typedef struct QuantStream QuantStream;

QuantStream *NewQuantileStream(const double *quantiles, size_t numQuantiles, size_t bufferLength);
void QS_Insert(QuantStream *qs, double val);
double QS_Query(QuantStream *qs, double val);
void QS_Free(QuantStream *qs);
void QS_Dump(const QuantStream *stream, FILE *fp);
size_t QS_GetCount(const QuantStream *stream);

#endif