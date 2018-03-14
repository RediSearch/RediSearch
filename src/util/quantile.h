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