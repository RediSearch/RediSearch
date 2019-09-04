#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <assert.h>
#include <stdio.h>
#include "util/block_alloc.h"
#include "quantile.h"
#include "rmalloc.h"

typedef struct Sample {
  // Variables are named per the paper
  double v;  // Value represented
  float g;   // Number of ranks
  float d;   // Delta between ranks

  struct Sample *prev;
  struct Sample *next;
} Sample;

struct QuantStream {
  double *buffer;
  size_t bufferLength;
  size_t bufferCap;

  Sample *firstSample;
  Sample *lastSample;
  size_t n;              // Total number of values
  size_t samplesLength;  // Number of samples currently in list

  double *quantiles;
  size_t numQuantiles;
  Sample *pool;
};

#define QUANT_EPSILON 0.01

static int dblCmp(const void *a, const void *b) {
  double da = *(const double *)a, db = *(const double *)b;
  return da < db ? -1 : da > db ? 1 : 0;
}

static double getMaxValUnknown(double r, double n) {
  return QUANT_EPSILON * 2.0 * r;
}

static double getMaxValFromQuantiles(double r, double n, const double *quantiles,
                                     size_t numQuantiles) {
  double m = DBL_MAX;
  double f;
  for (size_t ii = 0; ii < numQuantiles; ++ii) {
    double q = quantiles[ii];
    if (q * n <= r) {
      f = (2 * QUANT_EPSILON * r) / q;
    } else {
      f = (2 * QUANT_EPSILON * (n - r)) / (1.0 - q);
    }
    if (f < m) {
      m = f;
    }
  }
  return m;
}

static inline void verifyCount(const QuantStream *stream) {
  size_t ii = 0;
  for (const Sample *cur = stream->firstSample; cur; cur = cur->next) {
    ++ii;
  }
  size_t expSize = stream->samplesLength;
  if (ii != expSize) {
    fprintf(stderr, "[->] Expected %lu. Have %lu\n", expSize, ii);
    abort();
  }

  ii = 0;
  for (const Sample *cur = stream->lastSample; cur; cur = cur->prev) {
    ++ii;
  }
  if (ii != expSize) {
    fprintf(stderr, "[<-] Expected %lu. Have %lu\n", expSize, ii);
    abort();
  }
}

#define INSERT_BEFORE 0
#define INSERT_AFTER 1
static void QS_InsertSampleAt(QuantStream *stream, Sample *pos, Sample *sample) {
  assert(pos);
  // printf("Inserting. Num=%lu. First=%p. Last=%p. posPrev=%p. posNext=%p\n",
  // stream->samplesLength,
  //        stream->firstSample, stream->lastSample, pos->prev, pos->next);

  sample->next = pos;
  if (pos->prev) {
    pos->prev->next = sample;
    sample->prev = pos->prev;
  } else {
    // At head of the list
    stream->firstSample = sample;
  }

  pos->prev = sample;
  stream->samplesLength++;
  // verifyCount(stream);
}

static void QS_AppendSample(QuantStream *stream, Sample *sample) {
  assert(sample->prev == NULL && sample->next == NULL);
  if (stream->lastSample == NULL) {
    assert(stream->samplesLength == 0);
    stream->lastSample = stream->firstSample = sample;
  } else {
    sample->prev = stream->lastSample;
    stream->lastSample->next = sample;
    stream->lastSample = sample;
    assert(stream->samplesLength > 0);
  }

  stream->samplesLength++;
  // verifyCount(stream);
}

static void QS_RemoveSample(QuantStream *stream, Sample *sample) {
  if (sample->prev) {
    sample->prev->next = sample->next;
  }
  if (sample->next) {
    sample->next->prev = sample->prev;
  }
  if (sample == stream->lastSample) {
    stream->lastSample = sample->prev;
  }
  if (sample == stream->firstSample) {
    stream->firstSample = sample->next;
  }

  // Insert into pool
  sample->next = stream->pool;
  stream->pool = sample;
  stream->samplesLength--;
  // verifyCount(stream);
}

static Sample *QS_NewSample(QuantStream *stream) {
  if (stream->pool) {
    Sample *ret = stream->pool;
    stream->pool = ret->next;
    memset(ret, 0, sizeof(*ret));
    return ret;
  } else {
    return rm_calloc(1, sizeof(Sample));
  }
}

static double QS_GetMaxVal(const QuantStream *stream, double r) {
  if (stream->numQuantiles) {
    return getMaxValFromQuantiles(r, stream->n, stream->quantiles, stream->numQuantiles);
  } else {
    return getMaxValUnknown(r, stream->n);
  }
}

// Clear the buffer from pending samples
static void QS_Flush(QuantStream *stream) {
  qsort(stream->buffer, stream->bufferLength, sizeof(*stream->buffer), dblCmp);
  double r = 0;

  // Both the buffer and the samples are ordered. We use the first sample, and
  // insert
  Sample *pos = stream->firstSample;
  size_t posNum = 0;

  for (size_t ii = 0; ii < stream->bufferLength; ++ii) {
    double curBuf = stream->buffer[ii];
    int inserted = 0;
    Sample *newSample = QS_NewSample(stream);
    newSample->v = curBuf;
    newSample->g = 1;

    while (pos) {

      if (pos->v > curBuf) {
        newSample->d = floor(QS_GetMaxVal(stream, r)) - 1;
        // printf("[Is=%lu, Ip=%lu, R=%lf] Delta: %lf\n", ii, posNum++, r, newSample->d);
        QS_InsertSampleAt(stream, pos, newSample);
        inserted = 1;
        break;
      }
      // printf("Sample %f: Pos->G: %lf, Pos->Value: %lf\n", curBuf, pos->g, pos->v);
      r += pos->g;
      pos = pos->next;
      posNum++;
    }

    if (!inserted) {
      assert(pos == NULL);
      newSample->d = 0;
      QS_AppendSample(stream, newSample);
    }

    stream->n++;
  }

  // Clear the buffer
  stream->bufferLength = 0;

  // Verification
  // for (Sample *s = stream->firstSample; s && s->next; s = s->next) {
  //   if (s->v > s->next->v) {
  //     printf("s->v (%lf) > s->next->v (%lf). ABORT\n", s->v, s->next->v);
  //     abort();
  //   }
  // }
}

static void QS_Compress(QuantStream *stream) {
  if (stream->samplesLength < 2) {
    return;
  }

  // printf("COMPRESS\n");

  Sample *cur = stream->lastSample->prev;
  double r = stream->n - 1 - stream->lastSample->g;
  size_t ii = 0;

  while (cur) {
    Sample *nextCur = cur->prev;
    Sample *parent = cur->next;
    double gCur = cur->g;
    if (cur->g + parent->g + parent->d <= QS_GetMaxVal(stream, r)) {
      parent->g += gCur;
      QS_RemoveSample(stream, cur);
    }
    r -= gCur;
    cur = nextCur;
    ii++;
  }
}

void QS_Insert(QuantStream *stream, double val) {
  assert(stream->bufferLength != stream->bufferCap);
  stream->buffer[stream->bufferLength] = val;
  if (++stream->bufferLength == stream->bufferCap) {
    QS_Flush(stream);
    QS_Compress(stream);
  }
}

double QS_Query(QuantStream *stream, double q) {
  if (stream->bufferLength) {
    QS_Flush(stream);
  }

  double t = ceil(q * stream->n);
  t += ceil(QS_GetMaxVal(stream, t) / 2.0);
  const Sample *prev = stream->firstSample;
  double r = 0;

  if (!prev) {
    return 0;
  }

  for (const Sample *cur = prev->next; cur; cur = cur->next) {
    if (r + cur->g + cur->d > t) {
      break;
    }
    r += cur->g;
    prev = cur;
  }
  return prev->v;
}

QuantStream *NewQuantileStream(const double *quantiles, size_t numQuantiles, size_t bufferLength) {
  QuantStream *ret = rm_calloc(1, sizeof(QuantStream));
  if ((ret->numQuantiles = numQuantiles)) {
    ret->quantiles = rm_calloc(numQuantiles, sizeof(*quantiles));
    memcpy(ret->quantiles, quantiles, sizeof(*quantiles) * numQuantiles);
  }
  ret->bufferCap = bufferLength;
  ret->buffer = rm_malloc(bufferLength * sizeof(*ret->buffer));
  return ret;
}

void QS_Free(QuantStream *qs) {
  rm_free(qs->quantiles);
  rm_free(qs->buffer);

  // Chain freeing the pools!

  Sample *cur = qs->firstSample;
  while (cur) {
    Sample *next = cur->next;
    rm_free(cur);
    cur = next;
  }

  cur = qs->pool;
  while (cur) {
    Sample *next = cur->next;
    rm_free(cur);
    cur = next;
  }
  rm_free(qs);
}

void QS_Dump(const QuantStream *stream, FILE *fp) {
  size_t ii = 0;
  for (Sample *cur = stream->firstSample; cur; cur = cur->next, ++ii) {
    fprintf(fp, "[%lu]: Value: %lf. Width: %lf. Delta: %lf\n", ii, cur->v, cur->g, cur->d);
  }
  fprintf(fp, "N=%lu\n", stream->n);
  fprintf(fp, "NumSamples: %lu\n", stream->samplesLength);
}

size_t QS_GetCount(const QuantStream *stream) {
  return stream->n;
}
