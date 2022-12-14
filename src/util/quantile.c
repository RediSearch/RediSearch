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

inline void QuantStream::verifyCount() const {
  size_t ii = 0;
  for (const Sample *cur = firstSample; cur; cur = cur->next) {
    ++ii;
  }
  size_t expSize = samplesLength;
  if (ii != expSize) {
    fprintf(stderr, "[->] Expected %lu. Have %lu\n", expSize, ii);
    abort();
  }

  ii = 0;
  for (const Sample *cur = lastSample; cur; cur = cur->prev) {
    ++ii;
  }
  if (ii != expSize) {
    fprintf(stderr, "[<-] Expected %lu. Have %lu\n", expSize, ii);
    abort();
  }
}

#define INSERT_BEFORE 0
#define INSERT_AFTER 1
void QuantStream::InsertSampleAt(Sample *pos, Sample *sample) {
  assert(pos);
  // printf("Inserting. Num=%lu. First=%p. Last=%p. posPrev=%p. posNext=%p\n",
  // samplesLength,
  //        firstSample, lastSample, pos->prev, pos->next);

  sample->next = pos;
  if (pos->prev) {
    pos->prev->next = sample;
    sample->prev = pos->prev;
  } else {
    // At head of the list
    firstSample = sample;
  }

  pos->prev = sample;
  samplesLength++;
  // verifyCount();
}

void QuantStream::AppendSample(Sample *sample) {
  assert(sample->prev == NULL && sample->next == NULL);
  if (lastSample == NULL) {
    assert(samplesLength == 0);
    lastSample = firstSample = sample;
  } else {
    sample->prev = lastSample;
    lastSample->next = sample;
    lastSample = sample;
    assert(samplesLength > 0);
  }

  samplesLength++;
  // verifyCount();
}

void QuantStream::RemoveSample(Sample *sample) {
  if (sample->prev) {
    sample->prev->next = sample->next;
  }
  if (sample->next) {
    sample->next->prev = sample->prev;
  }
  if (sample == lastSample) {
    lastSample = sample->prev;
  }
  if (sample == firstSample) {
    firstSample = sample->next;
  }

  // Insert into pool
  sample->next = pool;
  pool = sample;
  samplesLength--;
  // verifyCount();
}

Sample *QuantStream::NewSample() {
  if (pool) {
    Sample *ret = pool;
    pool = ret->next;
    memset(ret, 0, sizeof *ret);
    return ret;
  } else {
    return static_cast<Sample *>(rm_calloc(1, sizeof(Sample)));
  }
}

double QuantStream::GetMaxVal(double r) const {
  if (numQuantiles) {
    return getMaxValFromQuantiles(r, n, quantiles, numQuantiles);
  } else {
    return getMaxValUnknown(r, n);
  }
}

// Clear the buffer from pending samples
void QuantStream::Flush() {
  qsort(buffer, bufferLength, sizeof(*buffer), dblCmp);
  double r = 0;

  // Both the buffer and the samples are ordered. We use the first sample, and
  // insert
  Sample *pos = firstSample;
  size_t posNum = 0;

  for (size_t ii = 0; ii < bufferLength; ++ii) {
    double curBuf = buffer[ii];
    int inserted = 0;
    Sample *newSample = NewSample();
    newSample->v = curBuf;
    newSample->g = 1;

    while (pos) {
      if (pos->v > curBuf) {
        newSample->d = floor(GetMaxVal(r)) - 1;
        // printf("[Is=%lu, Ip=%lu, R=%lf] Delta: %lf\n", ii, posNum++, r, newSample->d);
        InsertSampleAt(pos, newSample);
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
      AppendSample(newSample);
    }

    n++;
  }

  // Clear the buffer
  bufferLength = 0;

  // Verification
  // for (Sample *s = firstSample; s && s->next; s = s->next) {
  //   if (s->v > s->next->v) {
  //     printf("s->v (%lf) > s->next->v (%lf). ABORT\n", s->v, s->next->v);
  //     abort();
  //   }
  // }
}

void QuantStream::Compress() {
  if (samplesLength < 2) {
    return;
  }

  // printf("COMPRESS\n");

  Sample *cur = lastSample->prev;
  double r = n - 1 - lastSample->g;
  size_t ii = 0;

  while (cur) {
    Sample *nextCur = cur->prev;
    Sample *parent = cur->next;
    double gCur = cur->g;
    if (cur->g + parent->g + parent->d <= GetMaxVal(r)) {
      parent->g += gCur;
      RemoveSample(cur);
    }
    r -= gCur;
    cur = nextCur;
    ii++;
  }
}

void QuantStream::Insert(double val) {
  assert(bufferLength != bufferCap);
  buffer[bufferLength] = val;
  if (++bufferLength == bufferCap) {
    Flush();
    Compress();
  }
}

double QuantStream::Query(double q) {
  if (bufferLength) {
    Flush();
  }

  double t = ceil(q * n);
  t += ceil(GetMaxVal(t) / 2.0);
  const Sample *prev = firstSample;
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

QuantStream::QuantStream(const double *quantiles_, size_t numQuantiles_, size_t bufferLength_)
  : buffer{static_cast<double *>(rm_malloc(bufferLength_ * sizeof *buffer))}
  , bufferLength{bufferLength_}
  , bufferCap{bufferLength_}
  , numQuantiles{numQuantiles_}
{
  if (numQuantiles) {
    quantiles = static_cast<double *>(rm_calloc(numQuantiles, sizeof *quantiles));
    memcpy(quantiles, quantiles_, sizeof *quantiles * numQuantiles);
  }
}

QuantStream::~QuantStream() {
  rm_free(quantiles);
  rm_free(buffer);

  // Chain freeing the pools!

  Sample *cur = firstSample;
  while (cur) {
    Sample *next = cur->next;
    rm_free(cur);
    cur = next;
  }

  cur = pool;
  while (cur) {
    Sample *next = cur->next;
    rm_free(cur);
    cur = next;
  }
}

void QuantStream::Dump(FILE *fp) const {
  size_t ii = 0;
  for (Sample *cur = firstSample; cur; cur = cur->next, ++ii) {
    fprintf(fp, "[%lu]: Value: %lf. Width: %lf. Delta: %lf\n", ii, cur->v, cur->g, cur->d);
  }
  fprintf(fp, "N=%lu\n", n);
  fprintf(fp, "NumSamples: %lu\n", samplesLength);
}

size_t QuantStream::GetCount() const {
  return n;
}
