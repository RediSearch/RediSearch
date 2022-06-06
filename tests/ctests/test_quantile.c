#include "../util/quantile.h"
#include "../buffer.h"
#include "../rmutil/alloc.h"
#include "test_util.h"
#include <stdint.h>
#include <assert.h>
#include <stdio.h>

static FILE *fp;
static Buffer buf;
static double *input;
static size_t numInput;

static int testBasic() {
  double quantiles[] = {0.50, 0.90, 0.99};
  QuantStream *stream = new QuantileStream(quantiles, 3, 500);
  for (size_t ii = 0; ii < numInput; ++ii) {
    stream->Insert(input[ii]);
  }
  double res50 = stream->Query(0.50);
  double res90 = stream->Query(0.90);
  double res99 = stream->Query(0.99);
  size_t count = stream->GetCount();
  printf("50: %lf, 90: %lf, 99: %lf\n", res50, res90, res99);
  printf("Count: %lu\n", count);
  //   stream->Dump(stdout);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();

  fp = fopen("./quantile_data.txt", "rb");
  assert(fp);
  Buffer_Init(&buf, 4096);
  BufferWriter bw = NewBufferWriter(&buf);

  double d;
  while (fscanf(fp, "%lf", &d) != EOF) {
    Buffer_Write(&bw, &d, sizeof d);
    numInput++;
  }
  fclose(fp);
  printf("Have %lu items\n", numInput);
  input = (double *)buf.data;

  TESTFUNC(testBasic);

  Buffer_Free(&buf);
})
