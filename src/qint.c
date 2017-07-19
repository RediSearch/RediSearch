#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "buffer.h"
#include "rmalloc.h"
#include "qint.h"

QINT_API size_t qint_encode(BufferWriter *bw, uint32_t arr[], int len) {
  if (len <= 0 || len > 4) return 0;

  char leading = 0;
  // save the current buffer possition
  size_t pos = Buffer_Offset(bw->buf);

  // write a zero for the leading byte
  size_t ret = Buffer_Write(bw, "\0", 1);

  // encode the integers one by one
  for (int i = 0; i < len; i++) {
    int n = 0;
    do {
      // write one byte into the buffer and advance the byte count
      ret += Buffer_Write(bw, (char *)&arr[i], 1);

      n++;
      // shift right until we have no more bigger bytes that are non zero
      arr[i] = arr[i] >> 8;
    } while (arr[i] && n < 4);
    // encode the bit length of our integer into the leading byte.
    // 0 means 1 byte, 1 - 2 bytes, 2 - 3 bytes, 3 - bytes.
    // we encode it at the i*2th place in the leading byte
    leading |= (((n - 1) & 0x03) << i * 2);
  }

  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

static size_t __qint_encode(char *leading, BufferWriter *bw, uint32_t i, int offset);

/* internal function to encode just one number out of a larger array at a given offset (<=4) */
inline size_t __qint_encode(char *leading, BufferWriter *bw, uint32_t i, int offset) {
  size_t ret = 0;
  // byte size counter
  int n = 0;
  do {
    // write one byte into the buffer and advance the byte count
    ret += Buffer_Write(bw, (unsigned char *)&i, 1);
    n++;
    // shift right until we have no more bigger bytes that are non zero
    i = i >> 8;
  } while (i && n < 4);
  // encode the bit length of our integer into the leading byte.
  // 0 means 1 byte, 1 - 2 bytes, 2 - 3 bytes, 3 - bytes.
  // we encode it at the i*2th place in the leading byte
  *leading |= ((n - 1) & 0x03) << (offset * 2);
  return ret;
}

/* Encode one number ... */
QINT_API size_t qint_encode1(BufferWriter *bw, uint32_t i) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i, 0);
  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode two integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode three integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += __qint_encode(&leading, bw, i3, 2);
  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode 4 integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += __qint_encode(&leading, bw, i3, 2);
  ret += __qint_encode(&leading, bw, i4, 3);
  Buffer_WriteAt(bw, pos, &leading, 1);

  return ret;
}

#define QINT_DECODE_VALUE(lval, bits, ptr, nused) \
  do {                                            \
    switch (bits) {                               \
      case 2:                                     \
        lval = *(uint32_t *)ptr & 0xFFFFFF;       \
        nused = 3;                                \
        break;                                    \
      case 0:                                     \
        lval = *ptr;                              \
        nused = 1;                                \
        break;                                    \
      case 1:                                     \
        lval = *(uint32_t *)ptr & 0xFFFF;         \
        nused = 2;                                \
        break;                                    \
      default:                                    \
        lval = *(uint32_t *)ptr;                  \
        nused = 4;                                \
        break;                                    \
    }                                             \
  } while (0)

/* Decode up to 4 integers into an array. Returns the amount of data consumed or 0 if len invalid
 */
size_t qint_decode(BufferReader *__restrict__ br, uint32_t *__restrict__ arr, int len) {
  const uint8_t *start = (uint8_t *)BufferReader_Current(br);
  const uint8_t *p = start;
  uint8_t header = *p;
  p++;

  for (int i = 0; i < len; i++) {
    const uint8_t val = (header >> (i * 2)) & 0x03;
    size_t nused;

    QINT_DECODE_VALUE(arr[i], val, p, nused);
    p += nused;
  }

  size_t nread = p - start;
  Buffer_Skip(br, nread);
  return nread;
}

#define QINT_DECODE_MULTI(lval, pos, p, total, tmp)                            \
  do {                                                                         \
    QINT_DECODE_VALUE(lval, ((*p >> (pos * 2)) & 0x03), (p + total + 1), tmp); \
    total += tmp;                                                              \
  } while (0)

QINT_API size_t qint_decode1(BufferReader *br, uint32_t *i) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0, tmp = 0;
  QINT_DECODE_MULTI(*i, 0, p, total, tmp);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

QINT_API size_t qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0, tmp = 0;
  QINT_DECODE_MULTI(*i, 0, p, total, tmp);
  QINT_DECODE_MULTI(*i2, 1, p, total, tmp);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

QINT_API size_t qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0, tmp = 0;
  QINT_DECODE_MULTI(*i, 0, p, total, tmp);
  QINT_DECODE_MULTI(*i2, 1, p, total, tmp);
  QINT_DECODE_MULTI(*i3, 2, p, total, tmp);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

QINT_API size_t qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3,
                             uint32_t *i4) {
  const uint8_t *p = (uint8_t *)BufferReader_Current(br);
  size_t total = 0, tmp = 0;
  QINT_DECODE_MULTI(*i, 0, p, total, tmp);
  QINT_DECODE_MULTI(*i2, 1, p, total, tmp);
  QINT_DECODE_MULTI(*i3, 2, p, total, tmp);
  QINT_DECODE_MULTI(*i4, 3, p, total, tmp);
  Buffer_Skip(br, total + 1);
  return total + 1;
}

// void printConfig(unsigned char c) {

//   int off = 1;
//   int headerMasks[4] = {0xff, 0xffff, 0xffffff, 0xffffffff};

//   printf("{.fields = {");

//   for (int i = 0; i < 8; i += 2) {
//     printf("{%d, 0x%x},", off, headerMasks[(c >> i) & 0x03]);
//     off += ((c >> i) & 0x03) + 1;
//   }

//   printf("}, .size = %d }, \n", off);
// }

// #ifdef QINT_MAIN
// int main(int argc, char **argv) {

//   RM_PatchAlloc();

//   // for (uint16_t i = 0; i < 256; i++) {
//   //   printConfig(i);
//   // }
//   // return 0;
//   Buffer *b = NewBuffer(1024);
//   BufferWriter bw = NewBufferWriter(b);
//   size_t sz = 0;
//   int x = 0;
//   int N = 10;
//   while (x < N) {
//     uint32_t arr[4] = {1000, 100, 300, 4};

//     sz += qint_encode(&bw, arr, 4);
//     // printf("sz: %zd, x: %d\n",sz,  x);

//     x++;
//   }

//   unsigned char *buf = b->data;
//   qintConfig config = configs[*(uint8_t *)buf];
//   uint64_t total = 0;
//   TimeSample ts;

//   TimeSampler_Start(&ts);
//   BufferReader br = NewBufferReader(b);
//   uint32_t i1, i2, i3, i4;
//   for (int i = 0; i < N; i++) {
//     qint_decode4(&br, &i1, &i2, &i3, &i4);
//     printf("Decoded: %d, %d, %d, %d\n", i1, i2, i3, i4);
//     total += i1;

//     TimeSampler_Tick(&ts);
//   }
//   TimeSampler_End(&ts);
//   printf("Total: %zdms for %d iters, %fns/iter", TimeSampler_DurationMS(&ts), ts.num,
//          (double)TimeSampler_DurationNS(&ts) / (double)ts.num);
//   printf("%d\n", total);
//   //   //printf("%d %x\n",config.fields[i].offset, config.fields[i].headerMask);
//   //   printf("%d\n", );
//   // }
//   return 0;
// }
//#endif