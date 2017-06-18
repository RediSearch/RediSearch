#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "buffer.h"
#include "rmalloc.h"
#include "qint.h"

size_t qint_encode(BufferWriter *bw, uint32_t arr[], int len) {
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

size_t __qint_encode(char *leading, BufferWriter *bw, uint32_t i, int offset);

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
size_t qint_encode1(BufferWriter *bw, uint32_t i) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i, 0);
  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

/* Encode two integers with one leading byte. return the size of the encoded data written */
size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2) {
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
size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3) {
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
size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4) {
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

/* field configuration for the qint table */
typedef struct {
  int offset;
  uint32_t mask;
} qintField;

/* qint configuration per leading byte - including the size of the encoded block, and the offsets of
 * all fields */
typedef struct {
  size_t size;
  qintField fields[4];
} qintConfig;

/* configuration table - per leading byte value, we save the offsets and masks of all fields for
 * immediate access to them */
qintConfig
    configs[256] =
        {
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xff}, {4, 0xff},
                 },
             .size = 5},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xff}, {5, 0xff},
                 },
             .size = 6},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xff}, {5, 0xff},
                 },
             .size = 6},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffff}, {5, 0xff},
                 },
             .size = 6},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffff}, {6, 0xff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffff}, {12, 0xff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffffff}, {7, 0xff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffffff}, {8, 0xff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffffff}, {9, 0xff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffffff}, {12, 0xff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffffff}, {10, 0xff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffffff}, {11, 0xff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffffff}, {12, 0xff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffffff}, {13, 0xff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xff}, {4, 0xffff},
                 },
             .size = 6},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xff}, {5, 0xffff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xff}, {5, 0xffff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffff}, {5, 0xffff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffff}, {6, 0xffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffff}, {12, 0xffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffffff}, {7, 0xffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffffff}, {8, 0xffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffffff}, {9, 0xffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffffff}, {12, 0xffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffffff}, {10, 0xffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffffff}, {11, 0xffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffffff}, {12, 0xffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffffff}, {13, 0xffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xff}, {4, 0xffffff},
                 },
             .size = 7},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xff}, {5, 0xffffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xff}, {5, 0xffffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffff}, {5, 0xffffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffff}, {6, 0xffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffff}, {12, 0xffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffffff}, {7, 0xffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffffff}, {8, 0xffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffffff}, {9, 0xffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffffff}, {12, 0xffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffffff}, {10, 0xffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffffff}, {11, 0xffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffffff}, {12, 0xffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffffff}, {13, 0xffffff},
                 },
             .size = 16},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xff}, {4, 0xffffffff},
                 },
             .size = 8},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xff}, {5, 0xffffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xff}, {5, 0xffffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffff}, {5, 0xffffffff},
                 },
             .size = 9},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffff}, {6, 0xffffffff},
                 },
             .size = 10},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffff}, {12, 0xffffffff},
                 },
             .size = 16},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xff}, {3, 0xffffffff}, {7, 0xffffffff},
                 },
             .size = 11},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xff}, {4, 0xffffffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xff}, {5, 0xffffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xff}, {6, 0xffffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffff}, {4, 0xffffffff}, {8, 0xffffffff},
                 },
             .size = 12},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffff}, {5, 0xffffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffff}, {6, 0xffffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffff}, {7, 0xffffffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffff}, {5, 0xffffffff}, {9, 0xffffffff},
                 },
             .size = 13},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffff}, {6, 0xffffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffff}, {7, 0xffffffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffff}, {8, 0xffffffff}, {12, 0xffffffff},
                 },
             .size = 16},
            {.fields =
                 {
                     {1, 0xff}, {2, 0xffffffff}, {6, 0xffffffff}, {10, 0xffffffff},
                 },
             .size = 14},
            {.fields =
                 {
                     {1, 0xffff}, {3, 0xffffffff}, {7, 0xffffffff}, {11, 0xffffffff},
                 },
             .size = 15},
            {.fields =
                 {
                     {1, 0xffffff}, {4, 0xffffffff}, {8, 0xffffffff}, {12, 0xffffffff},
                 },
             .size = 16},
            {.fields =
                 {
                     {1, 0xffffffff}, {5, 0xffffffff}, {9, 0xffffffff}, {13, 0xffffffff},
                 },
             .size = 17},
};

/* Decode up to 4 integers into an array. Returns the amount of data consumed or 0 if len invalid */
size_t qint_decode(BufferReader *br, uint32_t *arr, int len) {
  qintConfig *qc = &configs[*(uint8_t *)BufferReader_Current(br)];
  // printf("qc %02x: size %zd\n",*(uint8_t*)BufferReader_Current(br), qc->size );
  for (int i = 0; i < len; i++) {
    arr[i] = *(uint32_t *)(BufferReader_Current(br) + qc->fields[i].offset) & qc->fields[i].mask;
  }
  Buffer_Skip(br, qc->size);
  return qc->size;
}

#define qint_member(p, i)                                       \
  (*(uint32_t *)(p + configs[*(uint8_t *)p].fields[i].offset) & \
   configs[*(uint8_t *)p].fields[i].mask)
#define qint_memberx(p, c, i) (*(uint32_t *)(p + c.fields[i].offset) & c.fields[i].mask)

size_t qint_decode1(BufferReader *br, uint32_t *i) {
  *i = qint_member(BufferReader_Current(br), 0);
  size_t offset = configs[*(uint8_t *)BufferReader_Current(br)].fields[1].offset;
  Buffer_Skip(br, offset);
  return offset;
}

size_t qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2) {
  *i = qint_member(BufferReader_Current(br), 0);
  *i2 = qint_member(BufferReader_Current(br), 1);
  size_t offset = configs[*(uint8_t *)BufferReader_Current(br)].fields[2].offset;
  Buffer_Skip(br, offset);
  return offset;
}

size_t qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3) {
  *i = qint_member(BufferReader_Current(br), 0);
  *i2 = qint_member(BufferReader_Current(br), 1);
  *i3 = qint_member(BufferReader_Current(br), 2);
  size_t offset = configs[*(uint8_t *)BufferReader_Current(br)].fields[3].offset;
  Buffer_Skip(br, offset);
  return offset;
}

size_t qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3, uint32_t *i4) {
  uint8_t pos = *(uint8_t *)BufferReader_Current(br);
  *i = qint_memberx(BufferReader_Current(br), configs[pos], 0);
  *i2 = qint_memberx(BufferReader_Current(br), configs[pos], 1);
  *i3 = qint_memberx(BufferReader_Current(br), configs[pos], 2);
  *i4 = qint_memberx(BufferReader_Current(br), configs[pos], 3);
  size_t offset = configs[pos].size;
  Buffer_Skip(br, offset);
  return offset;
}

// void printConfig(unsigned char c) {

//   int off = 1;
//   int masks[4] = {0xff, 0xffff, 0xffffff, 0xffffffff};

//   printf("{.fields = {");

//   for (int i = 0; i < 8; i += 2) {
//     printf("{%d, 0x%x},", off, masks[(c >> i) & 0x03]);
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
//   //   //printf("%d %x\n",config.fields[i].offset, config.fields[i].mask);
//   //   printf("%d\n", );
//   // }
//   return 0;
// }
//#endif