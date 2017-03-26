#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "../tests/time_sample.h"
#include "../buffer.h"
#include "../rmalloc.h"

size_t qint_encode(BufferWriter *bw, uint32_t arr[], int len) {
  if (len <= 0 || len > 4) return 0;
  
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  //printf("pos: %d\n", pos);
  size_t ret = Buffer_Write(bw, "\0", 1);
  
  for (int i = 0; i < len; i++) {
    int n = 0;
    do {
      ret+=Buffer_Write(bw, (char *)&arr[i], 1);
      n++;
      arr[i] = arr[i] >> 8;
    } while (arr[i] && n < 4);
    leading |= (((n-1) & 0x03) << i * 2);
  }

  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

size_t __qint_encode(char *leading, BufferWriter *bw, uint32_t i, int offset) {
  size_t ret = 0;
  int n = 0;
  do {
    ret += Buffer_Write(bw, (unsigned char *)&i, 1);
    n++;
    i = i >> 8;
  }while(i && n < 4);
  *leading |= ((n-1) & 0x03)<<(offset*2);
  return ret;
}

size_t qint_encode1(BufferWriter *bw, uint32_t i) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i, 0);
  Buffer_WriteAt(bw, pos, &leading, 1);
  return ret;
}

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

size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4) {
  size_t ret = 1;
  char leading = 0;
  size_t pos = Buffer_Offset(bw->buf);
  Buffer_Write(bw, "\0", 1);
  ret += __qint_encode(&leading, bw, i1, 0);
  ret += __qint_encode(&leading, bw, i2, 1);
  ret += __qint_encode(&leading, bw, i3, 2);
  ret += __qint_encode(&leading, bw, i4, 3);
  //printf("sz : %zd\n", ret);
  Buffer_WriteAt(bw, pos, &leading, 1);

  return ret;
}


typedef struct  {
  int offset;
  uint32_t mask;
}qintField;

typedef struct {
  size_t size;
  qintField fields[4];
} qintConfig;


qintConfig configs[256] = {
{.fields = {{1, 0xff},{2, 0xff},{3, 0xff},{4, 0xff},}, .size = 5 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xff},{5, 0xff},}, .size = 6 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xff},{5, 0xff},}, .size = 6 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffff},{5, 0xff},}, .size = 6 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffff},{6, 0xff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffff},{12, 0xff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffffff},{7, 0xff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffffff},{8, 0xff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffffff},{9, 0xff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffffff},{12, 0xff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffffff},{10, 0xff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffffff},{11, 0xff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffffff},{12, 0xff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffffff},{13, 0xff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xff},{4, 0xffff},}, .size = 6 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xff},{5, 0xffff},}, .size = 7 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xff},{5, 0xffff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffff},{5, 0xffff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffff},{6, 0xffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffff},{12, 0xffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffffff},{7, 0xffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffffff},{8, 0xffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffffff},{9, 0xffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffffff},{12, 0xffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffffff},{10, 0xffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffffff},{11, 0xffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffffff},{12, 0xffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffffff},{13, 0xffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xff},{4, 0xffffff},}, .size = 7 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xff},{5, 0xffffff},}, .size = 8 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xff},{5, 0xffffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffff},{5, 0xffffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffff},{6, 0xffffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffff},{12, 0xffffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffffff},{7, 0xffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffffff},{8, 0xffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffffff},{9, 0xffffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffffff},{12, 0xffffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffffff},{10, 0xffffff},}, .size = 13 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffffff},{11, 0xffffff},}, .size = 14 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffffff},{12, 0xffffff},}, .size = 15 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffffff},{13, 0xffffff},}, .size = 16 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xff},{4, 0xffffffff},}, .size = 8 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xff},{5, 0xffffffff},}, .size = 9 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xff},{5, 0xffffffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffff},{5, 0xffffffff},}, .size = 9 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffff},{6, 0xffffffff},}, .size = 10 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffff},{12, 0xffffffff},}, .size = 16 },
{.fields = {{1, 0xff},{2, 0xff},{3, 0xffffffff},{7, 0xffffffff},}, .size = 11 },
{.fields = {{1, 0xffff},{3, 0xff},{4, 0xffffffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffffff},{4, 0xff},{5, 0xffffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffffff},{5, 0xff},{6, 0xffffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xff},{2, 0xffff},{4, 0xffffffff},{8, 0xffffffff},}, .size = 12 },
{.fields = {{1, 0xffff},{3, 0xffff},{5, 0xffffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffffff},{4, 0xffff},{6, 0xffffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffffffff},{5, 0xffff},{7, 0xffffffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xff},{2, 0xffffff},{5, 0xffffffff},{9, 0xffffffff},}, .size = 13 },
{.fields = {{1, 0xffff},{3, 0xffffff},{6, 0xffffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffffff},{4, 0xffffff},{7, 0xffffffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xffffffff},{5, 0xffffff},{8, 0xffffffff},{12, 0xffffffff},}, .size = 16 },
{.fields = {{1, 0xff},{2, 0xffffffff},{6, 0xffffffff},{10, 0xffffffff},}, .size = 14 },
{.fields = {{1, 0xffff},{3, 0xffffffff},{7, 0xffffffff},{11, 0xffffffff},}, .size = 15 },
{.fields = {{1, 0xffffff},{4, 0xffffffff},{8, 0xffffffff},{12, 0xffffffff},}, .size = 16 },
{.fields = {{1, 0xffffffff},{5, 0xffffffff},{9, 0xffffffff},{13, 0xffffffff},}, .size = 17 },
};


void qint_decode(BufferReader *br, uint32_t *arr, int len) {
  qintConfig *qc = &configs[*(uint8_t*)br->pos];
  //printf("qc %02x: size %zd\n",*(uint8_t*)br->pos, qc->size );
  for (int i = 0; i < len; i++) {
    arr[i] = *(uint32_t*)(br->pos + qc->fields[i].offset) & qc->fields[i].mask;
  }
  Buffer_Skip(br, qc->size);
}


#define qint_member(p, i) (*(uint32_t*)(p+configs[*(uint8_t*)p].fields[i].offset) & configs[*(uint8_t*)p].fields[i].mask)

 void qint_decode1(BufferReader *br, uint32_t *i) {
  *i = qint_member(br->pos, 0);
  Buffer_Skip(br, configs[*(uint8_t*)br->pos].size);
}

 void qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2) {
  *i = qint_member(br->pos, 0);
  *i2 = qint_member(br->pos, 1);
  Buffer_Skip(br, configs[*(uint8_t*)br->pos].size);
}

 void qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3) {
  *i = qint_member(br->pos, 0);
  *i2 = qint_member(br->pos, 1);
  *i3 = qint_member(br->pos, 2);
  Buffer_Skip(br, configs[*(uint8_t*)br->pos].size);
}


 void qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3, uint32_t *i4) {
  
  *i = qint_member(br->pos, 0);
  *i2 = qint_member(br->pos, 1);
  *i3 = qint_member(br->pos, 2);
  *i4 = qint_member(br->pos, 3);
  Buffer_Skip(br, configs[*(uint8_t*)br->pos].size);
}


void printConfig(unsigned char c) {

  int off = 1;
  int masks[4] = {0xff, 0xffff, 0xffffff, 0xffffffff};

  printf("{.fields = {");
  
  
  for (int i = 0; i < 8; i+=2) {
    printf("{%d, 0x%x},", off, masks[(c >> i)&0x03]);
    off += ((c>>i)&0x03) + 1;
  }
  
  printf("}, .size = %d }, \n", off);
}

#ifdef QINT_MAIN
int main(int argc, char **argv){
  
  RM_PatchAlloc();
  
  // for (uint16_t i = 0; i < 256; i++) {
  //   printConfig(i);
  // }
  // return 0;
  Buffer *b = NewBuffer(1024);
  BufferWriter bw = NewBufferWriter(b);
  size_t sz = 0;
  int x = 0;
  int N = 10;
  while (x < N) {
      uint32_t arr[4] = {1000, 100, 300,4 };

    sz += qint_encode(&bw, arr, 4);
     //printf("sz: %zd, x: %d\n",sz,  x);

    x++;
  }

  
  unsigned char *buf = b->data;
  qintConfig config = configs[*(uint8_t*)buf];
  uint64_t total = 0;
  TimeSample ts;
  
  TimeSampler_Start(&ts);
  BufferReader br = NewBufferReader(b);
  uint32_t i1, i2, i3, i4;
    for (int i = 0; i < N; i++) {
        qint_decode4(&br, &i1, &i2, &i3, &i4);
        printf("Decoded: %d, %d, %d, %d\n", i1, i2, i3, i4);
        total += i1;

      
      TimeSampler_Tick(&ts);
    }
  TimeSampler_End(&ts);
  printf("Total: %zdms for %d iters, %fns/iter", TimeSampler_DurationMS(&ts), ts.num, (double)TimeSampler_DurationNS(&ts)/(double)ts.num);
  printf("%d\n", total);
  //   //printf("%d %x\n",config.fields[i].offset, config.fields[i].mask);
  //   printf("%d\n", );
  // }
  return 0;
}

#endif