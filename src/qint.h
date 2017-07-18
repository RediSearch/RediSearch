#ifndef __QINT_H__
#define __QINT_H__

#include <stdint.h>
#include <stdlib.h>
#include "buffer.h"

#ifndef QINT_API
#define QINT_API
#endif

/* QInt - fast encoding and decoding of up to 4 unsinged 32 bit integers  as variable width
 * integers. The algorithm uses a leading byte to encode the size of each integer in bits, and has a
 * table for the actual offsets of each integer, per possible leading byte */

/* Encode an array of up to 4 unsinged integers into a buffer */
QINT_API size_t qint_encode(BufferWriter *bw, uint32_t arr[], int len);

/* Encode one integer. with one leading byte. This is inefficent and if you find yourself needing
 * this, you should probably opt for normal varint */
QINT_API size_t qint_encode1(BufferWriter *bw, uint32_t i);

/* Encode two integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2);

/* Encode three integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3);

/* Encode 4 integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4);

/* Decode up to 4 integers into an array. Returns the amount of data consumed or 0 if len invalid */
QINT_API size_t qint_decode(BufferReader *__restrict__ br, uint32_t *__restrict__ arr, int len);

/* Decode one unsinged integer from the buffer. This can only be used if the record has been encoded
 * with encode1 */
QINT_API size_t qint_decode1(BufferReader *br, uint32_t *i);
/* Decode two unsinged integers from the buffer. This can only be used if the record has been
 * encoded
 * with encode2 */
QINT_API size_t qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2);

/* Decode 3 unsinged integer from the buffer. This can only be used if the record has been encoded
 * with encode3 */
QINT_API size_t qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3);

/* Decode 4 unsinged integer from the buffer. This can only be used if the record has been encoded
 * with encode4 or encoded array of len 4 */
QINT_API size_t qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3,
                             uint32_t *i4);

#endif