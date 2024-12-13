/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __QINT_H__
#define __QINT_H__

#include <stdint.h>
#include <stdlib.h>
#include "buffer.h"

#ifndef QINT_API
#define QINT_API
#endif

/* QInt - fast encoding and decoding of up to 4 unsigned 32 bit integers  as variable width
 * integers. The algorithm uses a leading byte to encode the size of each integer in bits, and has a
 * table for the actual offsets of each integer, per possible leading byte */

/* Encode two integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode2(BufferWriter *bw, uint32_t i1, uint32_t i2);

/* Encode three integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode3(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3);

/* Encode 4 integers with one leading byte. return the size of the encoded data written */
QINT_API size_t qint_encode4(BufferWriter *bw, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4);

/* Decode two unsigned integers from the buffer. This can only be used if the record has been
 * encoded
 * with encode2 */
QINT_API size_t qint_decode2(BufferReader *br, uint32_t *i, uint32_t *i2);

/* Decode 3 unsigned integer from the buffer. This can only be used if the record has been encoded
 * with encode3 */
QINT_API size_t qint_decode3(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3);

/* Decode 4 unsigned integer from the buffer. This can only be used if the record has been encoded
 * with encode4 or encoded array of len 4 */
QINT_API size_t qint_decode4(BufferReader *br, uint32_t *i, uint32_t *i2, uint32_t *i3,
                             uint32_t *i4);

#endif
