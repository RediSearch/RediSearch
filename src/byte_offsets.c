/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "byte_offsets.h"

#include "types_ffi.h"
#include "redismodule.h"
#include "rmalloc.h"

RSByteOffsets *NewByteOffsets() {
  RSByteOffsets *ret = rm_calloc(1, sizeof(*ret));
  return ret;
}

void RSByteOffsets_Free(RSByteOffsets *offsets) {
  RSOffsetVector_FreeData(&offsets->offsets);
  rm_free(offsets->fields);
  rm_free(offsets);
}

void RSByteOffsets_ReserveFields(RSByteOffsets *offsets, size_t numFields) {
  offsets->fields = rm_realloc(offsets->fields, sizeof(*offsets->fields) * numFields);
}

RSByteOffsetField *RSByteOffsets_AddField(RSByteOffsets *offsets, uint32_t fieldId,
                                          uint32_t startPos) {
  RSByteOffsetField *field = &(offsets->fields[offsets->numFields++]);
  field->fieldId = fieldId;
  field->firstTokPos = startPos;
  return field;
}

void ByteOffsetWriter_Move(ByteOffsetWriter *w, RSByteOffsets *offsets) {
  size_t len;
  char *data = (char *) VVW_TakeByteData(w->vw, &len);
  RSOffsetVector_SetData(&offsets->offsets, data, len);
}

void RSByteOffsets_Serialize(const RSByteOffsets *offsets, Buffer *b) {
  BufferWriter w = NewBufferWriter(b);

  Buffer_WriteU8(&w, offsets->numFields);

  for (size_t ii = 0; ii < offsets->numFields; ++ii) {
    Buffer_WriteU8(&w, offsets->fields[ii].fieldId);
    Buffer_WriteU32(&w, offsets->fields[ii].firstTokPos);
    Buffer_WriteU32(&w, offsets->fields[ii].lastTokPos);
  }

  Buffer_WriteU32(&w, RSOffsetVector_Len(&offsets->offsets));

  uint32_t offsets_len;
  const char *offsets_data = RSOffsetVector_GetData(&offsets->offsets, &offsets_len);
  Buffer_Write(&w, offsets_data, offsets_len);
}

RSByteOffsets *LoadByteOffsets(Buffer *buf) {
  // Buffer_Read does not check the buffer's bounds, so every read is checked against what
  // RSByteOffsets_Serialize wrote: a u8 field count, a (u8 id, u32 first, u32 last) record per
  // field, a u32 data length, and the data itself.
  const size_t fieldRecordSize = sizeof(uint8_t) + 2 * sizeof(uint32_t);
  BufferReader r = NewBufferReader(buf);

  if (BufferReader_Remaining(&r) < sizeof(uint8_t)) {
    return NULL;
  }
  uint8_t numFields = Buffer_ReadU8(&r);
  if (BufferReader_Remaining(&r) < numFields * fieldRecordSize + sizeof(uint32_t)) {
    return NULL;
  }

  RSByteOffsets *offsets = NewByteOffsets();
  RSByteOffsets_ReserveFields(offsets, numFields);

  for (size_t ii = 0; ii < numFields; ++ii) {
    uint8_t fieldId = Buffer_ReadU8(&r);
    uint32_t firstTok = Buffer_ReadU32(&r);
    uint32_t lastTok = Buffer_ReadU32(&r);
    RSByteOffsetField *fieldInfo = RSByteOffsets_AddField(offsets, fieldId, firstTok);
    fieldInfo->lastTokPos = lastTok;
  }

  uint32_t offsetsLen = Buffer_ReadU32(&r);
  if (BufferReader_Remaining(&r) < offsetsLen) {
    RSByteOffsets_Free(offsets);
    return NULL;
  }
  if (offsetsLen) {
    char *data = rm_malloc(offsetsLen);
    Buffer_Read(&r, data, offsetsLen);
    RSOffsetVector_SetData(&offsets->offsets, data, offsetsLen);
  } else {
    RSOffsetVector_SetData(&offsets->offsets, NULL, 0);
  }

  return offsets;
}

int RSByteOffset_Iterate(const RSByteOffsets *offsets, uint32_t fieldId,
                         RSByteOffsetIterator *iter) {
  const RSByteOffsetField *offField = NULL;
  for (size_t ii = 0; ii < offsets->numFields; ++ii) {
    if (offsets->fields[ii].fieldId == fieldId) {
      offField = offsets->fields + ii;
      break;
    }
  }
  if (!offField) {
    return REDISMODULE_ERR;
  }

  uint32_t offsets_len;
  const char *offsets_data = RSOffsetVector_GetData(&offsets->offsets, &offsets_len);

  iter->buf.cap = 0;
  iter->buf.data = (char *) offsets_data;
  iter->buf.offset = offsets_len;
  iter->rdr = NewBufferReader(&iter->buf);
  iter->curPos = 1;
  iter->endPos = offField->lastTokPos;

  iter->lastValue = 0;

  while (iter->curPos < offField->firstTokPos && !BufferReader_AtEnd(&iter->rdr)) {
    iter->lastValue = ReadVarint(&iter->rdr) + iter->lastValue;
    iter->curPos++;
  }

  iter->curPos--;
  return REDISMODULE_OK;
}

uint32_t RSByteOffsetIterator_Next(RSByteOffsetIterator *iter) {
  if (BufferReader_AtEnd(&iter->rdr) || ++iter->curPos > iter->endPos) {
    return RSBYTEOFFSET_EOF;
  }

  iter->lastValue = ReadVarint(&iter->rdr) + iter->lastValue;
  return iter->lastValue;
}
