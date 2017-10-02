#include "byte_offsets.h"

RSByteOffsets *NewByteOffsets() {
  RSByteOffsets *ret = rm_calloc(1, sizeof(*ret));
  return ret;
}

void RSByteOffsets_Free(RSByteOffsets *offsets) {
  rm_free(offsets->offsets.data);
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
  offsets->offsets.data = w->buf.data;
  offsets->offsets.len = w->buf.offset;
  memset(&w->buf, 0, sizeof w->buf);
}
