#include "buffer.h"
#include "rmalloc.h"
#include "query_error.h"

#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Initialize a static buffer and fill its data
Buffer::Buffer(size_t cap, size_t offset) : cap(cap), offset(offset) {
  data = rm_malloc(cap);
}

//---------------------------------------------------------------------------------------------

Buffer::~Buffer() {
  if (data) rm_free(data);
}

//---------------------------------------------------------------------------------------------

void Buffer::Reset() {
  rm_free(data);
  data = NULL;
  cap = 0;
  offset = 0;
}
//---------------------------------------------------------------------------------------------

void Buffer::Grow(size_t extraLen) {
  do {
    cap += MIN(1 + cap / 5, 1024 * 1024);
  } while (offset + extraLen > cap);

  data = rm_realloc(data, cap);
}

//---------------------------------------------------------------------------------------------

// Truncate the buffer to newlen. If newlen is 0 - trunacte capacity

size_t Buffer::Truncate(size_t newlen) {
  if (newlen == 0) {
    newlen = Offset();
  }

  // we might have an empty buffer, in this case we set the data to NULL and free it
  if (newlen == 0) {
    Reset();
    return 0;
  }
  data = rm_realloc(data, newlen);
  cap = newlen;
  return newlen;
}

///////////////////////////////////////////////////////////////////////////////////////////////

RMBuffer::RMBuffer(RedisModuleIO *rdb) {
  data = RedisModule_LoadStringBuffer(rdb, &cap);
  if (!data) {
    throw Error("RedisModule_LoadStringBuffer failed");
  }
  offset = 0;
}

//---------------------------------------------------------------------------------------------

RMBuffer::~RMBuffer() {
  RedisModule_Free(data);
  data = NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
Consme one byte from the buffer
@return 0 if at end, 1 if consumed
*/
inline size_t BufferReader::ReadByte(char *c) {
  *c = buf->data[pos++];
  return 1;
}

//---------------------------------------------------------------------------------------------

/**
Seek to a specific offset. If offset is out of bounds we seek to the end.
@return the effective seek position
*/
size_t BufferReader::Seek(size_t where) {
  pos = MIN(where, buf->cap);
  return where;
}

///////////////////////////////////////////////////////////////////////////////////////////////

size_t BufferWriter::WriteAt(size_t offset, void *data, size_t len) {
  size_t pos = buf->offset;
  Seek(offset);

  size_t sz = Write(data, len);
  Seek(pos);
  return sz;
}

//---------------------------------------------------------------------------------------------

size_t BufferWriter::Seek(size_t offset) {
  if (offset > buf->cap) {
    return buf->offset;
  }
  pos = buf->data + offset;
  buf->offset = offset;

  return offset;
}

///////////////////////////////////////////////////////////////////////////////////////////////
