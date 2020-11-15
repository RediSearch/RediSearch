#include "cndict_loader.h"
#include "buffer.h"

#include "dep/miniz/miniz.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <arpa/inet.h>  // htonl, etc.
#include <stdint.h>

///////////////////////////////////////////////////////////////////////////////////////////////

extern const char ChineseDict[];
extern const size_t ChineseDictCompressedLength;
extern const size_t ChineseDictFullLength;

enum RecordFlags {
  Record_HasSynonyms = 0x01 << 5,
  Record_HasFrequency = 0x02 << 5
};

#define LEXTYPE_MASK 0x1F

//---------------------------------------------------------------------------------------------

struct ReaderCtx {
  friso_dic_t dic;
  BufferReader *rdr;
};

//---------------------------------------------------------------------------------------------

static int readRecord(ReaderCtx *ctx) {
  BufferReader *rdr = ctx->rdr;
  // Read the flags
  char c;
  size_t nr = rdr.ReadByte(&c);
  if (!nr) {
    return 0;
  }

  uint32_t lexType = c & LEXTYPE_MASK;

  // Determine term length...
  const char *term = rdr->buf->data + rdr->pos;
  size_t termLen = strlen(term);

  rdr->pos += termLen + 1;
  uint16_t numSyns = 0;

  if (c & Record_HasSynonyms) {
    rdr.Read(&numSyns, 2);
    numSyns = htons(numSyns);
  }

  friso_array_t syns = NULL;

  if (numSyns) {
    syns = new_array_list_with_opacity(numSyns);

    // Read the synonyms
    for (size_t ii = 0; ii < numSyns; ++ii) {
      const char *curSyn = rdr->buf->data + rdr->pos;

      size_t synLen = strlen(curSyn);
      rdr->pos += synLen + 1;
      // Store the synonym somewhere?
      array_list_add(syns, rm_strdup(curSyn));
    }
  }

  // If there's a frequency, read that too.
  uint32_t freq = 0;
  if (c & Record_HasFrequency) {
    rdr.Read(&freq, 4);
    freq = htonl(freq);
  }

  // printf("Adding record TYPE: %u. TERM: %s. NSYNS: %u\n", lexType, term, numSyns);
  friso_dic_add_with_fre(ctx->dic, lexType, rm_strdup(term), syns, freq);
  return 1;
}

//---------------------------------------------------------------------------------------------

// Read the format
int ChineseDictLoad(friso_dic_t d) {
  // Before doing anything, verify the version:
  uint32_t version;
  const char *inbuf = ChineseDict;
  version = htonl(*(uint32_t *)inbuf);
  inbuf += 4;
  RS_LOG_ASSERT(version == 0, "Chinese dictionary version should be 0");

  // First load the symbol..
  char *expanded = rm_malloc(ChineseDictFullLength);
  mz_ulong dstLen = ChineseDictFullLength;
  int rv = mz_uncompress((unsigned char *)expanded, &dstLen, (const unsigned char *)inbuf,
                         ChineseDictCompressedLength);
  if (rv != MZ_OK) {
    printf("Failed to decompress: %s. Full Len=%lu. DestLen=%lu\n", mz_error(rv), dstLen,
           ChineseDictCompressedLength);
    printf("SrcLen|DstLen: 0%lx\n", dstLen | ChineseDictCompressedLength);
    abort();
  }

  // Now, let's see if we can read the records...
  Buffer tmpBuf;
  tmpBuf.data = expanded;
  tmpBuf.cap = dstLen;
  tmpBuf.offset = 0;
  BufferReader reader(&tmpBuf);

  ReaderCtx ctx = {.dic = d, .rdr = &reader};
  while (reader.pos < tmpBuf.cap && readRecord(&ctx)) {
    // Do nothing
  }

  rm_free(expanded);
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////
