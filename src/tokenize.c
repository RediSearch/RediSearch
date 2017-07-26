#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

int tokenize(const char *text, float score, t_fieldMask fieldId, void *ctx, TokenFunc f, Stemmer *s,
             u_int offset, StopWordList *stopwords) {
  TokenizerCtx tctx;
  tctx.text = text;
  tctx.pos = (char **)&text;
  tctx.separators = DEFAULT_SEPARATORS;
  tctx.fieldScore = score;
  tctx.tokenFunc = f;
  tctx.tokenFuncCtx = ctx;
  tctx.normalize = DefaultNormalize;
  tctx.fieldId = fieldId;
  tctx.stemmer = s;
  tctx.lastOffset = offset;
  tctx.stopwords = stopwords;

  return _tokenize(&tctx);
}

static const char sepMap[256] = {
        [0x0] = 0,  [0x1] = 0,  [0x2] = 0,  [0x3] = 0,  [0x4] = 0,  [0x5] = 0,  [0x6] = 0,
        [0x7] = 0,  [0x8] = 0,  [0x9] = 1,  [0xA] = 0,  [0xB] = 0,  [0xC] = 0,  [0xD] = 0,
        [0xE] = 0,  [0xF] = 0,  [0x10] = 0, [0x11] = 0, [0x12] = 0, [0x13] = 0, [0x14] = 0,
        [0x15] = 0, [0x16] = 0, [0x17] = 0, [0x18] = 0, [0x19] = 0, [0x1A] = 0, [0x1B] = 0,
        [0x1C] = 0, [0x1D] = 0, [0x1E] = 0, [0x1F] = 0, [0x20] = 1, [0x21] = 1, [0x22] = 1,
        [0x23] = 1, [0x24] = 1, [0x25] = 1, [0x26] = 1, [0x27] = 1, [0x28] = 1, [0x29] = 1,
        [0x2A] = 1, [0x2B] = 1, [0x2C] = 1, [0x2D] = 1, [0x2E] = 1, [0x2F] = 1, [0x30] = 0,
        [0x31] = 0, [0x32] = 0, [0x33] = 0, [0x34] = 0, [0x35] = 0, [0x36] = 0, [0x37] = 0,
        [0x38] = 0, [0x39] = 0, [0x3A] = 1, [0x3B] = 1, [0x3C] = 1, [0x3D] = 1, [0x3E] = 1,
        [0x3F] = 1, [0x40] = 1, [0x41] = 0, [0x42] = 0, [0x43] = 0, [0x44] = 0, [0x45] = 0,
        [0x46] = 0, [0x47] = 0, [0x48] = 0, [0x49] = 0, [0x4A] = 0, [0x4B] = 0, [0x4C] = 0,
        [0x4D] = 0, [0x4E] = 0, [0x4F] = 0, [0x50] = 0, [0x51] = 0, [0x52] = 0, [0x53] = 0,
        [0x54] = 0, [0x55] = 0, [0x56] = 0, [0x57] = 0, [0x58] = 0, [0x59] = 0, [0x5A] = 0,
        [0x5B] = 1, [0x5C] = 1, [0x5D] = 1, [0x5E] = 1, [0x5F] = 0, [0x60] = 1, [0x61] = 0,
        [0x62] = 0, [0x63] = 0, [0x64] = 0, [0x65] = 0, [0x66] = 0, [0x67] = 0, [0x68] = 0,
        [0x69] = 0, [0x6A] = 0, [0x6B] = 0, [0x6C] = 0, [0x6D] = 0, [0x6E] = 0, [0x6F] = 0,
        [0x70] = 0, [0x71] = 0, [0x72] = 0, [0x73] = 0, [0x74] = 0, [0x75] = 0, [0x76] = 0,
        [0x77] = 0, [0x78] = 0, [0x79] = 0, [0x7A] = 0, [0x7B] = 1, [0x7C] = 1, [0x7D] = 1,
        [0x7E] = 1, [0x7F] = 0, [0x80] = 0, [0x81] = 0, [0x82] = 0, [0x83] = 0, [0x84] = 0,
        [0x85] = 0, [0x86] = 0, [0x87] = 0, [0x88] = 0, [0x89] = 0, [0x8A] = 0, [0x8B] = 0,
        [0x8C] = 0, [0x8D] = 0, [0x8E] = 0, [0x8F] = 0, [0x90] = 0, [0x91] = 0, [0x92] = 0,
        [0x93] = 0, [0x94] = 0, [0x95] = 0, [0x96] = 0, [0x97] = 0, [0x98] = 0, [0x99] = 0,
        [0x9A] = 0, [0x9B] = 0, [0x9C] = 0, [0x9D] = 0, [0x9E] = 0, [0x9F] = 0, [0xA0] = 0,
        [0xA1] = 0, [0xA2] = 0, [0xA3] = 0, [0xA4] = 0, [0xA5] = 0, [0xA6] = 0, [0xA7] = 0,
        [0xA8] = 0, [0xA9] = 0, [0xAA] = 0, [0xAB] = 0, [0xAC] = 0, [0xAD] = 0, [0xAE] = 0,
        [0xAF] = 0, [0xB0] = 0, [0xB1] = 0, [0xB2] = 0, [0xB3] = 0, [0xB4] = 0, [0xB5] = 0,
        [0xB6] = 0, [0xB7] = 0, [0xB8] = 0, [0xB9] = 0, [0xBA] = 0, [0xBB] = 0, [0xBC] = 0,
        [0xBD] = 0, [0xBE] = 0, [0xBF] = 0, [0xC0] = 0, [0xC1] = 0, [0xC2] = 0, [0xC3] = 0,
        [0xC4] = 0, [0xC5] = 0, [0xC6] = 0, [0xC7] = 0, [0xC8] = 0, [0xC9] = 0, [0xCA] = 0,
        [0xCB] = 0, [0xCC] = 0, [0xCD] = 0, [0xCE] = 0, [0xCF] = 0, [0xD0] = 0, [0xD1] = 0,
        [0xD2] = 0, [0xD3] = 0, [0xD4] = 0, [0xD5] = 0, [0xD6] = 0, [0xD7] = 0, [0xD8] = 0,
        [0xD9] = 0, [0xDA] = 0, [0xDB] = 0, [0xDC] = 0, [0xDD] = 0, [0xDE] = 0, [0xDF] = 0,
        [0xE0] = 0, [0xE1] = 0, [0xE2] = 0, [0xE3] = 0, [0xE4] = 0, [0xE5] = 0, [0xE6] = 0,
        [0xE7] = 0, [0xE8] = 0, [0xE9] = 0, [0xEA] = 0, [0xEB] = 0, [0xEC] = 0, [0xED] = 0,
        [0xEE] = 0, [0xEF] = 0, [0xF0] = 0, [0xF1] = 0, [0xF2] = 0, [0xF3] = 0, [0xF4] = 0,
        [0xF5] = 0, [0xF6] = 0, [0xF7] = 0, [0xF8] = 0, [0xF9] = 0, [0xFA] = 0, [0xFB] = 0,
        [0xFC] = 0, [0xFD] = 0, [0xFE] = 0, [0xFF] = 0,
};

static char *mySep(char **s) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if (sepMap[*pos]) {
      *pos = '\0';
      *s = (char *)++pos;
      break;
    }
  }
  if (!*pos) {
    *s = NULL;
  }
  return orig;
}

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = ctx->lastOffset + 1;

  while (*ctx->pos != NULL) {
    // get the next token
    char *tok = mySep(ctx->pos);
    // this means we're at the end
    if (tok == NULL) break;

    // normalize the token
    size_t tlen;
    tok = ctx->normalize(tok, &tlen);

    // ignore tokens that turn into nothing
    if (tok == NULL || tlen == 0) {
      continue;
    }

    // skip stopwords
    if (StopWordList_Contains(ctx->stopwords, tok, tlen)) {
      continue;
    }
    // create the token struct
    Token t = {tok, tlen, ++pos, ctx->fieldScore, ctx->fieldId, DT_WORD, 0};

    // let it be handled - and break on non zero response
    if (ctx->tokenFunc(ctx->tokenFuncCtx, &t) != 0) {
      break;
    }

    // if we support stemming - try to stem the word
    if (ctx->stemmer) {
      size_t sl;
      const char *stem = ctx->stemmer->Stem(ctx->stemmer->ctx, tok, tlen, &sl);
      if (stem && strncmp(stem, tok, tlen)) {
        t.s = rm_strndup(stem, sl);
        t.type = DT_STEM;
        t.len = sl;
        t.fieldId = ctx->fieldId;
        t.stringFreeable = 1;
        if (ctx->tokenFunc(ctx->tokenFuncCtx, &t) != 0) {
          break;
        }
      }
    }
  }

  return pos;
}

char *DefaultNormalize(char *s, size_t *len) {
  char *dst = s, *src = s;
  *len = 0;
  while (*src != '\0') {
    if (isupper(*src)) {
      *dst++ = tolower(*src++);
    } else if (isblank(*src) || iscntrl(*src)) {
      src++;
      continue;
    } else {
      *dst++ = *src++;
    }
    ++(*len);
  }
  *dst = 0;
  return s;
}
