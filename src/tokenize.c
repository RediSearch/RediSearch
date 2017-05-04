#include "forward_index.h"
#include "stopwords.h"
#include "tokenize.h"
#include "rmalloc.h"
#include <ctype.h>
#include <stdlib.h>
#include <strings.h>

int tokenize(const char *text, float score, t_fieldMask fieldId, void *ctx, TokenFunc f, Stemmer *s,
             u_int offset) {
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

  return _tokenize(&tctx);
}

inline int isStopword(const char *w, size_t len, const char **stopwords) {
  if (!stopwords) {
    return 0;
  }
  for (int i = 0; stopwords[i] != NULL; ++i) {
    // printf("%s %s\n", w, stopwords[i]);
    size_t wlen = strlen(stopwords[i]);

    if (len == wlen && !strncasecmp(w, stopwords[i], len)) {
      return 1;
    }
    
  }

  return 0;
}

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = ctx->lastOffset + 1;

  while (*ctx->pos != NULL) {
    // get the next token
    char *tok = strsep(ctx->pos, ctx->separators);
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
    if (isStopword(tok, tlen, DEFAULT_STOPWORDS)) {
      continue;
    }
    // create the token struct
    Token t = {tok, tlen, ++pos, ctx->fieldScore, ctx->fieldId, DT_WORD, 0};

    // let it be handled - and break on non zero response
    if (ctx->tokenFunc(ctx->tokenFuncCtx, t) != 0) {
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
        if (ctx->tokenFunc(ctx->tokenFuncCtx, t) != 0) {
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
