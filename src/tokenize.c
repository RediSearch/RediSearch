#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include "tokenize.h"
#include "forward_index.h"

int tokenize(const char *text, float score, u_char fieldId, void *ctx,
             TokenFunc f, int doStem) {
  TokenizerCtx tctx;
  tctx.text = text;
  tctx.pos = (char **)&text;
  tctx.separators = DEFAULT_SEPARATORS;
  tctx.fieldScore = score;
  tctx.tokenFunc = f;
  tctx.tokenFuncCtx = ctx;
  tctx.normalize = DefaultNormalize;
  tctx.fieldId = fieldId;
  tctx.stemmer = doStem ? NewStemmer(SnowballStemmer, "english") : NULL;

  return _tokenize(&tctx);
}

inline int isStopword(const char *w) {
  int i = 0;
  while (stopwords[i] != NULL) {
    if (!strcmp(w, stopwords[i++])) {
      return 1;
    }
  }

  return 0;
}

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
  u_int pos = 0;

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
    if (isStopword(tok)) continue;

    // create the token struct
    Token t = {tok, tlen, ++pos, ctx->fieldScore, ctx->fieldId, DT_WORD};

    // let it be handled - and break on non zero response
    if (ctx->tokenFunc(ctx->tokenFuncCtx, t) != 0) {
      break;
    }

    // if we support stemming - try to stem the word
    if (ctx->stemmer) {
      size_t sl;
      const char *stem = ctx->stemmer->Stem(ctx->stemmer->ctx, tok, tlen, &sl);
      if (stem && strncmp(stem, tok, tlen)) {
        printf("Adding stem %s -> %s\n", tok, stem);
        t.s = stem;
        t.type = DT_STEM, t.len = sl;
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

QueryTokenizer NewQueryTokenizer(char *text, size_t len) {
  QueryTokenizer ret;
  ret.text = text;
  ret.len = len;
  ret.pos = text;
  ret.normalize = DefaultNormalize;
  ret.separators = QUERY_SEPARATORS;

  return ret;
}

QueryToken QueryTokenizer_Next(QueryTokenizer *t) {
  // we return null if there's nothing more to read
  if (t->pos >= t->text + t->len) {
    goto end;
  }
  char *end = (char *)t->text + t->len;
  char *currentTok = t->pos;
  while (t->pos < end) {
    // if this is a separator - either yield the token or move on
    if (strchr(t->separators, *t->pos) || iscntrl(*t->pos)) {
      if (t->pos > currentTok) {
        break;
      } else {
        // there is no token, just advance the token start
        currentTok = ++t->pos;
        continue;
      }
    }
    if (*t->pos == '\"') {
      if (t->pos > currentTok) {
        goto word;
      }

      t->pos++;
      return (QueryToken){NULL, 0, T_QUOTE};
    }
    t->pos++;
  }

  *t->pos = 0;

  t->pos++;
word : {
  char *w = strndup(currentTok, t->pos - currentTok);
  int stopword = isStopword(w);
  return (QueryToken){
      w, t->pos - currentTok, stopword ? T_STOPWORD : T_WORD,

  };
}
end:
  return (QueryToken){NULL, 0, T_END};
}

int QueryTokenizer_HasNext(QueryTokenizer *t) {
  return t->pos < t->text + t->len;
}