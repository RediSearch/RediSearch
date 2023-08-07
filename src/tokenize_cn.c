/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "tokenize.h"
#include "toksep.h"
#include "config.h"
#include "friso/friso.h"
#include "cndict_loader.h"
#include "util/minmax.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"
#include "separators.h"

static friso_config_t config_g;
static friso_t friso_g;

#define CNTOKENIZE_BUF_MAX 256

typedef struct {
  RSTokenizer base;
  friso_task_t fTask;
  char escapebuf[CNTOKENIZE_BUF_MAX];
  size_t nescapebuf;
} cnTokenizer;

// TODO: This is just a global init
static void maybeFrisoInit() {
  if (friso_g) {
    return;
  }

  const char *configfile = RSGlobalConfig.frisoIni;
  friso_g = friso_new();
  config_g = friso_new_config();

  if (configfile) {
    if (!friso_init_from_ifile(friso_g, config_g, (char *)configfile)) {
      fprintf(stderr, "Failed to initialize friso. Abort\n");
      abort();
    }
  } else {
    friso_dic_t dic = friso_dic_new();
    ChineseDictLoad(dic);
    ChineseDictConfigure(friso_g, config_g);
    friso_set_dic(friso_g, dic);
  }

  // Overrides:
  // Don't segment english text. We might use our actual tokenizer later if needed
  config_g->en_sseg = 0;
}

static void cnTokenizer_Start(RSTokenizer *base, char *text, size_t len, uint32_t options) {
  cnTokenizer *self = (cnTokenizer *)base;
  base->ctx.text = text;
  base->ctx.len = len;
  base->ctx.options = options;
  friso_set_text(self->fTask, text);
  self->nescapebuf = 0;
}

// check if the word has a trailing escape. assumes NUL-termination
static int hasTrailingEscape(const char *s, size_t n, SeparatorList *dl) {
  if (s[n] != '\\') {
    return 0;
  }
  if(dl != NULL) {
    return istoksep(s[n + 1], dl);
  } else {
    return istoksep(s[n + 1], NULL);
  }
}

static int appendToEscbuf(cnTokenizer *cn, const char *s, size_t n) {
  size_t toCp = Min(n, CNTOKENIZE_BUF_MAX - cn->nescapebuf);
  memcpy(cn->escapebuf + cn->nescapebuf, s, toCp);
  cn->nescapebuf += toCp;
  // printf("Added %.*s to escbuf\n", (int)n, s);
  return toCp == n;
}

/**
 * When we encounter a backslash, append the next character and continue
 * the loop
 */
#define ESCAPED_CHAR_SELF 1  // buf + len is the escaped character'
#define ESCAPED_CHAR_NEXT 2  // buf + len + 1 is the escaped character

/**
 * Append escaped characters, advancing the buffer internally. Returns true
 * if the current token needs more characters, or 0 if this token is
 * complete
 */
static int appendEscapedChars(cnTokenizer *self, friso_token_t ftok, int mode) {
  const char *escbegin = self->base.ctx.text + ftok->offset + ftok->length;
  size_t skipBy;
  if (mode == ESCAPED_CHAR_SELF) {
    skipBy = 1;
  } else {
    skipBy = 2;
    escbegin++;
  }

  if (appendToEscbuf(self, escbegin, 1)) {
    // printf("appending %.*s\n", 1, escbegin);
    self->fTask->idx += skipBy;

    // if there are more tokens...
    if (self->fTask->idx < self->base.ctx.len) {
      // and this token is not completed (i.e. character _after_ escape
      // is not itself a word separator)
      if (!istoksep(self->base.ctx.text[self->fTask->idx], self->base.ctx.separators)) {
        return 1;
      }
    }
  }
  return 0;
}

static void initToken(RSTokenizer *base, Token *t, const friso_token_t from) {
  t->raw = base->ctx.text + from->offset;
  t->rawLen = from->rlen;
  t->stem = NULL;
  t->stemLen = 0;
  t->flags = Token_CopyRaw | Token_CopyStem;
  t->pos = ++base->ctx.lastOffset;
}

static uint32_t cnTokenizer_Next(RSTokenizer *base, Token *t) {
  cnTokenizer *self = (cnTokenizer *)base;
  TokenizerCtx *ctx = &base->ctx;

  int useEscBuf = 0;
  int tokInit = 0;
  self->nescapebuf = 0;

  while (1) {
    friso_token_t tok = config_g->next_token(friso_g, config_g, self->fTask);
    if (tok == NULL) {
      if (useEscBuf) {
        RS_LOG_ASSERT(tokInit, "should not get here");
        t->tokLen = self->nescapebuf;
        t->tok = self->escapebuf;
        return t->pos;
      }
      return 0;
    }

    // Check if it's a stopword?
    if (ctx->stopwords && StopWordList_Contains(ctx->stopwords, tok->word, tok->length)) {
      continue;
    }
    // printf("Type: %d\n", tok->type);

    switch (tok->type) {
        // Skip words we know we don't care about.
      case __LEX_STOPWORDS__:
      case __LEX_ENPUN_WORDS__:
      case __LEX_CJK_UNITS__:
      case __LEX_NCSYN_WORDS__:
        continue;

      case __LEX_PUNC_WORDS__:
        if (tok->word[0] == '\\' && istoksep(ctx->text[tok->offset + 1], ctx->separators)) {
          if (!appendEscapedChars(self, tok, ESCAPED_CHAR_SELF)) {
            break;
          }

          if (!tokInit) {
            initToken(base, t, tok);
            tokInit = 1;
          }

          useEscBuf = 1;
          t->tok = self->escapebuf;
          t->tokLen = self->nescapebuf;
        }
        continue;

      default:
        break;
    }

    const char *bufstart = ctx->text + tok->offset;

    if (!tokInit) {
      initToken(base, t, tok);
      tokInit = 1;
    } else {
      t->rawLen = (ctx->text + ctx->len) - t->raw;
    }

    if (hasTrailingEscape(bufstart, tok->rlen, ctx->separators)) {
      // We must continue the friso loop, because we have found an escape..
      if (!useEscBuf) {
        useEscBuf = 1;
        t->tok = self->escapebuf;
      }
      t->tokLen = self->nescapebuf;
      if (!appendToEscbuf(self, tok->word, tok->length)) {
        t->tokLen = self->nescapebuf;
        return t->pos;
      }
      if (!appendEscapedChars(self, tok, ESCAPED_CHAR_NEXT)) {
        t->tokLen = self->nescapebuf;
        return t->pos;
      }
      continue;
    }

    if (useEscBuf) {
      appendToEscbuf(self, tok->word, tok->length);
      t->tokLen = self->nescapebuf;
    } else {
      // not an escape
      t->tok = tok->word;
      t->tokLen = tok->length;
    }
    return t->pos;
  }
}

static void cnTokenizer_Free(RSTokenizer *base) {
  cnTokenizer *self = (cnTokenizer *)base;
  friso_free_task(self->fTask);
  rm_free(self);
}

static void cnTokenizer_Reset(RSTokenizer *base, Stemmer *stemmer, StopWordList *stopwords,
                              uint32_t opts, SeparatorList *separators) {
  // Nothing to do here
  base->ctx.lastOffset = 0;
}

RSTokenizer *NewChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords,
                                uint32_t opts, SeparatorList *separators) {
  cnTokenizer *tokenizer = rm_calloc(1, sizeof(*tokenizer));
  tokenizer->fTask = friso_new_task();
  maybeFrisoInit();
  tokenizer->base.ctx.options = opts;
  tokenizer->base.ctx.stopwords = stopwords;
  tokenizer->base.ctx.separators = separators;
  tokenizer->base.Start = cnTokenizer_Start;
  tokenizer->base.Next = cnTokenizer_Next;
  tokenizer->base.Free = cnTokenizer_Free;
  tokenizer->base.Reset = cnTokenizer_Reset;
  return &tokenizer->base;
}
