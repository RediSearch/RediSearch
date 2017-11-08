#include "tokenize.h"
#include "dep/friso/friso.h"
#include <assert.h>

static friso_config_t config_g;
static friso_t friso_g;

typedef struct {
  RSTokenizer base;
  friso_task_t fTask;
} cnTokenizer;

// TODO: This is just a global init
static void maybeFrisoInit() {
  if (friso_g) {
    return;
  }

  const char *dictfile = getenv("DICTFILE");
  if (!dictfile) {
    fprintf(stderr, "No `DICTFILE` specified. Assuming friso/friso.ini\n");
    dictfile = "friso/friso.ini";
  }
  friso_g = friso_new();
  config_g = friso_new_config();
  if (!friso_init_from_ifile(friso_g, config_g, (char *)dictfile)) {
    fprintf(stderr, "Failed to initialize friso. Abort\n");
    abort();
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
}

static uint32_t cnTokenizer_Next(RSTokenizer *base, Token *t) {
  cnTokenizer *self = (cnTokenizer *)base;
  TokenizerCtx *ctx = &base->ctx;
  while (1) {
    friso_token_t tok = config_g->next_token(friso_g, config_g, self->fTask);
    if (tok == NULL) {
      return 0;
    }

    // Check if it's a stopword?
    if (ctx->stopwords && StopWordList_Contains(ctx->stopwords, tok->word, tok->length)) {
      continue;
    }

    switch (tok->type) {
      // Skip words we know we don't care about.
      case __LEX_STOPWORDS__:
      case __LEX_PUNC_WORDS__:
      case __LEX_ENPUN_WORDS__:
      case __LEX_CJK_UNITS__:
      case __LEX_NCSYN_WORDS__:
        continue;
      default:
        break;
    }

    // fprintf(stderr, "Pos: %u. Offset: %u. Len: %u. RLen: %u. Type: %u\n", tok->pos, tok->offset,
    //         tok->length, tok->rlen, tok->type);

    // We don't care if it's english, chinese, or a mix. They all get treated the same in
    // the index.
    *t = (Token){.tok = tok->word,
                 .tokLen = tok->length,
                 .raw = ctx->text + tok->offset,
                 .rawLen = tok->rlen,
                 .stem = NULL,
                 .flags = Token_CopyRaw | Token_CopyStem,
                 .pos = ++ctx->lastOffset};

    return t->pos;
  }
}

static void cnTokenizer_Free(RSTokenizer *base) {
  cnTokenizer *self = (cnTokenizer *)base;
  friso_free_task(self->fTask);
  free(self);
}

RSTokenizer *NewChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts) {
  cnTokenizer *tokenizer = calloc(1, sizeof(*tokenizer));
  tokenizer->fTask = friso_new_task();
  maybeFrisoInit();
  tokenizer->base.ctx.options = opts;
  tokenizer->base.ctx.stopwords = stopwords;
  tokenizer->base.Start = cnTokenizer_Start;
  tokenizer->base.Next = cnTokenizer_Next;
  tokenizer->base.Free = cnTokenizer_Free;
  return &tokenizer->base;
}