
#include "tokenize.h"
#include "toksep.h"
#include "config.h"
#include "cndict_loader.h"

#include "util/minmax.h"
#include "rmalloc.h"

#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

static friso_config_t config_g;
static friso_t friso_g;

//---------------------------------------------------------------------------------------------

// TODO: This is just a global init
void ChineseTokenizer::maybeFrisoInit() {
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

//---------------------------------------------------------------------------------------------

void ChineseTokenizer::Start(char *text_, size_t len_, uint32_t options_) {
  text = text_;
  len = len_;
  options = options_;
  friso_set_text(friso_task, text);
  nescapebuf = 0;
}

//---------------------------------------------------------------------------------------------

// check if the word has a trailing escape. assumes NUL-termination
static int hasTrailingEscape(const char *s, size_t n) {
  if (s[n] != '\\') {
    return 0;
  }
  return istoksep(s[n + 1]);
}

//---------------------------------------------------------------------------------------------

int ChineseTokenizer::appendToEscbuf(const char *s, size_t n) {
  size_t toCp = Min(n, CNTOKENIZE_BUF_MAX - nescapebuf);
  memcpy(escapebuf + nescapebuf, s, toCp);
  nescapebuf += toCp;
  // printf("Added %.*s to escbuf\n", (int)n, s);
  return toCp == n;
}

//---------------------------------------------------------------------------------------------

// When we encounter a backslash, append the next character and continue the loop

#define ESCAPED_CHAR_SELF 1  // buf + len is the escaped character'
#define ESCAPED_CHAR_NEXT 2  // buf + len + 1 is the escaped character

//---------------------------------------------------------------------------------------------

// Append escaped characters, advancing the buffer internally. Returns true
// if the current token needs more characters, or 0 if this token is complete.

int ChineseTokenizer::appendEscapedChars(friso_token_t ftok, int mode) {
  const char *escbegin = text + ftok->offset + ftok->length;
  size_t skipBy;
  if (mode == ESCAPED_CHAR_SELF) {
    skipBy = 1;
  } else {
    skipBy = 2;
    escbegin++;
  }

  if (appendToEscbuf(escbegin, 1)) {
    // printf("appending %.*s\n", 1, escbegin);
    friso_task->idx += skipBy;

    // if there are more tokens...
    if (friso_task->idx < len) {
      // and this token is not completed (i.e. character _after_ escape
      // is not itself a word separator)
      if (!istoksep(text[friso_task->idx])) {
        return 1;
      }
    }
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

void ChineseTokenizer::initToken(Token *t, const friso_token_t from) {
  t->raw = text + from->offset;
  t->rawLen = from->rlen;
  t->stem = NULL;
  t->stemLen = 0;
  t->flags = Token_CopyRaw | Token_CopyStem;
  t->pos = ++lastOffset;
}

//---------------------------------------------------------------------------------------------

uint32_t ChineseTokenizer::Next(Token *t) {
  int useEscBuf = 0;
  int tokInit = 0;
  nescapebuf = 0;

  for (;;) {
    friso_token_t tok = config_g->next_token(friso_g, config_g, friso_task);
    if (tok == NULL) {
      if (useEscBuf) {
        RS_LOG_ASSERT(tokInit, "should not get here");
        t->tokLen = nescapebuf;
        t->tok = escapebuf;
        return t->pos;
      }
      return 0;
    }

    // Check if it's a stopword?
    if (stopwords && stopwords->Contains(tok->word, tok->length)) {
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
        if (tok->word[0] == '\\' && istoksep(text[tok->offset + 1])) {
          if (!appendEscapedChars(tok, ESCAPED_CHAR_SELF)) {
            break;
          }

          if (!tokInit) {
            initToken(t, tok);
            tokInit = 1;
          }

          useEscBuf = 1;
          t->tok = escapebuf;
          t->tokLen = nescapebuf;
        }
        continue;

      default:
        break;
    }

    const char *bufstart = text + tok->offset;

    if (!tokInit) {
      initToken(base, t, tok);
      tokInit = 1;
    } else {
      t->rawLen = (text + len) - t->raw;
    }

    if (hasTrailingEscape(bufstart, tok->rlen)) {
      // We must continue the friso loop, because we have found an escape..
      if (!useEscBuf) {
        useEscBuf = 1;
        t->tok = escapebuf;
      }
      t->tokLen = nescapebuf;
      if (!appendToEscbuf(tok->word, tok->length)) {
        t->tokLen = nescapebuf;
        return t->pos;
      }
      if (!appendEscapedChars(tok, ESCAPED_CHAR_NEXT)) {
        t->tokLen = nescapebuf;
        return t->pos;
      }
      continue;
    }

    if (useEscBuf) {
      appendToEscbuf(tok->word, tok->length);
      t->tokLen = nescapebuf;
    } else {
      // not an escape
      t->tok = tok->word;
      t->tokLen = tok->length;
    }
    return t->pos;
  }
}

//---------------------------------------------------------------------------------------------

ChineseTokenizer::~ChineseTokenizer() {
  friso_free_task(fTask);
}

//---------------------------------------------------------------------------------------------

void ChineseTokenizer::Reset(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts) {
  // Nothing to do here
  lastOffset = 0;
}

//---------------------------------------------------------------------------------------------

ChineseTokenizer::ChineseTokenizer(Stemmer *stemmer, StopWordList *stopwords, uint32_t opts) :
  Tokenizer(stopwords, opts) {

  fTask = friso_new_task();
  maybeFrisoInit();
}

///////////////////////////////////////////////////////////////////////////////////////////////
