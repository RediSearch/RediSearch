#include "../stopwords.h"
#include "tokenizer.h"
#include "parser.h"
#include "../tokenize.h"
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>

QueryTokenizer NewQueryTokenizer(char *text, size_t len, const char **stopwords) {
  QueryTokenizer ret;
  ret.text = text;
  ret.len = len;
  ret.pos = text;
  ret.normalize = DefaultNormalize;
  ret.separators = QUERY_SEPARATORS;
  ret.stopwords = stopwords;

  return ret;
}
static char ctrls[256] = {['\"'] = QUOTE, ['|'] = OR,   ['('] = LP,    [')'] = RP,
                          [':'] = COLON,  ['@'] = AT,   ['-'] = MINUS, ['~'] = TILDE,
                          ['*'] = STAR,   ['['] = LSQB, [']'] = RSQB};

int toNumber(QueryToken *t) {
  char *p = (char *)t->s;
  errno = 0;
  t->numval = strtod(t->s, &p);

  if (*p != 0) return 0;
  if ((errno == ERANGE && (t->numval == HUGE_VAL || t->numval == -HUGE_VAL)) ||
      (errno != 0 && t->numval == 0)) {
    return 0;
  }
  return 1;
}

int QueryTokenizer_Next(QueryTokenizer *t, QueryToken *tok) {
start:
  // we return null if there's nothing more to read
  if (t->pos >= t->text + t->len) {
    goto end;
  }

  char *end = (char *)t->text + t->len;
  char *currentTok = t->pos;
  size_t toklen = 0;
  while (t->pos < end) {
    // if this is a separator - either yield the token or move on
    if (strchr(t->separators, *t->pos) || iscntrl(*t->pos)) {
      if (t->pos > currentTok) {
        break;
      } else {
        // there is no token, just advance the token start
        currentTok = ++t->pos;
        toklen = 0;
        continue;
      }
    }

    if (ctrls[(int)*t->pos]) {
      if (t->pos > currentTok) {
        goto word;
      }
      int rc = ctrls[(int)*t->pos];
      tok->len = 1;
      tok->s = t->pos;
      tok->pos = t->pos - t->text;
      ++t->pos;
      toklen = 0;
      return rc;
    }

    *t->pos = tolower(*t->pos);
    ++t->pos;
    ++toklen;
  }

  *t->pos = 0;
  t->pos++;
word : {
  char *w = strndup(currentTok, toklen);

  if (!isStopword(w, t->stopwords)) {
    *tok = (QueryToken){.s = w, .len = toklen, .pos = currentTok - t->text, .numval = 0};
    // if the token is convertible to number - convert and return a NUMBER token
    if (toNumber(tok)) {
      return NUMBER;
    }
    return TERM;
  } else {
    // we just skip this token and go to the beginning of the function
    goto start;
  }
}
end:
  return 0;  //(QueryToken){NULL, 0, T_END};
}

int QueryTokenizer_HasNext(QueryTokenizer *t) {
  return t->pos < t->text + t->len;
}