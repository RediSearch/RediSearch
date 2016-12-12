#include "tokenizer.h"
#include "query.h"
#include "../tokenize.h"
#include <string.h>
#include <strings.h>
#include <ctype.h>

QueryTokenizer NewQueryTokenizer(char *text, size_t len) {
  QueryTokenizer ret;
  ret.text = text;
  ret.len = len;
  ret.pos = text;
  ret.normalize = DefaultNormalize;
  ret.separators = QUERY_SEPARATORS;

  return ret;
}

int QueryTokenizer_Next(QueryTokenizer *t, QueryToken *tok) {
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

    if (*t->pos == '\"' || *t->pos == '(' || *t->pos == ')' || *t->pos == '|') {
      if (t->pos > currentTok) {
        goto word;
      }

      int rc;
      tok->len = 1;
      tok->s = t->pos;
      tok->pos = t->pos - t->text;
      switch (*t->pos) {
        case '"':
            rc = QUOTE;
            break;
        case '|':
            rc = OR;
            break;
        case '(':
            rc = LP;
            break;
        case ')':
        default:
            rc = RP;
            break;
      }
      ++t->pos;
      toklen = 0;

      return rc;
    }
    ++t->pos;
    ++toklen;
  }

  *t->pos = 0;
  t->pos++;
word : {
  char *w = strndup(currentTok, toklen);
  int stopword = isStopword(w);
  *tok = (QueryToken){.s = w, .len = toklen, .pos = currentTok - t->text };
  return TERM;
}
end:
  return 0; //(QueryToken){NULL, 0, T_END};
}

int QueryTokenizer_HasNext(QueryTokenizer *t) {
  return t->pos < t->text + t->len;
}