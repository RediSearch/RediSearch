#include "tokenizer.h"
#include "query.h"
#include <string.h>
#include <strings.h>
#include <ctype.h>
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

int isStopword(const char *w) {
    int i = 0;
    while (stopwords[i] != NULL) {
        if (!strcmp(w, stopwords[i++])) {
            return 1;
        }
    }

    return 0;
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
        if (*t->pos == '\"') {
            if (t->pos > currentTok) {
                goto word;
            }

            ++t->pos;
            toklen = 0;
            *tok = (QueryToken){NULL, 0};
            return QUOTE;
        }
        ++t->pos;
        ++toklen;
    }

    *t->pos = 0;
    t->pos++;
word : {
    char *w = strndup(currentTok, toklen);
    int stopword = isStopword(w);
    *tok = (QueryToken){ w, toklen };
    return TERM;
}
end:
    return 0;//(QueryToken){NULL, 0, T_END};
}

int QueryTokenizer_HasNext(QueryTokenizer *t) { return t->pos < t->text + t->len; }