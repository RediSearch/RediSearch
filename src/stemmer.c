#include "stemmer.h"
#include <string.h>
#include <stdio.h>
#include <sys/param.h>
#include "dep/snowball/include/libstemmer.h"

const char *__supportedLanguages[] = {"arabic",     "danish",   "dutch",     "english", "finnish",
                                      "french",     "german",   "hungarian", "italian", "norwegian",
                                      "portuguese", "romanian", "russian",   "spanish", "swedish",
                                      "tamil",      "turkish",  "chinese",   NULL};

int IsSupportedLanguage(const char *language, size_t len) {
  for (int i = 0; __supportedLanguages[i] != NULL; i++) {
    if (!strncasecmp(language, __supportedLanguages[i],
                     MAX(len, strlen(__supportedLanguages[i])))) {
      return 1;
    }
  }
  return 0;
}

// QueryNode *StemmerExpand(void *ctx, Query *q, QueryNode *n) {

//   if (n->type == QN_TOKEN && q->language) {

//     struct sb_stemmer *sb = sb_stemmer_new(q->language, NULL);
//     // No stemmer available for this language - just return the node so we won't
//     // be called again
//     if (!sb) {
//       return n;
//     }

//     const sb_symbol *b = (const sb_symbol *)n->tn.str;
//     const sb_symbol *stemmed = sb_stemmer_stem(sb, (const sb_symbol *)n->tn.str, n->tn.len);

//     QueryNode *ret = NULL;
//     if (stemmed) {

//       if (stemmed && strncasecmp(stemmed, n->tn.str, n->tn.len)) {
//         // we are now evaluating two tokens and not 1
//         q->numTokens++;
//         // Create a new union
//         ret = NewUnionNode();

//         int sl = sb_stemmer_length(sb);
//         // Add the token and the ste as the union's children
//         QueryUnionNode_AddChild(&ret->un, n);
//         QueryUnionNode_AddChild(&ret->un, NewTokenNode(q, strndup(stemmed, sl), sl));
//       }
//     }
//     sb_stemmer_delete(sb);
//     return ret;
//   }

//   return NULL;
// }

// void RegisterStemmerExpander() {

//   QueryExpander qx = (QueryExpander){.Expand = StemmerExpand, .Free = NULL, .ctx = NULL};

//   RegisterQueryExpander(STEMMER_EXPANDER_NAME, qx);
// }

const char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen) {
  const sb_symbol *b = (const sb_symbol *)word;
  struct sb_stemmer *sb = ctx;

  const sb_symbol *stemmed = sb_stemmer_stem(sb, b, (int)len);
  if (stemmed) {
    *outlen = sb_stemmer_length(sb);
    return (const char *)stemmed;
  }
  return NULL;
}

void __sbstemmer_Free(Stemmer *s) {
  sb_stemmer_delete(s->ctx);
  free(s);
}

Stemmer *__newSnowballStemmer(const char *language) {
  struct sb_stemmer *sb = sb_stemmer_new(language, NULL);
  // No stemmer available for this language
  if (!sb) {
    return NULL;
  }

  Stemmer *ret = malloc(sizeof(Stemmer));
  ret->ctx = sb;
  ret->Stem = __sbstemmer_Stem;
  ret->Free = __sbstemmer_Free;
  return ret;
}

Stemmer *NewStemmer(StemmerType type, const char *language) {
  switch (type) {
    case SnowballStemmer:

      return __newSnowballStemmer(language);
  }

  fprintf(stderr, "Invalid stemmer type");
  return NULL;
}
