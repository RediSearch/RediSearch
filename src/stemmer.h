#ifndef __RS_STEMMER_H__
#define __RS_STEMMER_H__
#include <stdlib.h>


typedef enum {
    SnowballStemmer
} StemmerType;

/* Abstract "interface" for a pluggable stemmer, ensuring we can use multiple stemmer libs */ 
typedef struct stemmer {
    void *ctx;
    const char *(*Stem)(void *ctx, const char *word, size_t len, size_t *outlen);
    void  (*Free)(struct stemmer*);
} Stemmer;
Stemmer *NewStemmer(StemmerType type, const char *language);


/* Snoball Stemmer wrapper implementation */
const  char *__sbstemmer_Stem(void *ctx, const char *word, size_t len, size_t *outlen);
void __sbstemmer_Free(Stemmer *s);
Stemmer *__newSnowballStemmer(const char *language);


#endif