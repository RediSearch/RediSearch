#ifndef __TOKENIZE_H__
#define __TOKENIZE_H__

#include <stdlib.h>
#include <strings.h>
#include <ctype.h>

#include "util/khash.h"
#include "index.h"

typedef struct  {
    const char *s;
    size_t len;
    u_int pos;
    u_short score;
    u_char fieldId;
} Token;


// A TokenFunc handles tokens in a tokenizer, for example aggregates them, or builds the query tree
typedef int(*TokenFunc)(void *ctx, Token t);

// A NormalizeFunc converts a raw token to the normalized form in which it will be stored
typedef char*(*NormalizeFunc)(char*, size_t*);

//! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ _ ` { | } ~
static const char *DEFAULT_SEPARATORS = " \t,./(){}[]:;/\\~!@#$%^&*-_=+|'`\"<>?";

typedef struct {
    const char *text;
    char **pos;
    const char *separators;
    u_short fieldScore;
    u_char fieldId;
    TokenFunc tokenFunc;
    void *tokenFuncCtx;
    NormalizeFunc normalize;
    
} TokenizerCtx;


KHASH_MAP_INIT_STR(32, IndexHit*);
typedef struct {
    khash_t(32) *hits;
    u_int totalFreq;
    t_docId docId;
    float docScore;
} ForwardIndex;


int forwardIndexTokenFunc(void *ctx, Token t);
void ForwardIndexFree(ForwardIndex *idx);
ForwardIndex *NewForwardIndex(t_docId docId, float docScore);

int _tokenize(TokenizerCtx *ctx);
int tokenize(const char *text, u_short score, u_char fieldId, void *ctx, TokenFunc f);
char*DefaultNormalize(char *s, size_t *len);

#endif