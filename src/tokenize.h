#ifndef __TOKENIZE_H__
#define __TOKENIZE_H__

#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include "types.h"
#include "util/khash.h"
#include "varint.h"

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
#define DEFAULT_SEPARATORS " \t,./(){}[]:;/\\~!@#$%^&*-_=+|'`\"<>?";

static const char *stopwords[] =  {
            "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by",
            "for", "if", "in", "into", "it",
            "no", "not", "of", "on", "or", "such",
            "that", "their", "then", "there", "these",
            "they", "this", "to", "was", "will", "with", NULL
    };
    
// TODO: Optimize this with trie or something...    
int isStopword(const char *w);

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


int _tokenize(TokenizerCtx *ctx);
int tokenize(const char *text, u_short score, u_char fieldId, void *ctx, TokenFunc f);
char*DefaultNormalize(char *s, size_t *len);

#endif