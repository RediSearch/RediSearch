#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include "tokenize.h"
#include "forward_index.h"


int tokenize(const char *text, u_short score, u_char fieldId, void *ctx, TokenFunc f) {
    TokenizerCtx tctx;
    tctx.text = text;
    tctx.pos = (char**)&text;
    tctx.separators = DEFAULT_SEPARATORS;
    tctx.fieldScore = score;
    tctx.tokenFunc = f;
    tctx.tokenFuncCtx = ctx;
    tctx.normalize = DefaultNormalize;
    tctx.fieldId = fieldId;

    return _tokenize(&tctx);
}

// tokenize the text in the context
int _tokenize(TokenizerCtx *ctx) {
    
    u_int pos = 0;
   
   
    while(*ctx->pos != NULL) {
        // get the next token
        char *tok = strsep(ctx->pos, ctx->separators);
        // this means we're at the end
        if (tok == NULL) break;
        
        // normalize the token
        size_t tlen;
        tok = ctx->normalize(tok, &tlen);
        
        // ignore tokens that turn into nothing
        if (tok == NULL || tlen == 0) {
            continue;
        }
        
        // create the token struct
        Token t = {
            tok,
            tlen,
            ++pos,
            ctx->fieldScore,
            ctx->fieldId
        };
        
        // let it be handled - and break on non zero response
        if (ctx->tokenFunc(ctx->tokenFuncCtx, t) != 0) {
            break;
        }
    }
    
    return pos;
}

char*DefaultNormalize(char *s, size_t *len) {
    
    char *dst = s, *src = s;
    *len = 0;
    while (*src != '\0') {
        if (isupper(*src)) {
            *dst++ = tolower(*src++);
        } else if(isblank(*src) || iscntrl(*src)) {
            src++;
            continue;
        } else {
            *dst++=*src++;
        }
        ++(*len);
    }
    *dst = 0;
    return s;
}

