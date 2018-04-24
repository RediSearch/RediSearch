%left LOWEST.
%left TILDE.
%left TAGLIST.
%left QUOTE.

%left COLON.
%left MINUS.
%left NUMBER.
%left STOPWORD.

%left TERMLIST.
%left TERM. 
%left PREFIX.

%right LP.
%left RP.
// needs to be above lp/rp
%left MODIFIER.
%left AND.
%left OR.
%left ORX.

%token_type {QueryToken}  

%syntax_error {  

    int len = TOKEN.len + 100;
    char buf[len];
    snprintf(buf, len, "Syntax error at offset %d near '%.*s'", TOKEN.pos, TOKEN.len, TOKEN.s);
    
    ctx->ok = 0;
    ctx->errorMsg = strdup(buf);
}   
   
%include {   

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "../rmutil/vector.h"
#include "../query_node.h"

// strndup + lowercase in one pass!
char *strdupcase(const char *s, size_t len) {
  char *ret = strndup(s, len);
  char *dst = ret;
  char *src = dst;
  while (*src) {
      // unescape 
      if (*src == '\\' && (ispunct(*(src+1)) || isspace(*(src+1)))) {
          ++src;
          continue;
      }
      *dst = tolower(*src);
      ++dst;
      ++src;

  }
  *dst = '\0';
  
  return ret;
}

// unescape a string (non null terminated) and return the new length (may be shorter than the original. This manipulates the string itself 
size_t unescapen(char *s, size_t sz) {
  
  char *dst = s;
  char *src = dst;
  char *end = s + sz;
  while (src < end) {
      // unescape 
      if (*src == '\\' && src + 1 < end &&
         (ispunct(*(src+1)) || isspace(*(src+1)))) {
          ++src;
          continue;
      }
      *dst++ = *src++;
  }
 
  return (size_t)(dst - s);
}
   
} // END %include  

%extra_argument { QueryParseCtx *ctx }
%default_type { QueryToken }
%default_destructor { }

%type expr { QueryNode * } 
%destructor expr { QueryNode_Free($$); }

%type prefix { QueryNode * } 
%destructor prefix { QueryNode_Free($$); }

%type termlist { QueryNode * } 
%destructor termlist { QueryNode_Free($$); }

%type union { QueryNode *}
%destructor union { QueryNode_Free($$); }

%type tag_list { QueryNode *}
%destructor tag_list { QueryNode_Free($$); }

%type geo_filter { GeoFilter *}
%destructor geo_filter { GeoFilter_Free($$); }

%type modifierlist { Vector* }
%destructor modifierlist { 
    for (size_t i = 0; i < Vector_Size($$); i++) {
        char *s;
        Vector_Get($$, i, &s);
        free(s);
    }
    Vector_Free($$); 
}

%type num { RangeNumber }

%type numeric_range { NumericFilter * }
%destructor numeric_range {
    NumericFilter_Free($$);
}

query ::= expr(A) . { 
 /* If the root is a negative node, we intersect it with a wildcard node */
 
    ctx->root = A;
 
}
query ::= . {
    ctx->root = NULL;
}

query ::= STAR . {
    ctx->root = NewWildcardNode();
}

/////////////////////////////////////////////////////////////////
// AND Clause / Phrase
/////////////////////////////////////////////////////////////////

expr(A) ::= expr(B) expr(C) . [AND] {

    // if both B and C are null we return null
    if (B == NULL && C == NULL) {
        A = NULL;
    } else {

        if (B && B->type == QN_PHRASE && B->pn.exact == 0 && 
            B->fieldMask == RS_FIELDMASK_ALL ) {
            A = B;
        } else {     
            A = NewPhraseNode(0);
            QueryPhraseNode_AddChild(A, B);
        }
        QueryPhraseNode_AddChild(A, C);
    }
} 


/////////////////////////////////////////////////////////////////
// Unions
/////////////////////////////////////////////////////////////////

expr(A) ::= union(B) . [ORX] {
    A = B;
}

union(A) ::= expr(B) OR expr(C) . [OR] {
    if (B == NULL && C == NULL) {
        A = NULL;
    } else if (B && B->type == QN_UNION && B->fieldMask == RS_FIELDMASK_ALL) {
        A = B;
    } else {
        A = NewUnionNode();
        QueryUnionNode_AddChild(A, B);
        if (B) 
         A->fieldMask |= B->fieldMask;

    } 
    if (C) {

        QueryUnionNode_AddChild(A, C);
        A->fieldMask |= C->fieldMask;
        QueryNode_SetFieldMask(A, A->fieldMask);
    }
    
}

union(A) ::= union(B) OR expr(C). [ORX] {
    
    A = B;
    QueryUnionNode_AddChild(A, C); 
    A->fieldMask |= C->fieldMask;
    QueryNode_SetFieldMask(C, A->fieldMask);


}

/////////////////////////////////////////////////////////////////
// Text Field Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON expr(C) . [MODIFIER] {
    if (C == NULL) {
        A = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(C, IndexSpec_GetFieldBit(ctx->sctx->spec, B.s, B.len));
        }
        A = C; 
    }
}


    // expr(A) ::= modifier(B) COLON TERM(C). [MODIFIER]  {

    //     A = NewTokenNode(ctx, strdupcase(C.s, C.len), -1);
    //     if (ctx->sctx->spec) {
    //         A->fieldMask = IndexSpec_GetFieldBit(ctx->sctx->spec, B.s, B.len); 
    //     }
    // }

expr(A) ::= modifierlist(B) COLON expr(C) . [MODIFIER] {
    
    if (C == NULL) {
        A = NULL;
    } else {
        //C->fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(B); i++) {
                char *p;
                Vector_Get(B, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                free(p);
            }
        }
        QueryNode_SetFieldMask(C, mask);
        Vector_Free(B);
        A=C;
    }
} 

expr(A) ::= LP expr(B) RP . {
    A = B;
}

/////////////////////////////////////////////////////////////////
// Term Lists
/////////////////////////////////////////////////////////////////

expr(A) ::= QUOTE termlist(B) QUOTE. [TERMLIST] {
    B->pn.exact =1;
    B->flags |= QueryNode_Verbatim;

    A = B;
}

expr(A) ::= QUOTE term(B) QUOTE. [TERMLIST] {
    A = NewTokenNode(ctx, strdupcase(B.s, B.len), -1);
    A->flags |= QueryNode_Verbatim;
    
}

expr(A) ::= term(B) . [LOWEST]  {
   A = NewTokenNode(ctx, strdupcase(B.s, B.len), -1);
}

expr(A) ::= prefix(B) . [PREFIX]  {
    A= B;
}

expr(A) ::= termlist(B) .  [TERMLIST] {
        A = B;
}

expr(A) ::= STOPWORD . [STOPWORD] {
    A = NULL;
}

termlist(A) ::= term(B) term(C). [TERMLIST]  {
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx, strdupcase(B.s, B.len), -1));
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx, strdupcase(C.s, C.len), -1));
}

termlist(A) ::= termlist(B) term(C) . [TERMLIST] {
    A = B;
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx, strdupcase(C.s, C.len), -1));
}

termlist(A) ::= termlist(B) STOPWORD . [TERMLIST] {
    A = B;
}

/////////////////////////////////////////////////////////////////
// Negative Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= MINUS expr(B) . { 
    A = NewNotNode(B);
}

/////////////////////////////////////////////////////////////////
// Optional Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= TILDE expr(B) . { 
    A = NewOptionalNode(B);
}

/////////////////////////////////////////////////////////////////
// Prefix experessions
/////////////////////////////////////////////////////////////////

prefix(A) ::= PREFIX(B) . [PREFIX] {
    B.s = strdupcase(B.s, B.len);
    A = NewPrefixNode(ctx, B.s, strlen(B.s));
}

/////////////////////////////////////////////////////////////////
// Field Modidiers
/////////////////////////////////////////////////////////////////

modifier(A) ::= MODIFIER(B) . {
    B.len = unescapen((char*)B.s, B.len);
    A = B;
 } 

modifierlist(A) ::= modifier(B) OR term(C). {
    A = NewVector(char *, 2);
    char *s = strdupcase(B.s, B.len);
    Vector_Push(A, s);
    s = strdupcase(C.s, C.len);
    Vector_Push(A, s);
}

modifierlist(A) ::= modifierlist(B) OR term(C). {
    char *s = strdupcase(C.s, C.len);
    Vector_Push(B, s);
    A = B;
}


/////////////////////////////////////////////////////////////////
// Tag Lists - curly braces separated lists of words
/////////////////////////////////////////////////////////////////
expr(A) ::= modifier(B) COLON tag_list(C) . {
    if (!C) {
        A= NULL;
    } else {
        char *s = strdupcase(B.s, B.len);
        A = NewTagNode(s, strlen(s));
        QueryTagNode_AddChildren(A, C->pn.children, C->pn.numChildren);
        
        // Set the children count on C to 0 so they won't get recursively free'd
        C->pn.numChildren = 0;
        QueryNode_Free(C);
    }
}

tag_list(A) ::= LB term(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, NewTokenNode(ctx, strdupcase(B.s, B.len), -1));
}

tag_list(A) ::= LB prefix(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, B);
}

tag_list(A) ::= LB termlist(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryPhraseNode_AddChild(A, B);
}

tag_list(A) ::= tag_list(B) OR term(C) . [TAGLIST] {
    QueryPhraseNode_AddChild(B, NewTokenNode(ctx, strdupcase(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR prefix(C) . [TAGLIST] {
    QueryPhraseNode_AddChild(B, C);
    A = B;
}

tag_list(A) ::= tag_list(B) OR termlist(C) . [TAGLIST] {
    QueryPhraseNode_AddChild(B, C);
    A = B;
}


tag_list(A) ::= tag_list(B) RB . [TAGLIST] {
    A = B;
}


/////////////////////////////////////////////////////////////////
// Numeric Ranges
/////////////////////////////////////////////////////////////////
expr(A) ::= modifier(B) COLON numeric_range(C). {
    // we keep the capitalization as is
    C->fieldName = strndup(B.s, B.len);
    A = NewNumericNode(C);
}

numeric_range(A) ::= LSQB num(B) num(C) RSQB. [NUMBER] {
    A = NewNumericFilter(B.num, C.num, B.inclusive, C.inclusive);
}

/////////////////////////////////////////////////////////////////
// Geo Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON geo_filter(C). {
    // we keep the capitalization as is
    C->property = strndup(B.s, B.len);
    A = NewGeofilterNode(C);
}

geo_filter(A) ::= LSQB num(B) num(C) num(D) TERM(E) RSQB. [NUMBER] {
    A = NewGeoFilter(B.num, C.num, D.num, strdupcase(E.s, E.len));
    char *err = NULL;
    if (!GeoFilter_IsValid(A, &err)) {
        ctx->ok = 0;
        ctx->errorMsg = strdup(err);
    }
}


/////////////////////////////////////////////////////////////////
// Primitives - numbers and strings
/////////////////////////////////////////////////////////////////
num(A) ::= NUMBER(B). {
    A.num = B.numval;
    A.inclusive = 1;
}

num(A) ::= LP num(B). {
    A=B;
    A.inclusive = 0;
}

num(A) ::= MINUS num(B). {
    B.num = -B.num;
    A = B;
}

term(A) ::= TERM(B) . {
    A = B; 
}

term(A) ::= NUMBER(B) . {
    A = B; 
}

