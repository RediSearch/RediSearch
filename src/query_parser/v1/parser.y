/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
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
%left PREFIX SUFFIX CONTAINS.
%left PERCENT.
%left ATTRIBUTE.
%right LP.
%left RP.
// needs to be above lp/rp
%left MODIFIER.
%left AND.
%left OR.
%left ORX.
%left ARROW.

%token_type {QueryToken}

%name RSQueryParser_v1_

%syntax_error {
    QueryError_SetWithUserDataFmt(ctx->status, QUERY_ESYNTAX,
        "Syntax error", " at offset %d near %.*s",
        TOKEN.pos, TOKEN.len, TOKEN.s);
}

%include {

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "../parse.h"
#include "util/arr.h"
#include "rmutil/vector.h"
#include "query_node.h"

// unescape a string (non null terminated) and return the new length (may be shorter than the original. This manipulates the string itself
static size_t unescapen(char *s, size_t sz) {

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

#define NODENN_BOTH_VALID 0
#define NODENN_BOTH_INVALID -1
#define NODENN_ONE_NULL 1
// Returns:
// 0 if a && b
// -1 if !a && !b
// 1 if a ^ b (i.e. !(a&&b||!a||!b)). The result is stored in `out`
static int one_not_null(void *a, void *b, void *out) {
    if (a && b) {
        return NODENN_BOTH_VALID;
    } else if (a == NULL && b == NULL) {
        return NODENN_BOTH_INVALID;
    } if (a) {
        *(void **)out = a;
        return NODENN_ONE_NULL;
    } else {
        *(void **)out = b;
        return NODENN_ONE_NULL;
    }
}

// optimize NOT nodes: NOT(NOT(A)) = A
// if the child is a NOT node, return its child instead of creating a double negation
static inline struct RSQueryNode* not_step(struct RSQueryNode* child) {
    if (!child) {
        return NULL;
    }

    // If the child is a NOT node, return its child (double negation elimination)
    if (child->type == QN_NOT) {
        struct RSQueryNode* grandchild = child->children[0];
        // Detach the grandchild from its parent to prevent it from being freed
        child->children[0] = NULL;
        // Free the NOT node (the parent)
        QueryNode_Free(child);
        return grandchild;
    }

    // Otherwise, create a new NOT node
    return NewNotNode(child);
}

} // END %include

%extra_argument { QueryParseCtx *ctx }
%default_type { QueryToken }
%default_destructor { }

%type expr { QueryNode * }
%destructor expr { QueryNode_Free($$); }

%type attribute { QueryAttribute }
%destructor attribute { rm_free((char*)$$.value); }

%type attribute_list {QueryAttribute *}
%destructor attribute_list { array_free_ex($$, rm_free((char*)((QueryAttribute*)ptr )->value)); }

%type affix { QueryNode * }
%destructor affix { QueryNode_Free($$); }

%type termlist { QueryNode * }
%destructor termlist { QueryNode_Free($$); }

%type union { QueryNode *}
%destructor union { QueryNode_Free($$); }

%type fuzzy { QueryNode *}
%destructor fuzzy { QueryNode_Free($$); }

%type tag_list { QueryNode *}
%destructor tag_list { QueryNode_Free($$); }

// v2.2.9 diff - geo_filter type changed to match current functions usage
%type geo_filter { QueryParam *}
%destructor geo_filter { QueryParam_Free($$); }

%type modifierlist { Vector* }
%destructor modifierlist {
    for (size_t i = 0; i < Vector_Size($$); i++) {
        char *s;
        Vector_Get($$, i, &s);
        rm_free(s);
    }
    Vector_Free($$);
}

%type num { RangeNumber }

// v2.2.9 diff - numeric_range type changed to match current functions usage
%type numeric_range { QueryParam * }
%destructor numeric_range { QueryParam_Free($$); }

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
    int rv = one_not_null(B, C, (void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        A = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- `out` is already assigned
    } else {
        if (B && B->type == QN_PHRASE && B->pn.exact == 0 &&
            B->opts.fieldMask == RS_FIELDMASK_ALL ) {
            A = B;
        } else {
            A = NewPhraseNode(0);
            QueryNode_AddChild(A, B);
        }
        QueryNode_AddChild(A, C);
    }
}


/////////////////////////////////////////////////////////////////
// Unions
/////////////////////////////////////////////////////////////////

expr(A) ::= union(B) . [ORX] {
    A = B;
}

union(A) ::= expr(B) OR expr(C) . [OR] {
    int rv = one_not_null(B, C, (void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        A = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (B->type == QN_UNION && B->opts.fieldMask == RS_FIELDMASK_ALL) {
            A = B;
        } else {
            A = NewUnionNode();
            QueryNode_AddChild(A, B);
        }

        // Handle C
        QueryNode_AddChild(A, C);
    }
}

union(A) ::= union(B) OR expr(C). [ORX] {
    int rv = one_not_null(B, C, (void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        A = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (B->type == QN_UNION && B->opts.fieldMask == RS_FIELDMASK_ALL) {
            A = B;
        } else {
            A = NewUnionNode();
            QueryNode_AddChild(A, B);
        }

        // Handle C
        QueryNode_AddChild(A, C);
    }
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


expr(A) ::= modifierlist(B) COLON expr(C) . [MODIFIER] {

    if (C == NULL) {
        for (size_t i = 0; i < Vector_Size(B); i++) {
          char *s;
          Vector_Get(B, i, &s);
          rm_free(s);
        }
        Vector_Free(B);
        A = NULL;
    } else {
        //C->opts.fieldMask = 0;
        t_fieldMask mask = 0;
        for (int i = 0; i < Vector_Size(B); i++) {
            char *p;
            Vector_Get(B, i, &p);
            if (ctx->sctx->spec) {
              mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p));
            }
            rm_free(p);
        }
        Vector_Free(B);
        QueryNode_SetFieldMask(C, mask);
        A=C;
    }
}

expr(A) ::= LP expr(B) RP . {
    A = B;
}

/////////////////////////////////////////////////////////////////
// Attributes
/////////////////////////////////////////////////////////////////

attribute(A) ::= ATTRIBUTE(B) COLON term(C). {
    A = (QueryAttribute){ .name = B.s, .namelen = B.len, .value = rm_strndup(C.s, C.len), .vallen = C.len };
}

attribute_list(A) ::= attribute(B) . {
    A = array_new(QueryAttribute, 2);
    array_append(A, B);
}

attribute_list(A) ::= attribute_list(B) SEMICOLON attribute(C) . {
    array_append(B, C);
    A = B;
}

attribute_list(A) ::= attribute_list(B) SEMICOLON . {
    A = B;
}

attribute_list(A) ::= . {
    A = NULL;
}

expr(A) ::= expr(B) ARROW  LB attribute_list(C) RB . {

    if (B && C) {
        QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
    }
    array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));
    A = B;
}

/////////////////////////////////////////////////////////////////
// Term Lists
/////////////////////////////////////////////////////////////////

expr(A) ::= QUOTE termlist(B) QUOTE. [TERMLIST] {
    B->pn.exact =1;
    B->opts.flags |= QueryNode_Verbatim;

    A = B;
}

expr(A) ::= QUOTE term(B) QUOTE. [TERMLIST] {
    A = NewTokenNode(ctx, rm_normalize(B.s, B.len), -1);
    A->opts.flags |= QueryNode_Verbatim;

}

expr(A) ::= term(B) . [LOWEST]  {
   A = NewTokenNode(ctx, rm_normalize(B.s, B.len), -1);
}

expr(A) ::= affix(B) . [PREFIX]  {
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
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_normalize(B.s, B.len), -1));
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_normalize(C.s, C.len), -1));
}

termlist(A) ::= termlist(B) term(C) . [TERMLIST] {
    A = B;
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_normalize(C.s, C.len), -1));
}

termlist(A) ::= termlist(B) STOPWORD . [TERMLIST] {
    A = B;
}

/////////////////////////////////////////////////////////////////
// Negative Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= MINUS expr(B) . {
    A = not_step(B);
}

/////////////////////////////////////////////////////////////////
// Optional Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= TILDE expr(B) . {
    if (B) {
        A = NewOptionalNode(B);
    } else {
        A = NULL;
    }
}

/////////////////////////////////////////////////////////////////
// Prefix expressions
/////////////////////////////////////////////////////////////////

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
affix(A) ::= PREFIX(B) . [PREFIX] {
    A = NewPrefixNode_WithParams(ctx, &B, true, false);
}

affix(A) ::= SUFFIX(B) . [PREFIX] {
    A = NewPrefixNode_WithParams(ctx, &B, false, true);
}

affix(A) ::= CONTAINS(B) . [PREFIX] {
    A = NewPrefixNode_WithParams(ctx, &B, true, true);
}

/////////////////////////////////////////////////////////////////
// Fuzzy terms
/////////////////////////////////////////////////////////////////

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::=  PERCENT term(B) PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 1);
}

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::= PERCENT PERCENT term(B) PERCENT PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 2);
}

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::= PERCENT PERCENT PERCENT term(B) PERCENT PERCENT PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 3);
}

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::=  PERCENT STOPWORD(B) PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 1);
}

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::= PERCENT PERCENT STOPWORD(B) PERCENT PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 2);
}

// v2.2.9 diff - string duplication are happening in NewPrefixNode_WithParams now.
expr(A) ::= PERCENT PERCENT PERCENT STOPWORD(B) PERCENT PERCENT PERCENT. [PREFIX] {
    A = NewFuzzyNode_WithParams(ctx, &B, 3);
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
    char *s = rm_strndup(B.s, B.len);
    Vector_Push(A, s);
    s = rm_strndup(C.s, C.len);
    Vector_Push(A, s);
}

modifierlist(A) ::= modifierlist(B) OR term(C). {
    char *s = rm_strndup(C.s, C.len);
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
        A = NewTagNode(NULL);
        QueryNode_AddChildren(A, C->children, QueryNode_NumChildren(C));

        // Set the children count on C to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(C, 0);
        QueryNode_Free(C);

        if (ctx->sctx->spec) {
            // Tag field names must be case sensitive, we can't do strdupcase
            B.len = unescapen((char*)B.s, B.len);
            A->tag.fs = IndexSpec_GetFieldWithLength(ctx->sctx->spec, B.s, B.len);
            if (!A->tag.fs) {
                QueryNode_Free(A);
                A = NULL;
            }
        }
    }
}

tag_list(A) ::= LB term(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_strndup(B.s, B.len), -1));
}

tag_list(A) ::= LB STOPWORD(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_strndup(B.s, B.len), -1));
}

tag_list(A) ::= LB affix(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, B);
}

tag_list(A) ::= LB termlist(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, B);
}

tag_list(A) ::= tag_list(B) OR term(C) . [TAGLIST] {
    QueryNode_AddChild(B, NewTokenNode(ctx, rm_strndup(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR STOPWORD(C) . [TAGLIST] {
    QueryNode_AddChild(B, NewTokenNode(ctx, rm_strndup(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR affix(C) . [TAGLIST] {
    QueryNode_AddChild(B, C);
    A = B;
}

tag_list(A) ::= tag_list(B) OR termlist(C) . [TAGLIST] {
    QueryNode_AddChild(B, C);
    A = B;
}


tag_list(A) ::= tag_list(B) RB . [TAGLIST] {
    A = B;
}


/////////////////////////////////////////////////////////////////
// Numeric Ranges
/////////////////////////////////////////////////////////////////
// v2.2.9 diff - geo_filter type changed to match current functions usage
expr(A) ::= modifier(B) COLON numeric_range(C). {
    // we keep the capitalization as is
    A = NULL;
    const FieldSpec *fs = ctx->sctx->spec ? IndexSpec_GetFieldWithLength(ctx->sctx->spec, B.s, B.len) : NULL;
    if (fs) {
        A = NewNumericNode(C, fs);
    } else if (C) {
        QueryParam_Free(C);
        C = NULL;
    }
}

// v2.2.9 diff - geo_filter type changed to match current functions usage
numeric_range(A) ::= LSQB num(B) num(C) RSQB. [NUMBER] {
  A = NewQueryParam(QP_NUMERIC_FILTER);
  A->nf = NewNumericFilter(B.num, C.num, B.inclusive, C.inclusive, true, NULL);
}

/////////////////////////////////////////////////////////////////
// Geo Filters
/////////////////////////////////////////////////////////////////

// v2.2.9 diff - geo_filter type changed to match current functions usage
expr(A) ::= modifier(B) COLON geo_filter(C). {
    // we keep the capitalization as is
    A = NewGeofilterNode(C);
    if (ctx->sctx->spec) {
        A->gn.gf->fieldSpec = IndexSpec_GetFieldWithLength(ctx->sctx->spec, B.s, B.len);
        if (!A->gn.gf->fieldSpec) {
            QueryNode_Free(A);
            A = NULL;
        }
    }
}

// v2.2.9 diff - geo_filter type changed to match current functions usage
geo_filter(A) ::= LSQB num(B) num(C) num(D) TERM(E) RSQB. [NUMBER] {
    A = NewQueryParam(QP_GEO_FILTER);
    A->gf = NewGeoFilter(B.num, C.num, D.num, E.s, E.len);
    GeoFilter_Validate(A->gf, ctx->status);
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
