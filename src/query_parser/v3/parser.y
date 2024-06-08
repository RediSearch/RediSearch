/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


// The priorities here are very important. please modify with care and test your changes!

%left LOWEST.

%left PUNCTUATION.
%left TEXTEXPR.

%left ORX.
%left OR.

%left EMPTY_STRING ISMISSING.
%left MODIFIER.

%left RP RB RSQB.

%left UNESCAPED_TAG.
%left TERM.
%left QUOTE.
%left LP LB LSQB.

%left TILDE MINUS PLUS.
%left AND.

%left EQUAL.
%left NOT_EQUAL EQUAL_EQUAL.
%left GE GT LE LT.

%left ARROW.
%left COLON.

%left NUMBER.
%left SIZE.
%left STAR.

%left TERMLIST.
%left PREFIX PREFIX_TAG SUFFIX SUFFIX_TAG CONTAINS CONTAINS_TAG.
%left PERCENT.
%left ATTRIBUTE.
%left VERBATIM WILDCARD.

// Thanks to these fallback directives, Any "as" appearing in the query,
// other than in a vector_query, Will either be considered as a term,
// if "as" (for instance) is not a stop-word, Or be considered as a stop-word if it is a stop-word.
%fallback TERM AS_T ISMISSING.

%token_type {QueryToken}

%name RSQueryParser_v3_

%stack_size 256

%stack_overflow {
  QueryError_SetErrorFmt(ctx->status, QUERY_ESYNTAX,
    "Parser stack overflow. Try moving nested parentheses more to the left");
}

%syntax_error {
  QueryError_SetErrorFmt(ctx->status, QUERY_ESYNTAX,
    "Syntax error at offset %d near %.*s",
    TOKEN.pos, TOKEN.len, TOKEN.s);
}

%include {

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "../parse.h"

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

static void setup_trace(QueryParseCtx *ctx) {
#ifdef PARSER_DEBUG
  void RSQueryParser_Trace(FILE*, char*);
  ctx->trace_log = fopen("/tmp/lemon_query.log", "w");
  RSQueryParser_Trace(ctx->trace_log, "tr: ");
#endif
}

static void reportSyntaxError(QueryError *status, QueryToken* tok, const char *msg) {
  if (tok->type == QT_TERM || tok->type == QT_TERM_CASE) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %.*s", msg, tok->pos, tok->len, tok->s);
  } else if (tok->type == QT_NUMERIC) {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %f", msg, tok->pos, tok->numval);
  } else {
    QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "%s at offset %d", msg, tok->pos);
  }
}

#define AND_EXPRESSION(B, C, OUT) \
    int rv = one_not_null(B, C, (void**)&OUT); \
    if (rv == NODENN_BOTH_INVALID) { \
        OUT = NULL; \
    } else if (rv == NODENN_ONE_NULL) { \
        /* Nothing- `OUT` is already assigned */ \
    } else { \
        if (B && B->type == QN_PHRASE && B->pn.exact == 0 && \
            B->opts.fieldMask == RS_FIELDMASK_ALL ) { \
            OUT = B; \
        } else { \
            OUT = NewPhraseNode(0); \
            QueryNode_AddChild(OUT, B); \
        } \
        QueryNode_AddChild(OUT, C); \
    }

#define OR_EXPRESSION(B, C, OUT) \
    int rv = one_not_null(B, C, (void**)&OUT); \
    if (rv == NODENN_BOTH_INVALID) { \
        OUT = NULL; \
    } else if (rv == NODENN_ONE_NULL) { \
        /* Nothing- already assigned */ \
    } else { \
        if (B && B->type == QN_UNION && B->opts.fieldMask == RS_FIELDMASK_ALL) { \
            OUT = B; \
        } else { \
            OUT = NewUnionNode(); \
            QueryNode_AddChild(OUT, B); \
            OUT->opts.fieldMask |= B->opts.fieldMask; \
        } \
        /* Handle C */ \
        QueryNode_AddChild(OUT, C); \
        OUT->opts.fieldMask |= C->opts.fieldMask; \
        QueryNode_SetFieldMask(OUT, OUT->opts.fieldMask); \
    }

} // END %include

%extra_argument { QueryParseCtx *ctx }
%default_type { QueryToken }
%default_destructor { }

// Notice about the %destructor directive:
// If a non-terminal is used by C-code, e.g., expr(A)
// then %destructor code will not be called for it
// (C-code is responsible for destroying it)
// Unless during error handling

%type expr { QueryNode * }
%destructor expr { QueryNode_Free($$); }

%type attribute { QueryAttribute }
%destructor attribute { rm_free((char*)$$.value); }

%type attribute_list {QueryAttribute *}
%destructor attribute_list { array_free_ex($$, rm_free((char*)((QueryAttribute*)ptr )->value)); }

%type affix { QueryNode * }
%destructor affix { QueryNode_Free($$); }

%type affix_tag { QueryNode * }
%destructor affix_tag { QueryNode_Free($$); }

%type suffix { QueryNode * }
%destructor suffix { QueryNode_Free($$); }

%type contains { QueryNode * }
%destructor contains { QueryNode_Free($$); }

%type verbatim { QueryNode * }
%destructor verbatim { QueryNode_Free($$); }

%type termlist { QueryNode * }
%destructor termlist { QueryNode_Free($$); }

%type union { QueryNode *}
%destructor union { QueryNode_Free($$); }

%type text_union { QueryNode *}
%destructor text_union { QueryNode_Free($$); }

%type text_expr { QueryNode * }
%destructor text_expr { QueryNode_Free($$); }

%type unaryop_text_expr { QueryNode * }
%destructor unaryop_text_expr { QueryNode_Free($$); }

%type fuzzy { QueryNode *}
%destructor fuzzy { QueryNode_Free($$); }

%type single_tag { QueryNode *}
%destructor single_tag { QueryNode_Free($$); }

%type geo_filter { QueryParam *}
%destructor geo_filter { QueryParam_Free($$); }

%type geometry_query { QueryNode *}
%destructor geometry_query { QueryNode_Free($$); }

%type vector_query { QueryNode *}
%destructor vector_query { QueryNode_Free($$); }

%type vector_command { QueryNode *}
%destructor vector_command { QueryNode_Free($$); }

%type vector_range_command { QueryNode *}
%destructor vector_range_command { QueryNode_Free($$); }

%type vector_attribute { SingleVectorQueryParam }
// This destructor is commented out because it's not reachable: every vector_attribute that created
// successfuly can successfuly be reduced to vector_attribute_list.
// %destructor vector_attribute { rm_free((char*)($$.param.value)); rm_free((char*)($$.param.name)); }

%type vector_attribute_list { VectorQueryParams }
%destructor vector_attribute_list {
  array_free($$.needResolve);
  array_free_ex($$.params, {
    rm_free((char*)((VecSimRawParam*)ptr)->value);
    rm_free((char*)((VecSimRawParam*)ptr)->name);
  });
}

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

%type numeric_range { QueryParam * }
%destructor numeric_range {
  QueryParam_Free($$);
}

%type numeric_operator { QueryParam * }
%destructor numeric_operator {
  QueryParam_Free($$);
}

query ::= expr(A) . {
  setup_trace(ctx);
  ctx->root = A;
}

query ::= . {
  ctx->root = NULL;
}

query ::= star . {
  setup_trace(ctx);
  ctx->root = NewWildcardNode();
}

star ::= STAR.

star ::= LP star RP.

// This rule switches from text contex to regular context.
// In general, we want to stay in text contex as long as we can (mostly for use of field modifiers).
expr(A) ::= text_expr(B). [TEXTEXPR] {
  A = B;
}

expr(A) ::= unaryop_text_expr(B). [TEXTEXPR] {
  A = B;
}
/////////////////////////////////////////////////////////////////
// AND Clause / Phrase
/////////////////////////////////////////////////////////////////

expr(A) ::= expr(B) expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

// This rule is needed for queries like "hello (world @loc:[15.65 -15.65 30 ft])", when we discover too late that
// inside the parentheses there is expr and not text_expr. this can lead to right recursion ONLY with parentheses.
expr(A) ::= text_expr(B) expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

expr(A) ::= expr(B) text_expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

// This rule is identical to "expr ::= expr expr",  "expr ::= text_expr expr", "expr ::= expr text_expr",
// but keeps the text context
text_expr(A) ::= text_expr(B) text_expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

text_expr(A) ::= text_expr(B) unaryop_text_expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

text_expr(A) ::= unaryop_text_expr(B) text_expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

text_expr(A) ::= unaryop_text_expr(B) unaryop_text_expr(C) . [AND] {
  AND_EXPRESSION(B, C, A);
}

/////////////////////////////////////////////////////////////////
// Unions
/////////////////////////////////////////////////////////////////

expr(A) ::= union(B) . [ORX] {
  A = B;
}

union(A) ::= expr(B) OR expr(C) . [OR] {
    OR_EXPRESSION(B, C, A);
}

union(A) ::= union(B) OR expr(C). [OR] {
    A = B;
    if (C) {
        QueryNode_AddChild(A, C);
        A->opts.fieldMask |= C->opts.fieldMask;
        QueryNode_SetFieldMask(C, A->opts.fieldMask);
    }
}

// This rule is needed for queries like "hello|(world @loc:[15.65 -15.65 30 ft])", when we discover too late that
// inside the parentheses there is expr and not text_expr. this can lead to right recursion ONLY with parentheses.
union(A) ::= text_expr(B) OR expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

union(A) ::= expr(B) OR text_expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

text_expr(A) ::= text_union(B) . [ORX] {
  A = B;
}

// This rule is identical to "union ::= expr OR expr", but keeps the text context.
text_union(A) ::= text_expr(B) OR text_expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

text_union(A) ::= unaryop_text_expr(B) OR text_expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

text_union(A) ::= unaryop_text_expr(B) OR unaryop_text_expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

text_union(A) ::= text_expr(B) OR unaryop_text_expr(C) . [OR] {
  OR_EXPRESSION(B, C, A);
}

text_union(A) ::= text_union(B) OR text_expr(C). [OR] {
    A = B;
    if (C) {
        QueryNode_AddChild(A, C);
        A->opts.fieldMask |= C->opts.fieldMask;
        QueryNode_SetFieldMask(C, A->opts.fieldMask);
    }
}

/////////////////////////////////////////////////////////////////
// Text Field Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON text_expr(C) . {
    if (C == NULL) {
        A = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(C, IndexSpec_GetFieldBit(ctx->sctx->spec, B.s, B.len));
        }
        A = C;
    }
}

expr(A) ::= modifier(B) COLON LP text_expr(C) RP . {
    if (C == NULL) {
        A = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(C, IndexSpec_GetFieldBit(ctx->sctx->spec, B.s, B.len));
        }
        A = C;
    }
}

expr(A) ::= modifier(B) COLON LP unaryop_text_expr(C) RP . {
    if (C == NULL) {
        A = NULL;
    } else {
        if (ctx->sctx->spec) {
            QueryNode_SetFieldMask(C, IndexSpec_GetFieldBit(ctx->sctx->spec, B.s, B.len));
        }
        A = C;
    }
}

expr(A) ::= modifierlist(B) COLON text_expr(C) . {

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

text_expr(A) ::= LP text_expr(B) RP . {
  A = B;
}

text_expr(A) ::= LP unaryop_text_expr(B) RP . {
  A = B;
}

/////////////////////////////////////////////////////////////////
// Attributes
/////////////////////////////////////////////////////////////////

attribute(A) ::= ATTRIBUTE(B) COLON param_term(C). {
  const char *value = rm_strndup(C.s, C.len);
  size_t value_len = C.len;
  if (C.type == QT_PARAM_TERM) {
    size_t found_value_len;
    const char *found_value = Param_DictGet(ctx->opts->params, value, &found_value_len, ctx->status);
    if (found_value) {
      rm_free((char*)value);
      value = rm_strndup(found_value, found_value_len);
      value_len = found_value_len;
    }
  }
  A = (QueryAttribute){ .name = B.s, .namelen = B.len, .value = value, .vallen = value_len };
}

attribute_list(A) ::= attribute(B) . {
  A = array_new(QueryAttribute, 2);
  A = array_append(A, B);
}

attribute_list(A) ::= attribute_list(B) SEMICOLON attribute(C) . {
  A = array_append(B, C);
}

attribute_list(A) ::= attribute_list(B) SEMICOLON . {
  A = B;
}

attribute_list(A) ::= . {
  A = NULL;
}

expr(A) ::= expr(B) ARROW LB attribute_list(C) RB . {

    if (B && C) {
        QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
    }
    array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));
    A = B;
}

text_expr(A) ::= text_expr(B) ARROW LB attribute_list(C) RB . {

    if (B && C) {
        QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
    }
    array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));
    A = B;
}

/////////////////////////////////////////////////////////////////
// Term Lists
/////////////////////////////////////////////////////////////////

text_expr(A) ::= QUOTE termlist(B) QUOTE. [TERMLIST] {
  // TODO: Quoted/verbatim string in termlist should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  B->pn.exact = 1;
  B->opts.flags |= QueryNode_Verbatim;

  A = B;
}

text_expr(A) ::= QUOTE term(B) QUOTE. [TERMLIST] {
  A = NewTokenNode(ctx, rm_strdupcase(B.s, B.len), -1);
  A->opts.flags |= QueryNode_Verbatim;
}

text_expr(A) ::= QUOTE ATTRIBUTE(B) QUOTE. [TERMLIST] {
  // Quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(B.len + 1);
  *s = '$';
  memcpy(s + 1, B.s, B.len);
  A = NewTokenNode(ctx, rm_strdupcase(s, B.len + 1), -1);
  rm_free(s);
  A->opts.flags |= QueryNode_Verbatim;
}

text_expr(A) ::= param_term(B) . [LOWEST]  {
  if (B.type == QT_TERM && StopWordList_Contains(ctx->opts->stopwords, B.s, B.len)) {
    A = NULL;
  } else {
    A = NewTokenNode_WithParams(ctx, &B);
  }
}

text_expr(A) ::= affix(B) . [PREFIX]  {
A = B;
}

text_expr(A) ::= verbatim(B) . [VERBATIM]  {
A = B;
}

text_expr(A) ::= EMPTY_STRING . [EMPTY_STRING] {
  char *empty_str = rm_strdup("");
  A = NewTokenNode(ctx, empty_str, 0);
  A->tn.nen = NON_EXIST_EMPTY;
  A->opts.fieldMask == RS_FIELDMASK_ALL;
  // Avoid any expansions
  A->opts.flags |= QueryNode_Verbatim;
}

termlist(A) ::= param_term(B) param_term(C). [TERMLIST]  {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &C));
}

termlist(A) ::= termlist(B) param_term(C) . [TERMLIST] {
    A = B;
    if (!(C.type == QT_TERM && StopWordList_Contains(ctx->opts->stopwords, C.s, C.len))) {
       QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &C));
    }
}


/////////////////////////////////////////////////////////////////
// Negative Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= MINUS expr(B) . {
    if (B) {
        A = NewNotNode(B);
    } else {
        A = NULL;
    }
}

unaryop_text_expr(A) ::= MINUS text_expr(B) . {
    if (B) {
        A = NewNotNode(B);
    } else {
        A = NULL;
    }
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

unaryop_text_expr(A) ::= TILDE text_expr(B) . {
    if (B) {
        A = NewOptionalNode(B);
    } else {
        A = NULL;
    }
}

/////////////////////////////////////////////////////////////////
// Prefix expressions
/////////////////////////////////////////////////////////////////

affix(A) ::= PREFIX(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, true, false);
}

affix(A) ::= SUFFIX(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, false, true);
}

affix(A) ::= CONTAINS(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, true, true);
}

/////////////////////////////////////////////////////////////////
// Prefix expressions based on tags
/////////////////////////////////////////////////////////////////

affix_tag(A) ::= PREFIX_TAG(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, true, false);
}

affix_tag(A) ::= SUFFIX_TAG(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, false, true);
}

affix_tag(A) ::= CONTAINS_TAG(B) . {
    A = NewPrefixNode_WithParams(ctx, &B, true, true);
}

// verbatim(A) ::= VERBATIM(B) . {
//    A = NewVerbatimNode_WithParams(ctx, &B);
// }

verbatim(A) ::= WILDCARD(B) . {
    A = NewWildcardNode_WithParams(ctx, &B);
}

/////////////////////////////////////////////////////////////////
// Fuzzy terms
/////////////////////////////////////////////////////////////////

text_expr(A) ::=  PERCENT param_term(B) PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 1);
}

text_expr(A) ::= PERCENT PERCENT param_term(B) PERCENT PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 2);
}

text_expr(A) ::= PERCENT PERCENT PERCENT param_term(B) PERCENT PERCENT PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 3);
}

/////////////////////////////////////////////////////////////////
// Field Modifiers
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

expr(A) ::= ISMISSING LP modifier(B) RP . {
  char *s = rm_strndup(B.s, B.len);
  size_t slen = unescapen(s, B.len);
  A = NewMissingNode(s, slen);
}

/////////////////////////////////////////////////////////////////
// Single Tag - tag enclosed in curly braces
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON LB single_tag(C) RB . {
    if (!C) {
        A = NULL;
    } else {
      // Tag field names must be case sensitive, we can't do rm_strdupcase
        char *s = rm_strndup(B.s, B.len);
        size_t slen = unescapen(s, B.len);

        A = NewTagNode(s, slen);
        QueryNode_AddChildren(A, C->children, QueryNode_NumChildren(C));

        // Set the children count on C to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(C, 0);
        QueryNode_Free(C);
    }
}

single_tag(A) ::= ATTRIBUTE(B) . {
  A = NewPhraseNode(0);
  B.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
}

single_tag(A) ::= UNESCAPED_TAG(B) . {
  A = NewPhraseNode(0);
  B.type = QT_TERM_CASE;
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
}

single_tag(A) ::= affix_tag(B) . {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, B);
}

single_tag(A) ::= verbatim(B) . {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, B);
}

// empty string as single tag
expr(A) ::= modifier(B) COLON LB EMPTY_STRING RB . {
  char *s = rm_strndup(B.s, B.len);
  size_t slen = unescapen(s, B.len);

  const FieldSpec *fs = IndexSpec_GetField(ctx->sctx->spec, s, slen);
  if (!fs) {
    // Non-existing field
    A = NULL;
    rm_free(s);
  } else {
    switch (fs->types) {
      case INDEXFLD_T_TAG:
        A = NewTagNode(s, slen);
        A->tag.nen = NON_EXIST_EMPTY;
        break;
      default:
        A = NULL;
        rm_free(s);
        break;
    }
  }
}

/////////////////////////////////////////////////////////////////
// Numeric Ranges
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON numeric_range(C). {
  if (C) {
    // we keep the capitalization as is
    C->nf->fieldName = rm_strndup(B.s, B.len);
    A = NewNumericNode(C);
  } else {
    A = NewQueryNode(QN_NULL);
  }
}

numeric_range(A) ::= LSQB param_num(B) param_num(C) RSQB. [NUMBER]{
  if (B.type == QT_PARAM_NUMERIC) {
    B.type = QT_PARAM_NUMERIC_MIN_RANGE;
  }
  if (C.type == QT_PARAM_NUMERIC) {
    C.type = QT_PARAM_NUMERIC_MAX_RANGE;
  }
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &C, 1, 1);
}

numeric_range(A) ::= LSQB exclusive_param_num(B) param_num(C) RSQB. [NUMBER]{
  if (B.type == QT_PARAM_NUMERIC) {
    B.type = QT_PARAM_NUMERIC_MIN_RANGE;
  }
  if (C.type == QT_PARAM_NUMERIC) {
    C.type = QT_PARAM_NUMERIC_MAX_RANGE;
  }
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &C, 0, 1);
}

numeric_range(A) ::= LSQB param_num(B) exclusive_param_num(C) RSQB. [NUMBER]{
  if (B.type == QT_PARAM_NUMERIC) {
    B.type = QT_PARAM_NUMERIC_MIN_RANGE;
  }
  if (C.type == QT_PARAM_NUMERIC) {
    C.type = QT_PARAM_NUMERIC_MAX_RANGE;
  }
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &C, 1, 0);
}

numeric_range(A) ::= LSQB exclusive_param_num(B) exclusive_param_num(C) RSQB. [NUMBER]{
  if (B.type == QT_PARAM_NUMERIC) {
    B.type = QT_PARAM_NUMERIC_MIN_RANGE;
  }
  if (C.type == QT_PARAM_NUMERIC) {
    C.type = QT_PARAM_NUMERIC_MAX_RANGE;
  }
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &C, 0, 0);
}

numeric_range(A) ::= LSQB param_num(B) RSQB. [NUMBER]{
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &B, 1, 1);
}

/////////////////////////////////////////////////////////////////
// Numeric Operators
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) NOT_EQUAL param_num(C). {
  QueryParam *D = NewNumericFilterQueryParam_WithParams(ctx, &C, &C, 1, 1);
  // we keep the capitalization as is
  D->nf->fieldName = rm_strndup(B.s, B.len);
  QueryNode* E = NewNumericNode(D);
  A = NewNotNode(E);
}

expr(A) ::= modifier(B) numeric_operator(C). {
  if (C) {
    // we keep the capitalization as is
    C->nf->fieldName = rm_strndup(B.s, B.len);
    A = NewNumericNode(C);
  } else {
    A = NewQueryNode(QN_NULL);
  }
}

numeric_operator(A) ::= EQUAL_EQUAL param_num(B). {
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, &B, 1, 1);
}

numeric_operator(A) ::= GT param_num(B). {
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, NULL, 0, 1);
}

numeric_operator(A) ::= GE param_num(B). {
  A = NewNumericFilterQueryParam_WithParams(ctx, &B, NULL, 1, 1);
}

numeric_operator(A) ::= LT param_num(B). {
  A = NewNumericFilterQueryParam_WithParams(ctx, NULL, &B, 1, 0);
}

numeric_operator(A) ::= LE param_num(B). {
  A = NewNumericFilterQueryParam_WithParams(ctx, NULL, &B, 1, 1);
}

/////////////////////////////////////////////////////////////////
// Geo Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON geo_filter(C). {
  if (C) {
    // we keep the capitalization as is
    C->gf->property = rm_strndup(B.s, B.len);
    A = NewGeofilterNode(C);
  } else {
    A = NewQueryNode(QN_NULL);
  }
}

geo_filter(A) ::= LSQB param_num(B) param_num(C) param_num(D) param_term(E) RSQB. [NUMBER] {
  if (B.type == QT_PARAM_NUMERIC)
    B.type = QT_PARAM_GEO_COORD;
  if (C.type == QT_PARAM_NUMERIC)
    C.type = QT_PARAM_GEO_COORD;

  if (E.type == QT_PARAM_TERM)
    E.type = QT_PARAM_GEO_UNIT;

  A = NewGeoFilterQueryParam_WithParams(ctx, &B, &C, &D, &E);
}

/////////////////////////////////////////////////////////////////
// Geometry Queries
/////////////////////////////////////////////////////////////////
expr(A) ::= modifier(B) COLON geometry_query(C). {
  if (C) {
    // we keep the capitalization as is
    C->gmn.geomq->attr = rm_strndup(B.s, B.len);
    A = C;
  } else {
    A = NewQueryNode(QN_NULL);
  }
}


geometry_query(A) ::= LSQB TERM(B) ATTRIBUTE(C) RSQB . {
  // Geometry param is actually a case sensitive term
  C.type = QT_PARAM_TERM_CASE;
  A = NewGeometryNode_FromWkt_WithParams(ctx, B.s, B.len, &C);
  if (!A) {
    reportSyntaxError(ctx->status, &C, "Syntax error: Expecting a geoshape predicate");
  }
}

/////////////////////////////////////////////////////////////////
// Vector Queries
/////////////////////////////////////////////////////////////////

// expr(A) ::= expr(B) ARROW LSQB vector_query(C) RSQB. {} // main parse, hybrid case.

// expr(A) ::= STAR ARROW LSQB vector_query(B) RSQB . { // main parse, simple vecsim search as subquery case.
//   switch (B->vn.vq->type) {
//     case VECSIM_QT_KNN:
//       B->vn.vq->knn.order = BY_ID;
//       break;
//   }
//   A = B;
// }

query ::= expr(A) ARROW LSQB vector_query(B) RSQB . { // main parse, hybrid query as entire query case.
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  ctx->root = B;
  if (A) {
    QueryNode_AddChild(B, A);
  }
}

query ::= text_expr(A) ARROW LSQB vector_query(B) RSQB . { // main parse, hybrid query as entire query case.
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  ctx->root = B;
  if (A) {
    QueryNode_AddChild(B, A);
  }
}

query ::= star ARROW LSQB vector_query(B) RSQB . { // main parse, simple vecsim search as entire query case.
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  B->vn.vq->knn.order = BY_SCORE;

  ctx->root = B;
}

// Vector query opt. 1 - full query.
vector_query(A) ::= vector_command(B) vector_attribute_list(C) vector_score_field(D). {
  if (B->vn.vq->scoreField) {
    rm_free(B->vn.vq->scoreField);
    B->vn.vq->scoreField = NULL;
  }
  B->params = array_grow(B->params, 1);
  memset(&array_tail(B->params), 0, sizeof(*B->params));
  QueryNode_SetParam(ctx, &(array_tail(B->params)), &(B->vn.vq->scoreField), NULL, &D);
  B->vn.vq->params = C;
  A = B;
}

// Vector query opt. 2 - score field only, no params.
vector_query(A) ::= vector_command(B) vector_score_field(D). {
  if (B->vn.vq->scoreField) {
    rm_free(B->vn.vq->scoreField);
    B->vn.vq->scoreField = NULL;
  }
  B->params = array_grow(B->params, 1);
  memset(&array_tail(B->params), 0, sizeof(*B->params));
  QueryNode_SetParam(ctx, &(array_tail(B->params)), &(B->vn.vq->scoreField), NULL, &D);
  A = B;
}

// Vector query opt. 3 - no score field, params only.
vector_query(A) ::= vector_command(B) vector_attribute_list(C). {
  B->vn.vq->params = C;
  A = B;
}

// Vector query opt. 4 - no score field and no params.
vector_query(A) ::= vector_command(B). {
  A = B;
}

as ::= AS_T.

vector_score_field(A) ::= as param_term_case(B). {
  A = B;
}

// Use query attributes syntax
query ::= expr(A) ARROW LSQB vector_query(B) RSQB ARROW LB attribute_list(C) RB. {
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  ctx->root = B;
  if (B && C) {
     QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
  }
  array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));

  if (A) {
      QueryNode_AddChild(B, A);
  }
}

query ::= text_expr(A) ARROW LSQB vector_query(B) RSQB ARROW LB attribute_list(C) RB. {
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  ctx->root = B;
  if (B && C) {
     QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
  }
  array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));

  if (A) {
    QueryNode_AddChild(B, A);
  }
}

query ::= star ARROW LSQB vector_query(B) RSQB ARROW LB attribute_list(C) RB. {
  setup_trace(ctx);
  RS_LOG_ASSERT(B->vn.vq->type == VECSIM_QT_KNN, "vector_query must be KNN");
  B->vn.vq->knn.order = BY_SCORE;

  ctx->root = B;
  if (B && C) {
     QueryNode_ApplyAttributes(B, C, array_len(C), ctx->status);
  }
  array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr )->value));

}

// Every vector query will have basic command part.
// It is this rule's job to create the new vector node for the query.
vector_command(A) ::= TERM(T) param_size(B) modifier(C) ATTRIBUTE(D). {
  if (T.len == strlen("KNN") && !strncasecmp("KNN", T.s, T.len)) {
    D.type = QT_PARAM_VEC;
    A = NewVectorNode_WithParams(ctx, VECSIM_QT_KNN, &B, &D);
    A->vn.vq->property = rm_strndup(C.s, C.len);
    RedisModule_Assert(-1 != (rm_asprintf(&A->vn.vq->scoreField, "__%.*s_score", C.len, C.s)));
  } else {
    reportSyntaxError(ctx->status, &T, "Syntax error: Expecting Vector Similarity command");
    A = NULL;
  }
}

vector_attribute(A) ::= TERM(B) param_term(C). {
  const char *value = rm_strndup(C.s, C.len);
  const char *name = rm_strndup(B.s, B.len);
  A.param = (VecSimRawParam){ .name = name, .nameLen = B.len, .value = value, .valLen = C.len };
  if (C.type == QT_PARAM_TERM) {
    A.needResolve = true;
  }
  else { // if C.type == QT_TERM
    A.needResolve = false;
  }
}

vector_attribute_list(A) ::= vector_attribute_list(B) vector_attribute(C). {
  A.params = array_append(B.params, C.param);
  A.needResolve = array_append(B.needResolve, C.needResolve);
}

vector_attribute_list(A) ::= vector_attribute(B). {
  A.params = array_new(VecSimRawParam, 1);
  A.needResolve = array_new(bool, 1);
  A.params = array_append(A.params, B.param);
  A.needResolve = array_append(A.needResolve, B.needResolve);
}

/*** Vector range queries ***/
expr(A) ::= modifier(B) COLON LSQB vector_range_command(C) RSQB. {
    C->vn.vq->property = rm_strndup(B.s, B.len);
    A = C;
}

vector_range_command(A) ::= TERM(T) param_num(B) ATTRIBUTE(C). {
  if (T.len == strlen("VECTOR_RANGE") && !strncasecmp("VECTOR_RANGE", T.s, T.len)) {
    C.type = QT_PARAM_VEC;
    A = NewVectorNode_WithParams(ctx, VECSIM_QT_RANGE, &B, &C);
  } else {
    reportSyntaxError(ctx->status, &T, "Syntax error: expecting vector similarity range command");
    A = NULL;
  }
}

/////////////////////////////////////////////////////////////////
// Primitives - numbers and strings
/////////////////////////////////////////////////////////////////

num(A) ::= SIZE(B). {
  A.num = B.numval;
  A.inclusive = 1;
}

num(A) ::= NUMBER(B). {
  A.num = B.numval;
  A.inclusive = 1;
}

num(A) ::= PLUS num(B). {
  A = B;
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

term(A) ::= SIZE(B). {
  A = B;
}

term(A) ::= UNESCAPED_TAG(B) . {
  A = B;
}

///////////////////////////////////////////////////////////////////////////////////
// Parameterized Primitives (actual numeric or string, or a parameter/placeholder)
///////////////////////////////////////////////////////////////////////////////////


// Number is treated as a term here
param_term(A) ::= term(B). {
  A = B;
  A.type = QT_TERM;
}

param_term(A) ::= ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_TERM;
}

param_term_case(A) ::= term(B). {
  A = B;
  A.type = QT_TERM_CASE;
}

param_term_case(A) ::= ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_TERM_CASE;
}

param_size(A) ::= SIZE(B). {
  A = B;
  A.type = QT_SIZE;
}

param_size(A) ::= ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_SIZE;
}

param_num(A) ::= ATTRIBUTE(B). {
    A = B;
    A.sign = 1; // default
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 1;
}

param_num(A) ::= MINUS ATTRIBUTE(B). {
    A = B;
    A.sign = -1;
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 1;
}

param_num(A) ::= PLUS ATTRIBUTE(B). {
    A = B;
    A.sign = 1;
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 1;
}

param_num(A) ::= num(B). {
  A.numval = B.num;
  A.inclusive = B.inclusive;
  A.type = QT_NUMERIC;
}

exclusive_param_num(A) ::= LP num(B). {
  A.numval = B.num;
  A.inclusive = 0;
  A.type = QT_NUMERIC;
}
exclusive_param_num(A) ::= LP ATTRIBUTE(B). {
    A = B;
    A.sign = 1;
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 0;
}

exclusive_param_num(A) ::= LP MINUS ATTRIBUTE(B). {
    A = B;
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 0;
    A.sign = -1;
}

exclusive_param_num(A) ::= LP PLUS ATTRIBUTE(B). {
    A = B;
    A.type = QT_PARAM_NUMERIC;
    A.inclusive = 0;
    A.sign = 1;
}
