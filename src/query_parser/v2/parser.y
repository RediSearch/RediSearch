/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


// The priorities here are very important. please modify with care and test your changes!

%left LOWEST.

%left TEXTEXPR.

%left ORX.
%left OR.

%left ISMISSING.
%left MODIFIER.

%left RP RB RSQB.

%left EXACT.
%left TERM.
%left QUOTE SQUOTE.
%left LP LB LSQB.

%left TILDE MINUS.
%left AND.

%left ARROW.
%left COLON.
%left NOT_EQUAL EQUALS.
%left GE GT LE LT.

%left NUMBER.
%left SIZE.
%left STAR.

%left TAGLIST.
%left TERMLIST.
%left PREFIX SUFFIX CONTAINS.
%left PERCENT.
%left ATTRIBUTE.
%left VERBATIM WILDCARD.

// Thanks to these fallback directives, Any "as" appearing in the query,
// other than in a vector_query, Will either be considered as a term,
// if "as" (for instance) is not a stop-word, Or be considered as a stop-word if it is a stop-word.
%fallback TERM EXACT AS_T ISMISSING.

%token_type {QueryToken}

%name RSQueryParser_v2_

%stack_size 256

%stack_overflow {
  QueryError_SetError(ctx->status, QUERY_ESYNTAX,
    "Parser stack overflow. Try moving nested parentheses more to the left");
}

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

static struct RSQueryNode* union_step(struct RSQueryNode* B, struct RSQueryNode* C) {
    struct RSQueryNode* A;
    int rv = one_not_null(B, C, (void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        return NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing - `A` is already assigned
    } else {
        struct RSQueryNode* child;
        if (B->type == QN_UNION && B->opts.fieldMask == RS_FIELDMASK_ALL) {
            A = B;
            child = C;
        } else if (C->type == QN_UNION && C->opts.fieldMask == RS_FIELDMASK_ALL) {
            A = C;
            child = B;
        } else {
            A = NewUnionNode();
            QueryNode_AddChild(A, B);
            child = C;
        }
        // Handle child
        QueryNode_AddChild(A, child);
    }
    return A;
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
    QueryError_SetWithoutUserDataFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %.*s", msg, tok->pos, tok->len, tok->s);
  } else if (tok->type == QT_NUMERIC) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ESYNTAX,
      "%s at offset %d near %f", msg, tok->pos, tok->numval);
  } else {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ESYNTAX, msg, " at offset %d", tok->pos);
  }
}

#define REPORT_WRONG_FIELD_TYPE(F, type_literal) \
  reportSyntaxError(ctx->status, &F.tok, "Expected a " type_literal " field")

//! " # % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ ` { | } ~
static const char ToksepParserMap_g[256] = {
    [' '] = 1, ['\t'] = 1, [','] = 1,  ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1, ['{'] = 1,
    ['}'] = 1, ['['] = 1,  [']'] = 1,  [':'] = 1, [';'] = 1, ['~'] = 1, ['!'] = 1, ['@'] = 1,
    ['#'] = 1,             ['%'] = 1,  ['^'] = 1, ['&'] = 1, ['*'] = 1, ['-'] = 1, ['='] = 1,
    ['+'] = 1, ['|'] = 1,  ['\''] = 1, ['`'] = 1, ['"'] = 1, ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

/**
 * Copy of toksep.h function to use a different map
 * Function reads string pointed to by `s` and indicates the length of the next
 * token in `tokLen`. `s` is set to NULL if this is the last token.
 */
static inline char *toksep2(char **s, size_t *tokLen) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  int escaped = 0;
  for (; *pos; ++pos) {
    if (ToksepParserMap_g[*pos] && !escaped) {
      *s = (char *)++pos;
      *tokLen = ((char *)pos - orig) - 1;
      if (!*pos) {
        *s = NULL;
      }
      return orig;
    }
    escaped = !escaped && *pos == '\\';
  }

  // Didn't find a terminating token. Use a simpler length calculation
  *s = NULL;
  *tokLen = (char *)pos - orig;
  return orig;
};

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

%type fuzzy { QueryNode *}
%destructor fuzzy { QueryNode_Free($$); }

%type tag_list { QueryNode *}
%destructor tag_list { QueryNode_Free($$); }

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
// successfully can successfully be reduced to vector_attribute_list.
// %destructor vector_attribute { rm_free((char*)($$.param.value)); rm_free((char*)($$.param.name)); }

%type vector_attribute_list { VectorQueryParams }
%destructor vector_attribute_list {
  array_free($$.needResolve);
  array_free_ex($$.params, {
    rm_free((char*)((VecSimRawParam*)ptr)->value);
    rm_free((char*)((VecSimRawParam*)ptr)->name);
  });
}

%type modifier { FieldName }

%type modifierlist { FieldName* }
%destructor modifierlist {
  array_free($$);
}

%type num { RangeNumber }

%type numeric_range { QueryParam * }
%destructor numeric_range {
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

// This rule switches from text context to regular context.
// In general, we want to stay in text context as long as we can (mostly for use of field modifiers).
expr(A) ::= text_expr(B). [TEXTEXPR] {
  A = B;
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

// This rule is needed for queries like "hello (world @loc:[15.65 -15.65 30 ft])", when we discover too late that
// inside the parentheses there is expr and not text_expr. this can lead to right recursion ONLY with parentheses.
expr(A) ::= text_expr(B) expr(C) . [AND] {
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

expr(A) ::= expr(B) text_expr(C) . [AND] {
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

// This rule is identical to "expr ::= expr expr",  "expr ::= text_expr expr", "expr ::= expr text_expr",
// but keeps the text context
text_expr(A) ::= text_expr(B) text_expr(C) . [AND] {
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
  A = union_step(B, C);
}

union(A) ::= union(B) OR expr(C). [OR] {
  A = union_step(B, C);
}

// This rule is needed for queries like "hello|(world @loc:[15.65 -15.65 30 ft])", when we discover too late that
// inside the parentheses there is expr and not text_expr. this can lead to right recursion ONLY with parentheses.
union(A) ::= text_expr(B) OR expr(C) . [OR] {
  A = union_step(B, C);
}

union(A) ::= expr(B) OR text_expr(C) . [OR] {
  A = union_step(B, C);
}

text_expr(A) ::= text_union(B) . [ORX] {
  A = B;
}

// This rule is identical to "union ::= expr OR expr", but keeps the text context.
text_union(A) ::= text_expr(B) OR text_expr(C) . [OR] {
  A = union_step(B, C);
}

text_union(A) ::= text_union(B) OR text_expr(C). [OR] {
  A = union_step(B, C);
}

/////////////////////////////////////////////////////////////////
// Text Field Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON text_expr(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_FULLTEXT)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_TEXT_STR);
    QueryNode_Free(C);
    A = NULL;
  } else if (C == NULL) {
    A = NULL;
  } else {
    if (ctx->sctx->spec) {
      QueryNode_SetFieldMask(C, FIELD_BIT(B.fs));
    }
    A = C;
  }
}

expr(A) ::= modifierlist(B) COLON text_expr(C) . {
  if (C == NULL) {
    array_free(B);
    A = NULL;
  } else {
    t_fieldMask mask = 0;
    if (ctx->sctx->spec) {
      for (int i = 0; i < array_len(B); i++) {
        mask |= FIELD_BIT(B[i].fs);
      }
    }
    array_free(B);
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

text_expr(A) ::= EXACT(B) . [TERMLIST] {
  char *str = rm_strndup(B.s, B.len);
  char *s = str;

  A = NewPhraseNode(0);

  while (str != NULL) {
    // get the next token
    size_t tokLen = 0;
    char *tok = toksep2(&str, &tokLen);
    if(tokLen > 0) {
      QueryNode *C = NewTokenNode(ctx, rm_normalize(tok, tokLen), -1);
      QueryNode_AddChild(A, C);
    }
  }

  rm_free(s);
  A->pn.exact = 1;
  A->opts.flags |= QueryNode_Verbatim;
}

text_expr(A) ::= QUOTE ATTRIBUTE(B) QUOTE. [TERMLIST] {
  // Quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(B.len + 1);
  *s = '$';
  memcpy(s + 1, B.s, B.len);
  A = NewTokenNode(ctx, rm_normalize(s, B.len + 1), -1);
  rm_free(s);
  A->opts.flags |= QueryNode_Verbatim;
}

text_expr(A) ::= SQUOTE ATTRIBUTE(B) SQUOTE. [TERMLIST] {
  // Single quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(B.len + 1);
  *s = '$';
  memcpy(s + 1, B.s, B.len);
  A = NewTokenNode(ctx, rm_normalize(s, B.len + 1), -1);
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

termlist(A) ::= param_term(B) param_term(C). [TERMLIST]  {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &C));
}

termlist(A) ::= termlist(B) param_term(C) . [TERMLIST] {
  A = B;
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &C));
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

text_expr(A) ::= MINUS text_expr(B) . {
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

text_expr(A) ::= TILDE text_expr(B) . {
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

// verbatim(A) ::= VERBATIM(B) . {
//   A = NewVerbatimNode_WithParams(ctx, &B);
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
  A.tok = B;
  if (ctx->sctx->spec) {
    A.fs = IndexSpec_GetFieldWithLength(ctx->sctx->spec, B.s, B.len);
    if (!A.fs) {
      reportSyntaxError(ctx->status, &A.tok, "Unknown field");
    }
  }
}

modifierlist(A) ::= modifier(B) OR term(C). {
  if (ctx->sctx->spec) {
    if (!FIELD_IS(B.fs, INDEXFLD_T_FULLTEXT)) {
      REPORT_WRONG_FIELD_TYPE(B, SPEC_TEXT_STR);
      A = NULL;
    } else {
      FieldName second = { .tok = C, .fs = IndexSpec_GetFieldWithLength(ctx->sctx->spec, C.s, C.len) };
      if (!second.fs) {
        reportSyntaxError(ctx->status, &second.tok, "Unknown field");
        A = NULL;
      } else if (!FIELD_IS(second.fs, INDEXFLD_T_FULLTEXT)) {
        REPORT_WRONG_FIELD_TYPE(second, SPEC_TEXT_STR);
        A = NULL;
      } else {
        A = array_new(FieldName, 2);
        array_append(A, B);
        array_append(A, second);
      }
    }
  } else {
    A = array_new(FieldName, 2);
    array_append(A, B);
    FieldName second = { .tok = C };
    array_append(A, second);
  }
}


modifierlist(A) ::= modifierlist(B) OR term(C). {
  if (ctx->sctx->spec) {
    FieldName second = { .tok = C, .fs = IndexSpec_GetFieldWithLength(ctx->sctx->spec, C.s, C.len) };
    if (!second.fs) {
      reportSyntaxError(ctx->status, &second.tok, "Unknown field");
      array_free(B);
      A = NULL;
    } else if (!FIELD_IS(second.fs, INDEXFLD_T_FULLTEXT)) {
      REPORT_WRONG_FIELD_TYPE(second, SPEC_TEXT_STR);
      array_free(B);
      A = NULL;
    } else {
      A = B;
      array_append(A, second);
    }
  } else {
    A = B;
    FieldName second = { .tok = C };
    array_append(A, second);
  }
}

expr(A) ::= ISMISSING LP modifier(B) RP . {
  if (ctx->sctx->spec && !FieldSpec_IndexesMissing(B.fs)) {
    reportSyntaxError(ctx->status, &B.tok, "'ismissing' requires defining the field with '" SPEC_INDEXMISSING_STR "'");
    A = NULL;
  } else {
    A = NewMissingNode(B.fs);
  }
}

/////////////////////////////////////////////////////////////////
// Tag Lists - curly braces separated lists of words
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON LB tag_list(C) RB . {
  A = NULL;
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_TAG)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_TAG_STR);
    QueryNode_Free(C);
  } else if (C) {
    A = NewTagNode(B.fs);
    QueryNode_AddChildren(A, C->children, QueryNode_NumChildren(C));

    // Set the children count on C to 0 so they won't get recursively free'd
    QueryNode_ClearChildren(C, 0);
    QueryNode_Free(C);
  }
}

tag_list(A) ::= param_term_case(B) . [TAGLIST] {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
}

tag_list(A) ::= affix(B) . [TAGLIST] {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, B);
}

tag_list(A) ::= verbatim(B) . [TAGLIST] {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, B);
}

tag_list(A) ::= termlist(B) . [TAGLIST] {
  A = NewPhraseNode(0);
  QueryNode_AddChild(A, B);
}

tag_list(A) ::= tag_list(B) OR param_term_case(C) . [TAGLIST] {
  QueryNode_AddChild(B, NewTokenNode_WithParams(ctx, &C));
  A = B;
}

tag_list(A) ::= tag_list(B) OR affix(C) . [TAGLIST] {
  QueryNode_AddChild(B, C);
  A = B;
}

tag_list(A) ::= tag_list(B) OR verbatim(C) . [TAGLIST] {
  QueryNode_AddChild(B, C);
  A = B;
}

tag_list(A) ::= tag_list(B) OR termlist(C) . [TAGLIST] {
  QueryNode_AddChild(B, C);
  A = B;
}

/////////////////////////////////////////////////////////////////
// Numeric Ranges
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON numeric_range(C). {
  A = NULL;
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    QueryParam_Free(C);
  } else if (C) {
    // we keep the capitalization as is
    A = NewNumericNode(C, B.fs);
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

expr(A) ::= modifier(B) NOT_EQUAL param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, &C, &C, 1, 1);
    QueryNode* E = NewNumericNode(qp, B.fs);
    A = NewNotNode(E);
  }
}

expr(A) ::= modifier(B) EQUALS param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, &C, &C, 1, 1);
    A = NewNumericNode(qp, B.fs);
  }
}

expr(A) ::= modifier(B) GT param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, &C, NULL, 0, 1);
    A = NewNumericNode(qp, B.fs);
  }
}

expr(A) ::= modifier(B) GE param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, &C, NULL, 1, 1);
    A = NewNumericNode(qp, B.fs);
  }
}

expr(A) ::= modifier(B) LT param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, NULL, &C, 1, 0);
    A = NewNumericNode(qp, B.fs);
  }
}

expr(A) ::= modifier(B) LE param_num(C) . {
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_NUMERIC)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_NUMERIC_STR);
    A = NULL;
  } else {
    QueryParam *qp = NewNumericFilterQueryParam_WithParams(ctx, NULL, &C, 1, 1);
    A = NewNumericNode(qp, B.fs);
  }
}

/////////////////////////////////////////////////////////////////
// Geo Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON geo_filter(C). {
  A = NULL;
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_GEO)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_GEO_STR);
    QueryParam_Free(C);
  } else if (C) {
    // we keep the capitalization as is
    C->gf->fieldSpec = B.fs;
    A = NewGeofilterNode(C);
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
  A = NULL;
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_GEOMETRY)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_GEOMETRY_STR);
    QueryNode_Free(C);
  } else if (C) {
    // we keep the capitalization as is
    C->gmn.geomq->fs = B.fs;
    A = C;
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
  array_free_ex(C, rm_free((char*)((QueryAttribute*)ptr)->value));

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
  if (ctx->sctx->spec && !FIELD_IS(C.fs, INDEXFLD_T_VECTOR)) {
    REPORT_WRONG_FIELD_TYPE(C, SPEC_VECTOR_STR);
    A = NULL;
  } else if (T.len == strlen("KNN") && !strncasecmp("KNN", T.s, T.len)) {
    D.type = QT_PARAM_VEC;
    A = NewVectorNode_WithParams(ctx, VECSIM_QT_KNN, &B, &D);
    A->vn.vq->field = C.fs;
    int n_written = rm_asprintf(&A->vn.vq->scoreField, "__%.*s_score", C.tok.len, C.tok.s);
    RS_ASSERT(n_written != -1);
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
  array_append(B.params, C.param);
  array_append(B.needResolve, C.needResolve);
  A.params = B.params;
  A.needResolve = B.needResolve;
}

vector_attribute_list(A) ::= vector_attribute(B). {
  A.params = array_new(VecSimRawParam, 1);
  A.needResolve = array_new(bool, 1);
  array_append(A.params, B.param);
  array_append(A.needResolve, B.needResolve);
}

/*** Vector range queries ***/
expr(A) ::= modifier(B) COLON LSQB vector_range_command(C) RSQB. {
  A = NULL;
  if (ctx->sctx->spec && !FIELD_IS(B.fs, INDEXFLD_T_VECTOR)) {
    REPORT_WRONG_FIELD_TYPE(B, SPEC_VECTOR_STR);
    QueryNode_Free(C);
  } else if (C) {
    C->vn.vq->field = B.fs;
    A = C;
  }
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
}

num(A) ::= NUMBER(B). {
  A.num = B.numval;
}

num(A) ::= MINUS num(B). {
  B.num = -B.num;
  A = B;
}

term(A) ::= TERM(B) . {
  A = B;
  A.type = QT_TERM;
}

term(A) ::= NUMBER(B) . {
  A = B;
  A.type = QT_NUMERIC;
}

term(A) ::= SIZE(B). {
  A = B;
  A.type = QT_SIZE;
}

///////////////////////////////////////////////////////////////////////////////////
// Parameterized Primitives (actual numeric or string, or a parameter/placeholder)
///////////////////////////////////////////////////////////////////////////////////

param_term(A) ::= term(B). {
  A = B;
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
  A.type = QT_PARAM_NUMERIC;
}

param_num(A) ::= MINUS ATTRIBUTE(B). {
  A = B;
  A.sign = -1;
  A.type = QT_PARAM_NUMERIC;
}

param_num(A) ::= num(B). {
  A.numval = B.num;
  A.type = QT_NUMERIC;
}

exclusive_param_num(A) ::= LP num(B). {
  A.numval = B.num;
  A.type = QT_NUMERIC;
}

exclusive_param_num(A) ::= LP ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_NUMERIC;
}

exclusive_param_num(A) ::= LP MINUS ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_NUMERIC;
  A.sign = -1;
}
