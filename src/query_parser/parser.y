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

%name RSQueryParser_

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

#include "parse.h"
#include "util/arr.h"
#include "rmutil/vector.h"
#include "query_node.h"
#include "vector_index.h"
#include "query_param.h"
#include "query_internal.h"
#include "util/strconv.h"

// strndup + lowercase in one pass!
char *strdupcase(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
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

void setup_trace(QueryParseCtx *ctx) {
#ifdef PARSER_DEBUG
  void RSQueryParser_Trace(FILE*, char*);
  ctx->trace_log = fopen("/tmp/lemon_query.log", "w");
  RSQueryParser_Trace(ctx->trace_log, "tr: ");
#endif
}

void reportSyntaxError(QueryError *status, QueryToken* tok, const char *msg) {
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

} // END %include  

%extra_argument { QueryParseCtx *ctx }
%default_type { QueryToken }
%default_destructor { }

// Notice about the %destructor directive:
// If a non-terminal is used by C-code, e.g., expr(A)
// then %destructor code will bot be called for it
// (C-code is responsible for destroying it)

%type expr { QueryNode * } 
%destructor expr { QueryNode_Free($$); }

%type attribute { QueryAttribute }
%destructor attribute { rm_free((char*)$$.value); }

%type attribute_list {QueryAttribute *}
%destructor attribute_list { array_free_ex($$, rm_free((char*)((QueryAttribute*)ptr )->value)); }

%type prefix { QueryNode * } 
%destructor prefix { QueryNode_Free($$); }

%type termlist { QueryNode * } 
%destructor termlist { QueryNode_Free($$); }

%type union { QueryNode *}
%destructor union { QueryNode_Free($$); }

%type fuzzy { QueryNode *}
%destructor fuzzy { QueryNode_Free($$); }

%type tag_list { QueryNode *}
%destructor tag_list { QueryNode_Free($$); }

//%type 
%type geo_filter { QueryParam *}
%destructor geo_filter { QueryParam_Free($$); }

%type vector_filter { QueryParam *}
%destructor vector_filter { QueryParam_Free($$); }

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

query ::= expr(A) . { 
  setup_trace(ctx);
  ctx->root = A;
 
}
query ::= . {
  ctx->root = NULL;
}

query ::= STAR . {
  setup_trace(ctx);
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
            A->opts.fieldMask |= B->opts.fieldMask;
        }

        // Handle C
        QueryNode_AddChild(A, C);
        A->opts.fieldMask |= C->opts.fieldMask;
        QueryNode_SetFieldMask(A, A->opts.fieldMask);
    }
    
}

union(A) ::= union(B) OR expr(C). [ORX] {
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
        A = NULL;
    } else {
        //C->opts.fieldMask = 0;
        t_fieldMask mask = 0; 
        if (ctx->sctx->spec) {
            for (int i = 0; i < Vector_Size(B); i++) {
                char *p;
                Vector_Get(B, i, &p);
                mask |= IndexSpec_GetFieldBit(ctx->sctx->spec, p, strlen(p)); 
                rm_free(p);
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
// Attributes
/////////////////////////////////////////////////////////////////

attribute(A) ::= ATTRIBUTE(B) COLON param_term(C). {
  const char *value = C.s;
  size_t value_len = C.len;
  if (C.type == QT_PARAM_TERM) {
    size_t found_value_len;
    const char *found_value = Param_DictGet(ctx->opts->params, value, &found_value_len, ctx->status);
    if (found_value) {
      value = found_value;
      value_len = found_value_len;
    }
  }
  A = (QueryAttribute){ .name = B.s, .namelen = B.len, .value = rm_strndup(value, value_len), .vallen = value_len };
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
  // TODO: Quoted/verbatim string in termlist should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  B->pn.exact = 1;
  B->opts.flags |= QueryNode_Verbatim;

  A = B;
}

expr(A) ::= QUOTE term(B) QUOTE. [TERMLIST] {
  A = NewTokenNode(ctx, rm_strdupcase(B.s, B.len), -1);
  A->opts.flags |= QueryNode_Verbatim;
}

expr(A) ::= QUOTE ATTRIBUTE(B) QUOTE. [TERMLIST] {
  // Quoted/verbatim string should not be handled as parameters
  // Also need to add the leading '$' which was consumed by the lexer
  char *s = rm_malloc(B.len + 1);
  *s = '$';
  memcpy(s + 1, B.s, B.len);
  A = NewTokenNode(ctx, rm_strdupcase(s, B.len + 1), -1);
  rm_free(s);
  A->opts.flags |= QueryNode_Verbatim;
}

expr(A) ::= param_term(B) . [LOWEST]  {
  A = NewTokenNode_WithParams(ctx, &B);
}

expr(A) ::= prefix(B) . [PREFIX]  {
    A = B;
}

expr(A) ::= termlist(B) .  [TERMLIST] {
        A = B;
}

expr(A) ::= STOPWORD . [STOPWORD] {
    A = NULL;
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

termlist(A) ::= termlist(B) STOPWORD . [TERMLIST] {
    A = B;
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
// Prefix experessions
/////////////////////////////////////////////////////////////////

prefix(A) ::= PREFIX(B) . [PREFIX] {
    A = NewPrefixNode_WithParams(ctx, &B);
}

/////////////////////////////////////////////////////////////////
// Fuzzy terms
/////////////////////////////////////////////////////////////////

expr(A) ::=  PERCENT param_term(B) PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 1);
}

expr(A) ::= PERCENT PERCENT param_term(B) PERCENT PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 2);
}

expr(A) ::= PERCENT PERCENT PERCENT param_term(B) PERCENT PERCENT PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 3);
}

expr(A) ::=  PERCENT STOPWORD(B) PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 1);
}

expr(A) ::= PERCENT PERCENT STOPWORD(B) PERCENT PERCENT. [PREFIX] {
  A = NewFuzzyNode_WithParams(ctx, &B, 2);
}

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
      // Tag field names must be case sensitive, we can't do rm_strdupcase
        char *s = rm_strndup(B.s, B.len);
        size_t slen = unescapen((char*)s, B.len);

        A = NewTagNode(s, slen);
        QueryNode_AddChildren(A, C->children, QueryNode_NumChildren(C));
        
        // Set the children count on C to 0 so they won't get recursively free'd
        QueryNode_ClearChildren(C, 0);
        QueryNode_Free(C);
    }
}

tag_list(A) ::= LB param_term(B) . [TAGLIST] {
  A = NewPhraseNode(0);
  if (B.type == QT_TERM)
    B.type = QT_TERM_CASE;
  else if (B.type == QT_PARAM_TERM)
    B.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(A, NewTokenNode_WithParams(ctx, &B));
}

tag_list(A) ::= LB STOPWORD(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, NewTokenNode(ctx, rm_strndup(B.s, B.len), -1));
}

tag_list(A) ::= LB prefix(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, B);
}

tag_list(A) ::= LB termlist(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    QueryNode_AddChild(A, B);
}

tag_list(A) ::= tag_list(B) OR param_term(C) . [TAGLIST] {
  if (C.type == QT_TERM)
    C.type = QT_TERM_CASE;
  else if (C.type == QT_PARAM_TERM)
    C.type = QT_PARAM_TERM_CASE;
  QueryNode_AddChild(B, NewTokenNode_WithParams(ctx, &C));
  A = B;
}

tag_list(A) ::= tag_list(B) OR STOPWORD(C) . [TAGLIST] {
    QueryNode_AddChild(B, NewTokenNode(ctx, rm_strndup(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR prefix(C) . [TAGLIST] {
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
expr(A) ::= modifier(B) COLON numeric_range(C). {
  if (C) {
    // we keep the capitalization as is
    C->nf->fieldName = rm_strndup(B.s, B.len);
    A = NewNumericNode(C);
  } else {
    A = NewQueryNode(QN_NULL);
  }
}

numeric_range(A) ::= LSQB param_any(B) param_any(C) RSQB. [NUMBER] {
  // Update token type to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;
  if (B.type == QT_PARAM_ANY)
    B.type = QT_PARAM_NUMERIC_MIN_RANGE;
  else if (B.type != QT_NUMERIC)
    badToken = &B;
  if (C.type == QT_PARAM_ANY)
    C.type = QT_PARAM_NUMERIC_MAX_RANGE;
  else if (!badToken && C.type != QT_NUMERIC)
    badToken = &C;

  if (!badToken) {
    A = NewNumericFilterQueryParam_WithParams(ctx, &B, &C, B.inclusive, C.inclusive);
  } else {
    reportSyntaxError(ctx->status, badToken, "Expecting numeric or parameter");
    A = NULL;
  }
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

geo_filter(A) ::= LSQB param_any(B) param_any(C) param_any(D) param_any(E) RSQB. [NUMBER] {
  // Update token type to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;

  if (B.type == QT_PARAM_ANY)
    B.type = QT_PARAM_GEO_COORD;
  else if (B.type != QT_NUMERIC)
    badToken = &B;
  if (C.type == QT_PARAM_ANY)
    C.type = QT_PARAM_GEO_COORD;
  else if (!badToken && C.type != QT_NUMERIC)
    badToken = &C;
  if (D.type == QT_PARAM_ANY)
    D.type = QT_PARAM_NUMERIC;
  else if (!badToken && D.type != QT_NUMERIC)
    badToken = &D;
  if (E.type == QT_PARAM_ANY)
    E.type = QT_PARAM_GEO_UNIT;
  else if (!badToken && E.type != QT_TERM)
    badToken = &E;

  if (!badToken) {
    A = NewGeoFilterQueryParam_WithParams(ctx, &B, &C, &D, &E);
  } else {
    reportSyntaxError(ctx->status, badToken, "Syntax error");
    A = NULL;
  }
}

expr(A) ::= modifier(B) COLON vector_filter(C). {
  // we keep the capitalization as is
  if (C) {
      C->vf->property = rm_strndup(B.s, B.len);
      A = NewVectorNode(C);
  } else {
      A = NewQueryNode(QN_NULL);
  }
}


vector_filter(A) ::= LSQB param_any(B) param_any(C) param_any(D) RSQB. [NUMBER] {
  // Update token types to be more specific if possible
  // and detect syntax errors
  QueryToken *badToken = NULL;
  if (B.type == QT_TERM) {
    B.type = QT_VEC;
    // FIXME: Remove hack for handling lexer/scanner of terms with trailing equal signs.
    //  Equal signs are currently considered as punct (punctuation) and are not included in a
    //  term, But in base64 encoding, it is used as padding to extend the string to a length
    //  which is a multiple of 3.
    size_t len = B.len;
    int remainder = len % 3;
    if (remainder == 1 && *((B.s) + len) == '=' && *((B.s) + len + 1) == '=')
      B.len = len + 2;
    else if (remainder == 2 && *((B.s) + len) == '=')
      B.len = len + 1;
  } else if (B.type == QT_PARAM_ANY) {
    B.type = QT_PARAM_VEC;
  } else {
    badToken = &B;
  }

  if (C.type == QT_PARAM_ANY) {
    C.type = QT_PARAM_VEC_SIM_TYPE;
  } else if (!badToken && C.type != QT_TERM) {
    badToken = &C;
  }

  if (D.type == QT_PARAM_ANY) {
    D.type = QT_PARAM_NUMERIC;
  } else if (!badToken && D.type != QT_NUMERIC) {
    badToken = &D;
  }

  if (!badToken) {
    A = NewVectorFilterQueryParam_WithParams(ctx, &B, &C, &D);
  } else {
    reportSyntaxError(ctx->status, badToken, "Syntax error");
    A = NULL;
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

///////////////////////////////////////////////////////////////////////////////////
// Parameterized Primitives (actual numeric or string, or a parameter/placeholder)
///////////////////////////////////////////////////////////////////////////////////

param_term(A) ::= TERM(B). {
  A = B;
  A.type = QT_TERM;
}

param_term(A) ::= NUMBER(B). {
  A = B;
  // Number is treated as a term here
  A.type = QT_TERM;
}

param_term(A) ::= ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_TERM;
}

//For generic parameter (param_any) its `type` could be refined by other rules which may have more accurate semantics,
// e.g., could know it should be numeric

param_any(A) ::= ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_ANY;
  A.inclusive = 1;
}

param_any(A) ::= LP ATTRIBUTE(B). {
  A = B;
  A.type = QT_PARAM_ANY;
  A.inclusive = 0; // Could be relevant if type is refined
}

param_any(A) ::= TERM(B). {
  A = B;
  A.type = QT_TERM;
}

param_any(A) ::= num(B). {
  A.numval = B.num;
  A.inclusive = B.inclusive;
  A.type = QT_NUMERIC;
}
