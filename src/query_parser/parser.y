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
    ctx->status->SetErrorFmt(QUERY_ESYNTAX,
        "Syntax error at offset %d near %.*s",
        TOKEN.pos, TOKEN.len, TOKEN.s);
}

///////////////////////////////////////////////////////////////////////////////////////////////

%include {

#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include "parse.h"
#include "util/arr.h"
#include "rmutil/vector.h"
#include "query_node.h"
#include "tokenizer.h"

//---------------------------------------------------------------------------------------------

// strndup + lowercase in one pass

char *strdupcase(const char *s, size_t len) {
  char *ret = rm_strndup(s, len);
  char *dst = ret;
  char *src = dst;
  while (*src) {
      // unescape
      if (*src == '\\' && (ispunct(src[1]) || isspace(src[1]))) {
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

//---------------------------------------------------------------------------------------------

String str_unescape_lcase(const char *s, size_t len) {
    String ss;
    while (len--) {
        // unescape
        if (*s == '\\' && (ispunct(s[1]) || isspace(s[1]))) {
            ++s;
            continue;
        }
        ss.push_back(tolower(*s));
    }
    return ss;
}

//---------------------------------------------------------------------------------------------

// unescape a string (non null terminated) and return the new length (may be shorter than the original.
// This manipulates the string itself.

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

//---------------------------------------------------------------------------------------------

String str_unescape(char *s, size_t len) {
    String ss;
    while (len--) {
      // unescape
      if (*s == '\\' && (ispunct(s[1]) || isspace(s[1]))) {
          ++s;
          continue;
      }
      ss.push_back(*s);
  }
  return ss;
}

//---------------------------------------------------------------------------------------------

#define NODENN_BOTH_VALID 0
#define NODENN_BOTH_INVALID -1
#define NODENN_ONE_NULL 1

// Returns:
// 0 if a && b
// -1 if !a && !b
// 1 if a ^ b (i.e. !(a&&b||!a||!b)). The result is stored in `out`

static int one_not_null(void *a, void *b, void *&out) {
    if (a && b) {
        return NODENN_BOTH_VALID;
    } else if (a == NULL && b == NULL) {
        return NODENN_BOTH_INVALID;
    } if (a) {
        out = a;
        return NODENN_ONE_NULL;
    } else {
        out = b;
        return NODENN_ONE_NULL;
    }
}

//---------------------------------------------------------------------------------------------

} // END %include

///////////////////////////////////////////////////////////////////////////////////////////////

%extra_argument { QueryParse *ctx }
%default_type { QueryToken }
%default_destructor { }

%type expr { QueryNode * }
%destructor expr { delete $$; }

%type attribute { QueryAttribute * }
%destructor attribute { delete $$; }

%type attribute_list { QueryAttributes * }
%destructor attribute_list { delete $$;; }

%type prefix { QueryNode * }
%destructor prefix { delete $$; }

%type termlist { QueryNode * }
%destructor termlist { delete $$; }

%type union { QueryNode *}
%destructor union { delete $$; }

%type fuzzy { QueryNode * }
%destructor fuzzy { delete $$; }

%type tag_list { QueryNode * }
%destructor tag_list { delete $$; }

%type geo_filter { GeoFilter * }
%destructor geo_filter { delete $$; }

%type modifierlist { Vector<String> * }
%destructor modifierlist {
    delete $$;
}

%type num { RangeNumber }

%type numeric_range { NumericFilter * }
%destructor numeric_range {
    delete $$;
}

query ::= expr(A) . {
    // If the root is a negative node, we intersect it with a wildcard node
    ctx->root = A;
}

query ::= . {
    ctx->root = NULL;
}

query ::= STAR . {
    ctx->root = new QueryWildcardNode();
}

/////////////////////////////////////////////////////////////////
// AND Clause / Phrase
/////////////////////////////////////////////////////////////////

expr(A) ::= expr(B) expr(C) . [AND] {
    int rv = one_not_null(B, C, *(void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        A = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- `out` is already assigned
    } else {
        if (B && B->type == QN_PHRASE && B->opts.fieldMask == RS_FIELDMASK_ALL) {
            QueryPhraseNode *pn = dynamic_cast<QueryPhraseNode*>(B);
            if (!pn) {
                throw Error("Invalid node: not PhraseNode");
            }
            if (!pn->exact) {
                A = B;
            } else {
                A = new QueryPhraseNode(0);
                A->AddChild(B);
            }
        } else {
            A = new QueryPhraseNode(0);
            A->AddChild(B);
        }
        A->AddChild(C);
    }
}

/////////////////////////////////////////////////////////////////
// Unions
/////////////////////////////////////////////////////////////////

expr(A) ::= union(B) . [ORX] {
    A = B;
}

union(A) ::= expr(B) OR expr(C) . [OR] {
    int rv = one_not_null(B, C, *(void**)&A);
    if (rv == NODENN_BOTH_INVALID) {
        A = NULL;
    } else if (rv == NODENN_ONE_NULL) {
        // Nothing- already assigned
    } else {
        if (B->type == QN_UNION && B->opts.fieldMask == RS_FIELDMASK_ALL) {
            A = B;
        } else {
            A = new QueryUnionNode();
            A->AddChild(B);
            A->opts.fieldMask |= B->opts.fieldMask;
        }

        // Handle C
        A->AddChild(C);
        A->opts.fieldMask |= C->opts.fieldMask;
        A->SetFieldMask(A->opts.fieldMask);
    }

}

union(A) ::= union(B) OR expr(C). [ORX] {
    A = B;
    if (C) {
        A->AddChild(C);
        A->opts.fieldMask |= C->opts.fieldMask;
        C->SetFieldMask(A->opts.fieldMask);
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
            C->SetFieldMask(ctx->sctx->spec->GetFieldBit(B.s));
        }
        A = C;
    }
}


expr(A) ::= modifierlist(B) COLON expr(C) . [MODIFIER] {
    if (C == NULL) {
        A = NULL;
    } else {
        t_fieldMask mask = 0;
        if (ctx->sctx->spec && B) {
            for (auto &mod: *B) {
                mask |= ctx->sctx->spec->GetFieldBit(mod.c_str());
            }
        }
        C->SetFieldMask(mask);
        delete B;
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

    A = new QueryAttribute{B.s, B.len, C.s, C.len};
}

attribute_list(A) ::= attribute(B) . {
    A = new QueryAttributes();
    A->push_back(B);
}

attribute_list(A) ::= attribute_list(B) SEMICOLON attribute(C) . {
    B->push_back(C);
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
        B->ApplyAttributes(C, ctx->status);
    }
    A = B;
}

/////////////////////////////////////////////////////////////////
// Term Lists
/////////////////////////////////////////////////////////////////

expr(A) ::= QUOTE termlist(B) QUOTE. [TERMLIST] {
    QueryPhraseNode *pn = dynamic_cast<QueryPhraseNode*>(B);
    if (!pn) {
        throw Error("Invalid node: not PhraseNode");
    }
    pn->exact = true;
    B->opts.flags |= QueryNode_Verbatim;
    A = B;
}

expr(A) ::= QUOTE term(B) QUOTE. [TERMLIST] {
    A = new QueryTokenNode(ctx, str_unescape_lcase(B.s, B.len));
    A->opts.flags |= QueryNode_Verbatim;
}

expr(A) ::= term(B) . [LOWEST]  {
   A = new QueryTokenNode(ctx, str_unescape_lcase(B.s, B.len));
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
    A = new QueryPhraseNode(0);
    A->AddChild(new QueryTokenNode(ctx, str_unescape_lcase(B.s, B.len)));
    A->AddChild(new QueryTokenNode(ctx, str_unescape_lcase(C.s, C.len)));
}

termlist(A) ::= termlist(B) term(C) . [TERMLIST] {
    A = B;
    A->AddChild(new QueryTokenNode(ctx, str_unescape_lcase(C.s, C.len)));
}

termlist(A) ::= termlist(B) STOPWORD . [TERMLIST] {
    A = B;
}

/////////////////////////////////////////////////////////////////
// Negative Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= MINUS expr(B) . {
    if (B) {
        A = new QueryNotNode(B);
    } else {
        A = NULL;
    }
}

/////////////////////////////////////////////////////////////////
// Optional Clause
/////////////////////////////////////////////////////////////////

expr(A) ::= TILDE expr(B) . {
    if (B) {
        A = new QueryOptionalNode(B);
    } else {
        A = NULL;
    }
}

/////////////////////////////////////////////////////////////////
// Prefix experessions
/////////////////////////////////////////////////////////////////

prefix(A) ::= PREFIX(B) . [PREFIX] {
    A = new QueryPrefixNode(ctx, str_unescape_lcase(B.s, B.len));
}

/////////////////////////////////////////////////////////////////
// Fuzzy terms
/////////////////////////////////////////////////////////////////

expr(A) ::=  PERCENT term(B) PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 1);
}

expr(A) ::= PERCENT PERCENT term(B) PERCENT PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 2);
}

expr(A) ::= PERCENT PERCENT PERCENT term(B) PERCENT PERCENT PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 3);
}

expr(A) ::=  PERCENT STOPWORD(B) PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 1);
}

expr(A) ::= PERCENT PERCENT STOPWORD(B) PERCENT PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 2);
}

expr(A) ::= PERCENT PERCENT PERCENT STOPWORD(B) PERCENT PERCENT PERCENT. [PREFIX] {
    A = new QueryFuzzyNode(ctx, str_unescape_lcase(B.s, B.len), 3);
}


/////////////////////////////////////////////////////////////////
// Field Modidiers
/////////////////////////////////////////////////////////////////

modifier(A) ::= MODIFIER(B) . {
    B.len = unescapen((char*)B.s, B.len);
    A = B;
 }

modifierlist(A) ::= modifier(B) OR term(C). {
    A = new Vector<String>(2);
    A->emplace_back(str_unescape_lcase(B.s, B.len));
    A->emplace_back(str_unescape_lcase(C.s, C.len));
}

modifierlist(A) ::= modifierlist(B) OR term(C). {
    B->push_back(str_unescape_lcase(C.s, C.len));
    A = B;
}


/////////////////////////////////////////////////////////////////
// Tag Lists - curly braces separated lists of words
/////////////////////////////////////////////////////////////////
expr(A) ::= modifier(B) COLON tag_list(C) . {
    if (!C) {
        A= NULL;
    } else {
        // Tag field names must be case sensitive, we we can't do strdupcase
        A = new QueryTagNode(str_unescape(B.s, B.len));
        A->AddChildren(C->children); // we transfer ownership of C->children

        // Set the children count on C to 0 so they won't get recursively free'd
        // C->ClearChildren(false);
        delete C;
    }
}

tag_list(A) ::= LB term(B) . [TAGLIST] {
    A = new QueryPhraseNode(0);
    A->AddChild(new QueryTokenNode(ctx, str_unescape_lcase(B.s, B.len)));
}

tag_list(A) ::= LB STOPWORD(B) . [TAGLIST] {
    A = NewPhraseNode(0);
    A->AddChild(new QueryTokenNode(ctx, strdupcase(B.s, B.len), -1));
}

tag_list(A) ::= LB prefix(B) . [TAGLIST] {
    A = new QueryPhraseNode(0);
    A->AddChild(B);
}

tag_list(A) ::= LB termlist(B) . [TAGLIST] {
    A = new QueryPhraseNode(0);
    A->AddChild(B);
}

tag_list(A) ::= tag_list(B) OR term(C) . [TAGLIST] {
    B->AddChild(new QueryTokenNode(ctx, strdupcase(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR STOPWORD(C) . [TAGLIST] {
    B->AddChild(new QueryTokenNode(ctx, strdupcase(C.s, C.len), -1));
    A = B;
}

tag_list(A) ::= tag_list(B) OR prefix(C) . [TAGLIST] {
    B->AddChild(C);
    A = B;
}

tag_list(A) ::= tag_list(B) OR termlist(C) . [TAGLIST] {
    B->AddChild(C);
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
    C->fieldName = rm_strndup(B.s, B.len);
    A = new QueryNumericNode(C);
}

numeric_range(A) ::= LSQB num(B) num(C) RSQB. [NUMBER] {
    A = new NumericFilter(B.num, C.num, B.inclusive, C.inclusive);
}

/////////////////////////////////////////////////////////////////
// Geo Filters
/////////////////////////////////////////////////////////////////

expr(A) ::= modifier(B) COLON geo_filter(C). {
    // we keep the capitalization as is
    C->property = rm_strndup(B.s, B.len);
    A = new QueryGeofilterNode(C);
}

geo_filter(A) ::= LSQB num(B) num(C) num(D) TERM(E) RSQB. [NUMBER] {
    char buf[16] = {0};
    if (E.len < 16) {
        memcpy(buf, E.s, E.len);
    } else {
        strcpy(buf, "INVALID");
    }
    A = new GeoFilter(B.num, C.num, D.num, buf);
    A->Validate(ctx->status);
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

///////////////////////////////////////////////////////////////////////////////////////////////
