/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <math.h>

#include "../parse.h"
#include "parser.h"
#include "../../query_node.h"
#include "../../stopwords.h"

/* forward declarations of stuff generated by lemon */

#define RSQuery_Parse_v2 RSQueryParser_v2_ // weird Lemon quirk.. oh well..
#define RSQuery_ParseAlloc_v2 RSQueryParser_v2_Alloc
#define RSQuery_ParseFree_v2 RSQueryParser_v2_Free

void RSQuery_Parse_v2(void *yyp, int yymajor, QueryToken yyminor, QueryParseCtx *ctx);
void *RSQuery_ParseAlloc_v2(void *(*mallocProc)(size_t));
void RSQuery_ParseFree_v2(void *p, void (*freeProc)(void *));

%%{

machine query;

inf = ['+\-']? 'inf' $ 4;
size = digit+ $ 2;
number = '-'? digit+('.' digit+)? (('E'|'e') '-'? digit+)? $ 3;

quote = '"';
or = '|';
lp = '(';
rp = ')';
lb = '{';
rb = '}';
colon = ':';
semicolon = ';';
arrow = '=>';
minus = '-';
tilde = '~';
star = '*';
percent = '%';
rsqb = ']';
lsqb = '[';
escape = '\\';
squote = "'";
escaped_character = escape (punct | space | escape);
exact = quote . ((any - quote) | (escape.quote))+ . quote;
term = (((any - (punct | cntrl | space | escape)) | escaped_character) | '_')+  $0 ;
empty_string = quote.quote | squote.squote;
mod = '@'.term $ 1;
attr = '$'.term $ 1;
mod_not_equal = '@'.term.(space*).'!=' $ 1;
mod_equal = '@'.term.(space*).'==' $ 1;
mod_gt = '@'.term.(space*).'>' $ 1;
mod_ge = '@'.term.(space*).'>=' $ 1;
mod_lt = '@'.term.(space*).'<' $ 1;
mod_le = '@'.term.(space*).'<=' $ 1;
contains = (star.term.star | star.number.star | star.attr.star) $1;
contains_exact = (star.exact.star) $1;
prefix = (term.star | number.star | attr.star) $1;
prefix_exact = (exact.star) $1;
suffix = (star.term | star.number | star.attr) $1;
suffix_exact = (star.exact) $1;
as = 'as'i;
verbatim = squote . ((any - squote - escape) | escape.any)+ . squote $4;
wildcard = 'w' . verbatim $4;
ismissing = 'ismissing'i $1;

main := |*

  size => { 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, SIZE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  number => { 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  mod => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, MODIFIER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  attr => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, ATTRIBUTE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_not_equal => {
    printf("mod_not_equal\n");
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 2;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1]) 
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_NOT_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_equal => {
    printf("mod_equal\n");
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 2;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_gt => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 1;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_GT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_ge => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 2;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_GE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_lt => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 1;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_LT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod_le => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1) - 2;
    tok.s = ts+1;

    // remove unescaped trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])
            && tok.s[tok.len - 2] != '\\') {
      tok.len--;
    }

    RSQuery_Parse_v2(pParser, MODIFIER_LE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  arrow => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, ARROW, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  as => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    RSQuery_Parse_v2(pParser, AS_T, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  inf => { 
    tok.pos = ts-q->raw;
    tok.s = ts;
    tok.len = te-ts;
    tok.numval = *ts == '-' ? -INFINITY : INFINITY;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  empty_string => {
    tok.pos = ts-q->raw;
    tok.s = "";
    tok.len = 0;
    RSQuery_Parse_v2(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  quote => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  or => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  lp => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  rp => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  lb => {
    // printf("LB\n");
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  rb => {
    // printf("RB\n");
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
   colon => { 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v2(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
   };
    semicolon => { 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v2(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
   };

  minus =>  { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  tilde => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
 star => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
   percent => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  lsqb => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LSQB, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }  
  };
  rsqb => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RSQB, tok, q);   
    if (!QPCTX_ISOK(q)) {
      fbreak;
    } 
  };
  space;
  punct;
  cntrl;
  
  ismissing => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    RSQuery_Parse_v2(pParser, ISMISSING, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  term => {
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  exact => {
    // printf("lexer exact %.*s\n", (int)(te - ts), ts);
    tok.len = te - (ts + 2);
    tok.s = ts + 1;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, EXACT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  prefix => {
    int is_attr = (*ts == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    // printf("prefix: %.*s\n", (int)tok.len, tok.s);

    RSQuery_Parse_v2(pParser, PREFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  prefix_exact => {
    tok.type = QT_TERM;
    tok.len = te - (ts + 3); // remove the quotes and the star at the end
    tok.s = ts + 1; // skip the quote
    tok.numval = 0;
    tok.pos = ts-q->raw;
    // printf("prefix_exact: %.*s\n", (int)tok.len, tok.s);

    RSQuery_Parse_v2(pParser, PREFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  suffix => {
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v2(pParser, SUFFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  suffix_exact => {
    tok.type = QT_TERM;
    tok.len = te - (ts + 3); // remove the quotes and the star at the end
    tok.s = ts + 2; // skip the star and the quote
    tok.numval = 0;
    tok.pos = ts-q->raw;
    // printf("suffix_exact: %.*s\n", (int)tok.len, tok.s);

    RSQuery_Parse_v2(pParser, SUFFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  contains => {
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 2 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v2(pParser, CONTAINS, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  contains_exact => {
    tok.type = QT_TERM;
    tok.len = te - (ts + 4); // remove the quotes and the stars
    tok.s = ts + 2; // skip the star and the quote
    tok.numval = 0;
    tok.pos = ts-q->raw;
    // printf("suffix_exact: %.*s\n", (int)tok.len, tok.s);

    RSQuery_Parse_v2(pParser, CONTAINS, tok, q);

    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  verbatim => {
    int is_attr = (*(ts+2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 2 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    RSQuery_Parse_v2(pParser, VERBATIM, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  wildcard => {
    int is_attr = (*(ts+2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.pos = ts-q->raw + 2;
    tok.len = te - (ts + 3 + is_attr);
    tok.s = ts + 2 + is_attr;
    tok.numval = 0;
    RSQuery_Parse_v2(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  
*|;
}%%

%% write data;

QueryNode *RSQuery_ParseRaw_v2(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v2(rm_malloc);

  
  int cs, act;
  const char* ts = q->raw;          // query start
  const char* te = q->raw + q->len; // query end
  %% write init;
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};
  
  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;
  
  %% write exec;
  
  if (QPCTX_ISOK(q)) {
    RSQuery_Parse_v2(pParser, 0, tok, q);
  }
  RSQuery_ParseFree_v2(pParser, rm_free);
  if (!QPCTX_ISOK(q) && q->root) {
    QueryNode_Free(q->root);
    q->root = NULL;
  }
  return q->root;
}

