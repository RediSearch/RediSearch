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

#define RSQuery_Parse_v3 RSQueryParser_v3_ // weird Lemon quirk.. oh well..
#define RSQuery_ParseAlloc_v3 RSQueryParser_v3_Alloc
#define RSQuery_ParseFree_v3 RSQueryParser_v3_Free

void RSQuery_Parse_v3(void *yyp, int yymajor, QueryToken yyminor, QueryParseCtx *ctx);
void *RSQuery_ParseAlloc_v3(void *(*mallocProc)(size_t));
void RSQuery_ParseFree_v3(void *p, void (*freeProc)(void *));

%%{

machine query;

inf = ('+'|'-')? 'inf'i $ 4;
size = digit+ $ 2;
number = ('+'|'-')? (digit+('.' digit+)? | ('.' digit+)) (('E'|'e') ('+'|'-')? digit+)? $ 3;

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
plus = '+';
tilde = '~';
star = '*';
percent = '%';
not_equal = '!=';
equal_equal = '==';
equal = '=';
gt = '>';
ge = '>=';
lt = '<';
le = '<=';
rsqb = ']';
lsqb = '[';
escape = '\\';
squote = "'";
escaped_character = escape (punct | space | escape);
escaped_term = (((any - (punct | cntrl | space | escape)) | escaped_character) | '_')+ $0;
empty_string = quote.quote | squote.squote;

# these are the punctuations that are not valid in a tag, they have special
# meaning and need to be escaped to be considered as part of a tag
tag_invalid_punct = (rb | star | escape | '$');

mod = '@'.escaped_term $ 1;
attr = '$'.escaped_term $ 1;
single_tag = ( (any - (tag_invalid_punct) ) | (escape (tag_invalid_punct)) )+ $2;

contains = (star.escaped_term.star | star.number.star | star.attr.star) $1;
prefix = (escaped_term.star | number.star | attr.star) $1;
suffix = (star.escaped_term | star.number | star.attr) $1;
as = 'as'i;
verbatim = squote . ((any - squote - escape) | escape.any)+ . squote $2;
wildcard = 'w' . verbatim $2;
ismissing = 'ismissing'i $1;

assign_attr = arrow lb attr colon escaped_term rb $2;

contains_tag = lb (star.single_tag.star | star.attr.star) rb $1;
prefix_tag = lb (single_tag.star | attr.star) rb $1;
suffix_tag = lb (star.single_tag | star.attr) rb $1;
empty_tag = lb empty_string rb $1;

# in case that the expression enclosed by {} matches the wildcard format, we
# need to escape it to be considered as a tag.
# i.e.: "@tag:{w'foo'}" will be considered as a wildcard (rule wildcard_tag), 
# while "@tag:{\\w'foo'}" will be considered as a tag (rule unescaped_tag2).
unescaped_tag1 = lb single_tag rb $1;
unescaped_tag2 = lb escape.wildcard rb $1;
wildcard_tag = lb wildcard rb $1;
wildcard_txt = lp wildcard rp $1;

main := |*

  size => { 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, SIZE, tok, q);
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
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  mod => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v3(pParser, MODIFIER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  attr => {
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v3(pParser, ATTRIBUTE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  arrow => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts+1;
    RSQuery_Parse_v3(pParser, ARROW, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  as => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, AS_T, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  inf => { 
    tok.pos = ts-q->raw;
    tok.s = ts;
    tok.len = te-ts;
    tok.numval = *ts == '-' ? -INFINITY : INFINITY;
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  empty_string => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, EMPTY_STRING, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  
  quote => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  not_equal => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, NOT_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  equal_equal => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, EQUAL_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  equal => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  gt => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  ge => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  lt => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  le => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LE, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  lt => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  or => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  lp => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  rp => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  lb => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  rb => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  colon => { 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
   };

  semicolon => { 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
   };

  minus =>  { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  plus =>  { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PLUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  tilde => { 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  star => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  percent => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  lsqb => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  rsqb => {
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  space;

  punct => {
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };
  
  cntrl;

  ismissing => {
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, ISMISSING, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  escaped_term => {
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  wildcard_tag => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 3 + is_attr) - 2;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  empty_tag => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 0;
    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, EMPTY_STRING, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  unescaped_tag1 => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = te - (ts + 2);
    tok.s = ts + 1;

    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }
    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
    tok.pos = tok.s - q->raw;
    tok.type = QT_TERM;
    RSQuery_Parse_v3(pParser, UNESCAPED_TAG, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  unescaped_tag2 => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = te - (ts + 2);
    tok.s = ts + 1;

    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }

    // remove escape character before 'w'
    tok.s++;
    tok.len--;

    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
    tok.pos = tok.s - q->raw;
    tok.type = QT_TERM;
    RSQuery_Parse_v3(pParser, UNESCAPED_TAG, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  suffix_tag => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    int is_attr = (*(ts + 2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 2 + is_attr) - 1;
    tok.s = ts + 2 + is_attr;
    tok.pos = tok.s - q->raw;
    
    // we don't remove the leading spaces, because the suffix starts when
    // '*' is found, then spaces are part of the tag

    // Invalid case: wildcard and suffix
    if(tok.len > 1 && tok.s[0] == 'w' && tok.s[1] == '\'') {
      fbreak;
    }

    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
    RSQuery_Parse_v3(pParser, SUFFIX_TAG, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  prefix_tag => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr) - 2;
    tok.s = ts + 1 + is_attr;
    tok.pos = tok.s - q->raw;

    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }

    // Invalid case: wildcard and prefix
    if(tok.len > 1 && tok.s[0] == 'w' && tok.s[1] == '\'') {
      fbreak;
    }

    // we don't remove the trailing spaces, because the prefix ends when
    // '*' is found, then the spaces are part of the tag.

    RSQuery_Parse_v3(pParser, PREFIX_TAG, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  contains_tag => {
    tok.numval = 0;
    tok.len = 1;

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    int is_attr = (*(ts + 2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 2 + is_attr) - 2;
    tok.s = ts + 2 + is_attr;
    tok.pos = tok.s - q->raw;

    // we don't remove leading/trailing spaces, all the text enclosed by the '*'
    // is part of the tag

    // Invalid case: wildcard and contains
    if(tok.len > 1 && tok.s[0] == 'w' && tok.s[1] == '\'') {
      fbreak;
    }

    RSQuery_Parse_v3(pParser, CONTAINS_TAG, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
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
    RSQuery_Parse_v3(pParser, PREFIX, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  suffix => {
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, SUFFIX, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  contains => {
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 2 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, CONTAINS, tok, q);
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
    RSQuery_Parse_v3(pParser, VERBATIM, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

  wildcard_txt => {
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 3 + is_attr) - 2;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
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
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      fbreak;
    }
  };

*|;
}%%

%% write data;

QueryNode *RSQuery_ParseRaw_v3(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v3(rm_malloc);

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
    RSQuery_Parse_v3(pParser, 0, tok, q);
  }
  RSQuery_ParseFree_v3(pParser, rm_free);
  if (!QPCTX_ISOK(q) && q->root) {
    QueryNode_Free(q->root);
    q->root = NULL;
  }
  return q->root;
}

