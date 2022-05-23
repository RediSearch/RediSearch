
/* #line 1 "lexer.rl" */
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


/* #line 263 "lexer.rl" */



/* #line 30 "lexer.c" */
static const char _query_actions[] = {
	0, 1, 0, 1, 1, 1, 12, 1, 
	13, 1, 14, 1, 15, 1, 16, 1, 
	17, 1, 18, 1, 19, 1, 20, 1, 
	21, 1, 22, 1, 23, 1, 24, 1, 
	25, 1, 26, 1, 27, 1, 28, 1, 
	29, 1, 30, 1, 31, 1, 32, 1, 
	33, 1, 34, 1, 35, 1, 36, 1, 
	37, 1, 38, 2, 2, 3, 2, 2, 
	4, 2, 2, 5, 2, 2, 6, 2, 
	2, 7, 2, 2, 8, 2, 2, 9, 
	2, 2, 10, 2, 2, 11
};

static const unsigned char _query_key_offsets[] = {
	0, 10, 20, 21, 22, 24, 27, 29, 
	39, 79, 90, 100, 111, 112, 115, 121, 
	126, 129, 145, 159, 172, 173, 183, 196, 
	206, 218
};

static const char _query_trans_keys[] = {
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 110, 102, 48, 57, 
	45, 48, 57, 48, 57, 9, 13, 32, 
	47, 58, 64, 91, 96, 123, 126, 32, 
	34, 36, 37, 39, 40, 41, 42, 43, 
	45, 58, 59, 61, 64, 65, 91, 92, 
	93, 95, 97, 105, 123, 124, 125, 126, 
	127, 0, 8, 9, 13, 14, 31, 33, 
	47, 48, 57, 60, 63, 94, 96, 42, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 92, 96, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 92, 96, 0, 
	47, 58, 64, 91, 94, 123, 127, 105, 
	105, 48, 57, 42, 46, 69, 101, 48, 
	57, 42, 69, 101, 48, 57, 42, 48, 
	57, 42, 46, 69, 92, 96, 101, 0, 
	47, 48, 57, 58, 64, 91, 94, 123, 
	127, 42, 45, 92, 96, 0, 47, 48, 
	57, 58, 64, 91, 94, 123, 127, 42, 
	92, 96, 0, 47, 48, 57, 58, 64, 
	91, 94, 123, 127, 62, 92, 96, 0, 
	47, 58, 64, 91, 94, 123, 127, 42, 
	83, 92, 96, 115, 0, 47, 58, 64, 
	91, 94, 123, 127, 9, 13, 32, 47, 
	58, 64, 91, 96, 123, 126, 42, 92, 
	96, 110, 0, 47, 58, 64, 91, 94, 
	123, 127, 42, 92, 96, 102, 0, 47, 
	58, 64, 91, 94, 123, 127, 0
};

static const char _query_single_lengths[] = {
	0, 0, 1, 1, 0, 1, 0, 0, 
	26, 3, 2, 3, 1, 1, 4, 3, 
	1, 6, 4, 3, 1, 2, 5, 0, 
	4, 4
};

static const char _query_range_lengths[] = {
	5, 5, 0, 0, 1, 1, 1, 5, 
	7, 4, 4, 4, 0, 1, 1, 1, 
	1, 5, 5, 5, 0, 4, 4, 5, 
	4, 4
};

static const unsigned char _query_index_offsets[] = {
	0, 6, 12, 14, 16, 18, 21, 23, 
	29, 63, 71, 78, 86, 88, 91, 97, 
	102, 105, 117, 127, 136, 138, 145, 155, 
	161, 170
};

static const char _query_indicies[] = {
	1, 1, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 0, 3, 0, 4, 0, 
	5, 0, 7, 8, 6, 8, 0, 9, 
	9, 9, 9, 9, 0, 11, 13, 14, 
	15, 16, 17, 18, 19, 16, 20, 22, 
	23, 24, 25, 26, 27, 28, 29, 30, 
	26, 31, 32, 33, 34, 35, 10, 10, 
	11, 10, 12, 21, 12, 12, 1, 36, 
	37, 0, 0, 0, 0, 0, 1, 39, 
	38, 38, 38, 38, 38, 2, 36, 39, 
	40, 40, 40, 40, 40, 2, 41, 38, 
	41, 43, 42, 36, 45, 46, 46, 43, 
	44, 36, 46, 46, 5, 44, 36, 8, 
	44, 36, 45, 48, 37, 47, 48, 47, 
	21, 47, 47, 47, 1, 36, 7, 37, 
	49, 49, 50, 49, 49, 49, 1, 36, 
	37, 44, 44, 50, 44, 44, 44, 1, 
	51, 38, 52, 0, 0, 0, 0, 0, 
	9, 36, 53, 37, 49, 53, 49, 49, 
	49, 49, 1, 1, 1, 1, 1, 1, 
	38, 36, 37, 49, 54, 49, 49, 49, 
	49, 1, 36, 37, 49, 55, 49, 49, 
	49, 49, 1, 0
};

static const char _query_trans_targs[] = {
	8, 9, 11, 3, 8, 15, 8, 6, 
	16, 21, 8, 8, 8, 8, 10, 8, 
	12, 8, 8, 8, 13, 17, 8, 8, 
	20, 21, 22, 8, 23, 8, 9, 24, 
	8, 8, 8, 8, 8, 0, 8, 1, 
	8, 2, 8, 14, 8, 4, 5, 8, 
	18, 8, 19, 8, 7, 9, 25, 9
};

static const char _query_trans_actions[] = {
	57, 83, 68, 0, 7, 62, 55, 0, 
	0, 65, 39, 35, 37, 9, 80, 29, 
	80, 13, 15, 27, 77, 59, 21, 23, 
	0, 80, 83, 31, 0, 33, 80, 83, 
	17, 11, 19, 25, 41, 0, 51, 0, 
	47, 0, 49, 62, 45, 0, 0, 43, 
	83, 53, 62, 5, 0, 71, 83, 74
};

static const char _query_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	1, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0
};

static const char _query_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	3, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0
};

static const unsigned char _query_eof_trans[] = {
	1, 1, 1, 1, 1, 7, 1, 1, 
	0, 1, 39, 41, 39, 43, 45, 45, 
	45, 48, 50, 45, 39, 1, 50, 39, 
	50, 50
};

static const int query_start = 8;
static const int query_first_final = 8;
static const int query_error = -1;

static const int query_en_main = 8;


/* #line 266 "lexer.rl" */

QueryNode *RSQuery_ParseRaw_v2(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v2(rm_malloc);

  
  int cs, act;
  const char* ts = q->raw;
  const char* te = q->raw + q->len;
  
/* #line 189 "lexer.c" */
	{
	cs = query_start;
	ts = 0;
	te = 0;
	act = 0;
	}

/* #line 275 "lexer.rl" */
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};
  
  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;
  
  
/* #line 206 "lexer.c" */
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
_resume:
	_acts = _query_actions + _query_from_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 1:
/* #line 1 "NONE" */
	{ts = p;}
	break;
/* #line 225 "lexer.c" */
		}
	}

	_keys = _query_trans_keys + _query_key_offsets[cs];
	_trans = _query_index_offsets[cs];

	_klen = _query_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _query_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _query_indicies[_trans];
_eof_trans:
	cs = _query_trans_targs[_trans];

	if ( _query_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _query_actions + _query_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 2:
/* #line 1 "NONE" */
	{te = p+1;}
	break;
	case 3:
/* #line 55 "lexer.rl" */
	{act = 1;}
	break;
	case 4:
/* #line 66 "lexer.rl" */
	{act = 2;}
	break;
	case 5:
/* #line 77 "lexer.rl" */
	{act = 3;}
	break;
	case 6:
/* #line 86 "lexer.rl" */
	{act = 4;}
	break;
	case 7:
/* #line 104 "lexer.rl" */
	{act = 6;}
	break;
	case 8:
/* #line 117 "lexer.rl" */
	{act = 7;}
	break;
	case 9:
/* #line 186 "lexer.rl" */
	{act = 16;}
	break;
	case 10:
/* #line 229 "lexer.rl" */
	{act = 23;}
	break;
	case 11:
/* #line 232 "lexer.rl" */
	{act = 25;}
	break;
	case 12:
/* #line 95 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, ARROW, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 13:
/* #line 117 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    tok.s = ts;
    tok.len = te-ts;
    tok.numval = *ts == '-' ? -INFINITY : INFINITY;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 14:
/* #line 128 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 15:
/* #line 135 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 16:
/* #line 142 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 17:
/* #line 150 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 18:
/* #line 157 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 19:
/* #line 164 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 20:
/* #line 171 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v2(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 21:
/* #line 178 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v2(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 22:
/* #line 193 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 23:
/* #line 200 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 24:
/* #line 207 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 25:
/* #line 214 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, LSQB, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }  
  }}
	break;
	case 26:
/* #line 221 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, RSQB, tok, q);   
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    } 
  }}
	break;
	case 27:
/* #line 228 "lexer.rl" */
	{te = p+1;}
	break;
	case 28:
/* #line 229 "lexer.rl" */
	{te = p+1;}
	break;
	case 29:
/* #line 230 "lexer.rl" */
	{te = p+1;}
	break;
	case 30:
/* #line 246 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*ts == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v2(pParser, PREFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 31:
/* #line 55 "lexer.rl" */
	{te = p;p--;{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, SIZE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 32:
/* #line 66 "lexer.rl" */
	{te = p;p--;{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 33:
/* #line 86 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, ATTRIBUTE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 34:
/* #line 186 "lexer.rl" */
	{te = p;p--;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 35:
/* #line 229 "lexer.rl" */
	{te = p;p--;}
	break;
	case 36:
/* #line 232 "lexer.rl" */
	{te = p;p--;{
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    if (!StopWordList_Contains(q->opts->stopwords, tok.s, tok.len)) {
      RSQuery_Parse_v2(pParser, TERM, tok, q);
    } else {
      RSQuery_Parse_v2(pParser, STOPWORD, tok, q);
    }
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 37:
/* #line 66 "lexer.rl" */
	{{p = ((te))-1;}{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 38:
/* #line 1 "NONE" */
	{	switch( act ) {
	case 1:
	{{p = ((te))-1;} 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, SIZE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 2:
	{{p = ((te))-1;} 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 3:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, MODIFIER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 4:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v2(pParser, ATTRIBUTE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 6:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    if (StopWordList_Contains(q->opts->stopwords, "as", 2)) {
      RSQuery_Parse_v2(pParser, AS_S, tok, q);
    } else {
      RSQuery_Parse_v2(pParser, AS_T, tok, q);
    }
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 7:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    tok.s = ts;
    tok.len = te-ts;
    tok.numval = *ts == '-' ? -INFINITY : INFINITY;
    RSQuery_Parse_v2(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 16:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v2(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 25:
	{{p = ((te))-1;}
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    if (!StopWordList_Contains(q->opts->stopwords, tok.s, tok.len)) {
      RSQuery_Parse_v2(pParser, TERM, tok, q);
    } else {
      RSQuery_Parse_v2(pParser, STOPWORD, tok, q);
    }
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	default:
	{{p = ((te))-1;}}
	break;
	}
	}
	break;
/* #line 709 "lexer.c" */
		}
	}

_again:
	_acts = _query_actions + _query_to_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 0:
/* #line 1 "NONE" */
	{ts = 0;}
	break;
/* #line 722 "lexer.c" */
		}
	}

	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	if ( _query_eof_trans[cs] > 0 ) {
		_trans = _query_eof_trans[cs] - 1;
		goto _eof_trans;
	}
	}

	_out: {}
	}

/* #line 283 "lexer.rl" */
  
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

