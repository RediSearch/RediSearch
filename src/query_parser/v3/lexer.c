
/* #line 1 "lexer.rl" */
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


/* #line 347 "lexer.rl" */



/* #line 36 "lexer.c" */
static const char _query_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	16, 1, 17, 1, 18, 1, 19, 1, 
	20, 1, 21, 1, 22, 1, 23, 1, 
	24, 1, 25, 1, 26, 1, 27, 1, 
	28, 1, 29, 1, 30, 1, 31, 1, 
	32, 1, 33, 1, 34, 1, 35, 1, 
	36, 1, 37, 1, 38, 1, 39, 1, 
	40, 1, 41, 1, 42, 1, 43, 1, 
	44, 1, 45, 1, 46, 1, 47, 1, 
	48, 1, 49, 2, 2, 3, 2, 2, 
	4, 2, 2, 5, 2, 2, 6, 2, 
	2, 7, 2, 2, 8, 2, 2, 9, 
	2, 2, 10, 2, 2, 11, 2, 2, 
	12, 2, 2, 13, 2, 2, 14, 2, 
	2, 15
};

static const short _query_key_offsets[] = {
	0, 10, 20, 22, 22, 32, 42, 44, 
	46, 50, 52, 54, 58, 60, 62, 64, 
	74, 76, 78, 78, 120, 131, 141, 152, 
	154, 169, 180, 186, 191, 194, 210, 223, 
	227, 233, 238, 241, 257, 272, 285, 286, 
	296, 309, 324, 337, 350, 363, 376, 389, 
	402, 412
};

static const char _query_trans_keys[] = {
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 39, 92, 9, 13, 
	32, 47, 58, 64, 91, 96, 123, 126, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 48, 57, 48, 57, 43, 45, 
	48, 57, 48, 57, 48, 57, 43, 45, 
	48, 57, 48, 57, 78, 110, 70, 102, 
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 39, 92, 39, 92, 32, 34, 
	36, 37, 39, 40, 41, 42, 43, 45, 
	58, 59, 61, 64, 65, 73, 91, 92, 
	93, 95, 97, 105, 119, 123, 124, 125, 
	126, 127, 0, 8, 9, 13, 14, 31, 
	33, 47, 48, 57, 60, 63, 94, 96, 
	42, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 92, 96, 0, 47, 58, 
	64, 91, 94, 123, 127, 42, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	39, 92, 36, 43, 45, 92, 96, 0, 
	47, 48, 57, 58, 64, 91, 94, 123, 
	127, 42, 92, 96, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 46, 69, 101, 
	48, 57, 42, 69, 101, 48, 57, 42, 
	48, 57, 42, 46, 69, 92, 96, 101, 
	0, 47, 48, 57, 58, 64, 91, 94, 
	123, 127, 42, 43, 45, 92, 96, 0, 
	47, 58, 64, 91, 94, 123, 127, 73, 
	105, 48, 57, 42, 46, 69, 101, 48, 
	57, 42, 69, 101, 48, 57, 42, 48, 
	57, 42, 46, 69, 92, 96, 101, 0, 
	47, 48, 57, 58, 64, 91, 94, 123, 
	127, 42, 43, 45, 92, 96, 0, 47, 
	48, 57, 58, 64, 91, 94, 123, 127, 
	42, 92, 96, 0, 47, 48, 57, 58, 
	64, 91, 94, 123, 127, 62, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 83, 92, 96, 115, 0, 47, 58, 
	64, 91, 94, 123, 127, 42, 78, 83, 
	92, 96, 110, 115, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 70, 92, 96, 
	102, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 69, 92, 96, 101, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 77, 
	92, 96, 109, 0, 47, 58, 64, 91, 
	94, 123, 127, 42, 80, 92, 96, 112, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 84, 92, 96, 116, 0, 47, 58, 
	64, 91, 94, 123, 127, 42, 89, 92, 
	96, 121, 0, 47, 58, 64, 91, 94, 
	123, 127, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 39, 42, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	0
};

static const char _query_single_lengths[] = {
	0, 0, 2, 0, 0, 2, 0, 0, 
	2, 0, 0, 2, 0, 2, 2, 0, 
	2, 2, 0, 28, 3, 2, 3, 2, 
	5, 3, 4, 3, 1, 6, 5, 2, 
	4, 3, 1, 6, 5, 3, 1, 2, 
	5, 7, 5, 5, 5, 5, 5, 5, 
	0, 4
};

static const char _query_range_lengths[] = {
	5, 5, 0, 0, 5, 4, 1, 1, 
	1, 1, 1, 1, 1, 0, 0, 5, 
	0, 0, 0, 7, 4, 4, 4, 0, 
	5, 4, 1, 1, 1, 5, 4, 1, 
	1, 1, 1, 5, 5, 5, 0, 4, 
	4, 4, 4, 4, 4, 4, 4, 4, 
	5, 4
};

static const short _query_index_offsets[] = {
	0, 6, 12, 15, 16, 22, 29, 31, 
	33, 37, 39, 41, 45, 47, 50, 53, 
	59, 62, 65, 66, 102, 110, 117, 125, 
	128, 139, 147, 153, 158, 161, 173, 183, 
	187, 193, 198, 201, 213, 224, 233, 235, 
	242, 252, 264, 274, 284, 294, 304, 314, 
	324, 330
};

static const char _query_indicies[] = {
	1, 1, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 0, 5, 6, 4, 4, 
	7, 7, 7, 7, 7, 0, 9, 8, 
	8, 8, 8, 8, 7, 10, 8, 12, 
	11, 13, 13, 14, 11, 14, 11, 15, 
	0, 17, 17, 18, 16, 18, 0, 19, 
	19, 0, 20, 20, 0, 21, 21, 21, 
	21, 21, 0, 22, 24, 23, 25, 24, 
	23, 23, 27, 29, 30, 31, 32, 33, 
	34, 35, 36, 37, 39, 40, 41, 42, 
	43, 44, 45, 46, 47, 48, 43, 44, 
	49, 50, 51, 52, 53, 26, 26, 27, 
	26, 28, 38, 28, 28, 1, 54, 55, 
	0, 0, 0, 0, 0, 1, 57, 56, 
	56, 56, 56, 56, 2, 54, 57, 58, 
	58, 58, 58, 58, 2, 56, 6, 4, 
	60, 61, 61, 9, 59, 59, 62, 59, 
	59, 59, 7, 64, 9, 63, 63, 63, 
	63, 63, 7, 64, 65, 66, 66, 10, 
	63, 64, 66, 66, 12, 63, 64, 14, 
	63, 64, 65, 67, 9, 63, 67, 63, 
	62, 63, 63, 63, 7, 64, 13, 13, 
	9, 63, 63, 63, 63, 63, 7, 69, 
	69, 68, 0, 54, 71, 72, 72, 68, 
	70, 54, 72, 72, 15, 70, 54, 18, 
	70, 54, 71, 74, 55, 73, 74, 73, 
	38, 73, 73, 73, 1, 54, 17, 17, 
	55, 75, 75, 76, 75, 75, 75, 1, 
	54, 55, 70, 70, 76, 70, 70, 70, 
	1, 77, 56, 78, 0, 0, 0, 0, 
	0, 21, 54, 79, 55, 75, 79, 75, 
	75, 75, 75, 1, 54, 80, 81, 55, 
	75, 80, 81, 75, 75, 75, 75, 1, 
	54, 82, 55, 75, 82, 75, 75, 75, 
	75, 1, 54, 83, 55, 75, 83, 75, 
	75, 75, 75, 1, 54, 84, 55, 75, 
	84, 75, 75, 75, 75, 1, 54, 85, 
	55, 75, 85, 75, 75, 75, 75, 1, 
	54, 86, 55, 75, 86, 75, 75, 75, 
	75, 1, 54, 87, 55, 75, 87, 75, 
	75, 75, 75, 1, 1, 1, 1, 1, 
	1, 56, 88, 54, 55, 75, 75, 75, 
	75, 75, 1, 0
};

static const char _query_trans_targs[] = {
	19, 20, 22, 19, 2, 19, 3, 25, 
	19, 4, 26, 19, 27, 9, 28, 33, 
	19, 12, 34, 14, 19, 39, 19, 17, 
	18, 19, 19, 19, 19, 19, 21, 19, 
	23, 19, 19, 24, 31, 31, 35, 19, 
	19, 38, 39, 40, 41, 19, 48, 19, 
	20, 49, 19, 19, 19, 19, 19, 0, 
	19, 1, 19, 19, 5, 6, 29, 19, 
	19, 7, 8, 30, 32, 13, 19, 10, 
	11, 19, 36, 19, 37, 19, 15, 20, 
	42, 43, 20, 44, 45, 46, 47, 20, 
	16
};

static const char _query_trans_actions[] = {
	73, 108, 84, 67, 0, 45, 0, 111, 
	65, 0, 5, 71, 5, 0, 0, 78, 
	63, 0, 0, 0, 9, 81, 69, 0, 
	0, 47, 39, 35, 37, 11, 102, 29, 
	5, 15, 17, 99, 96, 93, 75, 23, 
	25, 0, 102, 108, 108, 31, 0, 33, 
	102, 108, 19, 13, 21, 27, 41, 0, 
	57, 0, 53, 55, 0, 0, 111, 61, 
	43, 0, 0, 111, 78, 0, 51, 0, 
	0, 49, 108, 59, 78, 7, 0, 87, 
	108, 108, 90, 108, 108, 108, 108, 105, 
	0
};

static const char _query_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0
};

static const char _query_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0
};

static const short _query_eof_trans[] = {
	1, 1, 4, 4, 1, 9, 9, 12, 
	12, 12, 1, 17, 1, 1, 1, 1, 
	23, 23, 23, 0, 1, 57, 59, 57, 
	60, 64, 64, 64, 64, 64, 64, 1, 
	71, 71, 71, 74, 76, 71, 57, 1, 
	76, 76, 76, 76, 76, 76, 76, 76, 
	57, 76
};

static const int query_start = 19;
static const int query_first_final = 19;
static const int query_error = -1;

static const int query_en_main = 19;


/* #line 350 "lexer.rl" */

QueryNode *RSQuery_ParseRaw_v3(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v3(rm_malloc);

  
  int cs, act;
  const char* ts = q->raw;          // query start
  const char* te = q->raw + q->len; // query end
  
/* #line 275 "lexer.c" */
	{
	cs = query_start;
	ts = 0;
	te = 0;
	act = 0;
	}

/* #line 359 "lexer.rl" */
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};
  
  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;
  
  
/* #line 292 "lexer.c" */
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
/* #line 311 "lexer.c" */
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
/* #line 68 "lexer.rl" */
	{act = 1;}
	break;
	case 4:
/* #line 79 "lexer.rl" */
	{act = 2;}
	break;
	case 5:
/* #line 90 "lexer.rl" */
	{act = 3;}
	break;
	case 6:
/* #line 99 "lexer.rl" */
	{act = 4;}
	break;
	case 7:
/* #line 117 "lexer.rl" */
	{act = 6;}
	break;
	case 8:
/* #line 126 "lexer.rl" */
	{act = 7;}
	break;
	case 9:
/* #line 195 "lexer.rl" */
	{act = 16;}
	break;
	case 10:
/* #line 203 "lexer.rl" */
	{act = 17;}
	break;
	case 11:
/* #line 218 "lexer.rl" */
	{act = 19;}
	break;
	case 12:
/* #line 248 "lexer.rl" */
	{act = 24;}
	break;
	case 13:
/* #line 258 "lexer.rl" */
	{act = 26;}
	break;
	case 14:
/* #line 267 "lexer.rl" */
	{act = 27;}
	break;
	case 15:
/* #line 291 "lexer.rl" */
	{act = 29;}
	break;
	case 16:
/* #line 108 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts+1;
    RSQuery_Parse_v3(pParser, ARROW, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 17:
/* #line 126 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    tok.s = ts;
    tok.len = te-ts;
    tok.numval = *ts == '-' ? -INFINITY : INFINITY;
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 18:
/* #line 137 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 19:
/* #line 144 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 20:
/* #line 151 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 21:
/* #line 159 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 22:
/* #line 166 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 23:
/* #line 173 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 24:
/* #line 180 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 25:
/* #line 187 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 26:
/* #line 211 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 27:
/* #line 225 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 28:
/* #line 232 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LSQB, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }  
  }}
	break;
	case 29:
/* #line 239 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RSQB, tok, q);   
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    } 
  }}
	break;
	case 30:
/* #line 246 "lexer.rl" */
	{te = p+1;}
	break;
	case 31:
/* #line 248 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 32:
/* #line 256 "lexer.rl" */
	{te = p+1;}
	break;
	case 33:
/* #line 277 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*ts == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v3(pParser, PREFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 34:
/* #line 305 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 2 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v3(pParser, CONTAINS, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 35:
/* #line 320 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*(ts+2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 2 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    RSQuery_Parse_v3(pParser, VERBATIM, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 36:
/* #line 333 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*(ts+2) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.pos = ts-q->raw + 2;
    tok.len = te - (ts + 3 + is_attr);
    tok.s = ts + 2 + is_attr;
    tok.numval = 0;
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 37:
/* #line 68 "lexer.rl" */
	{te = p;p--;{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, SIZE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 38:
/* #line 79 "lexer.rl" */
	{te = p;p--;{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 39:
/* #line 99 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    tok.len = te - (ts + 1);
    tok.s = ts+1;
    RSQuery_Parse_v3(pParser, ATTRIBUTE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 40:
/* #line 218 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 41:
/* #line 248 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 42:
/* #line 267 "lexer.rl" */
	{te = p;p--;{
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 43:
/* #line 291 "lexer.rl" */
	{te = p;p--;{
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v3(pParser, SUFFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 44:
/* #line 79 "lexer.rl" */
	{{p = ((te))-1;}{ 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 45:
/* #line 218 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 46:
/* #line 248 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 47:
/* #line 267 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 48:
/* #line 291 "lexer.rl" */
	{{p = ((te))-1;}{
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v3(pParser, SUFFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 49:
/* #line 1 "NONE" */
	{	switch( act ) {
	case 1:
	{{p = ((te))-1;} 
    tok.s = ts;
    tok.len = te-ts;
    char *ne = (char*)te;
    tok.numval = strtod(tok.s, &ne);
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, SIZE, tok, q);
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
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
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
    RSQuery_Parse_v3(pParser, MODIFIER, tok, q);
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
    RSQuery_Parse_v3(pParser, ATTRIBUTE, tok, q);
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
    RSQuery_Parse_v3(pParser, AS_T, tok, q);
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
    RSQuery_Parse_v3(pParser, NUMBER, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 16:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 17:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PLUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 19:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 24:
	{{p = ((te))-1;}
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 26:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    tok.len = te - ts;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, ISEMPTY, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 27:
	{{p = ((te))-1;}
    tok.len = te-ts;
    tok.s = ts;
    tok.numval = 0;
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TERM, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 29:
	{{p = ((te))-1;}
    int is_attr = (*(ts+1) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 1 + is_attr);
    tok.s = ts + 1 + is_attr;
    tok.numval = 0;
    tok.pos = ts-q->raw;

    RSQuery_Parse_v3(pParser, SUFFIX, tok, q);
    
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	}
	}
	break;
/* #line 966 "lexer.c" */
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
/* #line 979 "lexer.c" */
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

/* #line 367 "lexer.rl" */
  
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

