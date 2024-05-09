
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


/* #line 410 "lexer.rl" */



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
	48, 1, 49, 1, 50, 1, 51, 1, 
	52, 1, 53, 1, 54, 1, 55, 2, 
	2, 3, 2, 2, 4, 2, 2, 5, 
	2, 2, 6, 2, 2, 7, 2, 2, 
	8, 2, 2, 9, 2, 2, 10, 2, 
	2, 11, 2, 2, 12, 2, 2, 13, 
	2, 2, 14, 2, 2, 15
};

static const short _query_key_offsets[] = {
	0, 10, 20, 22, 22, 32, 42, 45, 
	47, 51, 53, 55, 59, 61, 63, 65, 
	75, 77, 79, 79, 124, 135, 136, 146, 
	157, 159, 175, 186, 191, 194, 200, 216, 
	229, 234, 239, 242, 248, 250, 266, 281, 
	294, 295, 296, 297, 307, 320, 335, 348, 
	361, 374, 387, 400, 413, 423
};

static const char _query_trans_keys[] = {
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 39, 92, 9, 13, 
	32, 47, 58, 64, 91, 96, 123, 126, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 46, 48, 57, 48, 57, 43, 
	45, 48, 57, 48, 57, 48, 57, 43, 
	45, 48, 57, 48, 57, 78, 110, 70, 
	102, 9, 13, 32, 47, 58, 64, 91, 
	96, 123, 126, 39, 92, 39, 92, 32, 
	33, 34, 36, 37, 39, 40, 41, 42, 
	43, 45, 46, 58, 59, 60, 61, 62, 
	63, 64, 65, 73, 91, 92, 93, 95, 
	97, 105, 119, 123, 124, 125, 126, 127, 
	0, 8, 9, 13, 14, 31, 35, 47, 
	48, 57, 94, 96, 42, 92, 96, 0, 
	47, 58, 64, 91, 94, 123, 127, 61, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 42, 92, 96, 0, 47, 58, 
	64, 91, 94, 123, 127, 39, 92, 36, 
	43, 45, 46, 92, 96, 0, 47, 48, 
	57, 58, 64, 91, 94, 123, 127, 42, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 42, 69, 101, 48, 57, 42, 
	48, 57, 42, 46, 69, 101, 48, 57, 
	42, 46, 69, 92, 96, 101, 0, 47, 
	48, 57, 58, 64, 91, 94, 123, 127, 
	42, 43, 45, 92, 96, 0, 47, 58, 
	64, 91, 94, 123, 127, 46, 73, 105, 
	48, 57, 42, 69, 101, 48, 57, 42, 
	48, 57, 42, 46, 69, 101, 48, 57, 
	48, 57, 42, 46, 69, 92, 96, 101, 
	0, 47, 48, 57, 58, 64, 91, 94, 
	123, 127, 42, 43, 45, 92, 96, 0, 
	47, 48, 57, 58, 64, 91, 94, 123, 
	127, 42, 92, 96, 0, 47, 48, 57, 
	58, 64, 91, 94, 123, 127, 61, 62, 
	61, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 42, 83, 92, 96, 115, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 78, 83, 92, 96, 110, 115, 0, 
	47, 58, 64, 91, 94, 123, 127, 42, 
	70, 92, 96, 102, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 69, 92, 96, 
	101, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 77, 92, 96, 109, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 80, 
	92, 96, 112, 0, 47, 58, 64, 91, 
	94, 123, 127, 42, 84, 92, 96, 116, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 89, 92, 96, 121, 0, 47, 58, 
	64, 91, 94, 123, 127, 9, 13, 32, 
	47, 58, 64, 91, 96, 123, 126, 39, 
	42, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 0
};

static const char _query_single_lengths[] = {
	0, 0, 2, 0, 0, 2, 1, 0, 
	2, 0, 0, 2, 0, 2, 2, 0, 
	2, 2, 0, 33, 3, 1, 2, 3, 
	2, 6, 3, 3, 1, 4, 6, 5, 
	3, 3, 1, 4, 0, 6, 5, 3, 
	1, 1, 1, 2, 5, 7, 5, 5, 
	5, 5, 5, 5, 0, 4
};

static const char _query_range_lengths[] = {
	5, 5, 0, 0, 5, 4, 1, 1, 
	1, 1, 1, 1, 1, 0, 0, 5, 
	0, 0, 0, 6, 4, 0, 4, 4, 
	0, 5, 4, 1, 1, 1, 5, 4, 
	1, 1, 1, 1, 1, 5, 5, 5, 
	0, 0, 0, 4, 4, 4, 4, 4, 
	4, 4, 4, 4, 5, 4
};

static const short _query_index_offsets[] = {
	0, 6, 12, 15, 16, 22, 29, 32, 
	34, 38, 40, 42, 46, 48, 51, 54, 
	60, 63, 66, 67, 107, 115, 117, 124, 
	132, 135, 147, 155, 160, 163, 169, 181, 
	191, 196, 201, 204, 210, 212, 224, 235, 
	244, 246, 248, 250, 257, 267, 279, 289, 
	299, 309, 319, 329, 339, 345
};

static const char _query_indicies[] = {
	1, 1, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 0, 5, 6, 4, 4, 
	7, 7, 7, 7, 7, 0, 9, 8, 
	8, 8, 8, 8, 7, 10, 11, 8, 
	12, 0, 14, 14, 15, 13, 15, 13, 
	16, 0, 18, 18, 19, 17, 19, 0, 
	20, 20, 0, 21, 21, 0, 22, 22, 
	22, 22, 22, 0, 23, 25, 24, 26, 
	25, 24, 24, 28, 29, 30, 32, 33, 
	34, 35, 36, 37, 38, 39, 40, 42, 
	43, 44, 45, 46, 31, 47, 48, 49, 
	50, 51, 52, 53, 48, 49, 54, 55, 
	56, 57, 58, 27, 27, 28, 27, 31, 
	41, 31, 1, 59, 60, 0, 0, 0, 
	0, 0, 1, 62, 61, 63, 61, 61, 
	61, 61, 61, 2, 59, 63, 64, 64, 
	64, 64, 64, 2, 61, 6, 4, 66, 
	67, 67, 10, 9, 65, 65, 68, 65, 
	65, 65, 7, 70, 9, 69, 69, 69, 
	69, 69, 7, 70, 71, 71, 12, 69, 
	70, 15, 69, 70, 10, 71, 71, 11, 
	69, 70, 10, 72, 9, 69, 72, 69, 
	68, 69, 69, 69, 7, 70, 14, 14, 
	9, 69, 69, 69, 69, 69, 7, 73, 
	75, 75, 74, 0, 59, 77, 77, 16, 
	76, 59, 19, 76, 59, 73, 77, 77, 
	74, 76, 16, 61, 59, 73, 79, 60, 
	78, 79, 78, 41, 78, 78, 78, 1, 
	59, 18, 18, 60, 80, 80, 81, 80, 
	80, 80, 1, 59, 60, 76, 76, 81, 
	76, 76, 76, 1, 83, 82, 85, 84, 
	87, 86, 88, 0, 0, 0, 0, 0, 
	22, 59, 89, 60, 80, 89, 80, 80, 
	80, 80, 1, 59, 90, 91, 60, 80, 
	90, 91, 80, 80, 80, 80, 1, 59, 
	92, 60, 80, 92, 80, 80, 80, 80, 
	1, 59, 93, 60, 80, 93, 80, 80, 
	80, 80, 1, 59, 94, 60, 80, 94, 
	80, 80, 80, 80, 1, 59, 95, 60, 
	80, 95, 80, 80, 80, 80, 1, 59, 
	96, 60, 80, 96, 80, 80, 80, 80, 
	1, 59, 97, 60, 80, 97, 80, 80, 
	80, 80, 1, 1, 1, 1, 1, 1, 
	61, 98, 59, 60, 80, 80, 80, 80, 
	80, 1, 0
};

static const char _query_trans_targs[] = {
	19, 20, 23, 19, 2, 19, 3, 26, 
	19, 4, 7, 29, 27, 19, 9, 28, 
	33, 19, 12, 34, 14, 19, 43, 19, 
	17, 18, 19, 19, 19, 21, 19, 19, 
	22, 19, 24, 19, 19, 25, 32, 32, 
	36, 37, 19, 19, 40, 41, 42, 43, 
	44, 45, 19, 52, 19, 20, 53, 19, 
	19, 19, 19, 19, 0, 19, 19, 1, 
	19, 19, 5, 6, 30, 19, 19, 8, 
	31, 10, 35, 13, 19, 11, 19, 38, 
	19, 39, 19, 19, 19, 19, 19, 19, 
	15, 20, 46, 47, 20, 48, 49, 50, 
	51, 20, 16
};

static const char _query_trans_actions[] = {
	85, 120, 96, 79, 0, 51, 0, 123, 
	77, 0, 0, 123, 5, 83, 0, 0, 
	90, 75, 0, 0, 0, 9, 93, 81, 
	0, 0, 53, 45, 41, 0, 11, 43, 
	114, 35, 5, 21, 23, 111, 108, 105, 
	0, 87, 29, 31, 0, 0, 0, 114, 
	120, 120, 37, 0, 39, 114, 120, 25, 
	19, 27, 33, 47, 0, 69, 13, 0, 
	59, 67, 0, 0, 123, 73, 49, 0, 
	123, 0, 90, 0, 57, 0, 55, 120, 
	71, 90, 65, 17, 61, 7, 63, 15, 
	0, 99, 120, 120, 102, 120, 120, 120, 
	120, 117, 0
};

static const char _query_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0
};

static const char _query_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0
};

static const short _query_eof_trans[] = {
	1, 1, 4, 4, 1, 9, 9, 1, 
	14, 14, 1, 18, 1, 1, 1, 1, 
	24, 24, 24, 0, 1, 62, 62, 65, 
	62, 66, 70, 70, 70, 70, 70, 70, 
	1, 77, 77, 77, 62, 79, 81, 77, 
	83, 85, 87, 1, 81, 81, 81, 81, 
	81, 81, 81, 81, 62, 81
};

static const int query_start = 19;
static const int query_first_final = 19;
static const int query_error = -1;

static const int query_en_main = 19;


/* #line 413 "lexer.rl" */

QueryNode *RSQuery_ParseRaw_v3(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v3(rm_malloc);

  
  int cs, act;
  const char* ts = q->raw;          // query start
  const char* te = q->raw + q->len; // query end
  
/* #line 281 "lexer.c" */
	{
	cs = query_start;
	ts = 0;
	te = 0;
	act = 0;
	}

/* #line 422 "lexer.rl" */
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};
  
  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;
  
  
/* #line 298 "lexer.c" */
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
/* #line 317 "lexer.c" */
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
/* #line 74 "lexer.rl" */
	{act = 1;}
	break;
	case 4:
/* #line 85 "lexer.rl" */
	{act = 2;}
	break;
	case 5:
/* #line 96 "lexer.rl" */
	{act = 3;}
	break;
	case 6:
/* #line 105 "lexer.rl" */
	{act = 4;}
	break;
	case 7:
/* #line 123 "lexer.rl" */
	{act = 6;}
	break;
	case 8:
/* #line 132 "lexer.rl" */
	{act = 7;}
	break;
	case 9:
/* #line 258 "lexer.rl" */
	{act = 23;}
	break;
	case 10:
/* #line 266 "lexer.rl" */
	{act = 24;}
	break;
	case 11:
/* #line 281 "lexer.rl" */
	{act = 26;}
	break;
	case 12:
/* #line 311 "lexer.rl" */
	{act = 31;}
	break;
	case 13:
/* #line 321 "lexer.rl" */
	{act = 33;}
	break;
	case 14:
/* #line 330 "lexer.rl" */
	{act = 34;}
	break;
	case 15:
/* #line 354 "lexer.rl" */
	{act = 36;}
	break;
	case 16:
/* #line 114 "lexer.rl" */
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
/* #line 132 "lexer.rl" */
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
/* #line 143 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 19:
/* #line 151 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, NOT_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 20:
/* #line 175 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 21:
/* #line 191 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 22:
/* #line 207 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 23:
/* #line 214 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 24:
/* #line 222 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 25:
/* #line 229 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 26:
/* #line 236 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 27:
/* #line 243 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 28:
/* #line 250 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 29:
/* #line 274 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 30:
/* #line 288 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 31:
/* #line 295 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LSQB, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }  
  }}
	break;
	case 32:
/* #line 302 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RSQB, tok, q);   
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    } 
  }}
	break;
	case 33:
/* #line 309 "lexer.rl" */
	{te = p+1;}
	break;
	case 34:
/* #line 311 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 35:
/* #line 319 "lexer.rl" */
	{te = p+1;}
	break;
	case 36:
/* #line 340 "lexer.rl" */
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
	case 37:
/* #line 368 "lexer.rl" */
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
	case 38:
/* #line 383 "lexer.rl" */
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
	case 39:
/* #line 396 "lexer.rl" */
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
	case 40:
/* #line 74 "lexer.rl" */
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
	case 41:
/* #line 85 "lexer.rl" */
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
	case 42:
/* #line 105 "lexer.rl" */
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
	case 43:
/* #line 159 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 44:
/* #line 167 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 45:
/* #line 183 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 46:
/* #line 281 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 47:
/* #line 311 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 48:
/* #line 330 "lexer.rl" */
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
	case 49:
/* #line 354 "lexer.rl" */
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
	case 50:
/* #line 85 "lexer.rl" */
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
	case 51:
/* #line 281 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 52:
/* #line 311 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 53:
/* #line 330 "lexer.rl" */
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
	case 54:
/* #line 354 "lexer.rl" */
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
	case 55:
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
	case 23:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 24:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PLUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 26:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 31:
	{{p = ((te))-1;}
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 33:
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
	case 34:
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
	case 36:
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
/* #line 1032 "lexer.c" */
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
/* #line 1045 "lexer.c" */
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

/* #line 430 "lexer.rl" */
  
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

