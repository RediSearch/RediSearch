
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


/* #line 708 "lexer.rl" */



/* #line 36 "lexer.c" */
static const char _query_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	19, 1, 20, 1, 21, 1, 22, 1, 
	23, 1, 24, 1, 25, 1, 26, 1, 
	27, 1, 28, 1, 29, 1, 30, 1, 
	31, 1, 32, 1, 33, 1, 34, 1, 
	35, 1, 36, 1, 37, 1, 38, 1, 
	39, 1, 40, 1, 41, 1, 42, 1, 
	43, 1, 44, 1, 45, 1, 46, 1, 
	47, 1, 48, 1, 49, 1, 50, 1, 
	51, 1, 52, 1, 53, 1, 54, 1, 
	55, 1, 56, 1, 57, 1, 58, 1, 
	59, 1, 60, 1, 61, 1, 62, 1, 
	63, 1, 64, 1, 65, 1, 66, 1, 
	67, 1, 68, 2, 2, 3, 2, 2, 
	4, 2, 2, 5, 2, 2, 6, 2, 
	2, 7, 2, 2, 8, 2, 2, 9, 
	2, 2, 10, 2, 2, 11, 2, 2, 
	12, 2, 2, 13, 2, 2, 14, 2, 
	2, 15, 2, 2, 16, 2, 2, 17, 
	2, 2, 18
};

static const short _query_key_offsets[] = {
	0, 10, 20, 22, 22, 23, 25, 27, 
	28, 28, 38, 48, 51, 53, 57, 59, 
	61, 65, 67, 69, 71, 81, 83, 85, 
	85, 89, 90, 94, 98, 102, 103, 107, 
	112, 113, 115, 117, 118, 118, 123, 128, 
	133, 135, 136, 136, 140, 143, 147, 192, 
	203, 204, 214, 225, 227, 228, 244, 255, 
	260, 263, 269, 285, 298, 303, 308, 311, 
	317, 319, 335, 350, 363, 364, 366, 367, 
	377, 390, 405, 418, 431, 444, 457, 470, 
	483, 493, 505, 510
};

static const char _query_trans_keys[] = {
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 39, 92, 39, 39, 
	92, 39, 92, 41, 9, 13, 32, 47, 
	58, 64, 91, 96, 123, 126, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	46, 48, 57, 48, 57, 43, 45, 48, 
	57, 48, 57, 48, 57, 43, 45, 48, 
	57, 48, 57, 78, 110, 70, 102, 9, 
	13, 32, 47, 58, 64, 91, 96, 123, 
	126, 39, 92, 39, 92, 36, 42, 92, 
	125, 125, 36, 42, 92, 125, 36, 42, 
	92, 125, 36, 42, 92, 125, 125, 36, 
	42, 92, 125, 36, 42, 92, 119, 125, 
	39, 39, 92, 39, 92, 125, 36, 39, 
	42, 92, 125, 36, 39, 42, 92, 125, 
	36, 39, 42, 92, 125, 39, 92, 125, 
	36, 42, 92, 125, 39, 92, 125, 36, 
	42, 92, 125, 32, 33, 34, 36, 37, 
	39, 40, 41, 42, 43, 45, 46, 58, 
	59, 60, 61, 62, 63, 64, 65, 73, 
	91, 92, 93, 95, 97, 105, 119, 123, 
	124, 125, 126, 127, 0, 8, 9, 13, 
	14, 31, 35, 47, 48, 57, 94, 96, 
	42, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 61, 92, 96, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 92, 
	96, 0, 47, 58, 64, 91, 94, 123, 
	127, 39, 92, 119, 36, 43, 45, 46, 
	92, 96, 0, 47, 48, 57, 58, 64, 
	91, 94, 123, 127, 42, 92, 96, 0, 
	47, 58, 64, 91, 94, 123, 127, 42, 
	69, 101, 48, 57, 42, 48, 57, 42, 
	46, 69, 101, 48, 57, 42, 46, 69, 
	92, 96, 101, 0, 47, 48, 57, 58, 
	64, 91, 94, 123, 127, 42, 43, 45, 
	92, 96, 0, 47, 58, 64, 91, 94, 
	123, 127, 46, 73, 105, 48, 57, 42, 
	69, 101, 48, 57, 42, 48, 57, 42, 
	46, 69, 101, 48, 57, 48, 57, 42, 
	46, 69, 92, 96, 101, 0, 47, 48, 
	57, 58, 64, 91, 94, 123, 127, 42, 
	43, 45, 92, 96, 0, 47, 48, 57, 
	58, 64, 91, 94, 123, 127, 42, 92, 
	96, 0, 47, 48, 57, 58, 64, 91, 
	94, 123, 127, 61, 61, 62, 61, 92, 
	96, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 83, 92, 96, 115, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 78, 
	83, 92, 96, 110, 115, 0, 47, 58, 
	64, 91, 94, 123, 127, 42, 70, 92, 
	96, 102, 0, 47, 58, 64, 91, 94, 
	123, 127, 42, 69, 92, 96, 101, 0, 
	47, 58, 64, 91, 94, 123, 127, 42, 
	77, 92, 96, 109, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 80, 92, 96, 
	112, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 84, 92, 96, 116, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 89, 
	92, 96, 121, 0, 47, 58, 64, 91, 
	94, 123, 127, 9, 13, 32, 47, 58, 
	64, 91, 96, 123, 126, 39, 42, 92, 
	96, 0, 47, 58, 64, 91, 94, 123, 
	127, 36, 42, 92, 119, 125, 39, 92, 
	0
};

static const char _query_single_lengths[] = {
	0, 0, 2, 0, 1, 2, 2, 1, 
	0, 0, 2, 1, 0, 2, 0, 0, 
	2, 0, 2, 2, 0, 2, 2, 0, 
	4, 1, 4, 4, 4, 1, 4, 5, 
	1, 2, 2, 1, 0, 5, 5, 5, 
	2, 1, 0, 4, 3, 4, 33, 3, 
	1, 2, 3, 2, 1, 6, 3, 3, 
	1, 4, 6, 5, 3, 3, 1, 4, 
	0, 6, 5, 3, 1, 2, 1, 2, 
	5, 7, 5, 5, 5, 5, 5, 5, 
	0, 4, 5, 2
};

static const char _query_range_lengths[] = {
	5, 5, 0, 0, 0, 0, 0, 0, 
	0, 5, 4, 1, 1, 1, 1, 1, 
	1, 1, 0, 0, 5, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 6, 4, 
	0, 4, 4, 0, 0, 5, 4, 1, 
	1, 1, 5, 4, 1, 1, 1, 1, 
	1, 5, 5, 5, 0, 0, 0, 4, 
	4, 4, 4, 4, 4, 4, 4, 4, 
	5, 4, 0, 0
};

static const short _query_index_offsets[] = {
	0, 6, 12, 15, 16, 18, 21, 24, 
	26, 27, 33, 40, 43, 45, 49, 51, 
	53, 57, 59, 62, 65, 71, 74, 77, 
	78, 83, 85, 90, 95, 100, 102, 107, 
	113, 115, 118, 121, 123, 124, 130, 136, 
	142, 145, 147, 148, 153, 157, 162, 202, 
	210, 212, 219, 227, 230, 232, 244, 252, 
	257, 260, 266, 278, 288, 293, 298, 301, 
	307, 309, 321, 332, 341, 343, 346, 348, 
	355, 365, 377, 387, 397, 407, 417, 427, 
	437, 443, 452, 458
};

static const unsigned char _query_indicies[] = {
	1, 1, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 0, 5, 6, 4, 4, 
	8, 7, 7, 10, 9, 11, 10, 9, 
	12, 7, 9, 13, 13, 13, 13, 13, 
	0, 15, 14, 14, 14, 14, 14, 13, 
	16, 17, 14, 18, 0, 20, 20, 21, 
	19, 21, 19, 22, 0, 24, 24, 25, 
	23, 25, 0, 26, 26, 0, 27, 27, 
	0, 28, 28, 28, 28, 28, 0, 29, 
	31, 30, 32, 31, 30, 30, 33, 35, 
	36, 37, 34, 38, 33, 34, 34, 34, 
	34, 33, 33, 33, 40, 33, 39, 33, 
	41, 40, 42, 39, 43, 33, 39, 39, 
	39, 39, 33, 34, 34, 34, 44, 34, 
	33, 45, 33, 33, 47, 46, 48, 47, 
	46, 49, 33, 46, 33, 50, 35, 36, 
	37, 34, 52, 34, 53, 54, 55, 51, 
	52, 56, 53, 54, 55, 51, 57, 58, 
	52, 59, 0, 52, 33, 35, 36, 59, 
	34, 57, 58, 60, 52, 51, 51, 51, 
	51, 52, 62, 63, 64, 66, 67, 68, 
	69, 70, 71, 72, 73, 74, 76, 77, 
	78, 79, 80, 65, 81, 82, 83, 84, 
	85, 86, 87, 82, 83, 88, 89, 90, 
	91, 92, 61, 61, 62, 61, 65, 75, 
	65, 1, 93, 94, 0, 0, 0, 0, 
	0, 1, 96, 95, 97, 95, 95, 95, 
	95, 95, 2, 93, 97, 98, 98, 98, 
	98, 98, 2, 95, 6, 4, 100, 99, 
	102, 103, 103, 16, 15, 101, 101, 104, 
	101, 101, 101, 13, 106, 15, 105, 105, 
	105, 105, 105, 13, 106, 107, 107, 18, 
	105, 106, 21, 105, 106, 16, 107, 107, 
	17, 105, 106, 16, 108, 15, 105, 108, 
	105, 104, 105, 105, 105, 13, 106, 20, 
	20, 15, 105, 105, 105, 105, 105, 13, 
	109, 111, 111, 110, 0, 93, 113, 113, 
	22, 112, 93, 25, 112, 93, 109, 113, 
	113, 110, 112, 22, 95, 93, 109, 115, 
	94, 114, 115, 114, 75, 114, 114, 114, 
	1, 93, 24, 24, 94, 116, 116, 117, 
	116, 116, 116, 1, 93, 94, 112, 112, 
	117, 112, 112, 112, 1, 119, 118, 121, 
	122, 120, 124, 123, 125, 0, 0, 0, 
	0, 0, 28, 93, 126, 94, 116, 126, 
	116, 116, 116, 116, 1, 93, 127, 128, 
	94, 116, 127, 128, 116, 116, 116, 116, 
	1, 93, 129, 94, 116, 129, 116, 116, 
	116, 116, 1, 93, 130, 94, 116, 130, 
	116, 116, 116, 116, 1, 93, 131, 94, 
	116, 131, 116, 116, 116, 116, 1, 93, 
	132, 94, 116, 132, 116, 116, 116, 116, 
	1, 93, 133, 94, 116, 133, 116, 116, 
	116, 116, 1, 93, 134, 94, 116, 134, 
	116, 116, 116, 116, 1, 1, 1, 1, 
	1, 1, 95, 135, 93, 94, 116, 116, 
	116, 116, 116, 1, 136, 137, 138, 139, 
	136, 34, 57, 58, 52, 0
};

static const char _query_trans_targs[] = {
	46, 47, 50, 46, 2, 46, 3, 46, 
	5, 6, 8, 7, 46, 54, 46, 9, 
	12, 57, 55, 46, 14, 56, 61, 46, 
	17, 62, 19, 46, 71, 46, 22, 23, 
	46, 46, 24, 25, 26, 46, 46, 28, 
	30, 29, 46, 46, 32, 33, 34, 36, 
	35, 46, 38, 39, 40, 44, 45, 83, 
	43, 41, 42, 46, 83, 46, 46, 48, 
	46, 46, 49, 46, 51, 52, 46, 53, 
	60, 60, 64, 65, 46, 46, 68, 69, 
	70, 71, 72, 73, 46, 80, 46, 47, 
	81, 82, 46, 46, 46, 46, 0, 46, 
	46, 1, 46, 46, 4, 46, 10, 11, 
	58, 46, 46, 13, 59, 15, 63, 18, 
	46, 16, 46, 66, 46, 67, 46, 46, 
	46, 46, 46, 46, 46, 20, 47, 74, 
	75, 47, 76, 77, 78, 79, 47, 21, 
	46, 27, 31, 37
};

static const unsigned char _query_trans_actions[] = {
	105, 143, 116, 99, 0, 61, 0, 93, 
	0, 0, 0, 0, 63, 152, 97, 0, 
	0, 152, 5, 103, 0, 0, 110, 91, 
	0, 0, 0, 9, 113, 101, 0, 0, 
	65, 95, 0, 0, 0, 47, 53, 0, 
	0, 0, 51, 55, 0, 0, 0, 0, 
	0, 49, 0, 0, 0, 0, 0, 146, 
	0, 0, 0, 45, 149, 43, 39, 0, 
	11, 41, 137, 33, 5, 5, 23, 134, 
	131, 128, 0, 107, 27, 29, 0, 0, 
	0, 137, 143, 143, 35, 0, 37, 137, 
	143, 125, 21, 25, 31, 57, 0, 85, 
	13, 0, 71, 79, 0, 83, 0, 0, 
	152, 89, 59, 0, 152, 0, 110, 0, 
	69, 0, 67, 143, 87, 110, 77, 19, 
	73, 15, 7, 75, 17, 0, 119, 143, 
	143, 122, 143, 143, 143, 143, 140, 0, 
	81, 0, 0, 0
};

static const unsigned char _query_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 1, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0
};

static const unsigned char _query_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 3, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0
};

static const short _query_eof_trans[] = {
	1, 1, 4, 4, 8, 8, 8, 8, 
	8, 1, 15, 15, 1, 20, 20, 1, 
	24, 1, 1, 1, 1, 30, 30, 30, 
	34, 34, 34, 34, 34, 34, 34, 34, 
	34, 34, 34, 34, 34, 34, 34, 34, 
	1, 1, 1, 34, 34, 34, 0, 1, 
	96, 96, 99, 96, 100, 102, 106, 106, 
	106, 106, 106, 106, 1, 113, 113, 113, 
	96, 115, 117, 113, 119, 121, 124, 1, 
	117, 117, 117, 117, 117, 117, 117, 117, 
	96, 117, 137, 1
};

static const int query_start = 46;
static const int query_first_final = 46;
static const int query_error = -1;

static const int query_en_main = 46;


/* #line 711 "lexer.rl" */

QueryNode *RSQuery_ParseRaw_v3(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v3(rm_malloc);

  int cs, act;
  const char* ts = q->raw;          // query start
  const char* te = q->raw + q->len; // query end
  
/* #line 345 "lexer.c" */
	{
	cs = query_start;
	ts = 0;
	te = 0;
	act = 0;
	}

/* #line 719 "lexer.rl" */
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};

  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;

  
/* #line 362 "lexer.c" */
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
/* #line 381 "lexer.c" */
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
/* #line 97 "lexer.rl" */
	{act = 1;}
	break;
	case 4:
/* #line 108 "lexer.rl" */
	{act = 2;}
	break;
	case 5:
/* #line 119 "lexer.rl" */
	{act = 3;}
	break;
	case 6:
/* #line 128 "lexer.rl" */
	{act = 4;}
	break;
	case 7:
/* #line 146 "lexer.rl" */
	{act = 6;}
	break;
	case 8:
/* #line 155 "lexer.rl" */
	{act = 7;}
	break;
	case 9:
/* #line 260 "lexer.rl" */
	{act = 20;}
	break;
	case 10:
/* #line 289 "lexer.rl" */
	{act = 24;}
	break;
	case 11:
/* #line 297 "lexer.rl" */
	{act = 25;}
	break;
	case 12:
/* #line 313 "lexer.rl" */
	{act = 27;}
	break;
	case 13:
/* #line 347 "lexer.rl" */
	{act = 32;}
	break;
	case 14:
/* #line 357 "lexer.rl" */
	{act = 34;}
	break;
	case 15:
/* #line 367 "lexer.rl" */
	{act = 35;}
	break;
	case 16:
/* #line 407 "lexer.rl" */
	{act = 37;}
	break;
	case 17:
/* #line 530 "lexer.rl" */
	{act = 40;}
	break;
	case 18:
/* #line 626 "lexer.rl" */
	{act = 43;}
	break;
	case 19:
/* #line 137 "lexer.rl" */
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
	case 20:
/* #line 155 "lexer.rl" */
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
	case 21:
/* #line 166 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, QUOTE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 22:
/* #line 174 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, NOT_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 23:
/* #line 182 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, EQUAL_EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 24:
/* #line 206 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 25:
/* #line 222 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 26:
/* #line 238 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 27:
/* #line 253 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 28:
/* #line 267 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 29:
/* #line 274 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 30:
/* #line 281 "lexer.rl" */
	{te = p+1;{ 
     tok.pos = ts-q->raw;
     RSQuery_Parse_v3(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
   }}
	break;
	case 31:
/* #line 305 "lexer.rl" */
	{te = p+1;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TILDE, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 32:
/* #line 321 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 33:
/* #line 329 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 34:
/* #line 337 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 35:
/* #line 345 "lexer.rl" */
	{te = p+1;}
	break;
	case 36:
/* #line 347 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 37:
/* #line 355 "lexer.rl" */
	{te = p+1;}
	break;
	case 38:
/* #line 378 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 3 + is_attr) - 2;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 39:
/* #line 407 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 40:
/* #line 445 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 41:
/* #line 488 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
    RSQuery_Parse_v3(pParser, SUFFIX, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 42:
/* #line 530 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    // we don't remove the trailing spaces, because the prefix ends when
    // '*' is found, then the spaces are part of the tag.

    RSQuery_Parse_v3(pParser, PREFIX, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 43:
/* #line 574 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    RSQuery_Parse_v3(pParser, CONTAINS, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 44:
/* #line 613 "lexer.rl" */
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
	case 45:
/* #line 639 "lexer.rl" */
	{te = p+1;{
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
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
	case 46:
/* #line 652 "lexer.rl" */
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
	case 47:
/* #line 665 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 3 + is_attr) - 2;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 48:
/* #line 694 "lexer.rl" */
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
	case 49:
/* #line 97 "lexer.rl" */
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
	case 50:
/* #line 108 "lexer.rl" */
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
	case 51:
/* #line 128 "lexer.rl" */
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
	case 52:
/* #line 190 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, EQUAL, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 53:
/* #line 198 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, GT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 54:
/* #line 214 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, LT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 55:
/* #line 245 "lexer.rl" */
	{te = p;p--;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 56:
/* #line 260 "lexer.rl" */
	{te = p;p--;{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 57:
/* #line 313 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 58:
/* #line 347 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 59:
/* #line 367 "lexer.rl" */
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
	case 60:
/* #line 626 "lexer.rl" */
	{te = p;p--;{
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
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
	case 61:
/* #line 108 "lexer.rl" */
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
	case 62:
/* #line 245 "lexer.rl" */
	{{p = ((te))-1;}{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 63:
/* #line 260 "lexer.rl" */
	{{p = ((te))-1;}{ 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 64:
/* #line 313 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 65:
/* #line 347 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 66:
/* #line 367 "lexer.rl" */
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
	case 67:
/* #line 626 "lexer.rl" */
	{{p = ((te))-1;}{
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
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
	case 68:
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
	case 20:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 24:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, MINUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 25:
	{{p = ((te))-1;} 
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PLUS, tok, q);  
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 27:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 32:
	{{p = ((te))-1;}
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 34:
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
	case 35:
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
	case 37:
	{{p = ((te))-1;}
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 40:
	{{p = ((te))-1;}
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
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
      {p++; goto _out; }
    }

    // we don't remove the trailing spaces, because the prefix ends when
    // '*' is found, then the spaces are part of the tag.

    RSQuery_Parse_v3(pParser, PREFIX, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = 1;
    tok.s = te - 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 43:
	{{p = ((te))-1;}
    int is_attr = (*(ts + 1) == '$') ? 1 : 0;
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
/* #line 1499 "lexer.c" */
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
/* #line 1512 "lexer.c" */
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

/* #line 727 "lexer.rl" */

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

