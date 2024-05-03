
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


/* #line 671 "lexer.rl" */



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
	59, 1, 60, 1, 61, 2, 2, 3, 
	2, 2, 4, 2, 2, 5, 2, 2, 
	6, 2, 2, 7, 2, 2, 8, 2, 
	2, 9, 2, 2, 10, 2, 2, 11, 
	2, 2, 12, 2, 2, 13, 2, 2, 
	14, 2, 2, 15, 2, 2, 16, 2, 
	2, 17, 2, 2, 18
};

static const short _query_key_offsets[] = {
	0, 10, 20, 22, 22, 25, 28, 38, 
	48, 50, 52, 55, 57, 58, 59, 61, 
	64, 66, 67, 68, 70, 72, 73, 73, 
	78, 82, 83, 87, 91, 95, 96, 100, 
	105, 110, 114, 118, 123, 128, 130, 131, 
	131, 135, 139, 144, 149, 154, 156, 157, 
	157, 161, 164, 168, 178, 219, 230, 240, 
	251, 254, 256, 270, 281, 287, 292, 295, 
	311, 323, 324, 327, 333, 338, 341, 357, 
	371, 384, 386, 388, 390, 391, 401, 414, 
	427, 440, 453, 466, 479, 492, 502, 516
};

static const char _query_trans_keys[] = {
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 9, 13, 32, 47, 58, 64, 
	91, 96, 123, 126, 39, 92, 39, 92, 
	110, 39, 92, 102, 9, 13, 32, 47, 
	58, 64, 91, 96, 123, 126, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	48, 57, 48, 57, 45, 48, 57, 48, 
	57, 110, 102, 48, 57, 45, 48, 57, 
	48, 57, 119, 39, 39, 92, 39, 92, 
	41, 36, 42, 92, 119, 125, 36, 42, 
	92, 125, 125, 36, 42, 92, 125, 36, 
	42, 92, 125, 36, 42, 92, 125, 125, 
	36, 42, 92, 125, 36, 42, 92, 119, 
	125, 36, 39, 42, 92, 125, 36, 42, 
	92, 125, 36, 42, 92, 125, 36, 39, 
	42, 92, 125, 36, 39, 42, 92, 125, 
	39, 92, 125, 36, 42, 92, 125, 36, 
	42, 92, 125, 36, 39, 42, 92, 125, 
	36, 39, 42, 92, 125, 36, 39, 42, 
	92, 125, 39, 92, 125, 36, 42, 92, 
	125, 39, 92, 125, 36, 42, 92, 125, 
	9, 13, 32, 47, 58, 64, 91, 96, 
	123, 126, 32, 34, 36, 37, 39, 40, 
	41, 42, 43, 45, 58, 59, 61, 64, 
	65, 73, 91, 92, 93, 95, 97, 105, 
	123, 124, 125, 126, 127, 0, 8, 9, 
	13, 14, 31, 33, 47, 48, 57, 60, 
	63, 94, 96, 42, 92, 96, 0, 47, 
	58, 64, 91, 94, 123, 127, 92, 96, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 39, 92, 105, 39, 92, 
	36, 45, 92, 96, 0, 47, 48, 57, 
	58, 64, 91, 94, 123, 127, 42, 92, 
	96, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 46, 69, 101, 48, 57, 42, 
	69, 101, 48, 57, 42, 48, 57, 42, 
	46, 69, 92, 96, 101, 0, 47, 48, 
	57, 58, 64, 91, 94, 123, 127, 42, 
	45, 92, 96, 0, 47, 58, 64, 91, 
	94, 123, 127, 105, 105, 48, 57, 42, 
	46, 69, 101, 48, 57, 42, 69, 101, 
	48, 57, 42, 48, 57, 42, 46, 69, 
	92, 96, 101, 0, 47, 48, 57, 58, 
	64, 91, 94, 123, 127, 42, 45, 92, 
	96, 0, 47, 48, 57, 58, 64, 91, 
	94, 123, 127, 42, 92, 96, 0, 47, 
	48, 57, 58, 64, 91, 94, 123, 127, 
	40, 123, 39, 92, 39, 92, 62, 92, 
	96, 0, 47, 58, 64, 91, 94, 123, 
	127, 42, 83, 92, 96, 115, 0, 47, 
	58, 64, 91, 94, 123, 127, 42, 83, 
	92, 96, 115, 0, 47, 58, 64, 91, 
	94, 123, 127, 42, 69, 92, 96, 101, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	42, 77, 92, 96, 109, 0, 47, 58, 
	64, 91, 94, 123, 127, 42, 80, 92, 
	96, 112, 0, 47, 58, 64, 91, 94, 
	123, 127, 42, 84, 92, 96, 116, 0, 
	47, 58, 64, 91, 94, 123, 127, 42, 
	89, 92, 96, 121, 0, 47, 58, 64, 
	91, 94, 123, 127, 9, 13, 32, 47, 
	58, 64, 91, 96, 123, 126, 42, 83, 
	92, 96, 110, 115, 0, 47, 58, 64, 
	91, 94, 123, 127, 42, 92, 96, 102, 
	0, 47, 58, 64, 91, 94, 123, 127, 
	0
};

static const char _query_single_lengths[] = {
	0, 0, 2, 0, 3, 3, 0, 2, 
	0, 0, 1, 0, 1, 1, 0, 1, 
	0, 1, 1, 2, 2, 1, 0, 5, 
	4, 1, 4, 4, 4, 1, 4, 5, 
	5, 4, 4, 5, 5, 2, 1, 0, 
	4, 4, 5, 5, 5, 2, 1, 0, 
	4, 3, 4, 0, 27, 3, 2, 3, 
	3, 2, 4, 3, 4, 3, 1, 6, 
	4, 1, 1, 4, 3, 1, 6, 4, 
	3, 2, 2, 2, 1, 2, 5, 5, 
	5, 5, 5, 5, 5, 0, 6, 4
};

static const char _query_range_lengths[] = {
	5, 5, 0, 0, 0, 0, 5, 4, 
	1, 1, 1, 1, 0, 0, 1, 1, 
	1, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 5, 7, 4, 4, 4, 
	0, 0, 5, 4, 1, 1, 1, 5, 
	4, 0, 1, 1, 1, 1, 5, 5, 
	5, 0, 0, 0, 0, 4, 4, 4, 
	4, 4, 4, 4, 4, 5, 4, 4
};

static const short _query_index_offsets[] = {
	0, 6, 12, 15, 16, 20, 24, 30, 
	37, 39, 41, 44, 46, 48, 50, 52, 
	55, 57, 59, 61, 64, 67, 69, 70, 
	76, 81, 83, 88, 93, 98, 100, 105, 
	111, 117, 122, 127, 133, 139, 142, 144, 
	145, 150, 155, 161, 167, 173, 176, 178, 
	179, 184, 188, 193, 199, 234, 242, 249, 
	257, 261, 264, 274, 282, 288, 293, 296, 
	308, 317, 319, 322, 328, 333, 336, 348, 
	358, 367, 370, 373, 376, 378, 385, 395, 
	405, 415, 425, 435, 445, 455, 461, 472
};

static const unsigned char _query_indicies[] = {
	1, 1, 1, 1, 1, 0, 2, 2, 
	2, 2, 2, 0, 4, 5, 3, 3, 
	4, 5, 7, 3, 4, 5, 8, 3, 
	9, 9, 9, 9, 9, 0, 11, 10, 
	10, 10, 10, 10, 9, 12, 10, 14, 
	13, 15, 16, 13, 16, 13, 17, 0, 
	18, 0, 19, 0, 21, 22, 20, 22, 
	0, 24, 23, 25, 23, 23, 27, 26, 
	28, 27, 26, 29, 23, 26, 23, 31, 
	32, 33, 23, 30, 23, 34, 35, 36, 
	30, 37, 23, 30, 30, 30, 30, 23, 
	23, 23, 39, 23, 38, 23, 40, 39, 
	41, 38, 42, 23, 38, 38, 38, 38, 
	23, 30, 30, 30, 43, 30, 23, 23, 
	45, 23, 46, 23, 44, 23, 23, 46, 
	47, 44, 44, 44, 44, 44, 23, 49, 
	44, 49, 50, 51, 48, 49, 52, 49, 
	50, 51, 48, 53, 54, 49, 36, 0, 
	49, 23, 23, 46, 36, 44, 48, 48, 
	48, 48, 49, 23, 55, 34, 35, 36, 
	30, 57, 30, 58, 59, 60, 56, 57, 
	61, 58, 59, 60, 56, 62, 63, 57, 
	64, 0, 57, 23, 34, 35, 36, 30, 
	62, 63, 65, 57, 56, 56, 56, 56, 
	57, 66, 66, 66, 66, 66, 0, 68, 
	70, 71, 72, 73, 74, 75, 76, 77, 
	78, 80, 81, 82, 83, 84, 85, 86, 
	87, 88, 89, 84, 90, 91, 92, 93, 
	94, 67, 67, 68, 67, 69, 79, 69, 
	69, 1, 95, 96, 0, 0, 0, 0, 
	0, 1, 98, 97, 97, 97, 97, 97, 
	2, 95, 98, 99, 99, 99, 99, 99, 
	2, 97, 5, 100, 3, 4, 5, 3, 
	103, 104, 11, 102, 102, 105, 102, 102, 
	102, 9, 107, 11, 106, 106, 106, 106, 
	106, 9, 107, 108, 109, 109, 12, 106, 
	107, 109, 109, 14, 106, 107, 16, 106, 
	107, 108, 110, 11, 106, 110, 106, 105, 
	106, 106, 106, 9, 107, 15, 11, 106, 
	106, 106, 106, 106, 9, 111, 97, 111, 
	113, 112, 95, 115, 116, 116, 113, 114, 
	95, 116, 116, 19, 114, 95, 22, 114, 
	95, 115, 118, 96, 117, 118, 117, 79, 
	117, 117, 117, 1, 95, 21, 96, 119, 
	119, 120, 119, 119, 119, 1, 95, 96, 
	114, 114, 120, 114, 114, 114, 1, 122, 
	123, 121, 53, 54, 49, 62, 63, 57, 
	125, 97, 126, 0, 0, 0, 0, 0, 
	66, 95, 127, 96, 119, 127, 119, 119, 
	119, 119, 1, 95, 128, 96, 119, 128, 
	119, 119, 119, 119, 1, 95, 129, 96, 
	119, 129, 119, 119, 119, 119, 1, 95, 
	130, 96, 119, 130, 119, 119, 119, 119, 
	1, 95, 131, 96, 119, 131, 119, 119, 
	119, 119, 1, 95, 132, 96, 119, 132, 
	119, 119, 119, 119, 1, 95, 133, 96, 
	119, 133, 119, 119, 119, 119, 1, 1, 
	1, 1, 1, 1, 97, 95, 128, 96, 
	119, 134, 128, 119, 119, 119, 119, 1, 
	95, 96, 119, 135, 119, 119, 119, 119, 
	1, 0
};

static const char _query_trans_targs[] = {
	52, 53, 55, 2, 52, 3, 52, 5, 
	57, 59, 52, 6, 60, 52, 61, 11, 
	62, 13, 52, 68, 52, 16, 69, 52, 
	18, 19, 20, 22, 21, 52, 24, 27, 
	31, 42, 25, 26, 52, 52, 28, 30, 
	29, 52, 52, 32, 33, 35, 34, 52, 
	36, 37, 41, 74, 40, 38, 39, 43, 
	44, 45, 49, 50, 75, 48, 46, 47, 
	52, 75, 77, 52, 52, 52, 52, 54, 
	52, 56, 52, 52, 58, 65, 66, 70, 
	73, 52, 76, 77, 78, 79, 52, 85, 
	52, 53, 86, 52, 52, 52, 52, 52, 
	0, 52, 1, 52, 4, 52, 52, 7, 
	8, 63, 52, 52, 9, 10, 64, 12, 
	52, 67, 52, 14, 15, 52, 71, 52, 
	72, 52, 17, 23, 52, 52, 51, 53, 
	80, 81, 82, 83, 84, 53, 87, 53
};

static const unsigned char _query_trans_actions[] = {
	91, 126, 102, 0, 55, 0, 87, 0, 
	108, 138, 85, 0, 5, 89, 5, 0, 
	0, 0, 9, 96, 81, 0, 0, 83, 
	0, 0, 0, 0, 0, 57, 0, 0, 
	0, 0, 0, 0, 39, 47, 0, 0, 
	0, 45, 49, 0, 0, 0, 0, 41, 
	0, 0, 0, 132, 0, 0, 0, 0, 
	0, 0, 0, 0, 129, 0, 0, 0, 
	43, 135, 99, 37, 33, 35, 11, 120, 
	27, 120, 15, 17, 117, 120, 114, 93, 
	111, 23, 0, 120, 126, 126, 29, 0, 
	31, 120, 126, 19, 13, 21, 25, 51, 
	0, 73, 0, 63, 0, 65, 71, 0, 
	0, 138, 79, 53, 0, 0, 138, 0, 
	69, 96, 61, 0, 0, 59, 126, 75, 
	96, 67, 0, 0, 77, 7, 0, 105, 
	126, 126, 126, 126, 126, 123, 126, 108
};

static const unsigned char _query_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 1, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0
};

static const unsigned char _query_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 3, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0
};

static const short _query_eof_trans[] = {
	1, 1, 1, 1, 7, 7, 1, 11, 
	11, 14, 14, 14, 1, 1, 1, 21, 
	1, 24, 24, 24, 24, 24, 24, 24, 
	24, 24, 24, 24, 24, 24, 24, 24, 
	24, 24, 24, 24, 24, 1, 1, 1, 
	24, 24, 24, 24, 24, 1, 1, 1, 
	24, 24, 24, 1, 0, 1, 98, 100, 
	98, 102, 103, 107, 107, 107, 107, 107, 
	107, 98, 113, 115, 115, 115, 118, 120, 
	115, 122, 125, 1, 98, 1, 120, 120, 
	120, 120, 120, 120, 120, 98, 120, 120
};

static const int query_start = 52;
static const int query_first_final = 52;
static const int query_error = -1;

static const int query_en_main = 52;


/* #line 674 "lexer.rl" */

QueryNode *RSQuery_ParseRaw_v3(QueryParseCtx *q) {
  void *pParser = RSQuery_ParseAlloc_v3(rm_malloc);

  int cs, act;
  const char* ts = q->raw;          // query start
  const char* te = q->raw + q->len; // query end
  
/* #line 346 "lexer.c" */
	{
	cs = query_start;
	ts = 0;
	te = 0;
	act = 0;
	}

/* #line 682 "lexer.rl" */
  QueryToken tok = {.len = 0, .pos = 0, .s = 0};

  //parseCtx ctx = {.root = NULL, .ok = 1, .errorMsg = NULL, .q = q};
  const char* p = q->raw;
  const char* pe = q->raw + q->len;
  const char* eof = pe;

  
/* #line 363 "lexer.c" */
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
/* #line 382 "lexer.c" */
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
/* #line 91 "lexer.rl" */
	{act = 1;}
	break;
	case 4:
/* #line 103 "lexer.rl" */
	{act = 2;}
	break;
	case 5:
/* #line 115 "lexer.rl" */
	{act = 3;}
	break;
	case 6:
/* #line 125 "lexer.rl" */
	{act = 4;}
	break;
	case 7:
/* #line 145 "lexer.rl" */
	{act = 6;}
	break;
	case 8:
/* #line 155 "lexer.rl" */
	{act = 7;}
	break;
	case 9:
/* #line 214 "lexer.rl" */
	{act = 14;}
	break;
	case 10:
/* #line 230 "lexer.rl" */
	{act = 16;}
	break;
	case 11:
/* #line 246 "lexer.rl" */
	{act = 18;}
	break;
	case 12:
/* #line 279 "lexer.rl" */
	{act = 23;}
	break;
	case 13:
/* #line 289 "lexer.rl" */
	{act = 25;}
	break;
	case 14:
/* #line 299 "lexer.rl" */
	{act = 26;}
	break;
	case 15:
/* #line 310 "lexer.rl" */
	{act = 27;}
	break;
	case 16:
/* #line 369 "lexer.rl" */
	{act = 28;}
	break;
	case 17:
/* #line 487 "lexer.rl" */
	{act = 31;}
	break;
	case 18:
/* #line 596 "lexer.rl" */
	{act = 34;}
	break;
	case 19:
/* #line 135 "lexer.rl" */
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
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, OR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 23:
/* #line 182 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 24:
/* #line 190 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 25:
/* #line 198 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 26:
/* #line 206 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 27:
/* #line 222 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, SEMICOLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 28:
/* #line 238 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, TILDE, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 29:
/* #line 254 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, PERCENT, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 30:
/* #line 262 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, LSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 31:
/* #line 270 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, RSQB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 32:
/* #line 277 "lexer.rl" */
	{te = p+1;}
	break;
	case 33:
/* #line 279 "lexer.rl" */
	{te = p+1;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 34:
/* #line 287 "lexer.rl" */
	{te = p+1;}
	break;
	case 35:
/* #line 310 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = te - (ts + 3);
    tok.s = ts + 2;

    bool parsed = false;
    if(tok.len > 3) {
      if(tok.s[0] == 'w' && tok.s[1] == '\'' && tok.s[tok.len - 1] == '\'') {
        int is_attr = (*(ts + 4) == '$') ? 1 : 0;
        tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
        tok.len = te - (ts + 6 + is_attr);
        tok.s = ts + 4 + is_attr;
        tok.pos = tok.s - q->raw;
        RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
        parsed = true;
      }
    }

    if(!parsed) {
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
    }
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
	case 36:
/* #line 369 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = te - (ts + 3);
    tok.s = ts + 2;
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
	case 37:
/* #line 404 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 4) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 6 + is_attr);
    tok.s = ts + 4 + is_attr;
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
	case 38:
/* #line 439 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 3 + is_attr) - 1;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    // Invalid case: wildcard and suffix
    if(tok.s[0] == 'w' && tok.s[1] == '\'') {
      {p++; goto _out; }
    }
    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
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
	case 39:
/* #line 487 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

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
    // Invalid case: wildcard and prefix
    if(tok.s[0] == 'w' && tok.s[1] == '\'') {
      {p++; goto _out; }
    }
    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }
    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
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
	case 40:
/* #line 535 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 2;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 3) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_TERM : QT_TERM;
    tok.len = te - (ts + 3 + is_attr) - 2;
    tok.s = ts + 3 + is_attr;
    tok.pos = tok.s - q->raw;
    // Invalid case: wildcard and contains
    if(tok.s[0] == 'w' && tok.s[1] == '\'') {
      {p++; goto _out; }
    }
    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }
    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
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
	case 41:
/* #line 583 "lexer.rl" */
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
	case 42:
/* #line 609 "lexer.rl" */
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
	case 43:
/* #line 622 "lexer.rl" */
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
	case 44:
/* #line 635 "lexer.rl" */
	{te = p+1;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LP, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    int is_attr = (*(ts + 4) == '$') ? 1 : 0;
    tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
    tok.len = te - (ts + 6 + is_attr);
    tok.s = ts + 4 + is_attr;
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
	case 45:
/* #line 91 "lexer.rl" */
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
	case 46:
/* #line 103 "lexer.rl" */
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
	case 47:
/* #line 125 "lexer.rl" */
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
	case 48:
/* #line 155 "lexer.rl" */
	{te = p;p--;{
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
	case 49:
/* #line 214 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 50:
/* #line 230 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, MINUS, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 51:
/* #line 246 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 52:
/* #line 279 "lexer.rl" */
	{te = p;p--;{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 53:
/* #line 299 "lexer.rl" */
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
	case 54:
/* #line 369 "lexer.rl" */
	{te = p;p--;{
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = te - (ts + 3);
    tok.s = ts + 2;
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
	case 55:
/* #line 596 "lexer.rl" */
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
	case 56:
/* #line 103 "lexer.rl" */
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
	case 57:
/* #line 214 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 58:
/* #line 246 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 59:
/* #line 279 "lexer.rl" */
	{{p = ((te))-1;}{
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }}
	break;
	case 60:
/* #line 596 "lexer.rl" */
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
	case 61:
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
	case 14:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
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
	case 18:
	{{p = ((te))-1;}
    tok.pos = ts-q->raw;
    RSQuery_Parse_v3(pParser, STAR, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 23:
	{{p = ((te))-1;}
    tok.pos = ts - q->raw;
    RSQuery_Parse_v3(pParser, PUNCTUATION, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }
  }
	break;
	case 25:
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
	case 26:
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
	case 27:
	{{p = ((te))-1;}
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = te - (ts + 3);
    tok.s = ts + 2;

    bool parsed = false;
    if(tok.len > 3) {
      if(tok.s[0] == 'w' && tok.s[1] == '\'' && tok.s[tok.len - 1] == '\'') {
        int is_attr = (*(ts + 4) == '$') ? 1 : 0;
        tok.type = is_attr ? QT_PARAM_WILDCARD : QT_WILDCARD;
        tok.len = te - (ts + 6 + is_attr);
        tok.s = ts + 4 + is_attr;
        tok.pos = tok.s - q->raw;
        RSQuery_Parse_v3(pParser, WILDCARD, tok, q);
        parsed = true;
      }
    }

    if(!parsed) {
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
    }
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
	case 28:
	{{p = ((te))-1;}
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.s = ts + 1;
    tok.pos = tok.s - q->raw;
    RSQuery_Parse_v3(pParser, LB, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

    tok.len = te - (ts + 3);
    tok.s = ts + 2;
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
	case 31:
	{{p = ((te))-1;}
    tok.numval = 0;
    tok.len = 1;
    tok.s = ts;
    RSQuery_Parse_v3(pParser, COLON, tok, q);
    if (!QPCTX_ISOK(q)) {
      {p++; goto _out; }
    }

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
    // Invalid case: wildcard and prefix
    if(tok.s[0] == 'w' && tok.s[1] == '\'') {
      {p++; goto _out; }
    }
    // remove leading spaces
    while(tok.len && isspace(tok.s[0])) {
      tok.s++;
      tok.len--;
    }
    // remove trailing spaces
    while(tok.len > 1 && isspace(tok.s[tok.len - 1])) {
      tok.len--;
    }
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
	case 34:
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
/* #line 1548 "lexer.c" */
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
/* #line 1561 "lexer.c" */
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

/* #line 690 "lexer.rl" */

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

