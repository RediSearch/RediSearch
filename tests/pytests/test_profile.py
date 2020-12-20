# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env

string1 = 'For the exchange of decimal floating-point numbers, \
					 interchange formats of any multiple of 32 bits are defined. \
					 As with binary interchange, the encoding scheme for the decimal interchange formats encodes the sign, exponent, and significand. \
					 Two different bit-level encodings are defined, and interchange is complicated by the fact that some external indicator of the encoding in use may be required. \
					 The two options allow the significand to be encoded as a compressed sequence of decimal digits using densely packed decimal or, alternatively, as a binary integer. \
					 The former is more convenient for direct hardware implementation of the standard, while the latter is more suited to software emulation on a binary computer. \
					 In either case, the set of numbers (combinations of sign, significand, and exponent) \
					 that may be encoded is identical, and special values (±zero with the minimum exponent, ±infinity, quiet NaNs, and signaling NaNs) have identical encodings.'

string2 = 'For the binary formats, the representation is made unique by choosing the smallest representable exponent allowing the value to be represented exactly. \
					 Further, the exponent is not represented directly, but a bias is added so that the smallest representable exponent is represented as 1, with 0 used for subnormal numbers. \
					 For numbers with an exponent in the normal range (the exponent field being neither all ones nor all zeros), \
					 the leading bit of the significand will always be 1. \
					 Consequently, a leading 1 can be implied rather than explicitly present in the memory encoding, \
					 and under the standard the explicitly represented part of the significand will lie between 0 and 1. \
					 This rule is called leading bit convention, implicit bit convention, or hidden bit convention. \
					 This rule allows the binary format to have an extra bit of precision. \
					 The leading bit convention cannot be used for the subnormal numbers as they have an exponent outside \
					 the normal exponent range and scale by the smallest represented exponent as used for the smallest normal numbers.'

def testProfileSearch(env):
	conn = getConnectionByEnv(env)
 	env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)

 	env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
	conn.execute_command('hset', '1', 't', 'hello')
	conn.execute_command('hset', '2', 't', 'world')

	actual_res = conn.execute_command('ft.profile', 'search', 'idx', '*', 'nocontent')
	expected_res = [2L, ['Parsing and iterator creation time', '0.040000000000000001'],
											[['Wildcard iterator', 3L],
											'Index', '0.0070000000000000001', 3L,
											'Scorer', '0.0050000000000000001', 3L,
											'Sorter', '0.005000000000000001', 1L],
											'1', '2']
	env.assertEqual(len(actual_res), len(expected_res))
	result_processors = ['Index', 'Scorer', 'Sorter']
	for item in result_processors:
		env.assertIn(item, actual_res[2])

	actual_res = conn.execute_command('ft.profile', 'search', 'idx', 'hello', 'nocontent')
	expected_res = [1L, ['Parsing and iterator creation time', '0.017999999999999999'],
											[['Reader', 'hello', 2L],
											'Index', '0.0050000000000000001', 2L,
											'Scorer', '0.0030000000000000001', 2L,
											'Sorter', '0.002', 1L],
											'1']
	env.assertEqual(len(actual_res), len(expected_res))

	actual_res = conn.execute_command('ft.profile', 'search', 'idx', 'hello|world', 'nocontent')
	expected_res = [2L, ['Parsing and iterator creation time', '0.027'],
											[['Union iterator', 3L, ['Reader', 'hello', 2L], 
																							['Reader', 'world', 2L]],
											'Index', '0.014', 3L,
											'Scorer', '0.0060000000000000001', 3L,
											'Sorter', '0.0029999999999999992', 1L],
											 '1', '2']
	env.assertEqual(len(actual_res), len(expected_res))

	actual_res = conn.execute_command('ft.profile', 'search', 'idx', 'hello world', 'nocontent')
	expected_res = [0L, ['Parsing and iterator creation time', '0.028000000000000001'],
											[['Intersect iterator', 1L, ['Reader', 'hello', 2L],
																									['Reader', 'world', 1L]],
											'Index', '0.0080000000000000002', 1L,
											'Scorer', '0', 1L,
											'Sorter', '0.002', 1L]]
	env.assertEqual(len(actual_res), len(expected_res))

	actual_res = conn.execute_command('ft.profile', 'aggregate', 'idx', 'hello',
																		'groupby', 1, '@t',
																		'REDUCE', 'count', '0', 'as', 'sum')
	env.assertEqual(len(actual_res), 4)

def testProfileNumeric(env):
	conn = getConnectionByEnv(env)
 	env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
	 
 	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	conn.execute_command('hset', '1', 'n', '1.2')
	conn.execute_command('hset', '2', 'n', '1.5')
	conn.execute_command('hset', '3', 'n', '8.2')
	conn.execute_command('hset', '4', 'n', '6.7')
	conn.execute_command('hset', '5', 'n', '-14')

	actual_res = conn.execute_command('ft.profile', 'search', 'idx', '@n:[0,100]', 'nocontent')
	expected_res = [1L, ['Parsing and iterator creation time', '0.017999999999999999'],
											[['Reader', 'hello', 2L],
											'Index', '0.0050000000000000001', 2L,
											'Scorer', '0.0030000000000000001', 2L,
											'Sorter', '0.002', 1L],
											'1']
	env.assertEqual(len(actual_res), 7)

def _testProfileSearch(env):
	docs = 10000
	copies = 10
	queries = 0

	conn = getConnectionByEnv(env)
	pl = conn.pipeline()
 	env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
 	env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
 	for i in range(docs):
 		pl.execute_command('hset', i, 't', str(i / copies), 'hello', string1, 'world', string2)
 		if (i % 999) is 0:
 			pl.execute()
 	pl.execute()
	
	print "finished loading"
	#search_string = '12*|87*|42*'
	#search_string = '(4|5) (5|6)'
	#search_string = '1(1(1(1(1(1(1))))))'
	search_string = '1(1(1(1(1))))'
	#print env.cmd('FT.search', 'idx', '12*|69*', 'limit', 0, 0)
	for i in range(queries):
		pl.execute_command('FT.PROFILE', 'search', 'idx', search_string, 'limit', 0, 1000)
		if (i % 999) is 0:
			pl.execute()
	pl.execute()
	res = env.cmd('FT.PROFILE', 'search', 'idx', search_string, 'limit', 0, 1000)
	print res[0:3]