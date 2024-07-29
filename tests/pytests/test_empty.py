from common import *
import json

EMPTY_RESULT = [0]

def TestEmptyNonIndexed():
    """Tests that we throw and error in case of a query with an empty string
    for a field that doesn't index empty values."""

    env = DialectEnv()
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT').ok()
    conn.execute_command('HSET', 'h2', 'tag', '', 'text', '')

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # A query with no field mask should return an empty result, not throwing
        # an error
        res = conn.execute_command('FT.SEARCH', 'idx', '')
        env.assertEqual(res, EMPTY_RESULT)
        res = conn.execute_command('FT.SEARCH', 'idx', '""')
        env.assertEqual(res, EMPTY_RESULT)
        res = conn.execute_command('FT.SEARCH', 'idx', "''")
        env.assertEqual(res, EMPTY_RESULT)

        # Any query containing "@tag:{''}" or "@text:''" should throw an error
        for query in ['@tag:{""}', "@tag:{''}", '@text:""', "@text:''"]:
            env.expect(
                'FT.SEARCH', 'idx', query
            ).error().contains('Use `INDEXEMPTY` in field creation in order to index and query for empty strings')

        # Bad syntax for empty tag should return empty result (compatibility..)
        for query in ['@tag:""', "@tag:''", '@tag:("")', "@tag:('')"]:
            env.expect(
                'FT.SEARCH', 'idx', query
            ).equal(EMPTY_RESULT)

    env.flush()

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text1', 'TEXT', 'text2', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'text1', 'TEXT', 'text2', 'TEXT', 'INDEXEMPTY').ok()

    # Multiple fields in one expression
    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # An error should be thrown in case no field in the mask indexes empty
        # values
        env.expect(
            'FT.SEARCH', 'idx', '@text1|text2:""'
        ).error().contains('Use `INDEXEMPTY` in field creation in order to index and query for empty strings')

        # A result should be returned in case at least on of the fields indexes
        # empty values
        env.expect(
            'FT.SEARCH', 'idx2', '@text1|text2:""'
        ).equal(EMPTY_RESULT)
        conn.execute_command('HSET', 'h1', 'text1', '', 'text2', '')
        env.expect(
            'FT.SEARCH', 'idx2', '@text1|text2:""'
        ).equal([1, 'h1', ['text1', '', 'text2', '']])
        conn.execute_command('DEL', 'h1')

def EmptyTagJSONTest(env, idx, dialect):
    """Tests the indexing and querying of empty values for a TAG field of a
    JSON index"""

    conn = getConnectionByEnv(env)

    # Populate the db with a document that has an empty TAG field
    empty_j = {
    't': ''
    }
    empty_js = json.dumps(empty_j, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', empty_js).equal('OK')

    # Search for a single document, via its indexed empty value
    res = conn.execute_command('FT.SEARCH', idx, '@t:{""}')
    if dialect >= 3:
        empty_js = json.dumps([empty_j], separators=(',', ':'))
    expected = [1, 'j', ['$', empty_js]]
    env.assertEqual(res, expected)

    # Multi-value (automatically dereferenced).
    # add a document with an empty value
    j = {
        't': ['a', '', 'c']
    }
    js = json.dumps(j, separators=(',', ':'))
    conn.execute_command('JSON.SET', 'j', '$', js)
    res = conn.execute_command('FT.SEARCH', idx, '@t:{""}')
    # add a document where all values are non-empty
    k = {
        't': ['a', 'b', 'c']
    }
    ks = json.dumps(k, separators=(',', ':'))
    conn.execute_command('JSON.SET', 'k', '$', ks)
    if dialect >= 3:
        js = json.dumps([j], separators=(',', ':'))
    expected = [1, 'j', ['$', js]]
    env.assertEqual(res, expected)

def EmptyTextJSONTest(env, idx, dialect):
    """Tests the indexing and querying of empty values for a TEXT field of a
    JSON index"""

    conn = getConnectionByEnv(env)

    # Populate the db with a document that has an empty TEXT field
    empty_j = {
    't': ''
    }
    empty_js = json.dumps(empty_j, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', empty_js).equal('OK')

    # Search for a single document, via its indexed empty value
    res = conn.execute_command('FT.SEARCH', idx, '@t:("")')
    if dialect >= 3:
        empty_js = json.dumps([empty_j], separators=(',', ':'))
    expected = [1, 'j', ['$', empty_js]]
    env.assertEqual(res, expected)

    # Multi-value (automatically dereferenced).
    # add a document with an empty value
    j = {
        't': ['a', '', 'c']
    }
    js = json.dumps(j, separators=(',', ':'))
    conn.execute_command('JSON.SET', 'j', '$', js)
    # add a document where all values are non-empty
    k = {
        't': ['a', 'b', 'c']
    }
    ks = json.dumps(k, separators=(',', ':'))
    conn.execute_command('JSON.SET', 'k', '$', ks)

    res = conn.execute_command('FT.SEARCH', idx, '@t:""')
    if dialect >= 3:
        js = json.dumps([j], separators=(',', ':'))
    expected = [1, 'j', ['$', js]]
    env.assertEqual(res, expected)

def testEmptyTag():
    """Tests that empty values are indexed properly"""

    env = DialectEnv()
    conn = getConnectionByEnv(env)
    MAX_DIALECT = set_max_dialect(env)

    def testEmptyTagHash(env, conn, idx, dialect):
        """Tests the indexing and querying of empty values for a TAG field of a
        hash index"""

        # Populate the db with a document that has an empty value for a TAG field
        conn.execute_command('HSET', 'h1', 't', '')

        # ------------------------- Simple retrieval ---------------------------
        # Search for a single document, via its indexed empty value
        res = conn.execute_command('FT.SEARCH', idx, '@t:{""}')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, "@t:{''}")
        env.assertEqual(res, expected)

        # Make sure the document is NOT returned when searching for a non-empty
        # value
        res = conn.execute_command('FT.SEARCH', idx, '@t:foo')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        # ------------------------------ Negation ------------------------------
        # Search for a negation of an empty value, make sure the document is NOT
        # returned
        res = conn.execute_command('FT.SEARCH', idx, '-@t:{""}')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, "-@t:{''}")
        env.assertEqual(res, expected)

        # Search for a negation of a non-empty value, make sure the document is
        # returned
        res = conn.execute_command('FT.SEARCH', idx, '-@t:{foo}')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        # --------------------- Optional Operator ------------------------------
        res = conn.execute_command('FT.SEARCH', idx, '~@t:{""}')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        conn.execute_command('HSET', 'h2', 't', 'bar')
        res = conn.execute_command(
            'FT.SEARCH', idx, '~@t:{""}', 'SORTBY', 't', 'ASC', 'WITHCOUNT')
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'bar']]
        env.assertEqual(res, expected)
        conn.execute_command('DEL', 'h2')

        # ------------------------------- Union --------------------------------
        # Union of empty and non-empty values
        res = conn.execute_command('FT.SEARCH', idx, '@t:{""} | @t:{foo}')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, "@t:{''} | @t:{foo}")
        env.assertEqual(res, expected)

        # adding documents with two tags, one of which is empty
        conn.execute_command('HSET', 'h2', 't', 'bar,')

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:{""} | @t:{foo}',
            'SORTBY', 't', 'ASC', 'WITHCOUNT')
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'bar,']]
        env.assertEqual(res, expected)

        conn.execute_command('DEL', 'h2')

        # adding another document with an non-empty value
        conn.execute_command('HSET', 'h2', 't', 'bar')
        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:{""} | @t:{foo} | @t:{bar}', 
            'SORTBY', 't', 'ASC', 'WITHCOUNT')
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'bar']]
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:{foo} | @t:{""} | @t:{bar}', 
            'SORTBY', 't', 'ASC', 'WITHCOUNT')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:{foo} | @t:{bar} | @t:{""}', 
            'SORTBY', 't', 'ASC', 'WITHCOUNT')
        env.assertEqual(res, expected)

        conn.execute_command('DEL', 'h2')

        # ---------------------------- Intersection ----------------------------
        # Intersection of empty and non-empty values
        res = conn.execute_command('FT.SEARCH', idx, '@t:{""} @t:{foo}')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        # adding documents with two tags, one of which is empty
        conn.execute_command('HSET', 'h2', 't', 'bar,')
        res = conn.execute_command('FT.SEARCH', idx, '@t:{""} @t:{bar}')
        expected = [1, 'h2', ['t', 'bar,']]
        env.assertEqual(res, expected)

        conn.execute_command('DEL', 'h2')

        # ------------------------------- Prefix -------------------------------
        # We shouldn't get the document when searching for a prefix of "__empty"
        cmd = f'FT.SEARCH {idx} @t:{{*pty}}'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # ------------------------------- Suffix -------------------------------
        # We shouldn't get the document when searching for a suffix of "__empty"
        cmd = f'FT.SEARCH {idx} @t:{{__em*}}'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Add a document that will be found by the suffix search
        conn.execute_command('HSET', 'h2', 't', 'empty')
        cmd = f'FT.SEARCH {idx} @t:{{*pty}}'.split(' ')
        expected = [1, 'h2', ['t', 'empty']]
        cmd_assert(env, cmd, expected)
        conn.execute_command('DEL', 'h2')

        # -------------------- Combination with other fields -------------------
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello | @t:{""}']
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, [1, 'h1', ['t', '']])

        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello @t:{""}']
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, EMPTY_RESULT)

        # Non-empty intersection with another field
        conn.execute_command('HSET', 'h1', 'text', 'hello')
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello @t:{""}']
        expected = [1, 'h1', ['t', '', 'text', 'hello']]
        cmd_assert(env, cmd, expected)

        # Non-empty union with another field
        conn.execute_command('HSET', 'h2', 'text', 'love you', 't', 'movie')
        res = conn.execute_command('FT.SEARCH', idx, 'love | @t:{""}', 'SORTBY', 'text', 'ASC')
        expected = [
            2, 'h1', ['text', 'hello', 't', ''], 'h2', ['text', 'love you', 't', 'movie']
        ]
        env.assertEqual(res, expected)

        if dialect == 2:
            # Checking the functionality of our pipeline with empty values
            # ------------------------------- APPLY --------------------------------
            # Populate with some data that we will be able to see the `APPLY`
            res = conn.execute_command(
                'FT.AGGREGATE', idx, '*', 'LOAD', 1, '@t', 
                'APPLY', 'upper(@t)', 'as', 'upper_t',
                'SORTBY', 4, '@t', 'ASC', '@upper_t', 'ASC')
            expected = [
                ANY, \
                ['t', '', 'upper_t', ''], \
                ['t', 'movie', 'upper_t', 'MOVIE']
            ]
            env.assertEqual(res, expected)

            # ------------------------------ SORTBY --------------------------------
            cmd = f'FT.AGGREGATE {idx} * LOAD * SORTBY 2 @t ASC'.split(' ')
            expected = [
                ANY, \
                ['t', '', 'text', 'hello'], \
                ['t', 'movie', 'text', 'love you']
            ]
            cmd_assert(env, cmd, expected)

            # Reverse order
            cmd = f'FT.AGGREGATE {idx} * LOAD * SORTBY 2 @t DESC'.split(' ')
            expected = [
                ANY, \
                ['t', 'movie', 'text', 'love you'], \
                ['t', '', 'text', 'hello']
            ]
            cmd_assert(env, cmd, expected)

        # ------------------------------ GROUPBY -------------------------------
        conn.execute_command('HSET', 'h3', 't', 'movie')
        conn.execute_command('HSET', 'h4', 't', '')
        cmd = f'FT.AGGREGATE {idx} * GROUPBY 1 @t REDUCE COUNT 0 AS count'.split(' ')
        expected = [
            ANY, \
            ['t', '', 'count', '2'], \
            ['t', 'movie', 'count', '2']
        ]
        cmd_assert(env, cmd, expected)

        # --------------------------- SEPARATOR --------------------------------
        # Remove added documents
        for i in range(2, 5):
            conn.execute_command('DEL', f'h{i}')

        # Validate that separated empty fields are indexed as empty as well
        conn.execute_command('HSET', 'h5', 't', ', bar')
        conn.execute_command('HSET', 'h6', 't', 'bat, ')
        conn.execute_command('HSET', 'h7', 't', 'bat,')
        conn.execute_command('HSET', 'h8', 't', 'bat, , bat2')
        conn.execute_command('HSET', 'h9', 't', ',')
        res = conn.execute_command('FT.SEARCH', idx, '@t:{""}', 'SORTBY', 't', 'ASC', 'WITHCOUNT')
        expected = [
            ANY,
            'h1', ['t', '', 'text', 'hello'],
            'h9', ['t', ','],
            'h5', ['t', ', bar'],
            'h7', ['t', 'bat,'],
            'h6', ['t', 'bat, '],
            'h8', ['t', 'bat, , bat2']
        ]
        env.assertEqual(res, expected)

        # ------------------------ Priority vs. Intersection -----------------------
        res = env.cmd('FT.SEARCH', idx, '@t:{""} -@t:{""}')
        env.assertEqual(res, EMPTY_RESULT)

        res = env.cmd('FT.SEARCH', idx, '-@t:{""} @t:{""}')
        env.assertEqual(res, EMPTY_RESULT)

    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # Create an index with a TAG field, that also indexes empty strings, another
        # TAG field that doesn't index empty values, and a TEXT field
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx', dialect)
        env.flush()

        # ----------------------------- SORTABLE case ------------------------------
        # Create an index with a SORTABLE TAG field, that also indexes empty strings
        env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 'SORTABLE', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx_sortable', dialect)
        env.flush()

        # --------------------------- WITHSUFFIXTRIE case --------------------------
        # Create an index with a TAG field, that also indexes empty strings, while
        # using a suffix trie
        env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 'WITHSUFFIXTRIE', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx_suffixtrie', dialect)
        env.flush()

        # Test that when we index many docs, we find the wanted portion of them upon
        # empty value indexing
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY').ok()
        n_docs = 1000
        for i in range(n_docs):
            conn.execute_command('HSET', f'h{i}', 't', '' if i % 2 == 0 else f'{i}')
        res = env.cmd('FT.SEARCH', 'idx', '@t:{""}', 'WITHCOUNT', 'LIMIT', '0', '0')
        env.assertEqual(int(res[0]), 500)
        res = env.cmd('FT.SEARCH', 'idx', '-@t:{""}', 'WITHCOUNT', 'LIMIT', '0', '0')
        env.assertEqual(int(res[0]), 500)
        env.flush()
    
@skip(no_json=True)   
def testEmptyTagJSON():
    env = DialectEnv()
    conn = getConnectionByEnv(env)
    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)
                # ---------------------------------- JSON ----------------------------------
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'INDEXEMPTY').ok()
        EmptyTagJSONTest(env, 'jidx', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'INDEXEMPTY', 'SORTABLE').ok()
        EmptyTagJSONTest(env, 'jidx_sortable', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'INDEXEMPTY', 'WITHSUFFIXTRIE').ok()
        EmptyTagJSONTest(env, 'jidx_suffix', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$arr[*]', 'AS', 'arr', 'TAG', 'INDEXEMPTY').ok()
        # Empty array values ["a", "", "c"] with explicit array components indexing
        arr = {
            'arr': ['a', '', 'c']
        }
        arrs = json.dumps(arr, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
        res = conn.execute_command('FT.SEARCH', 'jidx', '@arr:{""}')
        if dialect >= 3:
            arrs = json.dumps([arr], separators=(',', ':'))
        expected = [1, 'j', ['$', arrs]]
        env.assertEqual(res, expected)

        # Empty arrays shouldn't be indexed for this indexing mechanism
        arr = {
            'arr': []
        }
        arrs = json.dumps(arr, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
        res = conn.execute_command('FT.SEARCH', 'jidx', '@arr:{""}')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)
        conn.execute_command('DEL', 'j')

        # Empty object shouldn't be indexed for this indexing mechanism (flatten, [*])
        obj = {
            'arr': {}
        }
        objs = json.dumps(obj, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', objs).equal('OK')
        res = conn.execute_command('FT.SEARCH', 'jidx', '@arr:{""}', 'RETURN', '1', 'arr')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        env.flush()

        # An attempt to index a non-empty object as a TAG (and in general) should fail (coverage)
        j = {
            "t": {"lala": "lali"}
        }
        js = json.dumps(j)
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG', 'INDEXEMPTY').ok()
        env.expect('JSON.SET', 'j', '$', js).equal('OK')
        cmd = f'FT.SEARCH jidx @t:{{""}}'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Make sure we experienced an indexing failure, via `FT.INFO`
        info = index_info(env, 'jidx')
        env.assertEqual(info['hash_indexing_failures'], 1)

        env.flush()
        
def testEmptyText():
    """Tests the indexing and querying of empty TEXT (field type) values"""

    env = DialectEnv()
    MAX_DIALECT = set_max_dialect(env)

    def testEmptyTextHash(env, idx, dialect):
        """Tests the indexing and querying of empty values for a TEXT field of a
        hash index
        Extensive tests are added here, specifically to the query part, due to
        the addition of the `isempty` function syntax added to the parser.
        """

        conn = getConnectionByEnv(env)

        # Populate the db with a document that has an empty value for a TEXT field
        conn.execute_command('HSET', 'h1', 't', '')

        # ------------------------- Simple retrieval ---------------------------
        # Search for a single document, via its indexed empty value
        res = conn.execute_command('FT.SEARCH', idx, "@t:''")
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        # Search without the field name
        res = conn.execute_command('FT.SEARCH', idx, '""')
        env.assertEqual(res, expected)

        # Search using double quotes
        res = conn.execute_command('FT.SEARCH', idx, '@t:""')
        env.assertEqual(res, expected)

        # Search using double quotes and parentheses
        res = conn.execute_command('FT.SEARCH', idx, '@t:("")')
        env.assertEqual(res, expected)

        # Search using single quotes and parentheses
        res = conn.execute_command('FT.SEARCH', idx, "@t:('')")
        env.assertEqual(res, expected)

        # ------------------------------ Negation ------------------------------
        # Search for a negation of an empty value, make sure the document is NOT
        # returned
        res = conn.execute_command('FT.SEARCH', idx, '-@t:("")')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, '-""')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, '-@t:""')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, '-@t:("")')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, "-@t:('')")
        env.assertEqual(res, expected)

        # Search for a negation of a non-empty value, make sure the document is
        # returned
        res = conn.execute_command('FT.SEARCH', idx, '-@t:foo')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH', idx, '-foo')
        env.assertEqual(res, expected)

        # --------------------- Optional Operator ------------------------------
        res = conn.execute_command('FT.SEARCH', idx, '~@t:""')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        conn.execute_command('HSET', 'h2', 't', 'bar')
        res = conn.execute_command(
            'FT.SEARCH', idx, '~@t:("")', 'SORTBY', 't', 'ASC', 'WITHCOUNT')
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'bar']]
        env.assertEqual(res, expected)
        conn.execute_command('DEL', 'h2')

        # ------------------------------- Union --------------------------------
        # Union of empty and non-empty values
        res = conn.execute_command('FT.SEARCH', idx, '@t:("") | @t:foo')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        # Same in opposite order
        res = conn.execute_command('FT.SEARCH', idx, '@t:foo | @t:("")')
        env.assertEqual(res, expected)

        if dialect < 5:
            res = conn.execute_command('FT.SEARCH', idx, '@t:(foo | "")')
            env.assertEqual(res, expected)

            res = conn.execute_command('FT.SEARCH', idx, '@t:("" | foo)')
            env.assertEqual(res, expected)

        # adding another document with an non-empty value
        conn.execute_command('HSET', 'h2', 't', 'bar')

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:"" | @t:foo | @t:bar', 'SORTBY', 't', 'ASC')
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'bar']]
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:foo | @t:"" | @t:bar', 'SORTBY', 't', 'ASC')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:foo | @t:bar | @t:""', 'SORTBY', 't', 'ASC')
        env.assertEqual(res, expected)

        res = conn.execute_command(
            'FT.SEARCH', idx, "@t:foo | @t:bar | @t:''", 'SORTBY', 't', 'ASC')
        env.assertEqual(res, expected)

        if dialect < 5:
            res = conn.execute_command(
                'FT.SEARCH', idx, '@t:(foo | "" | bar)', 'SORTBY', 't', 'ASC')
            env.assertEqual(res, expected)

            res = conn.execute_command(
                'FT.SEARCH', idx, '@t:("" | foo | bar)', 'SORTBY', 't', 'ASC')
            env.assertEqual(res, expected)

            res = conn.execute_command(
                'FT.SEARCH', idx, '@t:(foo | bar | "")', 'SORTBY', 't', 'ASC')
            env.assertEqual(res, expected)

            res = conn.execute_command(
                'FT.SEARCH', idx, "@t:(foo | bar | '')", 'SORTBY', 't', 'ASC')
            env.assertEqual(res, expected)

        # ---------------------------- Intersection ----------------------------
        # Empty intersection
        res = conn.execute_command('FT.SEARCH',idx, '@t:("") @t:foo')
        expected = EMPTY_RESULT
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH',idx, '@t:"" @t:foo')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH',idx, "@t:'' @t:foo")
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH',idx, "'' foo")
        env.assertEqual(res, expected)

        # Non-empty intersection
        res = conn.execute_command('FT.SEARCH',idx, '@t:"" -@t:foo')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH',idx, '"" -foo')
        env.assertEqual(res, expected)

        # Same in opposite order
        res = conn.execute_command('FT.SEARCH',idx, '-@t:foo @t:""')
        env.assertEqual(res, expected)

        res = conn.execute_command('FT.SEARCH',idx, '-foo ""')
        env.assertEqual(res, expected)

        # ------------------------------- Prefix -------------------------------
        cmd = f'FT.SEARCH {idx} @t:*pty'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # ------------------------------- Suffix -------------------------------
        cmd = f'FT.SEARCH {idx} @t:__em*'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Add a document that will be found by the suffix search
        conn.execute_command('HSET', 'h2', 't', 'empty')
        cmd = f'FT.SEARCH {idx} @t:*pty'.split(' ')
        expected = [1, 'h2', ['t', 'empty']]
        cmd_assert(env, cmd, expected)
        conn.execute_command('DEL', 'h2')

        # ------------------------------- Summarization ------------------------
        # When searching for such a query, we expect to get an empty value, and
        # thus an "empty summary".
        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:("")', 'SUMMARIZE', 'FIELDS', 1, 't',
            'FRAGS', 3, 'LEN', 10)
        expected = [
            1,
            "h1",
            [
                "t",
                "... "
            ]
        ]
        env.assertEqual(res, expected)

        # ---------------------------- Highlighting ----------------------------
        # When searching for such a query, we expect to get an empty value, and
        # thus an "empty highlight".
        res = conn.execute_command(
            'FT.SEARCH', idx, '@t:("")', 'HIGHLIGHT', 'FIELDS', 1, 't')
        expected = [
            1,
            "h1",
            [
                "t",
                "<b></b>"
            ]
        ]
        env.assertEqual(res, expected)

    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # Create an index with a TAG field, that also indexes empty strings, another
        # TAG field that doesn't index empty values, and a TEXT field
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'INDEXEMPTY').ok()
        testEmptyTextHash(env, 'idx', dialect)
        env.flush()

        # ----------------------------- SORTABLE case ------------------------------
        # Create an index with a SORTABLE TAG field, that also indexes empty strings
        env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TEXT', 'INDEXEMPTY', 'SORTABLE').ok()
        testEmptyTextHash(env, 'idx_sortable', dialect)
        env.flush()

        # --------------------------- WITHSUFFIXTRIE case --------------------------
        # Create an index with a TAG field, that also indexes empty strings, while
        # using a suffix trie
        env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TEXT', 'INDEXEMPTY', 'WITHSUFFIXTRIE').ok()
        testEmptyTextHash(env, 'idx_suffixtrie', dialect)
        env.flush()

        # ------------------------------- Phonetic ---------------------------------
        # Create an index with a TEXT field, that also indexes empty strings, and
        # uses phonetic indexing
        env.expect('FT.CREATE', 'idx_phonetic', 'SCHEMA', 't', 'TEXT', 'INDEXEMPTY', 'PHONETIC', 'dm:en').ok()
        testEmptyTextHash(env, 'idx_phonetic', dialect)
        env.flush()

      
@skip(no_json=True)
def testEmptyTextJSON():
    env = DialectEnv()
    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)
      # ---------------------------------- JSON ----------------------------------
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'INDEXEMPTY').ok()
        EmptyTextJSONTest(env, 'jidx', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'INDEXEMPTY', 'SORTABLE').ok()
        EmptyTextJSONTest(env, 'jidx_sortable', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'INDEXEMPTY', 'WITHSUFFIXTRIE').ok()
        EmptyTextJSONTest(env, 'jidx_suffix', dialect)
        env.flush()
    

def testEmptyInfo():
    """Tests that the `FT.INFO` command returns the correct information
    regarding the indexing of empty values for a field"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    # Create an index with the currently supported field types (TAG, TEXT)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 'text', 'TEXT', 'INDEXEMPTY').ok()

    info = index_info(env, 'idx')
    tag_info = info['attributes'][0]
    env.assertEqual(tag_info[-1], 'INDEXEMPTY')
    text_info = info['attributes'][1]
    env.assertEqual(text_info[-1], 'INDEXEMPTY')

def testEmptyExplainCli():
    """Tests the output of `FT.EXPAINCLI` for queries that include empty values,
    for both TAG and TEXT fields, for all supporting dialects ({2 ,3, 4, 5})."""

    env = DialectEnv()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'INDEXEMPTY', 'text', 'TEXT', 'INDEXEMPTY').ok()

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # ------------------------------ TAG field -----------------------------
        res = env.cmd('FT.EXPLAINCLI', 'idx', '-@tag:{""} @tag:{""}')
        expected = [
            'INTERSECT {',
            '  NOT{',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '  TAG:@tag {',
            '    ""',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{bar} | @tag:{foo} @tag:{""}')
        expected = [
            'UNION {',
            '  TAG:@tag {',
            '    bar',
            '  }',
            '  INTERSECT {',
            '    TAG:@tag {',
            '      foo',
            '    }',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '-@tag:{bar} | @tag:{foo} @tag:{""}')
        expected = [
            'UNION {',
            '  NOT{',
            '    TAG:@tag {',
            '      bar',
            '    }',
            '  }',
            '  INTERSECT {',
            '    TAG:@tag {',
            '      foo',
            '    }',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{bar} | -@tag:{foo} -@tag:{""}')
        expected = [
            'UNION {',
            '  TAG:@tag {',
            '    bar',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@tag {',
            '        foo',
            '      }',
            '    }',
            '    NOT{',
            '      TAG:@tag {',
            '        ""',
            '      }',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '-@tag:{""} @tag:{""} | @tag:{bar}')
        expected = [
            'UNION {',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@tag {',
            '        ""',
            '      }',
            '    }',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '  TAG:@tag {',
            '    bar',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{""} | -@tag:{bar} -@tag:{""}')
        expected = [
            'UNION {',
            '  TAG:@tag {',
            '    ""',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@tag {',
            '        bar',
            '      }',
            '    }',
            '    NOT{',
            '      TAG:@tag {',
            '        ""',
            '      }',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '-@tag:{""} | -@tag:{bar} @tag:{""}')
        expected = [
            'UNION {',
            '  NOT{',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@tag {',
            '        bar',
            '      }',
            '    }',
            '    TAG:@tag {',
            '      ""',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{"" | bar}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  ""',
                '  bar',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  "" | bar',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{foo | ""}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  foo',
                '  ""',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  foo | ""',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{"" | ""}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  ""',
                '  ""',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  "" | ""',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{foo ""}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  INTERSECT {',
                '    foo',
                '    ""',
                '  }',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  foo ""',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{"" bar}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  INTERSECT {',
                '    ""',
                '    bar',
                '  }',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  "" bar',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@tag:{"" ""}')
        if dialect < 5:
            expected = [
                'TAG:@tag {',
                '  INTERSECT {',
                '    ""',
                '    ""',
                '  }',
                '}',
                ''
            ]
        else:
            expected = [
                'TAG:@tag {',
                '  "" ""',
                '}',
                ''
            ]
        env.assertEqual(res, expected)

        # # ------------------------------ TEXT field ----------------------------
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:""')
        expected = [
            '@text:\"\"',
            ''
        ]
        env.assertEqual(res, expected)

        # Same with wrapping parentheses.
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:("")')
        expected = [
            '@text:\"\"',
            ''
        ]
        env.assertEqual(res, expected)

        # Intersection
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:"" @text:foo')
        expected = [
            'INTERSECT {',
            '  @text:""',
            '  @text:UNION {',
            '    @text:foo',
            '    @text:+foo(expanded)',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # Intersection with general query
        res = env.cmd('FT.EXPLAINCLI', 'idx', 'foo @text:""')
        expected = [
            'INTERSECT {',
            '  UNION {',
            '    foo',
            '    +foo(expanded)',
            '  }',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # Other way around
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:"" foo')
        expected = [
            'INTERSECT {',
            '  @text:""',
            '  UNION {',
            '    foo',
            '    +foo(expanded)',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # Union
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:"" | @text:foo')
        expected = [
            'UNION {',
            '  @text:""',
            '  @text:UNION {',
            '    @text:foo',
            '    @text:+foo(expanded)',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # Other way around
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:foo | @text:""')
        expected = [
            'UNION {',
            '  @text:UNION {',
            '    @text:foo',
            '    @text:+foo(expanded)',
            '  }',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # UNION operator for a single TEXT field
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:(foo | "")')
        expected = [
            '@text:UNION {',
            '  @text:UNION {',
            '    @text:foo',
            '    @text:+foo(expanded)',
            '  }',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:("" | bar)')
        expected = [
            '@text:UNION {',
            '  @text:""',
            '  @text:UNION {',
            '    @text:bar',
            '    @text:+bar(expanded)',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:("" | "")')
        expected = [
            '@text:UNION {',
            '  @text:""',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        # INTERSECTION operator for a single TEXT field
        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:(foo "")')
        expected = [
            '@text:INTERSECT {',
            '  @text:UNION {',
            '    @text:foo',
            '    @text:+foo(expanded)',
            '  }',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:("" bar)')
        expected = [
            '@text:INTERSECT {',
            '  @text:""',
            '  @text:UNION {',
            '    @text:bar',
            '    @text:+bar(expanded)',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', 'idx', '@text:("" "")')
        expected = [
            '@text:INTERSECT {',
            '  @text:""',
            '  @text:""',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

def testInvalidUseOfEmptyString():
    """Tests that invalid syntax for empty values is rejected by the parser"""

    env = DialectEnv()
    conn = getConnectionByEnv(env)

    dim = 4
    # Create an index
    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        't', 'TAG', 'INDEXEMPTY',
        'text', 'TEXT', 'INDEXEMPTY', 'PHONETIC', 'dm:en',
        'location', 'GEO',
        'v', 'VECTOR', 'FLAT', '6', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
        'TYPE', 'FLOAT32',).ok()
    conn.execute_command('HSET', 'h1', 't', '')
    conn.execute_command('HSET', 'h2', 't', 'a')

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # Unsupported empty string in fuzzy terms
        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%""%)')
        env.assertEqual(res, EMPTY_RESULT)

        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%%""%%)')
        env.assertEqual(res, EMPTY_RESULT)

        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%%%""%%%)')
        env.assertEqual(res, EMPTY_RESULT)

        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%$p%)', 
                                   'PARAMS', 2, 'p', '""')
        env.assertEqual(res, EMPTY_RESULT)

        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%%$p%%)', 
                                   'PARAMS', 2, 'p', '""')
        env.assertEqual(res, EMPTY_RESULT)

        res = conn.execute_command('FT.SEARCH', 'idx', '@text:(%%%$p%%%)', 
                                   'PARAMS', 2, 'p', '""')
        env.assertEqual(res, EMPTY_RESULT)
        
        # Invalid use of empty string in geo filter
        expected_error = 'Invalid GeoFilter unit'
        env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 10 ""]').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 10 $p]',
                   'PARAMS', 2, 'p', '').error().\
            contains(expected_error)
        
        expected_error = 'Syntax error'
        env.expect('FT.SEARCH', 'idx', '@location:['' 4.56 10 km]').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@location:[1.23 '' 10 km]').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 '' km]').error().\
            contains(expected_error)
        
        # Fix this tests after implementing MOD-7025
        # empty string is PARAM evaluated as 0
        res = env.execute_command(
            'FT.SEARCH', 'idx', '@location:[$long 4.56 10 km]',
            'PARAMS', 2, 'long', '')
        env.assertEqual(res, EMPTY_RESULT)
        res = env.execute_command(
            'FT.SEARCH', 'idx', '@location:[1.23 $lat 10 km]',
            'PARAMS', 2, 'lat', '')
        env.assertEqual(res, EMPTY_RESULT)
        env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 $radius km]',
            'PARAMS', 2, 'radius', '').error().\
            contains('Invalid GeoFilter radius')

        # Invalid use of empty string as $weight value
        expected_error = 'Invalid value () for `weight`'
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$weight:""}').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$weight:$p}',
                   'PARAMS', 2, 'p', '').error().contains(expected_error)

        # Invalid use of empty string as $inorder value
        expected_error = 'Invalid value () for `inorder`'
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$inorder:""}').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$inorder:$p}',
                   'PARAMS', 2, 'p', '').error().contains(expected_error)
        
        # Invalid use of empty string as $slop value
        expected_error = 'Invalid value () for `slop`'
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$slop:""}').error().\
            contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@t:{abc}=>{$slop:$p}',
                   'PARAMS', 2, 'p', '').error().contains(expected_error)

        # Invalid use of empty string as $phonetic value
        expected_error = 'Invalid value () for `phonetic`'
        env.expect('FT.SEARCH', 'idx', '@x:(hello=>{$phonetic:""} world)')\
            .error().contains(expected_error)
        env.expect('FT.SEARCH', 'idx', '@x:(hello=>{$phonetic:$p} world)',
                   'PARAMS', 2, 'p', '').error().contains(expected_error)

        # Invalid use of empty string as $yield_distance_as value
        expected_error = 'Invalid value () for `yield_distance_as`'
        env.expect(
            'FT.AGGREGATE', 'idx', '*=>[KNN 3 @v $blob]=>{$yield_distance_as:""}',
            'PARAMS', '2', 'blob', create_np_array_typed([0] * dim).tobytes()).\
            error().contains(expected_error)
        env.expect(
            'FT.AGGREGATE', 'idx', '*=>[KNN 3 @v $blob]=>{$yield_distance_as:$p}',
            'PARAMS', 4, 'blob', create_np_array_typed([0] * dim).tobytes(),
            'p', '').error().contains(expected_error)

        # Invalid use of empty string as part of modifier list
        env.expect('FT.SEARCH', 'idx', '@text|"":(abc)').error().\
            contains('Syntax error')
        
        env.expect('FT.SEARCH', 'idx', '@""|text:(abc)').error().\
            contains('Syntax error')
        
        env.expect('FT.SEARCH', 'idx', '@text|text|"":(abc)').error().\
            contains('Syntax error')

        env.expect('FT.SEARCH', 'idx', '@t|"":{abc}').error().\
            contains('Syntax error')
        
        env.expect('FT.SEARCH', 'idx', '@""|t:{abc}').error().\
            contains('Syntax error')
        
        env.expect('FT.SEARCH', 'idx', '@t|t|"":{abc}').error().\
            contains('Syntax error')
        
        # Invalid use of an empty string in a vector query
        env.expect('FT.SEARCH', 'idx', '*=>["" 4 @v $blob AS dist]').error().\
            contains('Expecting Vector Similarity command')
        env.expect('FT.SEARCH', 'idx', '*=>[KNN "" @v $blob AS dist]').error().\
            contains('Syntax error')

def testEmptyParam():
    """Tests that we can use an empty string as a parameter in a query"""

    env = DialectEnv()
    conn = getConnectionByEnv(env)

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 't2',
               'TAG', 'text1', 'TEXT', 'INDEXEMPTY', 'text2', 'TEXT').ok()

    # Add a document with an empty value for a TAG field
    conn.execute_command('HSET', 'h1', 't', '')
    conn.execute_command('HSET', 'h2', 't2', '')
    conn.execute_command('HSET', 'h3', 'text1', '')
    conn.execute_command('HSET', 'h4', 'text2', '')

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # Test that we can use an empty string as a parameter
        res = env.cmd('FT.SEARCH', 'idx', '@t:{$p} | @t2:{$p}', 'PARAMS', 2, 'p', '')
        expected = [1, 'h1', ['t', '']]
        env.assertEqual(res, expected)

        # Test that we can use an empty string as a parameter
        res = env.expect(
            'FT.SEARCH', 'idx', '@text1:($p) | @text2:($p)',
                      'PARAMS', 2, 'p', ''
        ).error().contains('Use `INDEXEMPTY` in field creation in order to index and query for empty strings')

        # Same with result
        res = env.cmd('FT.SEARCH', 'idx', '@text1:($p)',
                      'PARAMS', 2, 'p', '')
        expected = [1, 'h3', ['text1', '']]
        env.assertEqual(res, expected)

def testemptyfuzzy():
    """Tests that the fuzzy search is compatible with the empty string, when
    indexed (relevant for TEXT fields only)."""

    env = DialectEnv()
    conn = getConnectionByEnv(env)

    # Create an index with a TEXT field, that also indexes empty strings
    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        't', 'TEXT', 'INDEXEMPTY',
        't2', 'TEXT'
    ).ok()

    # Add a document with an empty value for a TEXT field
    conn.execute_command('HSET', 'h1', 't', '')
    conn.execute_command('HSET', 'h2', 't', 'sr')

    queries = ['%s%', '%%s%%', '%%%s%%%', '%%sr%%', '%%%srs%%%']
    field_queries = [f'@t:{s}' for s in queries]
    wrong_field_queries = [f'@t2:{s}' for s in queries]

    MAX_DIALECT = set_max_dialect(env)
    for dialect in range(2, MAX_DIALECT + 1):
        env.set_dialect(dialect)

        # Search for the document, it should be found (the second is for sanity) for
        # all supported distances ({1, 2, 3})
        for s in queries:
            res = env.cmd('FT.SEARCH', 'idx', s)
            expected = [2, 'h1', ['t', ''], 'h2', ['t', 'sr']]
            env.assertEqual(res, expected)

        # The results should not be returned for a field that does not have
        # matching values
        for s in wrong_field_queries:
            res = env.cmd('FT.SEARCH', 'idx', s)
            expected = EMPTY_RESULT
            env.assertEqual(res, expected)

        # On the other hand, they should be found for the correct field
        for s in field_queries:
            res = env.cmd('FT.SEARCH', 'idx', s)
            expected = [2, 'h1', ['t', ''], 'h2', ['t', 'sr']]
            env.assertEqual(res, expected)

        # We should be able to search for strings that are in some distance from the
        # empty string as well
        res = env.cmd('FT.SEARCH', 'idx', "%%''%%")
        expected = [2, 'h1', ['t', ''], 'h2', ['t', 'sr']]
        env.assertEqual(res, expected)

    # We shouldn't return results for DIALECT 1
    env.set_dialect(1)
    for s in queries + field_queries:
        res = env.cmd('FT.SEARCH', 'idx', s)
        expected = [1, 'h2', ['t', 'sr']]
        env.assertEqual(res, expected)
