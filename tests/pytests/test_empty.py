from common import *
import json

EMPTY_RESULT = [0]

def testEmptyTag():
    """Tests that empty values are indexed properly"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    def testEmptyTextHash(env, idx):
        """Tests the indexing and querying of empty values for a TAG field of a
        hash index"""

        # Populate the db with a document that has an empty value for a TAG field
        conn.execute_command('HSET', 'h1', 't', '')

        # ------------------------- Simple retrieval ---------------------------
        # Search for a single document, via its indexed empty value
        cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # Make sure the document is NOT returned when searching for a non-empty
        # value
        cmd = f'FT.SEARCH {idx} @t:foo'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # ------------------------------ Negation ------------------------------
        # Search for a negation of an empty value, make sure the document is NOT
        # returned
        cmd = f'FT.SEARCH {idx} -isempty(@t)'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Search for a negation of a non-empty value, make sure the document is
        # returned
        cmd = f'FT.SEARCH {idx} -@t:{{foo}}'.split(' ')
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # ------------------------------- Union --------------------------------
        # Union of empty and non-empty values
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) | @t:{foo}']
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # ---------------------------- Intersection ----------------------------
        # Intersection of empty and non-empty values
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) @t:{foo}']
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

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
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello | isempty(@t)']
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, [1, 'h1', ['t', '']])

        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello isempty(@t)']
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, EMPTY_RESULT)

        # Non-empty intersection with another field
        conn.execute_command('HSET', 'h1', 'text', 'hello')
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['hello isempty(@t)']
        expected = [1, 'h1', ['t', '', 'text', 'hello']]
        cmd_assert(env, cmd, expected)

        # Non-empty union with another field
        conn.execute_command('HSET', 'h2', 'text', 'love you', 't', 'movie')
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['love | isempty(@t)', 'SORTBY', 'text', 'ASC']
        expected = [
            2, 'h1', ['text', 'hello', 't', ''], 'h2', ['text', 'love you', 't', 'movie']
        ]
        cmd_assert(env, cmd, expected)

        # Checking the functionality of our pipeline with empty values
        # ------------------------------- APPLY --------------------------------
        # Populate with some data that we will be able to see the `APPLY`
        cmd = f'FT.AGGREGATE {idx} * LOAD 1 @t APPLY upper(@t) as upper_t SORTBY 4 @t ASC @upper_t ASC'.split(' ')
        expected = [
            ANY, \
            ['t', '', 'upper_t', ''], \
            ['t', 'movie', 'upper_t', 'MOVIE']
        ]
        cmd_assert(env, cmd, expected)

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
        cmd = f'FT.SEARCH {idx} isempty(@t) SORTBY t ASC'.split(' ')
        expected = [
            ANY,
            'h1', ['t', '', 'text', 'hello'],
            'h9', ['t', ','],
            'h5', ['t', ', bar'],
            'h7', ['t', 'bat,'],
            'h6', ['t', 'bat, '],
            'h8', ['t', 'bat, , bat2']
        ]
        cmd_assert(env, cmd, expected)

        # Make sure we don't index h5, h6, h7 in case of a non-empty indexing
        # tag field
        env.cmd('FT.CREATE', 'temp_idx', 'SCHEMA', 't', 'TAG')
        cmd = f'FT.SEARCH temp_idx isempty(@t) SORTBY t ASC'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)
        env.cmd('FT.DROPINDEX', 'temp_idx')

        # ------------------------ Priority vs. Intersection -----------------------
        res = env.cmd('FT.SEARCH', idx, 'isempty(@t) -isempty(@t)')
        env.assertEqual(res, EMPTY_RESULT)

        res = env.cmd('FT.SEARCH', idx, '-isempty(@t) isempty(@t)')
        env.assertEqual(res, EMPTY_RESULT)

        res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) isempty(@t)')
        expected = [
            'INTERSECT {',
            '  NOT{',
            '    TAG:@t {',
            '      <ISEMPTY>', '    }',
            '  }',
            '  TAG:@t {',
            '    <ISEMPTY>',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, '@t:{bar} | @t:{foo} isempty(@t)')
        expected = [
            'UNION {',
            '  TAG:@t {',
            '    bar',
            '  }',
            '  INTERSECT {',
            '    TAG:@t {',
            '      foo',
            '    }',
            '    TAG:@t {',
            '      <ISEMPTY>',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, '-@t:{bar} | @t:{foo} isempty(@t)')
        expected = [
            'UNION {',
            '  NOT{',
            '    TAG:@t {',
            '      bar',
            '    }',
            '  }',
            '  INTERSECT {',
            '    TAG:@t {',
            '      foo',
            '    }',
            '    TAG:@t {',
            '      <ISEMPTY>',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, '@t:{bar} | -@t:{foo} -isempty(@t)')
        expected = [
            'UNION {',
            '  TAG:@t {',
            '    bar',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@t {',
            '        foo',
            '      }',
            '    }',
            '    NOT{',
            '      TAG:@t {',
            '        <ISEMPTY>',
            '      }',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) isempty(@t) | @t:{bar}')
        expected = [
            'UNION {',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@t {',
            '        <ISEMPTY>',
            '      }',
            '    }',
            '    TAG:@t {',
            '      <ISEMPTY>',
            '    }',
            '  }',
            '  TAG:@t {',
            '    bar',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, 'isempty(@t) | -@t:{bar} -isempty(@t)')
        expected = [
            'UNION {',
            '  TAG:@t {',
            '    <ISEMPTY>',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@t {',
            '        bar',
            '      }',
            '    }',
            '    NOT{',
            '      TAG:@t {',
            '        <ISEMPTY>',
            '      }',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

        res = env.cmd('FT.EXPLAINCLI', idx, '-isempty(@t) | -@t:{bar} isempty(@t)')
        expected = [
            'UNION {',
            '  NOT{',
            '    TAG:@t {',
            '      <ISEMPTY>',
            '    }',
            '  }',
            '  INTERSECT {',
            '    NOT{',
            '      TAG:@t {',
            '        bar',
            '      }',
            '    }',
            '    TAG:@t {',
            '      <ISEMPTY>',
            '    }',
            '  }',
            '}',
            ''
        ]
        env.assertEqual(res, expected)

    # Create an index with a TAG field, that also indexes empty strings, another
    # TAG field that doesn't index empty values, and a TEXT field
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'text', 'TEXT').ok()
    testEmptyTextHash(env, 'idx')
    env.flush()

    # ----------------------------- SORTABLE case ------------------------------
    # Create an index with a SORTABLE TAG field, that also indexes empty strings
    env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'SORTABLE', 'text', 'TEXT').ok()
    testEmptyTextHash(env, 'idx_sortable')
    env.flush()

    # --------------------------- WITHSUFFIXTRIE case --------------------------
    # Create an index with a TAG field, that also indexes empty strings, while
    # using a suffix trie
    env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE', 'text', 'TEXT').ok()
    testEmptyTextHash(env, 'idx_suffixtrie')
    env.flush()

    # ---------------------------------- JSON ----------------------------------
    def testEmptyTagJSON(env, idx):
        """Tests the indexing and querying of empty values for a TAG field of a
        JSON index"""

        # Populate the db with a document that has an empty TAG field
        empty_j = {
        't': ''
        }
        empty_js = json.dumps(empty_j, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', empty_js).equal('OK')

        # Search for a single document, via its indexed empty value
        cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
        expected = [1, 'j', ['$', empty_js]]
        cmd_assert(env, cmd, expected)

        # Multi-value
        j = {
            't': ['a', '', 'c']
        }
        js = json.dumps(j, separators=(',', ':'))
        conn.execute_command('JSON.SET', 'j', '$', js)
        cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
        expected = [1, 'j', ['$', js]]
        cmd_assert(env, cmd, expected)

        # Empty array
        # On sortable case, empty arrays are not indexed (MOD-6936)
        if idx != 'jidx_sortable':
            j = {
                't': []
            }
            js = json.dumps(j, separators=(',', ':'))
            conn.execute_command('JSON.SET', 'j', '$', js)
            cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
            expected = [1, 'j', ['$', js]]
            cmd_assert(env, cmd, expected)

        # Empty object
        j = {
            't': {}
        }
        js = json.dumps(j, separators=(',', ':'))
        conn.execute_command('JSON.SET', 'j', '$', js)
        cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
        expected = [1, 'j', ['$', js]]
        cmd_assert(env, cmd, expected)


    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
    testEmptyTagJSON(env, 'jidx')
    env.flush()

    env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'SORTABLE').ok()
    testEmptyTagJSON(env, 'jidx_sortable')
    env.flush()

    env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE').ok()
    testEmptyTagJSON(env, 'jidx_suffix')
    env.flush()

    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$arr[*]', 'AS', 'arr', 'TAG', 'ISEMPTY').ok()
    # Empty array values ["a", "", "c"] with explicit array components indexing
    arr = {
        'arr': ['a', '', 'c']
    }
    arrs = json.dumps(arr, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
    expected = [1, 'j', ['$', arrs]]
    cmd_assert(env, cmd, expected)

    # Empty arrays shouldn't be indexed for this indexing mechanism
    arr = {
        'arr': []
    }
    arrs = json.dumps(arr, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
    expected = EMPTY_RESULT
    cmd_assert(env, cmd, expected)
    conn.execute_command('DEL', 'j')

    # Empty object shouldn't be indexed for this indexing mechanism (flatten, [*])
    obj = {
        'arr': {}
    }
    objs = json.dumps(obj, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', objs).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
    expected = EMPTY_RESULT
    cmd_assert(env, cmd, expected)

    # Searching for emptiness of a non-existing field should return an error
    obj = {
        'obj': {}
    }
    objs = json.dumps(obj, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', objs).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@obj)'.split(' ')
    expected = EMPTY_RESULT
    env.expect(*cmd).error().contains('Syntax error: Field not found')

    env.flush()

    # Embedded empty object
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t.b', 'AS', 'b', 'TAG', 'ISEMPTY').ok()
    j = {
        "t": {"b": {}}
    }
    js = json.dumps(j, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', js).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@b)'.split(' ')
    expected = [1, 'j', ['$', js]]
    cmd_assert(env, cmd, expected)

    # Embedded empty array
    j = {
        "t": {"b": []}
    }
    js = json.dumps(j, separators=(',', ':'))
    env.expect('JSON.SET', 'j', '$', js).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@b)'.split(' ')
    expected = [1, 'j', ['$', js]]
    cmd_assert(env, cmd, expected)

    env.flush()

    # An attempt to index a non-empty object as a TAG should fail (coverage)
    j = {
        "t": {"lala": "lali"}
    }
    js = json.dumps(j)
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
    env.expect('JSON.SET', 'j', '$', js).equal('OK')
    cmd = f'FT.SEARCH jidx isempty(@t)'.split(' ')
    cmd_assert(env, cmd, EMPTY_RESULT)

    # Make sure we experienced an indexing failure, via `FT.INFO`
    info = index_info(env, 'jidx')
    env.assertEqual(info['hash_indexing_failures'], 1)

    env.flush()

    # Test that when we index many docs, we find the wanted portion of them upon
    # empty value indexing
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY').ok()
    n_docs = 1000
    for i in range(n_docs):
        conn.execute_command('HSET', f'h{i}', 't', '' if i % 2 == 0 else f'{i}')
    res = env.cmd('FT.SEARCH', 'idx', 'isempty(@t)', 'LIMIT', '0', '0')
    env.assertEqual(int(res[0]), 500)

def testEmptyText():
    """Tests the indexing and querying of empty TEXT (field type) values"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")
    conn = getConnectionByEnv(env)

    def testEmptyTextHash(idx):
        """Tests the indexing and querying of empty values for a TEXT field of a
        hash index
        Extensive tests are added here, specifically to the query part, due to
        the addition of the `isempty` function syntax added to the parser.
        """

        # Populate the db with a document that has an empty value for a TEXT field
        conn.execute_command('HSET', 'h1', 't', '')

        # ------------------------- Simple retrieval ---------------------------
        # Search for a single document, via its indexed empty value
        cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # ------------------------------ Negation ------------------------------
        # Search for a negation of an empty value, make sure the document is NOT
        # returned
        cmd = f'FT.SEARCH {idx} -isempty(@t)'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Search for a negation of a non-empty value, make sure the document is
        # returned
        cmd = f'FT.SEARCH {idx} -@t:foo'.split(' ')
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # ------------------------------- Union --------------------------------
        # Union of empty and non-empty values
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) | @t:foo']
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # Same in opposite order
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['@t:foo | isempty(@t)']
        cmd_assert(env, cmd, expected)

        # ---------------------------- Intersection ----------------------------
        # Empty intersection
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) @t:foo']
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        # Non-empty intersection
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['isempty(@t) -@t:foo']
        expected = [1, 'h1', ['t', '']]
        cmd_assert(env, cmd, expected)

        # Same in opposite order
        cmd = f'FT.SEARCH {idx}'.split(' ') + ['-@t:foo isempty(@t)']
        cmd_assert(env, cmd, expected)

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

        # TBD:
            # More complex queries - check EXPLAINCLI output
            # Fuzzy
            # Phonetic
            # Stemming
            # Exact match
            # Use in aggregation pipelines
            # Infix (*lala*) (contains)
            # Summarization
            # Highlighting
            # Scoring?
            # Synonyms?

    # Create an index with a TAG field, that also indexes empty strings, another
    # TAG field that doesn't index empty values, and a TEXT field
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'tag', 'TAG').ok()
    testEmptyTextHash(env, 'idx')
    env.flush()

    # ----------------------------- SORTABLE case ------------------------------
    # Create an index with a SORTABLE TAG field, that also indexes empty strings
    env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'SORTABLE', 'tag', 'TAG').ok()
    testEmptyTextHash(env, 'idx_sortable')
    env.flush()

    # --------------------------- WITHSUFFIXTRIE case --------------------------
    # Create an index with a TAG field, that also indexes empty strings, while
    # using a suffix trie
    env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'WITHSUFFIXTRIE', 'tag', 'TAG').ok()
    testEmptyTextHash(env, 'idx_suffixtrie')
    env.flush()

    def testEmptyTextJSON(idx):
        """Tests the indexing and querying of empty values for a TEXT field of a
        JSON index"""
        pass

