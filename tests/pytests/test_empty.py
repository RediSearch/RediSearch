from common import *
import json

EMPTY_RESULT = [0]

def testEmptyValidations():
    """Validates the edge-cases of the `ISEMPTY` field option"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    # Create an index with a TAG and a TEXT field, both of which don't index
    # empty values
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT').ok()

    # Test that we get an error in case of a user tries to use "isempty(@field)"
    # when `field` does not index empty values.
    env.expect('FT.SEARCH', 'idx', 'isempty(@tag)').error().contains(
        'Field `tag` should enable `ISEMPTY` in the index SCHEMA in order to support empty values'
        )
    env.expect('FT.SEARCH', 'idx', 'isempty(@text)').error().contains(
        'Field `text` should enable `ISEMPTY` in the index SCHEMA in order to support empty values'
    )


def EmptyJSONTest(env, idx, dialect):
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
    cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
    if dialect >= 3:
        empty_js = json.dumps([empty_j], separators=(',', ':'))
    expected = [1, 'j', ['$', empty_js]]
    cmd_assert(env, cmd, expected)

    # Multi-value (automatically dereferenced).
    j = {
        't': ['a', '', 'c']
    }
    js = json.dumps(j, separators=(',', ':'))
    conn.execute_command('JSON.SET', 'j', '$', js)
    cmd = f'FT.SEARCH {idx} isempty(@t)'.split(' ')
    if dialect >= 3:
        js = json.dumps([j], separators=(',', ':'))
    expected = [1, 'j', ['$', js]]
    cmd_assert(env, cmd, expected)

def testEmptyTag(env):
    """Tests that empty values are indexed properly"""

    MAX_DIALECT = set_max_dialect(env)

    def testEmptyTagHash(env, conn, idx):
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

        if dialect == 2:
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
        cmd = f'FT.SEARCH {idx} isempty(@t) SORTBY t ASC WITHCOUNT'.split(' ')
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

    for dialect in range(2, MAX_DIALECT + 1):
        env = Env(moduleArgs="DEFAULT_DIALECT " + str(dialect))
        conn = getConnectionByEnv(env)

        # Create an index with a TAG field, that also indexes empty strings, another
        # TAG field that doesn't index empty values, and a TEXT field
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx')
        env.flush()

        # ----------------------------- SORTABLE case ------------------------------
        # Create an index with a SORTABLE TAG field, that also indexes empty strings
        env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'SORTABLE', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx_sortable')
        env.flush()

        # --------------------------- WITHSUFFIXTRIE case --------------------------
        # Create an index with a TAG field, that also indexes empty strings, while
        # using a suffix trie
        env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE', 'text', 'TEXT').ok()
        testEmptyTagHash(env, conn, 'idx_suffixtrie')
        env.flush()

        # ---------------------------------- JSON ----------------------------------
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
        EmptyJSONTest(env, 'jidx', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'SORTABLE').ok()
        EmptyJSONTest(env, 'jidx_sortable', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TAG', 'ISEMPTY', 'WITHSUFFIXTRIE').ok()
        EmptyJSONTest(env, 'jidx_suffix', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$arr[*]', 'AS', 'arr', 'TAG', 'ISEMPTY').ok()
        # Empty array values ["a", "", "c"] with explicit array components indexing
        arr = {
            'arr': ['a', '', 'c']
        }
        arrs = json.dumps(arr, separators=(',', ':'))
        env.expect('JSON.SET', 'j', '$', arrs).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@arr)'.split(' ')
        if dialect >= 3:
            arrs = json.dumps([arr], separators=(',', ':'))
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
        cmd = f'FT.SEARCH jidx isempty(@arr) RETURN 1 arr'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

        env.flush()

        # An attempt to index a non-empty object as a TAG (and in general) should fail (coverage)
        j = {
            "t": {"lala": "lali"}
        }
        js = json.dumps(j)
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG', 'ISEMPTY').ok()
        env.expect('JSON.SET', 'j', '$', js).equal('OK')
        cmd = f'FT.SEARCH jidx isempty(@t)'.split(' ')
        expected = EMPTY_RESULT
        cmd_assert(env, cmd, expected)

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
        res = env.cmd('FT.SEARCH', 'idx', 'isempty(@t)', 'WITHCOUNT', 'LIMIT', '0', '0')
        env.assertEqual(int(res[0]), 500)
        res = env.cmd('FT.SEARCH', 'idx', '-isempty(@t)', 'WITHCOUNT', 'LIMIT', '0', '0')
        env.assertEqual(int(res[0]), 500)
        env.flush()

def testEmptyText(env):
    """Tests the indexing and querying of empty TEXT (field type) values"""

    MAX_DIALECT = set_max_dialect(env)

    def testEmptyTextHash(idx, env):
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

        # --------------------------- EXPLAINCLI -------------------------------
        cmd = f'FT.EXPLAINCLI {idx} isempty(@t)'.split(' ')
        expected = [
            '@t:<ISEMPTY>',
            ''
        ]

        # ----------------------------- Fuzzy search ---------------------------
        # We don't expect to get empty results in a fuzzy search.
        cmd = f'FT.SEARCH {idx} %e%'.split(' ')
        expected = [0]
        cmd_assert(env, cmd, expected)

        # ------------------------------- Summarization ------------------------
        # When searching for such a query, we expect to get an empty value, and
        # thus an "empty summary".
        cmd = f'FT.SEARCH {idx} isempty(@t) SUMMARIZE FIELDS 1 t FRAGS 3 LEN 10'.split(' ')
        expected = [
            1,
            "h1",
            [
                "t",
                "... "
            ]
        ]
        cmd_assert(env, cmd, expected)

        # ---------------------------- Highlighting ----------------------------
        # When searching for such a query, we expect to get an empty value, and
        # thus an "empty highlight".
        cmd = f'FT.SEARCH {idx} isempty(@t) HIGHLIGHT FIELDS 1 t'.split(' ')
        expected = [
            1,
            "h1",
            [
                "t",
                "<b></b>"
            ]
        ]
        cmd_assert(env, cmd, expected)

    for dialect in range(2, MAX_DIALECT + 1):
        env = Env(moduleArgs="DEFAULT_DIALECT " + str(dialect))

        # Create an index with a TAG field, that also indexes empty strings, another
        # TAG field that doesn't index empty values, and a TEXT field
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'ISEMPTY').ok()
        testEmptyTextHash('idx', env)
        env.flush()

        # ----------------------------- SORTABLE case ------------------------------
        # Create an index with a SORTABLE TAG field, that also indexes empty strings
        env.expect('FT.CREATE', 'idx_sortable', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'SORTABLE').ok()
        testEmptyTextHash('idx_sortable', env)
        env.flush()

        # --------------------------- WITHSUFFIXTRIE case --------------------------
        # Create an index with a TAG field, that also indexes empty strings, while
        # using a suffix trie
        env.expect('FT.CREATE', 'idx_suffixtrie', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'WITHSUFFIXTRIE').ok()
        testEmptyTextHash('idx_suffixtrie', env)
        env.flush()

        # ------------------------------- Phonetic ---------------------------------
        # Create an index with a TEXT field, that also indexes empty strings, and
        # uses phonetic indexing
        env.expect('FT.CREATE', 'idx_phonetic', 'SCHEMA', 't', 'TEXT', 'ISEMPTY', 'PHONETIC', 'dm:en').ok()
        testEmptyTextHash('idx_phonetic', env)
        env.flush()

        # ---------------------------------- JSON ----------------------------------
        env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'ISEMPTY').ok()
        EmptyJSONTest(env, 'jidx', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_sortable', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'ISEMPTY', 'SORTABLE').ok()
        EmptyJSONTest(env, 'jidx_sortable', dialect)
        env.flush()

        env.expect('FT.CREATE', 'jidx_suffix', 'ON', 'JSON', 'SCHEMA', '$t', 'AS', 't', 'TEXT', 'ISEMPTY', 'WITHSUFFIXTRIE').ok()
        EmptyJSONTest(env, 'jidx_suffix', dialect)
        env.flush()

def testEmptyInfo():
    """Tests that the `FT.INFO` command returns the correct information
    regarding the indexing of empty values for a field"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    # Create an index with the currently supported field types (TAG, TEXT)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'ISEMPTY', 'text', 'TEXT', 'ISEMPTY').ok()

    info = index_info(env, 'idx')
    tag_info = info['attributes'][0]
    env.assertEqual(tag_info[-1], 'ISEMPTY')
    text_info = info['attributes'][1]
    env.assertEqual(text_info[-1], 'ISEMPTY')
