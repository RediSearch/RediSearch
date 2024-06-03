from common import *
import json

fields_and_values = [
    # Field, Type, CommonOptions, Value, Field1Options
    ('ta', 'TAG', '', 'foo', 'bar', ''),
    ('ta', 'TAG', '', 'foo', 'bar', 'SORTABLE'),
    ('ta', 'TAG', '', 'foo', 'bar', 'WITHSUFFIXTRIE'),
    ('te', 'TEXT', '', 'foo', 'bar', ''),
    ('te', 'TEXT', '', 'foo', 'bar', 'SORTABLE'),
    ('te', 'TEXT', '', 'foo', 'bar', 'WITHSUFFIXTRIE'),
    ('n', 'NUMERIC', '', 42, 44, ''),
    ('n', 'NUMERIC', '', 42, 44, 'SORTABLE'),
    ('loc', 'GEO', '', '1.23, 4.56', '1.44, 4.88', ''),
    ('loc', 'GEO', '', '1.23, 4.56', '1.44, 4.88', 'SORTABLE'),
    ('gs', 'GEOSHAPE', '', 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 'POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))', ''),
    ('v', 'VECTOR', 'FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2', [0.0, 0.0], [1.0, 1.0], ''),
]
DOC_WITH_ONLY_FIELD1 = 'doc_with_only_field1'
DOC_WITH_ONLY_FIELD2 = 'doc_with_only_field2'
DOC_WITH_BOTH = 'both'
DOC_WITH_NONE = 'none'
DOC_WITH_BOTH_AND_TEXT = 'both_and_text'
ALL_DOCS = [5, DOC_WITH_ONLY_FIELD1, DOC_WITH_ONLY_FIELD2, DOC_WITH_BOTH, DOC_WITH_NONE, DOC_WITH_BOTH_AND_TEXT]
ALL_DOCS_WRONG_ORDER = [5, DOC_WITH_BOTH, DOC_WITH_BOTH_AND_TEXT, DOC_WITH_ONLY_FIELD1, DOC_WITH_ONLY_FIELD2, DOC_WITH_NONE]

def testMissingValidations():
    """Tests the validations for missing values indexing"""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    # Validate successful index creation with the `ISMISSING` keyword for TAG,
    # TEXT fields
    env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'ta', 'TAG', 'ISMISSING', 'te', 'TEXT', 'ISMISSING').ok()

    # Same with SORTABLE, WITHSUFFIXTRIE
    env.expect('FT.CREATE', 'idx2', 'SCHEMA',
               'ta', 'TAG', 'ISMISSING', 'WITHSUFFIXTRIE', 'SORTABLE',
               'te', 'TEXT', 'ISMISSING', 'WITHSUFFIXTRIE', 'SORTABLE'
               ).ok()

    # Create an index with a TAG, TEXT and a NUMERIC field, which don't index
    # empty values
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tag', 'TAG', 'text', 'TEXT',
               'numeric', 'NUMERIC').ok()

    # Test that we get an error in case of a user tries to use "ismissing(@field)"
    # when `field` does not index missing values.
    env.expect('FT.SEARCH', 'idx', 'ismissing(@tag)').error().contains(
        '`ISMISSING` applied to field `tag`, which does not index missing values'
        )
    env.expect('FT.SEARCH', 'idx', 'ismissing(@text)').error().contains(
        '`ISMISSING` applied to field `text`, which does not index missing values'
    )
    env.expect('FT.SEARCH', 'idx', 'ismissing(@numeric)').error().contains(
        '`ISMISSING` applied to field `numeric`, which does not index missing values'
    )

    # Empty search on a non-existing field
    env.expect('FT.SEARCH', 'idx', 'ismissing(@non_existing)').error().contains('Field not found')

def testMissingInfo():
    """Tests that we get the `ISMISSING` keyword in the INFO response for fields
    that index missing values."""

    env = Env(moduleArgs="DEFAULT_DIALECT 2")

    # Create an index with TAG and TEXT fields that index empty fields.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'ta', 'TAG', 'ISMISSING', 'te', 'TEXT', 'ISMISSING').ok()

    # Validate `INFO` response
    res = env.cmd('FT.INFO', 'idx')

    # The fields' section is in different places for cluster and standalone builds
    n_found = 0
    fields = res[7]
    for field in fields:
        env.assertEqual(field[-1], "ISMISSING")
        n_found += 1
    env.assertEqual(n_found, 2)

def testMissingBasic():
    """Tests the basic functionality of missing values indexing."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create an index with TAG and TEXT fields that index missing values, i.e.,
    # index documents that do not have these fields.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'ta', 'TAG', 'ISMISSING', 'te', 'TEXT', 'ISMISSING').ok()

    # Add some documents, with\without the indexed fields.
    conn.execute_command('HSET', 'all', 'ta', 'foo', 'te', 'foo')
    conn.execute_command('HSET', 'no_text', 'ta', 'foo')
    conn.execute_command('HSET', 'no_tag', 'te', 'foo')
    conn.execute_command('HSET', DOC_WITH_NONE, 'shwallalimbo', 'Shigoreski')

    # Search for the documents with the indexed fields (sanity)
    res = env.cmd('FT.SEARCH', 'idx', '@ta:{foo} @te:foo', 'NOCONTENT')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], 'all')

    # Search for the documents without the indexed fields via the
    # `ismissing(@field)` syntax
    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@ta)', 'NOCONTENT', 'SORTBY', 'te', 'ASC')
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], 'no_tag')
    env.assertEqual(res[2], DOC_WITH_NONE)

    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@te)', 'NOCONTENT', 'SORTBY', 'ta', 'ASC')
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], 'no_text')
    env.assertEqual(res[2], DOC_WITH_NONE)

    # Intersection of missing fields
    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@te) ismissing(@ta)', 'NOCONTENT')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], DOC_WITH_NONE)

def MissingTestIndex(env, conn, idx, ftype, field1, field2, val1, val2, field1Opt, isjson=False, twomissing=False):
    """Performs tests for missing values indexing on hash documents for all
    supported field types separately."""

    # For vector fields in hash, we need to convert the value to bytes
    if ftype == 'VECTOR' and not isjson:
        val1 = np.array(val1, dtype=np.float32).tobytes()
        val2 = np.array(val2, dtype=np.float32).tobytes()

    # ------------------------- Simple retrieval ---------------------------
    # Search for the documents WITHOUT the indexed fields via the
    # `ismissing(@field)` syntax
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', 'id', 'ASC', 'WITHCOUNT')
    env.assertEqual(res, [2, DOC_WITH_ONLY_FIELD2, DOC_WITH_NONE])

    # ------------------------------ Negation ------------------------------
    # Search for the documents WITH the indexed fields
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1})', 'NOCONTENT',
        'WITHCOUNT', 'SORTBY', 'id', 'ASC')
    if isjson and env.isCluster():
        # TODO: The order of the results is wrong in cluster using JSON
        env.assertEqual(res, [3, DOC_WITH_BOTH, DOC_WITH_BOTH_AND_TEXT, DOC_WITH_ONLY_FIELD1])
    else:
        env.assertEqual(res, [3, DOC_WITH_ONLY_FIELD1, DOC_WITH_BOTH, DOC_WITH_BOTH_AND_TEXT])
        


    # ------------------------------- Union --------------------------------
    # Search for the documents WITH or WITHOUT the indexed fields
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1}) | ismissing(@{field1})',
        'NOCONTENT', 'SORTBY', 'id', 'ASC', 'WITHCOUNT')
    if isjson and env.isCluster():
        # TODO: The order of the results is wrong in cluster using JSON
        env.assertEqual(res, ALL_DOCS_WRONG_ORDER)
    else:
        env.assertEqual(res, ALL_DOCS)

    # --------------------- Optional operator-------------------------------
    expected = [1, DOC_WITH_ONLY_FIELD2]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'~ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', 'id', 'ASC', 'WITHCOUNT')
    if isjson and env.isCluster():
        # TODO: The order of the results is wrong in cluster using JSON
        env.assertEqual(res, ALL_DOCS_WRONG_ORDER)
    else:
        env.assertEqual(res, ALL_DOCS)

    # ---------------------------- Intersection ----------------------------
    # Empty intersection
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1}) ismissing(@{field1})',
        'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, [0])

    # Non-empty intersection
    expected = [1, DOC_WITH_NONE]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1}) @text:(dummy)',
        'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'@text:(dummy) ismissing(@{field1})',
        'NOCONTENT')
    env.assertEqual(res, expected)

    # Non-empty intersection using negation operator
    expected = [1, DOC_WITH_ONLY_FIELD2]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1}) -@text:(dummy)',
        'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-@text:(dummy) ismissing(@{field1})',
        'NOCONTENT')
    env.assertEqual(res, expected)

    # ---------------------------- FT.AGGREGATE --------------------------------
    # TODO: For dialect > 3, FT.AGGREGATE returns "Success (not an error)"
    dialect = int(env.cmd(config_cmd(), 'GET', 'DEFAULT_DIALECT')[0][1])
    if dialect in [3]:
        res = conn.execute_command(
            'FT.AGGREGATE', idx, '*', 'GROUPBY', '1', f'@{field1}', 
            'REDUCE', 'COUNT', '0', 'AS', 'count',
            'SORTBY', 2, '@count', 'DESC'
        )

        if not isjson:
            if ftype == 'VECTOR':
                    # Decode the string
                    s = f'{val1}'
                    s = s[2:-1]
                    val1 = bytes(s, 'utf-8').decode('unicode_escape')            
            expected = [2, [field1, f'{val1}', 'count', '3'], [field1, None, 'count', '2']]
        else: # JSON
            if dialect == 2:
                expected = [2, [field1, f'{val1}', 'count', '3'], [field1, None, 'count', '2']]
            else:
                # TODO: is this correct? why the value format depends on "SORTABLE"?
                if field1Opt == 'SORTABLE':
                    expected = [2, [field1, f'{val1}', 'count', '3'], [field1, None, 'count', '2']]
                else:
                    exp_val = f'"{val1}"'
                    if ftype == 'VECTOR':
                        exp_val = "[0.0,0.0]"
                    if ftype == 'NUMERIC':
                        exp_val = f'{val1}'

                    expected = [2, [field1, f'[{exp_val}]', 'count', '3'], [field1, None, 'count', '2']]

        env.assertEqual(res, expected)

    if twomissing:
        # ---------------------------- Intersection ----------------------------
        # Non-empty intersection
        expected = [1, DOC_WITH_NONE]
        res = conn.execute_command(
            'FT.SEARCH', idx, f'ismissing(@{field1}) ismissing(@{field2})',
            'NOCONTENT'
        )
        env.assertEqual(res, expected)

        # TODO: This fails when running with raw DocID encoding
        # Non-empty intersection using negation operator
        # expected = [1, DOC_WITH_ONLY_FIELD2]
        # res = conn.execute_command(
        #     'FT.SEARCH', idx, f'ismissing(@{field1}) -ismissing(@{field2})',
        #     'NOCONTENT', 'WITHCOUNT', 'SORTBY', 'id', 'ASC'
        # )
        # env.assertEqual(res, expected)
        # res = conn.execute_command(
        #     'FT.SEARCH', idx, f'-ismissing(@{field2}) ismissing(@{field1})',
        #     'NOCONTENT', 'WITHCOUNT', 'SORTBY', 'id', 'ASC'
        # )
        # env.assertEqual(res, expected)

        # Union of missing fields
        expected = [3, DOC_WITH_ONLY_FIELD1, DOC_WITH_ONLY_FIELD2, DOC_WITH_NONE]
        res = conn.execute_command(
            'FT.SEARCH', idx, f'ismissing(@{field1}) | ismissing(@{field2})',
            'NOCONTENT', 'WITHCOUNT', 'SORTBY', 'id', 'ASC'
        )
        env.assertEqual(res, expected)

        # Union of missing fields using negation operator
        expected = [4, DOC_WITH_ONLY_FIELD1, DOC_WITH_BOTH, DOC_WITH_NONE, DOC_WITH_BOTH_AND_TEXT]
        res = conn.execute_command(
            'FT.SEARCH', idx, f'-ismissing(@{field1}) | ismissing(@{field2})',
            'NOCONTENT', 'WITHCOUNT', 'SORTBY', 'id', 'ASC'
        )
        env.assertEqual(res, expected)

        # Union of missing fields using optional operator
        res = conn.execute_command(
            'FT.SEARCH', idx, f'~ismissing(@{field1}) | ismissing(@{field2})',
            'NOCONTENT', 'WITHCOUNT', 'SORTBY', 'id', 'ASC'
        )
        env.assertEqual(res, ALL_DOCS)

    # ---------------------- Update docs and search ------------------------
    # Update a document to have the indexed field
    # Add field1 to DOC_WITH_ONLY_FIELD2
    if not isjson:
        conn.execute_command('HSET', DOC_WITH_ONLY_FIELD2, field1, val1, field2, val2, 'id', 2)
    else:
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD2, '$', json.dumps({field1: val1, field2: val2, 'id': 2}))
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT')
    env.assertEqual(res, [1, DOC_WITH_NONE])
    # Restore original value of DOC_WITH_ONLY_FIELD2
    if not isjson:
        conn.execute_command('HDEL', DOC_WITH_ONLY_FIELD2, field1)
    else:
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD2, '$', json.dumps({field2: val2, 'id': 2}))

    # Update a document to not have the indexed field
    # Remove field1 from DOC_WITH_ONLY_FIELD1
    if not isjson:
        conn.execute_command('HDEL', DOC_WITH_ONLY_FIELD1, field1)
    else:
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD1, '$', json.dumps({'id': 1}))

    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', 'id', 'ASC', 'WITHCOUNT')
    if isjson and not env.isCluster():
        # TODO: The order of the results is wrong using JSON
        env.assertEqual(res, [3, DOC_WITH_NONE, DOC_WITH_ONLY_FIELD2, DOC_WITH_ONLY_FIELD1])
    else:
        env.assertEqual(res, [3, DOC_WITH_ONLY_FIELD1, DOC_WITH_ONLY_FIELD2, DOC_WITH_NONE])

    # Restore original value of DOC_WITH_ONLY_FIELD1
    if not isjson:
        conn.execute_command('HSET', DOC_WITH_ONLY_FIELD1, field1, val2, 'id', 1)
    else:
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD1, '$', json.dumps({field1: val1, 'id': 1}))

    # ---------------------- Delete docs and search ------------------------
    # Delete the document without the indexed field
    if not isjson:
        conn.execute_command('DEL', DOC_WITH_ONLY_FIELD2)
    else:
        conn.execute_command('JSON.DEL', DOC_WITH_ONLY_FIELD2)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT')
    env.assertEqual(res, [1, DOC_WITH_NONE])

def JSONMissingTest(env, conn):
    """Performs tests for missing values indexing on JSON documents for all
    supported field types separately."""

    def _populateJSONDB(conn, ftype, field1, field2, val1, val2):
        j_with_only_field1 = {
            field1: val1,
            'id'  : 1
        }
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD1, '$', json.dumps(j_with_only_field1))
        j_with_only_field2 = {
            field2: val2,
            'id'  : 2
        }
        conn.execute_command('JSON.SET', DOC_WITH_ONLY_FIELD2, '$', json.dumps(j_with_only_field2))
        j_with_both = {
            field1: val1,
            field2: val2,
            'id'  : 3
        }
        conn.execute_command('JSON.SET', DOC_WITH_BOTH, '$', json.dumps(j_with_both))
        j_with_none = {
            'text': 'dummy',
            'id'  : 4,
        }
        conn.execute_command('JSON.SET', DOC_WITH_NONE, '$', json.dumps(j_with_none))
        j_with_both_and_text = {
            field1: val1,
            field2: val2,
            'text': 'dummy',
            'id'  : 1
        }
        conn.execute_command('JSON.SET', DOC_WITH_BOTH_AND_TEXT, '$', json.dumps(j_with_both_and_text))

    # Create an index with multiple fields types that index missing values, i.e.,
    # index documents that do not have these fields.
    for field, ftype, opt, val1, val2, field1Opt in fields_and_values:
        idx = f'idx_{ftype}'
        field1 = field + '1'
        field2 = field + '2'

        # Create JSON index
        jidx = 'j' + idx
        cmd = (
            f'FT.CREATE {jidx} ON JSON SCHEMA '
            f'$.{field1} AS {field1} {ftype} {opt if len(opt) > 0 else ""} '
            f'ISMISSING {field1Opt if len(field1Opt) > 0 else ""} '
            f'$.{field2} AS {field2} {ftype} {opt if len(opt) > 0 else ""} '
            f'$.text AS text TEXT '
            f'id NUMERIC SORTABLE'
        )
        env.expect(cmd).ok

        # Populate db
        _populateJSONDB(conn, ftype, field1, field2, val1, val2)

        # Perform the "isolated" tests per field-type
        MissingTestIndex(env, conn, jidx, ftype, field1, field2, val1, val2, field1Opt, True)
        env.flush()

def HashMissingTest(env, conn):
    """Tests the missing values indexing feature thoroughly."""

    def _populateHashDB(conn, ftype, field1, field2, val1, val2):
            if ftype == 'VECTOR':
                val1 = np.array(val1, dtype=np.float32).tobytes()
                val2 = np.array(val2, dtype=np.float32).tobytes()

            # Populate db
            conn.execute_command('HSET', DOC_WITH_ONLY_FIELD1, field1, val1,
                                 'id', 1)
            conn.execute_command('HSET', DOC_WITH_ONLY_FIELD2, field2, val2,
                                 'id', 2)
            conn.execute_command('HSET', DOC_WITH_BOTH, field1, val1,
                                 field2, val2, 'id', 3)
            conn.execute_command('HSET', DOC_WITH_NONE, 'text', 'dummy',
                                 'id', 4)
            conn.execute_command('HSET', DOC_WITH_BOTH_AND_TEXT, field1, val1, 
                                field2, val2, 'text', 'dummy', 'id', 5)
    
    # Create an index with multiple fields types that index missing values, i.e.,
    # index documents that do not have these fields.
    for field, ftype, opt, val1, val2, field1Opt in fields_and_values:
        idx = f'idx_{ftype}'
        field1 = field + '1'
        field2 = field + '2'

        # Create Hash index with a single ISMISSING field
        cmd = (
            f'FT.CREATE {idx} SCHEMA '
            f'{field1} {ftype} {opt if len(opt) > 0 else ""} '
            f'ISMISSING {field1Opt if len(field1Opt) > 0 else ""} '
            f'{field2} {ftype} {opt if len(opt) > 0 else ""} '
            f'text TEXT id NUMERIC SORTABLE'
        )
        env.expect(cmd).ok()

        # Populate db
        _populateHashDB(conn, ftype, field1, field2, val1, val2)

        # Perform the "isolated" tests per field-type
        MissingTestIndex(env, conn, idx, ftype, field1, field2, val1, val2, field1Opt)
        env.flush()

        # Create Hash index with two ISMISSING fields
        cmd = (
            f'FT.CREATE {idx} SCHEMA '
            f'{field1} {ftype} {opt if len(opt) > 0 else ""} '
            f'ISMISSING {field1Opt if len(field1Opt) > 0 else ""} '
            f'{field2} {ftype} {opt if len(opt) > 0 else ""} ISMISSING '
            f'text TEXT id NUMERIC SORTABLE'
        )
        env.expect(cmd).ok()

        # Populate db
        _populateHashDB(conn, ftype, field1, field2, val1, val2)

        # Perform the tests using two missing fields of the same type
        MissingTestIndex(env, conn, idx, ftype, field1, field2, val1, val2, field1Opt, False, True)
        env.flush()

def testMissing(env):
    """Tests the missing values indexing feature thoroughly."""

    MAX_DIALECT = set_max_dialect(env)

    for dialect in range(2, MAX_DIALECT + 1):
        env = Env(moduleArgs="DEFAULT_DIALECT " + str(dialect))
        conn = getConnectionByEnv(env)

        # Test missing fields indexing on hash documents
        HashMissingTest(env, conn)

        # Test missing fields indexing on JSON documents
        JSONMissingTest(env, conn)



    # Things to test:
    # INTERSECT, UNION, NOT, Other query operators..
    # TEXT features: HIGHLIGHT, SUMMARIZE, PHONETIC, FUZZY.. ? Not sure that they are interesting.
    # FT.SEARCH & FT.AGGREGATE
    # `ismissing()` of two fields that index missing values.
    # Scoring.
    # SORTBY missing fields (what do we expect?)
