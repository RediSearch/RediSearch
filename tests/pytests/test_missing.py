from common import *
import json

fields_and_values = [
    ('ta', 'TAG', 'foo'),
    ('te', 'TEXT', 'foo'),
    #  ('n', 'NUMERIC', '42'),
    #  ('loc', 'GEO', '1.23, 4.56'),
    #  ('gs', 'GEOSHAPE', 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')
    # ('v', 'VECTOR', ...)
]
DOC_WITH_FIELD = 'doc_with_field'
DOC_WITHOUT_FIELD = 'doc_without_field'
DOC_WITH_BOTH = 'both'
DOC_WITH_NONE = 'none'
DOC_WITH_BOTH_AND_TEXT = 'both_and_text'
ALL_DOCS = [5, DOC_WITH_FIELD, DOC_WITH_BOTH, DOC_WITH_BOTH_AND_TEXT, DOC_WITHOUT_FIELD, DOC_WITH_NONE]

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
    if env.isCluster():
        fields = res[5]
        for field in fields:
            env.assertEqual(field[-1], "ISMISSING")
            n_found += 1
    else:
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

def JSONMissingTest(env, conn, idx):
    """Performs tests for missing values indexing on JSON documents for all
    supported field types separately."""
    pass

def MissingTestIndex(env, conn, idx, field1, field2, val, isjson=False):
    """Performs tests for missing values indexing on hash documents for all
    supported field types separately."""

    # ------------------------- Simple retrieval ---------------------------
    # Search for the documents WITHOUT the indexed fields via the
    # `ismissing(@field)` syntax
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', field1, 'ASC')
    env.assertEqual(res, [2, DOC_WITHOUT_FIELD, DOC_WITH_NONE])

    # ------------------------------ Negation ------------------------------
    # Search for the documents WITH the indexed fields
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', field1, 'ASC')
    env.assertEqual(res, [3, DOC_WITH_FIELD, DOC_WITH_BOTH, DOC_WITH_BOTH_AND_TEXT])

    # ------------------------------- Union --------------------------------
    # Search for the documents WITH or WITHOUT the indexed fields
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1}) | ismissing(@{field1})',
        'NOCONTENT', 'SORTBY', field1, 'ASC')
    env.assertEqual(res, ALL_DOCS)

    # --------------------- Optional operator-------------------------------
    expected = [1, DOC_WITHOUT_FIELD]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'~ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', field1, 'ASC')
    env.assertEqual(res, ALL_DOCS)

    # ---------------------------- Intersection ----------------------------
    # Empty intersection
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-ismissing(@{field1}) ismissing(@{field1})',
        'NOCONTENT')
    env.assertEqual(res, [0])

    # Non-empty intersection
    expected = [1, DOC_WITH_NONE]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1}) @text:(dummy)',
        'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'@text:(dummy) ismissing(@{field1})',
        'NOCONTENT')
    env.assertEqual(res, expected)

    # Non-empty intersection using negation operator
    expected = [1, DOC_WITHOUT_FIELD]
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1}) -@text:(dummy)',
        'NOCONTENT')
    env.assertEqual(res, expected)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'-@text:(dummy) ismissing(@{field1})',
        'NOCONTENT')
    env.assertEqual(res, expected)

    # ---------------------- Update docs and search ------------------------
    # Update a document to have the indexed field
    if not isjson:
        conn.execute_command('HSET', DOC_WITHOUT_FIELD, field1, val, field2, val)
    else:
        conn.execute_command('JSON.SET', DOC_WITHOUT_FIELD, '$', json.dumps({field1: val, field2: val}))
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT')
    env.assertEqual(res, [1, DOC_WITH_NONE])

    # Update a document to not have the indexed field
    if not isjson:
        conn.execute_command('HDEL', DOC_WITHOUT_FIELD, field1)
    else:
        conn.execute_command('JSON.SET', DOC_WITHOUT_FIELD, '$', json.dumps({field2: val}))
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT',
        'SORTBY', field1, 'ASC')
    env.assertEqual(res, [2, DOC_WITH_NONE, DOC_WITHOUT_FIELD])

    # ---------------------- Delete docs and search ------------------------
    # Delete the document without the indexed field
    if not isjson:
        conn.execute_command('DEL', DOC_WITHOUT_FIELD)
    else:
        conn.execute_command('JSON.DEL', DOC_WITHOUT_FIELD)
    res = conn.execute_command(
        'FT.SEARCH', idx, f'ismissing(@{field1})', 'NOCONTENT')
    env.assertEqual(res, [1, DOC_WITH_NONE])

def testMissing():
    """Tests the missing values indexing feature thoroughly."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    def _populateHashDB(conn, field1, field2, val):
            # Populate db
            conn.execute_command('HSET', DOC_WITH_FIELD, field1, val)
            conn.execute_command('HSET', DOC_WITHOUT_FIELD, field2, val)
            conn.execute_command('HSET', DOC_WITH_BOTH, field1, val, field2, val)
            conn.execute_command('HSET', DOC_WITH_NONE, 'text', 'dummy')
            conn.execute_command('HSET', DOC_WITH_BOTH_AND_TEXT, field1, val, 
                                field2, val, 'text', 'dummy')
            
    def _populateJSONDB(conn, field1, field2, val):
        j_with_field = {
            field1: val
        }
        conn.execute_command('JSON.SET', DOC_WITH_FIELD, '$', json.dumps(j_with_field))
        j_without_field = {
            field2: val
        }
        conn.execute_command('JSON.SET', DOC_WITHOUT_FIELD, '$', json.dumps(j_without_field))
        j_with_both = {
            field1: val,
            field2: val
        }
        conn.execute_command('JSON.SET', DOC_WITH_BOTH, '$', json.dumps(j_with_both))
        j_with_none = {
            'text': 'dummy'
        }
        conn.execute_command('JSON.SET', DOC_WITH_NONE, '$', json.dumps(j_with_none))
        j_with_both_and_text = {
            field1: val,
            field2: val,
            'text': 'dummy'
        }
        conn.execute_command('JSON.SET', DOC_WITH_BOTH_AND_TEXT, '$', json.dumps(j_with_both_and_text))

    # Create an index with multiple fields types that index missing values, i.e.,
    # index documents that do not have these fields.
    for field, ftype, val in fields_and_values:
        idx = f'idx_{ftype}'
        field1 = field + '1'
        field2 = field + '2'

        # Create Hash index
        conn.execute_command(
            'FT.CREATE', idx, 'SCHEMA',
            field1, ftype, 'ISMISSING',
            field2, ftype,
            'text', 'TEXT'
        )

        # Populate db
        _populateHashDB(conn, field1, field2, val)

        # Perform the "isolated" tests per field-type
        MissingTestIndex(env, conn, idx, field1, field2, val)
        env.flush()

        # ----------------------------- SORTABLE case --------------------------
        # Create an index with a ISMISSING SORTABLE field
        sidx = f'idx_{ftype}_sortable'
        conn.execute_command(
            'FT.CREATE', sidx, 'SCHEMA',
            field1, ftype, 'ISMISSING', 'SORTABLE',
            field2, ftype,
            'text', 'TEXT'
        )

        # Populate db
        _populateHashDB(conn, field1, field2, val)

        # Perform the "isolated" tests per field-type for the SORTABLE case
        MissingTestIndex(env, conn, sidx, field1, field2, val)
        env.flush()

        # Create JSON index
        jidx = 'j' + idx
        conn.execute_command(
            'FT.CREATE', jidx, 'ON', 'JSON', 'SCHEMA',
            f'$.{field1}', 'AS', field1, ftype, 'ISMISSING',
            f'$.{field2}', 'AS', field2, ftype,
            '$.text', 'AS', 'text', 'TEXT'
        )

        # Populate db
        _populateJSONDB(conn, field1, field2, val)

        # Perform the "isolated" tests per field-type
        MissingTestIndex(env, conn, jidx, field1, field2, val, True)
        env.flush()

        # ----------------------------- SORTABLE case --------------------------
        # Create a JSON index with a ISMISSING SORTABLE field
        sjidx = 'sj' + idx
        conn.execute_command(
            'FT.CREATE', sjidx, 'ON', 'JSON', 'SCHEMA',
            f'$.{field1}', 'AS', field1, ftype, 'ISMISSING', 'SORTABLE',
            f'$.{field2}', 'AS', field2, ftype,
            '$.text', 'AS', 'text', 'TEXT'
        )
        # Populate db
        _populateJSONDB(conn, field1, field2, val)

        # Perform the "isolated" tests per field-type for the SORTABLE case
        MissingTestIndex(env, conn, sjidx, field1, field2, val, True)
        env.flush()

    # Things to test:
    # INTERSECT, UNION, NOT, Other query operators..
    # TEXT features: HIGHLIGHT, SUMMARIZE, PHONETIC, FUZZY.. ? Not sure that they are interesting.
    # EXPLAINCLI
    # FT.SEARCH & FT.AGGREGATE
    # `ismissing()` of two fields that index missing values.
    # Scoring.
    # SORTBY missing fields (what do we expect?)


    # Other fields (GEO, GEOSHAPE, NUMERIC, VECTOR)?
