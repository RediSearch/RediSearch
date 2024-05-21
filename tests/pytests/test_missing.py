from common import *

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
    conn.execute_command('HSET', 'none', 'shwallalimbo', 'Shigoreski')

    # Search for the documents with the indexed fields (sanity)
    res = env.cmd('FT.SEARCH', 'idx', '@ta:{foo} @te:foo', 'NOCONTENT')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], 'all')

    # Search for the documents without the indexed fields via the
    # `ismissing(@field)` syntax
    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@ta)', 'NOCONTENT', 'SORTBY', 'te', 'ASC')
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], 'no_tag')
    env.assertEqual(res[2], 'none')

    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@te)', 'NOCONTENT', 'SORTBY', 'ta', 'ASC')
    env.assertEqual(res[0], 2)
    env.assertEqual(res[1], 'no_text')
    env.assertEqual(res[2], 'none')

    # Intersection of missing fields
    res = env.cmd('FT.SEARCH', 'idx', 'ismissing(@te) ismissing(@ta)', 'NOCONTENT')
    env.assertEqual(res[0], 1)
    env.assertEqual(res[1], 'none')

def testMissing():
    """Tests the missing values indexing feature thoroughly."""

    # TBD
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    fields_values = [ ('ta', 'TAG', 'foo'), ('te', 'TEXT', 'foo'),
                    #  ('n', 'NUMERIC', '42'), 
                    #  ('loc', 'GEO', '1.23, 4.56'),
                    #  ('gs', 'GEOSHAPE', 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))') 
                     ]

    # Create an index with multiple fields types that index missing values, i.e.,
    # index documents that do not have these fields.
    for field, ftype, value in fields_values:
        idx = f'idx_{ftype}'
        field1 = field + '1'
        field2 = field + '2'
        doc_field = f'doc_{ftype}'
        doc_no_field = f'doc_no_{ftype}'


        print('FT.CREATE', idx, 'SCHEMA',
                             field1, ftype, 'ISMISSING',
                             field2, ftype)
        conn.execute_command('FT.CREATE', idx, 'SCHEMA',
                             field1, ftype, 'ISMISSING',
                             field2, ftype)
        conn.execute_command('HSET', doc_field, field1, value)
        conn.execute_command('HSET', doc_no_field, field2, value)
        conn.execute_command('HSET', 'both', field1, value, field2, value)
        conn.execute_command('HSET', 'none', 'shwallalimbo', 'Shigoreski')

        # Search for the documents WITHOUT the indexed fields via the
        # `ismissing(@field)` syntax
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [2, doc_no_field, 'none'])

        # Search for the documents WITH the indexed fields
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'-ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [2, doc_field, 'both'])

        # Search for the documents WITH or WITHOUT the indexed fields
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'-ismissing(@{field1}) | ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [4, doc_field, doc_no_field, 'both', 'none'])

        # Search for the documents WITH AND WITHOUT the indexed fields,
        # shoult return 0 results
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'-ismissing(@{field1}) ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [0])

        # Update a document to have the indexed field
        conn.execute_command('HSET', doc_no_field, field1, value, field2, value)
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [1, 'none'])

        # Update a document to not have the indexed field
        conn.execute_command('HDEL', doc_no_field, field1)
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'ismissing(@{field1})',
                                   'NOCONTENT', 'SORTBY', field1, 'ASC')
        env.assertEqual(res, [2, 'none', doc_no_field])

        # Delete the document without the indexed field
        conn.execute_command('DEL', doc_no_field)
        res = conn.execute_command('FT.SEARCH', f'idx_{ftype}',
                                   f'ismissing(@{field1})',
                                   'NOCONTENT')
        env.assertEqual(res, [1, 'none'])

        # Delete the documents
        conn.execute_command('DEL', doc_field, doc_no_field, 'both', 'none')

