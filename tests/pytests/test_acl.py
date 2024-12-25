import copy

from common import *

READ_SEARCH_COMMANDS = ['FT.SEARCH', 'FT.AGGREGATE', 'FT.CURSOR', 'FT.CURSOR',
                 'FT.PROFILE', 'FT.SUGGET', 'FT.SUGLEN']
WRITE_SEARCH_COMMANDS = ['FT.DROPINDEX', 'FT.SUGADD', 'FT.SUGDEL']
INTERNAL_SEARCH_COMMANDS = {
        '_FT.ALIASDEL': 'alias',
        '_FT.AGGREGATE': 'idx *',
        '_FT.ALIASADD': 'idx alias',
        '_FT.ALIASUPDATE': 'idx alias2',
        '_FT.CURSOR': ['READ 1234', 'DEL 1234'],
        '_FT.INFO': 'idx',
        '_FT.ALTER': 'idx SCHEMA ADD f2 NUMERIC',
        '_FT.DICTDEL': 'dict word',
        '_FT.SYNUPDATE': 'synonym hello hi',
        '_FT.SPELLCHECK': 'idx held DISTANCE 2',
        '_FT.CREATE': 'idx SCHEMA f1 NUMERIC',
        '_FT.DICTADD': 'dict word',
        '_FT.PROFILE': 'idx SEARCH *',
        '_FT.SEARCH': 'idx *',
        '_FT.DEBUG': 'DUMP_PREFIX_TRIE',
        '_FT.CONFIG': 'GET foo',
        '_FT.TAGVALS': 'idx tag',
        '_FT._ALTERIFNX': 'idx SCHEMA ADD f2 NUMERIC',
        '_FT._ALIASDELIFX': 'alias',
        '_FT._ALIASADDIFNX': 'idx alias',
        '_FT._DROPINDEXIFX': 'idx',
        '_FT.DROPINDEX': 'idx',
        '_FT.ADD': 'idx doc:1 fields f1 val',
        '_FT.DROP': 'idx',
        '_FT.GET': 'doc:1',
        '_FT._CREATEIFNX': 'idx SCHEMA f1 NUMERIC',
        '_FT.MGET': 'doc:1 doc:2',
        '_FT.DEL': 'doc:1',
        '_FT._DROPIFX': 'idx',
        '_FT.SAFEADD': 'idx doc:1 fields f1 val'
}

def test_acl_category(env):
    """Test that the `search` category was added appropriately in module
    load"""
    res = env.cmd('ACL', 'CAT')
    for category in ['search', '_search_internal']:
        env.assertContains(category, res)

@skip(redis_less_than="8.0")
def test_acl_search_commands(env):
    """Tests that the RediSearch commands are registered to the `search`
    ACL category"""

    # `search` command category
    res = env.cmd('ACL', 'CAT', 'search')
    commands = [
        'FT.EXPLAINCLI', 'FT.SPELLCHECK', 'FT.SYNUPDATE', 'FT.ALIASUPDATE',
        'FT._LIST', 'FT.ALIASADD', 'FT.SEARCH', 'FT.INFO', 'FT.SUGDEL',
        'FT.DICTDUMP', 'FT.EXPLAIN', 'FT.AGGREGATE', 'FT.SUGLEN',
        'FT.PROFILE', 'FT.ALTER', 'FT.SUGGET', 'FT.DICTDEL', 'FT.CURSOR',
        'FT.ALIASDEL', 'FT.SUGADD', 'FT.SYNDUMP', 'FT.CREATE', 'FT.DICTADD',
        'FT._ALIASDELIFX', 'FT._CREATEIFNX', 'search.CLUSTERREFRESH',
        'FT._ALIASADDIFNX', 'FT._ALTERIFNX', 'search.CLUSTERSET',
        'search.CLUSTERINFO', 'FT._DROPINDEXIFX', 'FT.DROPINDEX', 'FT.TAGVALS',
        'FT._DROPIFX', 'FT.DROP', 'FT.GET', 'FT.SYNADD', 'FT.ADD', 'FT.MGET',
        'FT.DEL'
    ]
    if not env.isCluster():
        commands.append('FT.CONFIG')

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(commands))

    # ---------------- internal search command category ----------------
    res = env.cmd('ACL', 'CAT', '_search_internal')
    commands = INTERNAL_SEARCH_COMMANDS.keys()

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(commands))

    # Check that one of our commands is listed in a non-search category
    res = env.cmd('ACL', 'CAT', 'read')
    env.assertTrue('FT.SEARCH' in res)

def test_acl_non_default_user(env):
    """Tests that a user with a non-default ACL can't access the search
    category"""

    with env.getClusterConnectionIfNeeded() as conn:
        # Create a user with no command permissions (full keyspace and pubsub access)
        conn.execute_command('ACL', 'SETUSER', 'test', 'on', '>123', '~*', '&*')
        res = conn.execute_command('AUTH', 'test', '123')
        env.assertEqual(res, True)

        # Such a user shouldn't be able to run any RediSearch commands (or any other commands)
        try:
            conn.execute_command('FT.SEARCH', 'idx', '*')
            env.assertTrue(False)
        except Exception as e:
            env.assertEqual("User test has no permissions to run the 'FT.SEARCH' command", str(e))

        # Add `test` read permissions
        conn.execute_command('AUTH', 'default', '')
        conn.execute_command('ACL', 'SETUSER', 'test', '+@read')
        conn.execute_command('AUTH', 'test', '123')

        # `test` should now be able to run `read` commands like `FT.SEARCH', but not
        # `search` (only) commands like `FT.CREATE`
        for cmd in READ_SEARCH_COMMANDS:
            try:
                conn.execute_command(cmd)
                env.assertTrue(False) # Fail due to incomplete command, not permissions-related
            except Exception as e:
                env.assertNotContains("User test has no permissions to run", str(e))

        # `test` should not be able to run `search` commands that are not `read`,
        # like `FT.CREATE`
        try:
            conn.execute_command('FT.CREATE', 'idx', '*')
            env.assertTrue(False)
        except Exception as e:
            env.assertContains("User test has no permissions to run the 'FT.CREATE' command", str(e))

        # Let's add `write` permissions to `test`
        conn.execute_command('AUTH', 'default', '')
        conn.execute_command('ACL', 'SETUSER', 'test', '+@write')
        conn.execute_command('AUTH', 'test', '123')

        # `test` should now be able to run `write` commands
        for cmd in WRITE_SEARCH_COMMANDS:
            try:
                res = conn.execute_command(cmd)
                # Should still fail due to incomplete command, not permissions-related
                env.assertTrue(False)
            except Exception as e:
                env.assertNotContains("User test has no permissions", str(e))

        # Add `test` search permissions
        conn.execute_command('AUTH', 'default', '')
        conn.execute_command('ACL', 'SETUSER', 'test', '+@search', '+@_search_internal')
        conn.execute_command('AUTH', 'test', '123')

        # `test` should now be able to run `search` commands like `FT.CREATE`
        res = conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT')
        env.assertEqual(res, 'OK')
        res = conn.execute_command('FT.SEARCH', 'idx', '*')
        env.assertEqual(res, [0])

def test_sug_commands_acl(env):
    """Tests that our FT.SUG* commands are properly validated for ACL key
    permissions.
    """

    with env.getClusterConnectionIfNeeded() as conn:
        # Create a suggestion key
        res = conn.execute_command('FT.SUGADD', 'test_key', 'hello world', '1')
        env.assertEqual(res, 1)

        # Create an ACL user without permissions to the key we're going to use
        res = conn.execute_command('ACL', 'SETUSER', 'test_user', 'on', '>123', '~h:*', '&*', '+@all')
        env.assertEqual(res, 'OK')
        res = conn.execute_command('AUTH', 'test_user', '123')
        env.assertEqual(res, True)
        try:
            res = conn.execute_command('FT.SUGADD', 'test_key_2', 'hello world', '1')
            env.assertTrue(False)
        except Exception as e:
            env.assertEqual(str(e), "No permissions to access a key")

        # Test that `test_user` can create a key it has permissions to access
        res = conn.execute_command('FT.SUGADD', 'h:test_key', 'hello world', '1')
        env.assertEqual(res, 1)

        # Test other `FT.SUG*` commands. They should fail on `test_key` but
        # succeed on `h:test_key`
        # FT.SUGGET
        try:
            conn.execute_command('FT.SUGGET', 'test_key', 'hello')
            env.assertTrue(False)
        except Exception as e:
            env.assertEqual(str(e), "No permissions to access a key")
        res = conn.execute_command('FT.SUGGET', 'h:test_key', 'hello')
        env.assertEqual(res, ['hello world'])

        # FT.SUGLEN
        try:
            conn.execute_command('FT.SUGLEN', 'test_key')
            env.assertTrue(False)
        except Exception as e:
            env.assertEqual(str(e), "No permissions to access a key")
        res = conn.execute_command('FT.SUGLEN', 'h:test_key')
        env.assertEqual(res, 1)

        # FT.SUGDEL
        try:
            conn.execute_command('FT.SUGDEL', 'test_key', 'hello world')
            env.assertTrue(False)
        except Exception as e:
            env.assertEqual(str(e), "No permissions to access a key")
        res = conn.execute_command('FT.SUGDEL', 'h:test_key', 'hello world')
        env.assertEqual(res, 1)

def test_internal_commands(env):
    """Tests the internal commands' category"""

    # Create a user with all command permissions (full keyspace and pubsub access)
    env.expect('ACL', 'SETUSER', 'test', 'on', '>123', '~*', '&*', '+@all').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` user should be able to execute internal commands
    env.expect('_FT.SEARCH', 'idx', '*').error().contains("idx: no such index")

    # Remove the `_search_internal` permissions from the `test` user
    env.expect('ACL', 'SETUSER', 'test', '-@_search_internal').ok()

    # `test` user should still be able to run non-internal commands
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Now `test` should not be able to execute RediSearch internal commands
    # `_FT.DEBUG` has only subcommands, so we check it separately.
    for command, args_or_args_list in INTERNAL_SEARCH_COMMANDS.items():
        args_list = args_or_args_list if isinstance(args_or_args_list, list) else [args_or_args_list]
        for args in args_list:
            arg_list = args.split(' ')
            env.expect(command, *arg_list).error().contains("User test has no permissions to run")

@skip(redis_less_than="8.0")
def test_acl_key_permissions_validation(env):
    """Tests that the key permission validation works properly"""

    conn = env.getClusterConnectionIfNeeded()

    # Create an ACL user with partial key-space permissions
    env.expect('ACL', 'SETUSER', 'test', 'on', '>123', '~h:*', '&*', '+@all').ok()

    # Create an index on the key the user does not have permissions to access
    # and one that it can access
    no_perm_index = 'noPermIdx'
    perm_index = 'permIdx'
    env.expect('FT.CREATE', no_perm_index, 'SCHEMA', 'n', 'NUMERIC')
    env.expect('FT.CREATE', perm_index, 'PREFIXES', 1, 'h:','SCHEMA', 'n', 'NUMERIC')

    # Create another index an alias for it, that will soon be dropped.
    env.expect('FT.CREATE', 'index_to_drop', 'SCHEMA', 'n', 'NUMERIC')

    # Authenticate as the user
    env.expect('AUTH', 'test', '123').true()

    # The `test` user should not be able to access the index, with any of the
    # following commands:
    index_commands = [
        ['FT.AGGREGATE', no_perm_index, '*'],
        ['FT.INFO', no_perm_index],
        ['FT.SEARCH', no_perm_index, '*'],
        ['FT.PROFILE', no_perm_index, 'AGGREGATE', 'QUERY', '*'],
        ['FT.PROFILE', no_perm_index, 'SEARCH', 'QUERY', '*'],
        ['FT.CURSOR', 'READ', no_perm_index, '555'],
        ['FT.CURSOR', 'DEL', no_perm_index, '555'],
        ['FT.CURSOR', 'GC', no_perm_index, '555'],
        ['FT.SPELLCHECK', no_perm_index, 'name'],
        ['FT.ALIASADD', 'myAlias', no_perm_index],
        ['FT.ALIASUPDATE', 'myAlias', no_perm_index],
        ['FT.ALTER', no_perm_index, 'SCHEMA', 'ADD', 'n2', 'NUMERIC', 'SORTABLE'],
        ['FT.EXPLAIN', no_perm_index, '*'],
        ['FT.EXPLAINCLI', no_perm_index, '*'],
        ['FT.INFO', no_perm_index],
        ['FT.DROPINDEX', 'index_to_drop'],
    ]
    for command in index_commands:
        env.expect(*command).error().contains("-NOPERM User does not have the required permissions to query the index")

    # the `test` user should be able to execute all commands that do not refer to a
    # specific index
    non_index_commands = [
        [config_cmd(), 'GET', 'TIMEOUT'],
        [config_cmd(), 'SET', 'TIMEOUT', '1000'],
        ['FT.CREATE', 'idx2', 'SCHEMA', 'n', 'NUMERIC'],  # TODO: Currently here - consider moving and validating ACL key permissions
        ['FT.DICTADD', 'dict', 'hello'],
        ['FT.DICTDEL', 'dict', 'hello'],
        ['FT.DICTDUMP', 'dict'],
        ['FT.ALIASDEL', 'myAlias'],
        ['FT.SUGADD', 'h:sug', 'hello', '1'],
        ['FT.SUGDEL', 'h:sug', 'hello'],
        ['FT.SUGGET', 'h:sug', 'hello'],
        ['FT.SUGLEN', 'h:sug']
    ]
    for command in non_index_commands:
        try:
            conn.execute_command(*command)
        except Exception as e:
            # Should not fail on permissions
            env.assertNotContains("User does not have the required permissions", str(e))

    # The `test` user should also be able to access the index it has permissions
    # to access.
    # Modify the `DROPINDEX` command that does not have the same index
    index_commands[-1][1] = perm_index
    for command in index_commands:
        # Switch the index name to the one the user has permissions to access
        if no_perm_index in command:
            command[command.index(no_perm_index)] = perm_index
        try:
            env.execute_command(*command)
        except Exception as e:
            env.assertNotContains("User does not have the required permissions", str(e))

    # For completeness, we verify that the default user, which has permissions
    # to access all keys, can access the index
    env.expect('AUTH', 'default', 'nopass').true()
    for command in index_commands + non_index_commands:
        try:
            env.execute_command(*command)
        except Exception as e:
            env.assertNotContains("User does not have the required permissions", str(e))
