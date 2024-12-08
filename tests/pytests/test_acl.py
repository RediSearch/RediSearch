from common import *

READ_SEARCH_COMMANDS = ['FT.SEARCH', 'FT.AGGREGATE', 'FT.CURSOR', 'FT.CURSOR',
                 'FT.PROFILE', 'FT.SUGGET', 'FT.SUGLEN']
WRITE_SEARCH_COMMANDS = ['FT.DROPINDEX', 'FT.SUGADD', 'FT.SUGDEL']
INTERNAL_SEARCH_COMMANDS = [
        '_FT.ALIASDEL', '_FT.AGGREGATE', '_FT.ALIASADD', '_FT.ALIASUPDATE',
        '_FT.CURSOR', '_FT.INFO', '_FT.ALTER', '_FT.DICTDEL', '_FT.SYNUPDATE',
        '_FT.SPELLCHECK', '_FT.CREATE', '_FT.DICTADD', '_FT.PROFILE',
        '_FT.SEARCH', '_FT.DEBUG', '_FT.CONFIG', '_FT.TAGVALS', '_FT._ALTERIFNX',
        '_FT._ALIASDELIFX', '_FT._ALIASADDIFNX', '_FT._DROPINDEXIFX',
        '_FT.DROPINDEX', '_FT.ADD', '_FT.DROP', '_FT.GET', '_FT._CREATEIFNX',
        '_FT.MGET', '_FT.DEL', '_FT._DROPIFX', '_FT.SAFEADD'
    ]

def test_acl_category(env):
    """Test that the `search` category was added appropriately in module
    load"""
    res = env.cmd('ACL', 'CAT')
    for category in ['search', '_search_internal']:
        env.assertContains(category, res)

@skip(redis_less_than="7.9.0")
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
        'FT.SYNFORCEUPDATE', 'FT._ALIASDELIFX', 'FT._CREATEIFNX',
        'search.CLUSTERREFRESH', 'FT._ALIASADDIFNX', 'FT._ALTERIFNX',
        'search.CLUSTERSET', 'search.CLUSTERINFO', 'FT._DROPINDEXIFX',
        'FT.DROPINDEX', 'FT.TAGVALS', 'FT._DROPIFX', 'FT.DROP', 'FT.GET',
        'FT.SYNADD', 'FT.ADD', 'FT.MGET', 'FT.DEL'
    ]
    if not env.isCluster():
        commands.append('FT.CONFIG')

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(commands))

    # ---------------- internal search command category ----------------
    res = env.cmd('ACL', 'CAT', '_search_internal')
    commands = INTERNAL_SEARCH_COMMANDS

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
    internal_commands = INTERNAL_SEARCH_COMMANDS[::]
    internal_commands.remove('_FT.DEBUG')
    for command in internal_commands:
        env.expect(command).error().contains("User test has no permissions to run")

    # Check `_FT.DEBUG`
    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').error().contains("User test has no permissions to run")

    # Authenticate as `default`, and run the internal debug command
    env.expect('AUTH', 'default', 'nopass').true()
    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').equal([])

@skip(redis_less_than="8.0.3")
def test_acl_key_permissions_validation(env):
    """Tests that the key permission validation works properly"""

    # Create an ACL user with partial key-space permissions
    env.expect('ACL', 'SETUSER', 'test', 'on', '>123', '~h:*', '&*', '+@all').ok()

    # Create an index on the key the user does not have permissions to access
    # All prefixes will do (default)
    idx_name = 'idx'
    env.expect('FT.CREATE', idx_name, 'SCHEMA', 'n', 'NUMERIC')

    # Create another index an alias for it, that will soon be dropped.
    env.expect('FT.CREATE', 'index_to_drop', 'SCHEMA', 'n', 'NUMERIC')
    env.expect('FT.ALIASADD', 'myAlias', 'index_to_drop')

    # Authenticate as the user
    env.expect('AUTH', 'test', '123').true()

    # The `test` user should not be able to access the index, with any of the
    # following commands:
    index_commands = [
        ['FT.AGGREGATE', idx_name, '*'],
        ['FT.INFO', idx_name],
        ['FT.SEARCH', idx_name, '*'],
        ['FT.PROFILE', idx_name, 'AGGREGATE', 'QUERY', '*'],
        ['FT.PROFILE', idx_name, 'SEARCH', 'QUERY', '*'],
        ['FT.CURSOR', 'READ', idx_name, '555'],
        ['FT.CURSOR', 'DEL', idx_name, '555'],
        ['FT.CURSOR', 'GC', idx_name, '555'],
        ['FT.SPELLCHECK', idx_name, 'name'],
        ['FT.ALIASADD', 'myAlias', idx_name],
        ['FT.ALIASUPDATE', 'myAlias', idx_name],
        ['FT.ALTER', idx_name, 'SCHEMA', 'ADD', 'n2', 'NUMERIC', 'SORTABLE'],
        ['FT.DROPINDEX', 'index_to_drop'],
        ['FT.EXPLAIN', idx_name, '*'],
        ['FT.EXPLAINCLI', idx_name, '*'],
        ['FT.INFO', idx_name],
    ]
    for command in index_commands:
        env.expect(*command).error().contains("-NOPERM User does not have the required permissions to query the index")

    # the user should be able to execute all commands that do not refer to a
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
        env.expect(*command).noError()

    # For completeness, we verify that the default user, which has permissions
    # to access all keys, can access the index
    env.expect('AUTH', 'default', 'nopass').true()
    for command in index_commands + non_index_commands:
        try:
            env.execute_command(*command)
        except Exception as e:
            env.assertNotContains("User does not have the required permissions", str(e))
