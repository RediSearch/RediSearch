from common import *

READ_SEARCH_COMMANDS = ['FT.SEARCH', 'FT.AGGREGATE', 'FT.CURSOR', 'FT.CURSOR',
                 'FT.PROFILE', 'FT.SUGGET', 'FT.SUGLEN']
WRITE_SEARCH_COMMANDS = ['FT.DROPINDEX', 'FT.SUGADD', 'FT.SUGDEL']

def test_acl_category(env):
    """Test that the `search` category was added appropriately in module
    load"""
    res = env.cmd('ACL', 'CAT')
    env.assertContains('search', res)

@skip(redis_less_than="7.9.22")
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
        'FT._ALIASDELIFX', 'FT._CREATEIFNX', 'FT._ALIASADDIFNX', 'FT._ALTERIFNX',
        'FT._DROPINDEXIFX', 'FT.DROPINDEX', 'FT.TAGVALS', 'FT._DROPIFX',
        'FT.DROP', 'FT.GET', 'FT.SYNADD', 'FT.ADD', 'FT.MGET', 'FT.DEL',
        '_FT.CONFIG', '_FT.DEBUG', '_FT.SAFEADD'
    ]
    if not env.isCluster():
        commands.append('FT.CONFIG')
    if env.env != 'enterprise':
        commands.extend(['search.CLUSTERINFO', 'search.CLUSTERREFRESH', 'search.CLUSTERSET'])

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(commands))

    # Check that one of our commands is listed in a non-search category (sanity)
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
        conn.execute_command('ACL', 'SETUSER', 'test', '+@search')
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
    """
    Tests that internal commands are not allowed for non-internal
    connections, and are by internal connections.
    """

    # Internal commands are treated as unknown commands for non-internal
    # connections
    env.expect('_FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').error().contains("unknown command")

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    env.expect('_FT.SEARCH', 'idx', '*').error().contains("unknown command")

    # Promote the connection to internal
    env.expect('DEBUG', 'PROMOTE-CONN-INTERNAL').ok()
    env.expect('_FT.SEARCH', 'idx', '*').equal([0])

@skip(redis_less_than="7.9.22")
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
        env.expect(*command).error().contains("User does not have the required permissions to query the index")

    # the `test` user should be able to execute all commands that do not refer to a
    # specific index
    non_index_commands = [
        [config_cmd(), 'GET', 'TIMEOUT'],
        [config_cmd(), 'SET', 'TIMEOUT', '1000'],
        ['FT.CREATE', 'idx2', 'SCHEMA', 'n', 'NUMERIC'],
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
