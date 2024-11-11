from common import *

READ_SEARCH_COMMANDS = ['FT.SEARCH', 'FT.AGGREGATE', 'FT.CURSOR', 'FT.CURSOR',
                 'FT.PROFILE', 'FT.SUGGET', 'FT.SUGLEN']
WRITE_SEARCH_COMMANDS = ['FT.DROPINDEX', 'FT.SUGADD', 'FT.SUGDEL']

def test_acl_category(env):
    """Test that the `search` category was added appropriately in module
    load"""
    res = env.cmd('ACL', 'CAT')
    env.assertTrue('search' in res)

@skip(redis_less_than="7.9.0")
def test_acl_search_commands(env):
    """Tests that the RediSearch commands are registered to the `search`
    ACL category"""
    res = env.cmd('ACL', 'CAT', 'search')
    commands = [
        'FT.EXPLAINCLI', '_FT.ALIASDEL', 'FT.SPELLCHECK', 'FT.SYNUPDATE',
        'FT.ALIASUPDATE', '_FT.AGGREGATE', '_FT.ALIASADD', 'FT._LIST',
        '_FT.ALIASUPDATE', 'FT.ALIASADD', 'FT.SEARCH',
        '_FT.CURSOR', 'FT.INFO', 'FT.SUGDEL', '_FT.INFO',
        '_FT.ALTER', '_FT.DICTDEL', '_FT.SYNUPDATE', 'FT.DICTDUMP',
        'FT.EXPLAIN', '_FT.SPELLCHECK', 'FT.AGGREGATE', 'FT.SUGLEN',
        'FT.PROFILE', 'FT.ALTER', 'FT.SUGGET', '_FT.CREATE',
        'FT.DICTDEL', 'FT.CURSOR', 'FT.ALIASDEL', 'FT.SUGADD', '_FT.DICTADD',
        'FT.SYNDUMP', 'FT.CREATE', '_FT.PROFILE', '_FT.SEARCH', 'FT.DICTADD',
        'FT.SYNFORCEUPDATE', 'FT._ALIASDELIFX', 'FT._CREATEIFNX',
        '_FT.DEBUG', '_FT.CONFIG', '_FT.TAGVALS',
        'search.CLUSTERREFRESH', 'FT._ALIASADDIFNX', '_FT._ALTERIFNX',
     'FT._ALTERIFNX', '_FT._ALIASDELIFX', 'search.CLUSTERSET',
        'search.CLUSTERINFO', '_FT._ALIASADDIFNX', '_FT._DROPINDEXIFX',
        'FT._DROPINDEXIFX', '_FT.DROPINDEX', 'FT.DROPINDEX','_FT.ADD',
        '_FT.DROP', 'FT.TAGVALS', '_FT.GET', 'FT._DROPIFX', '_FT._CREATEIFNX',
        'FT.DROP', 'FT.GET', '_FT.MGET', 'FT.SYNADD', 'FT.ADD', '_FT.DEL',
        'FT.MGET', '_FT._DROPIFX', '_FT.SAFEADD', 'FT.DEL'
    ]
    if not env.isCluster():
        commands.append('FT.CONFIG')

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(commands))

    # Check that one of our commands is listed in a non-search category
    res = env.cmd('ACL', 'CAT', 'read')
    env.assertTrue('FT.SEARCH' in res)

def test_acl_non_default_user(env):
    """Tests that a user with a non-default ACL can't access the search
    category"""
    # Create a user with no command permissions (full keyspace and pubsub access)
    env.expect('ACL', 'SETUSER', 'test', 'on', '>123', '~*', '&*').ok()
    env.expect('AUTH', 'test', '123').true()

    # Such a user shouldn't be able to run any RediSearch commands (or any other commands)
    env.expect('FT.SEARCH', 'idx', '*').error().contains(
        "User test has no permissions to run the 'FT.SEARCH' command")

    # Add `test` read permissions
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'test', '+@read').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` should now be able to run `read` commands like `FT.SEARCH', but not
    # `search` commands like `FT.CREATE`
    for cmd in READ_SEARCH_COMMANDS:
        env.expect(cmd).error().notContains(
            "User test has no permissions")

    # `test` should not be able to run `search` commands that are not `read`,
    # like `FT.CREATE`
    env.expect('FT.CREATE', 'idx', '*').error().contains(
        "User test has no permissions to run the 'FT.CREATE' command")

    # Let's add `write` permissions to `test`
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'test', '+@write').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` should now be able to run `write` commands
    for cmd in WRITE_SEARCH_COMMANDS:
        env.expect(cmd).error().notContains(
            "User test has no permissions")

    # Add `test` `search` permissions
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'test', '+@search').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` should now be able to run `search` commands like `FT.CREATE`
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

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
