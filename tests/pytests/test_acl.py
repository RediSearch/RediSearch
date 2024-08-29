from common import *

def test_acl_category(env):
    """Test that the `search` category was added appropriately in module
    load"""
    res = env.cmd('ACL', 'CAT')
    env.assertTrue('search' in res)

@skip(redis_less_than="7.4.1")
def test_acl_search_commands(env):
    """Tests that the RediSearch commands are registered to the `search`
    ACL category"""
    res = env.cmd('ACL', 'CAT', 'search')
    # TODO: Do we want to hide the internal commands (e.g., "_FT.*")?
    commands = [
        'FT.EXPLAINCLI', '_FT.ALIASDEL', 'FT.SPELLCHECK', 'FT.SYNUPDATE',
        'FT.ALIASUPDATE', '_FT.AGGREGATE', '_FT.ALIASADD', 'FT._LIST',
        '_FT.ALIASUPDATE', 'FT.ALIASADD', 'FT.SEARCH', '_FT.SUGGET',
        '_FT.CURSOR', 'FT.INFO', 'FT.SUGDEL', '_FT.INFO', '_FT.SUGDEL',
        '_FT.ALTER', '_FT.DICTDEL', '_FT.SYNUPDATE', 'FT.DICTDUMP',
        'FT.EXPLAIN', '_FT.SPELLCHECK', 'FT.AGGREGATE', 'FT.SUGLEN',
        '_FT.SUGLEN', 'FT.PROFILE', 'FT.ALTER', 'FT.SUGGET', '_FT.CREATE',
        'FT.DICTDEL', 'FT.CURSOR', 'FT.ALIASDEL', 'FT.SUGADD', '_FT.DICTADD',
        'FT.SYNDUMP', 'FT.CREATE', '_FT.PROFILE', '_FT.SEARCH', 'FT.DICTADD',
        '_FT.SUGADD'
    ]
    # Use a set since the order of the response is not consistent.
    commands = set(commands)
    env.assertEqual(set(res), commands)

    # Check that one of our commands is listed in a non-search category
    res = env.cmd('ACL', 'CAT', 'read')
    env.assertTrue('FT.SEARCH' in res)

def test_acl_non_default_user(env):
    """Tests that a user with a non-default ACL can't access the search
    category"""
    # Create a user with no command permissions
    env.expect('ACL', 'SETUSER', 'test', 'on', '>123', '~*', '&*').ok()
    env.expect('AUTH', 'test', '123').true()

    # Such a user shouldn't be able to run any RediSearch commands (or any other commands)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT').error().contains(
        "User test has no permissions to run the 'FT.CREATE' command")

    env.expect('AUTH', 'default', '').true()
    # Add `test` read permissions
    env.expect('ACL', 'SETUSER', 'test', '+@read').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` should now be able to run `read` commands like `FT.SEARCH', but not
    # `write` commands like `FT.CREATE`
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT').error().contains(
        "User test has no permissions to run the 'FT.CREATE' command")
    env.expect('FT.SEARCH', 'idx', 'hello').error().contains(
        "no such index")

    # Add `test` write permissions
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'test', '+@write').ok()
    env.expect('AUTH', 'test', '123').true()

    # `test` should now be able to run `write` commands like `FT.CREATE`
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # A user with the `search` category should be able to run RediSearch commands
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'search_user', '+@search').ok()
    env.expect('AUTH', 'search_user', '123').true()

    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'txt', 'TEXT').ok()
    env.expect('FT.SEARCH', 'idx2', '*').equal([0])
