from common import *


@skip_redis_less_than(redis_less_than="7.4.1")
def test_acl_category(env):
    """Test that the `json` category was added appropriately in module load"""
    res = env.cmd('ACL', 'CAT')
    env.assertTrue('json' in res)

@skip_redis_less_than(redis_less_than="7.99.99")
def test_acl_json_commands(env):
    """Tests that the RedisJSON commands are registered to the `json` ACL category"""
    res = env.cmd('ACL', 'CAT', 'json')
    COMMANDS = [
        'json.del', 'json.get', 'json.mget', 'json.set', 'json.mset', 'json.type', 'json.numincrby', 'json.toggle',
        'json.nummultby', 'json.numpowby', 'json.strappend', 'json.strlen', 'json.arrappend', 'json.arrindex',
        'json.arrinsert', 'json.arrlen', 'json.arrpop', 'json.arrtrim', 'json.objkeys', 'json.objlen', 'json.clear',
        'json.debug', 'json.forget', 'json.resp', 'json.merge',
    ]

    # Use a set since the order of the response is not consistent.
    env.assertEqual(set(res), set(COMMANDS))

    # Check that one of our commands is listed in a non-json category
    res = env.cmd('ACL', 'CAT', 'read')
    env.assertTrue('json.get' in res)

@skip_redis_less_than(redis_less_than="7.4.1")
def test_acl_non_default_user(env):
    """Tests that a user with a non-default ACL can't access the json category"""

    # Create a user with no command permissions (full keyspace and pubsub access)
    env.expect('ACL', 'SETUSER', 'testusr', 'on', '>123', '~*', '&*').ok()
	
    env.expect('AUTH', 'testusr', '123').true()

    # Such a user shouldn't be able to run any RedisJSON commands (or any other commands)
    env.expect('json.get', 'idx', '$', '*').error().contains(
        "User testusr has no permissions to run the 'json.get' command")

    # Add `testusr` read permissions
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'testusr', '+@read').ok()
    env.expect('AUTH', 'testusr', '123').true()

    READ_JSON_COMMANDS = [
        'json.get', 'json.type', 'json.strlen', 'json.arrlen', 'json.objlen',
        'json.debug', 'json.arrindex', 'json.objkeys',
    ]

    # `testusr` should now be able to run `read` commands like `json.get'
    for cmd in READ_JSON_COMMANDS:
        env.expect(cmd).error().notContains(
            "User testusr has no permissions")

    # `testusr` should not be able to run `json` commands that are not `read`
    env.expect('json.set', 'idx', '$', '0').error().contains(
        "User testusr has no permissions to run the 'json.set' command")

    # Add `write` permissions to `testusr`
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'testusr', '+@write').ok()
    env.expect('AUTH', 'testusr', '123').true()

    WRITE_JSON_COMMANDS = [
        'json.del', 'json.set', 'json.merge', 'json.clear', 'json.forget', 'json.strappend',
        'json.arrappend', 'json.arrinsert', 'json.arrpop', 'json.arrtrim',
    ]

    # `testusr` should now be able to run `write` commands
    for cmd in WRITE_JSON_COMMANDS:
        env.expect(cmd).error().notContains(
            "User testusr has no permissions")

    # Add `testusr` `json` permissions
    env.expect('AUTH', 'default', '').true()
    env.expect('ACL', 'SETUSER', 'testusr', '+@json').ok()
    env.expect('AUTH', 'testusr', '123').true()

    # `testusr` should now be able to run `json` commands like `json.set`
    env.expect('json.set', 'idx', '$', '0').ok()
    env.expect('json.get', 'idx', '$').equal('[0]')
