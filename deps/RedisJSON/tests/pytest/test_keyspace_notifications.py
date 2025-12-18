import json

import time

import redis
from RLTest import Env,Defaults
import time
Defaults.decode_responses = True

def assert_msg(env, msg, expected_type, expected_data):
    env.assertEqual(expected_type, msg['type']) 
    env.assertEqual(expected_data, msg['data']) 

def test_keyspace_set(env):
    env.skipOnVersionSmaller('6.2')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('JSON.SET', 'test_key', '$', '{"foo": "bar", "fu": 131}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('OK', r.execute_command('JSON.SET', 'test_key', '$.foo', '"gogo"'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('OK', r.execute_command('JSON.MSET', 'test_key', '$.foo', '"gogo"', 'test_key{test_key}', '$', '{"a":"fufu"}'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.mset')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.mset')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key{test_key}')

        env.assertEqual([8], r.execute_command('JSON.STRAPPEND', 'test_key', '$.foo', '"toto"'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.strappend')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('OK', r.execute_command('JSON.MERGE', 'test_key', '$', '{"mu":1}'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.merge')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        # Negative tests should not get an event
        # Read-only commands should not get an event
        env.assertEqual(None, r.execute_command('JSON.SET', 'test_key', '$.foo.a', '"nono"'))
        env.assertEqual(None, pubsub.get_message(timeout=1))       

        env.assertEqual(None, r.execute_command('JSON.MERGE', 'test_key', '$.foo.a', '{"zu":1}'))
        env.assertEqual(None, pubsub.get_message(timeout=1)) 

        env.assertEqual([8], r.execute_command('JSON.STRLEN', 'test_key', '$.foo'))
        env.assertEqual(None, pubsub.get_message(timeout=1))       

        env.assertEqual('["gogototo"]', r.execute_command('JSON.GET', 'test_key', '$.foo'))
        env.assertEqual(None, pubsub.get_message(timeout=1))       

        env.assertEqual(['["gogototo"]', None], r.execute_command('JSON.MGET', 'test_key', 'test_key1', '$.foo'))
        env.assertEqual(None, pubsub.get_message(timeout=1))       

        env.assertEqual([['foo', 'fu', 'mu']], r.execute_command('JSON.OBJKEYS', 'test_key', '$'))
        env.assertEqual(None, pubsub.get_message(timeout=1))       

        env.assertEqual([3], r.execute_command('JSON.OBJLEN', 'test_key', '$'))
        env.assertEqual(None, pubsub.get_message(timeout=1))

        env.assertEqual([None], r.execute_command('JSON.STRAPPEND', 'test_key', '$.fu', '"bark"'))
        env.assertEqual(None, pubsub.get_message(timeout=1))


def test_keyspace_arr(env):
    env.skipOnVersionSmaller('6.2')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('JSON.SET', 'test_key_arr', '$', '{"foo": [], "bar": 31}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual([2], r.execute_command('JSON.ARRAPPEND', 'test_key_arr', '$.foo', '"gogo1"', '"gogo2"'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.arrappend')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual([4], r.execute_command('JSON.ARRINSERT', 'test_key_arr', '$.foo', 1, '"gogo3"', '"gogo4"'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.arrinsert')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual(['"gogo3"'], r.execute_command('JSON.ARRPOP', 'test_key_arr', '$.foo', 1))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.arrpop')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual([2], r.execute_command('JSON.ARRTRIM', 'test_key_arr', '$.foo', 0, 1))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.arrtrim')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        # Negative tests should not get an event
        # Read-only commands should not get an event
        env.assertEqual([0], r.execute_command('JSON.ARRINDEX', 'test_key_arr', '$.foo', '"gogo1"'))
        env.assertEqual(None, pubsub.get_message(timeout=1))   

        env.assertEqual([2], r.execute_command('JSON.ARRLEN', 'test_key_arr', '$.foo'))
        env.assertEqual(None, pubsub.get_message(timeout=1))

        env.assertEqual(1, r.execute_command('JSON.CLEAR', 'test_key_arr', '$.foo'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.clear')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual(1, r.execute_command('JSON.CLEAR', 'test_key_arr', '$.bar'))  # Not an array
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.clear')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key_arr')

        env.assertEqual([None], r.execute_command('JSON.ARRPOP', 'test_key_arr', '$.foo'))  # Empty array
        env.assertEqual(None, pubsub.get_message(timeout=1))
        env.assertEqual([None], r.execute_command('JSON.ARRPOP', 'test_key_arr', '$'))  # Not an array
        env.assertEqual(None, pubsub.get_message(timeout=1))

        # TODO add more negative test for arr path not found
        env.assertEqual([None], r.execute_command('JSON.ARRTRIM', 'test_key_arr', '$.bar', 0, 1))   # Not an array
        env.assertEqual(None, pubsub.get_message(timeout=1))
        env.assertEqual([None], r.execute_command('JSON.ARRINSERT', 'test_key_arr', '$.bar', 1, '"barian"'))    # Not an array
        env.assertEqual(None, pubsub.get_message(timeout=1))
        env.assertEqual([None], r.execute_command('JSON.ARRAPPEND', 'test_key_arr', '$.bar', '"barian"'))   # Not an array
        env.assertEqual(None, pubsub.get_message(timeout=1))
        env.assertEqual([None], r.execute_command('JSON.ARRPOP', 'test_key_arr', '$.bar'))  # Not an array
        env.assertEqual(None, pubsub.get_message(timeout=1))        


def test_keyspace_del(env):
    env.skipOnVersionSmaller('6.2')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('JSON.SET', 'test_key', '$', '{"foo": "bar", "foo2":"bar2", "foo3":"bar3"}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual(1, r.execute_command('JSON.DEL', 'test_key', '$.foo'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.del')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual(1, r.execute_command('JSON.FORGET', 'test_key', '$.foo3'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.del')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual(0, r.execute_command('JSON.DEL', 'test_key', '$.foo'))
        env.assertEqual(None, pubsub.get_message(timeout=1))      

def test_keyspace_num(env):
    env.skipOnVersionSmaller('6.2')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('JSON.SET', 'test_key', '$', '{"foo": 1, "bar": "baro"}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('[4]', r.execute_command('JSON.NUMINCRBY', 'test_key', '$.foo', 3))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.numincrby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('[12]', r.execute_command('JSON.NUMMULTBY', 'test_key', '$.foo', 3))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.nummultby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('[144]', r.execute_command('JSON.NUMPOWBY', 'test_key', '$.foo', 2))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.numpowby')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        # Negative tests should not get an event
        env.assertEqual('[null]', r.execute_command('JSON.NUMINCRBY', 'test_key', '$.bar', 3))
        env.assertEqual(None, pubsub.get_message(timeout=1))

        env.assertEqual('[null]', r.execute_command('JSON.NUMMULTBY', 'test_key', '$.bar', 3))
        env.assertEqual(None, pubsub.get_message(timeout=1))

        env.assertEqual('[null]', r.execute_command('JSON.NUMPOWBY', 'test_key', '$.bar', 3))
        env.assertEqual(None, pubsub.get_message(timeout=1))


def test_keyspace_bool(env):
    env.skipOnVersionSmaller('6.2')
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type']) 

        r.execute_command('JSON.SET', 'test_key', '$', '{"foo": true, "bar": "baro"}')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.set')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual('false', r.execute_command('JSON.TOGGLE', 'test_key', '.foo'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.toggle')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual([1], r.execute_command('JSON.TOGGLE', 'test_key', '$..foo'))
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'json.toggle')
        assert_msg(env, pubsub.get_message(timeout=1), 'pmessage', 'test_key')

        env.assertEqual([None], r.execute_command('JSON.TOGGLE', 'test_key', '$.bar'))
        env.assertEqual(None, pubsub.get_message(timeout=1))

        env.expect('JSON.TOGGLE', 'test_key', '.bar').raiseError()
        env.assertEqual(None, pubsub.get_message(timeout=1))