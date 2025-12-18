from RLTest import Env, Defaults

Defaults.decode_responses = True


def test_json_set_escaping(env):
    """Test JSON.SET with backslash escaping"""
    env.cmd('JSON.SET', 'j', '$', '{}')
    env.cmd('JSON.SET', 'j', '$', r'{"\\":1,"\\\\":2}')
    result = env.cmd('JSON.GET', 'j')
    env.assertEqual(result, r'{"\\":1,"\\\\":2}')


def test_json_get_escaping(env):
    """Test JSON.GET with backslash escaping in JSONPath"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\":1,"\\\\":2}')
    result1 = env.cmd('JSON.GET', 'j', r'$["\\"]')
    env.assertEqual(result1, '[1]')
    result2 = env.cmd('JSON.GET', 'j', r'$["\\\\"]')
    env.assertEqual(result2, '[2]')


def test_json_del_escaping(env):
    """Test JSON.DEL with backslash escaping in JSONPath"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\":1,"\\\\":2}')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\\"]')
    env.assertEqual(deleted, 1)
    result = env.cmd('JSON.GET', 'j')
    env.assertEqual(result, r'{"\\\\":2}')


def test_json_del_escaping_second_key(env):
    """Test JSON.DEL with double backslash escaping in JSONPath"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\":1,"\\\\":2}')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\\\\"]')
    env.assertEqual(deleted, 1)
    result = env.cmd('JSON.GET', 'j')
    env.assertEqual(result, r'{"\\":1}')


def test_triple_backslash(env):
    """Test with 3 backslashes"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\\\\\":3}')
    result = env.cmd('JSON.GET', 'j', r'$["\\\\\\"]')
    env.assertEqual(result, '[3]')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\\\\\\"]')
    env.assertEqual(deleted, 1)
    result_after = env.cmd('JSON.GET', 'j')
    env.assertEqual(result_after, None)


def test_quad_backslash(env):
    """Test with 4 backslashes"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\\\\\\\":4}')
    result = env.cmd('JSON.GET', 'j', r'$["\\\\\\\\"]')
    env.assertEqual(result, '[4]')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\\\\\\\\"]')
    env.assertEqual(deleted, 1)
    result_after = env.cmd('JSON.GET', 'j')
    env.assertEqual(result_after, None)


def test_mixed_backslashes(env):
    """Test with all different backslash counts"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\":1,"\\\\":2,"\\\\\\":3,"\\\\\\\\":4}')
    for i, path in enumerate([r'$["\\"]', r'$["\\\\"]', r'$["\\\\\\"]', r'$["\\\\\\\\"]'], 1):
        result = env.cmd('JSON.GET', 'j', path)
        env.assertEqual(result, f'[{i}]')

def test_quote_escaping(env):
    """Test with quote characters"""
    env.cmd('JSON.SET', 'j', '$', r'{"\"":1}')
    result = env.cmd('JSON.GET', 'j', r'$["\""]')
    env.assertEqual(result, '[1]')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\""]')
    env.assertEqual(deleted, 1)
    result_after = env.cmd('JSON.GET', 'j')
    env.assertEqual(result_after, None)


def test_backslash_and_quote(env):
    """Test with backslash and quote combined"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\\"":1}')
    result = env.cmd('JSON.GET', 'j', r'$["\\\""]')
    env.assertEqual(result, '[1]')
    deleted = env.cmd('JSON.DEL', 'j', r'$["\\\""]')
    env.assertEqual(deleted, 1)
    result_after = env.cmd('JSON.GET', 'j')
    env.assertEqual(result_after, None)


def test_path_tracking(env):
    """Test that path tracking returns correct paths"""
    env.cmd('JSON.SET', 'j', '$', r'{"\\":{"nested":1},"\\\\":{"nested":2}}')
    result1 = env.cmd('JSON.GET', 'j', r'$["\\"].nested')
    env.assertEqual(result1, '[1]')
    result2 = env.cmd('JSON.GET', 'j', r'$["\\\\"].nested')
    env.assertEqual(result2, '[2]')


def test_raw_string_input(env):
    """Test what happens when we pass the actual JSON string vs escaped"""
    env.cmd('JSON.SET', 'test1', '$', '{"\\\\": 1}')
    result1 = env.cmd('JSON.GET', 'test1')
    env.assertEqual(result1, r'{"\\":1}')

    env.cmd('JSON.SET', 'test2', '$', r'{"\\":1}')
    result2 = env.cmd('JSON.GET', 'test2')
    env.assertEqual(result2, r'{"\\":1}')
