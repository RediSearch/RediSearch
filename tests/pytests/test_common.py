from common import *

def test_compare_lists(env):
    #test types
    compare_lists(env, 1, 1)
    compare_lists(env, 1.0, 1.0)
    compare_lists(env, "1", "1")

    compare_lists(env, [1], [1])
    compare_lists(env, [1.0], [1.0])
    compare_lists(env, ["1"], ["1"])

    compare_lists(env, {"1": 1}, {"1": 1})
    compare_lists(env, {"1": 1.0}, {"1": 1.0})
    compare_lists(env, {"1": "1"}, {"1": "1"})

    compare_lists(env, (1), (1))
    compare_lists(env, (1.0), (1.0))
    compare_lists(env, ("1"), ("1"))

    # test collections
    compare_lists(env, [1, 2, 3], [1, 2, 3])
    compare_lists(env, [1, 2, 3, 4], [1, 2, 3, 4])

    # test failures - disabled for now
    env.assertFalse(compare_lists(env, [1, 2, 3], [1, 2, 3, 4], _assert=False))
    env.assertFalse(compare_lists(env, [1, 2, 3, 4], [1, 2, 3], _assert=False))

def test_float(env):    
    compare_lists(env, 0.3333333333333333, 0.33333333333333331)
    compare_lists(env, '0.3333333333333333', '0.33333333333333331')

def test_compare_lists_with_delta(env):
    #env.assertTrue(compare_lists(env, 1.0, 2.0, delta=1, _assert=False))
    #env.assertTrue(compare_lists(env, 1.0, 2.0, delta=1))
    #env.assertFalse(compare_lists(env, 1.0, 2.0, delta=0.1, _assert=False))

    env.assertTrue(compare_lists(env, [1.0, 1.1, 2.0], [1.1, 1.1, 2.0], delta=1, _assert=False))
    env.assertTrue(compare_lists(env, [1.0, 1.1, 2.0], [1.1, 1.1, 2.0], delta=0.2, _assert=False))
    env.assertTrue(compare_lists(env, [1.0, 1.1, 2.0], [1.1, 1.1, 2.0], delta=0.1))
    env.assertFalse(compare_lists(env, [1.0, 1.1, 2.0], [1.5, 1.1, 2.0], delta=0.1, _assert=False))
