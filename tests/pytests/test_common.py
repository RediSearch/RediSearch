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

def test_compare_numeric_dicts(env):
    d1 = {"cat": "0.1", "dog": 2, "bird": 3.0, "fish": 489}
    d2 = d1.copy()
    # compare dicts successfully
    env.assertTrue(compare_numeric_dicts(env, d1, d2, _assert=False), message="comparing identical dicts")

    # compare dicts with different values
    d2["fish"] = "0.2"
    env.assertFalse(compare_numeric_dicts(env, d1, d2, _assert=False), message="comparing dicts with different values")
    # compare dicts with different keys
    d3 = d1.copy()
    d1["rabbit"] = 5
    try:
        compare_numeric_dicts(env, d1, d3, _assert=False)
        env.assertTrue(False, message="Comparing dicts with different keys")
    except KeyError:
        pass

def test_recursive_index():
    # Test with a simple list
    assert recursive_index([1, 2, 3], 2) == [1]
    assert recursive_index([1, 2, 3], 4) is None
    # Test with a nested list
    assert recursive_index([1, [2, 3], 4], 3) == [1, 1]
    assert recursive_index([1, [2, 3], 4], 5) is None
    # Test with a deeply nested list
    assert recursive_index([1, [2, [3, 4]], 5], 4) == [1, 1, 1]
    assert recursive_index([1, [2, [3, 4]], 5], 6) is None
    # Test with a list of strings
    assert recursive_index(["a", "b", "c"], "b") == [1]
    assert recursive_index(["a", "b", "c"], "d") is None
    # Test with a nested list of strings
    assert recursive_index(["a", ["b", "c"], "d"], "c") == [1, 1]
    assert recursive_index(["a", ["b", "c"], "d"], "e") is None
    # Test with a deeply nested list of strings
    assert recursive_index(["a", ["b", ["c", "d"]], "e"], "d") == [1, 1, 1]
    assert recursive_index(["a", ["b", ["c", "d"]], "e"], "f") is None

def test_access_nested_list():
    # Test with a simple list
    assert access_nested_list([1, 2, 3], [1]) == 2
    # Test with a nested list
    assert access_nested_list([1, [2, 3], 4], [1, 1]) == 3
    # Test with a deeply nested list
    assert access_nested_list([1, [2, [3, 4]], 5], [1, 1, 1]) == 4

def test_recursive_index_and_access_nested_list():
    # Test with a nested list
    nested_list = [1, [2, 3], 4]
    target = 3
    assert access_nested_list(nested_list, recursive_index(nested_list, target)) == target
    # Test with a deeply nested list
    nested_list = [1, [2, [3, 4]], 5]
    target = 4
    assert access_nested_list(nested_list, recursive_index(nested_list, target)) == target
