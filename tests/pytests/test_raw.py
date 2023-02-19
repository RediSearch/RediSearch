from RLTest import Env
from common import *
from includes import *

@SearchTest(readers = 1)
def test_sanity(searchEnv):
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', [b'name', b'foo']])

@SearchTest(readers = 1)
def test_drop_index(searchEnv):
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    searchEnv.expect('ft.drop', 'idx').ok()
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', [b'name', b'foo']])


# @SearchTest(readers = 1)
# def test_async(searchEnv):
    # async_res = searchEnv.async_cmd("SET", "foo", "bar")
    # searchEnv.assertEqual(async_res.read_response(), True)


# @SearchTest(readers = 1)
# def test(SearchEnv):
#     reader = SearchEnv.attach_reader(1) # how to get the reader thread id?
#     async_res = SearchEnv.execute_command_async('FT.SEARCH ....')
#     reader.run_to_label("before_background_search")
#     SearchEnv.execute_command('FT.DROP ...')
#     reader.cont()
#     async_res.read_response().equal('Unknown index')