from RLTest import Env
from common import *
from includes import *

@SearchTest(readers = 1)
def test_sanity(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', [b'name', b'foo']])

@SearchTest(readers = 1)
def test_add_doc_index_strong_ref(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    # Here we drop the index while the reader is waiting for the spec lock and has a strong ref to the index
    # The reader should:
    # 1. Not crash
    # 2. Not block forever
    # 3. Perform successfully the search
    # 4. Return valid results from the keyspace, including the added document
    searchEnv.expect('hset', 'doc2', 'name', 'foo').equal(1)
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [2, b'doc1', [b'name', b'foo'], b'doc2', [b'name', b'foo']])

@SearchTest(readers = 1)
def test_edit_doc_index_strong_ref(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'bar')
    worker_debuger.run_to_label("reader_before_spec_lock")
    # Here we drop the index while the reader is waiting for the spec lock and has a strong ref to the index
    # The reader should:
    # 1. Not crash
    # 2. Not block forever
    # 3. Perform successfully the search
    # 4. Return valid results from the keyspace - doc should be updated
    searchEnv.expect('hset', 'doc1', 'name', 'bar').equal(0)
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', [b'name', b'bar']])

@SearchTest(readers = 1)
def test_drop_index_strong_ref(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    # Here we drop the index while the reader is waiting for the spec lock and has a strong ref to the index
    # The reader should:
    # 1. Not crash
    # 2. Not block forever
    # 3. Perform successfully the search
    # 4. Return valid results from the keyspace, as the document was not deleted
    searchEnv.expect('ft.dropindex', 'idx').ok()
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', [b'name', b'foo']])

@SearchTest(readers = 1)
def test_drop_index_delete_docs_strong_ref(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_spec_lock")
    # Here we drop the index while the reader is waiting for the spec lock and has a strong ref to the index
    # The reader should:
    # 1. Not crash
    # 2. Not block forever
    # 3. Perform successfully the search
    # 4. Return no results from the keyspace
    searchEnv.expect('ft.drop', 'idx').ok()
    worker_debuger.run_to_label("reader_after_spec_lock")
    worker_debuger.detach()
    searchEnv.assertEquals(async_res.read_response(),  [1, b'doc1', None])

@SearchTest(readers = 1)
def test_drop_index_weak_ref(searchEnv):
    if not DEBUG:
        searchEnv.skip()
    searchEnv.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    searchEnv.expect('hset', 'doc1', 'name', 'foo').equal(1)
    worker_debuger = searchEnv.attach_to_worker(0)
    async_res = searchEnv.async_cmd('FT.SEARCH', 'idx', 'foo')
    worker_debuger.run_to_label("reader_before_strong_ref")
    # Here we drop the index before the reader could get a strong ref to the index
    # The reader should:
    # 1. Not crash
    # 2. Not block forever
    # 3. Fail on performing the search, since it is got a weak ref to the index and unable to get a strong ref
    searchEnv.expect('ft.dropindex', 'idx').ok()
    worker_debuger.detach()
    res = async_res.read_response()
    searchEnv.assertEquals(str(res),  "The index was dropped before the query could be executed")
