"""
Step definitions for RediSearchDisk deletion tests.
"""
from common import *
import json

@when(parsers.parse('I delete "{key}"'))
def delete_document(redis_env, key):
    """Delete a document."""
    redis_env.cmd('DEL', key)

@then(parsers.parse('index "{idx}" should have {count} documents'))
@then(parsers.parse('index "{idx}" should have {count} document'))
def check_number_of_documents(redis_env, idx, count):
    """Check the number of documents in the index."""
    info = redis_env.cmd('FT.INFO', idx)
    n_docs = int(info[9])
    assert n_docs == int(count)

@then(parsers.parse('I should get results with keys {keys}'))
def check_keys_in_result(redis_env, request, keys):
    """Checks that the expected keys were recieved from the query.
    This test assumes the results were recieved from a `FT.SEARCH` command"""
    result = request.node.search_result
    result_keys = [result[i] for i in range(1, len(result), 2)]
    expected_keys = json.loads(keys)
    assert sorted(result_keys) == sorted(expected_keys)
