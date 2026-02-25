"""
Step definitions for RediSearchDisk basic functionality tests.
"""
from common import *

# Given steps
@given('the RediSearchDisk module is loaded')
def redis_module_loaded(redis_env):
    """Verify the RediSearchDisk module is loaded in the RLTest environment."""
    # Module loading is verified by RLTest framework
    # The redis_env fixture ensures the module is loaded
    assert redis_env is not None


# When steps
@given(parsers.parse('I create an index "{index_name}" with schema field "{field_name}" as {field_type}'))
@when(parsers.parse('I create an index "{index_name}" with schema field "{field_name}" as {field_type}'))
def create_index_single_field(redis_env, index_name, field_name, field_type):
    """Create an index with a single field."""
    redis_env.expect('FT.CREATE', index_name, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', field_name, field_type).ok()


@given(parsers.parse('I create an index "{index_name}" with schema field "{field_name}" as {field_type} NOSTEM'))
@when(parsers.parse('I create an index "{index_name}" with schema field "{field_name}" as {field_type} NOSTEM'))
def create_index_single_field_nostem(redis_env, index_name, field_name, field_type):
    """Create an index with a single field with NOSTEM (no stemming)."""
    redis_env.expect('FT.CREATE', index_name, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA', field_name, field_type, 'NOSTEM').ok()


# Usage example:
#    When I create an index "idx" with fields:
#      | field    | type |
#      | title    | TEXT |
#      | category | TAG  |
# the datatable variable will be a list of lists: [['field', 'type'], ['title', 'TEXT'], ...]
# bdd framework will pass the datatable as a list of lists automatically
@when(parsers.parse('I create an index "{index_name}" with fields:'))
def create_index_multiple_fields(redis_env, index_name, datatable):
    """Create an index with multiple fields from a data table."""
    schema_args = ['FT.CREATE', index_name, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA']

    # datatable is a list of lists: [['field', 'type'], ['title', 'TEXT'], ...]
    # Skip the header row
    for row in datatable[1:]:
        schema_args.extend([row[0], row[1]])

    redis_env.expect(*schema_args).ok()


@when(parsers.parse('I add a document "{doc_id}" with field "{field_name}" set to "{field_value}"'))
def add_document_single_field(redis_env, doc_id, field_name, field_value):
    """Add a document with a single field."""
    redis_env.expect('HSET', doc_id, field_name, field_value).equal(1)


# Usage example:
#    Given I add documents to index "idx":
#      | id   | field   | value                  |
#      | doc1 | content | hello world            |
#      | doc2 | content | hello world again      |
#      | doc3 | content | hello world again more |
@given(parsers.parse('I add documents to index "{index_name}":'))
@when(parsers.parse('I add documents to index "{index_name}":'))
def add_documents_from_table(redis_env, index_name, datatable):
    """Add multiple documents from a data table.

    The table must have columns: id, field, value
    Multiple rows with the same id will set multiple fields on the same document.
    """
    # datatable is a list of lists: [['id', 'field', 'value'], ['doc1', 'content', 'hello'], ...]
    # First row is the header
    headers = datatable[0]
    id_idx = headers.index('id')
    field_idx = headers.index('field')
    value_idx = headers.index('value')

    for row in datatable[1:]:  # Skip header row
        doc_id = row[id_idx]
        field_name = row[field_idx]
        field_value = row[value_idx]
        # Use cmd instead of expect to allow for both new docs (returns 1) and updates (returns 0)
        redis_env.cmd('HSET', doc_id, field_name, field_value)


# Usage example:
#    When I add a document "doc1" with fields:
#      | field    | value       |
#      | title    | hello world |
#      | category | electronics |
# the datatable variable will be a list of lists: [['field', 'value'], ['title', 'hello'], ...]
@when(parsers.parse('I add a document "{doc_id}" with fields:'))
def add_document_multiple_fields(redis_env, doc_id, datatable):
    """Add a document with multiple fields from a data table."""
    hset_args = ['HSET', doc_id]

    # datatable is a list of lists: [['field', 'value'], ['title', 'hello'], ...]
    # Skip the header row
    for row in datatable[1:]:
        hset_args.extend([row[0], row[1]])

    expected_count = len(datatable) - 1  # Subtract header row
    redis_env.expect(*hset_args).equal(expected_count)


@when(parsers.parse('I search the index "{index_name}" for "{query}"'))
def search_index(redis_env, request, index_name, query):
    """Search an index with a query."""
    result = redis_env.cmd('FT.SEARCH', index_name, query)
    # Store result in request node for access by then steps
    request.node.search_result = result


@when(parsers.parse('I search the index "{index_name}" for "{query}" with numeric filter "{field}" from {min_val:d} to {max_val:d}'))
def search_with_numeric_filter(redis_env, request, index_name, query, field, min_val, max_val):
    """Search with a numeric filter."""
    result = redis_env.cmd('FT.SEARCH', index_name, query, 'FILTER', field, str(min_val), str(max_val))
    request.node.search_result = result


@when(parsers.parse('I search the index "{index_name}" for tag "{field}" with value "{value}"'))
def search_with_tag(redis_env, request, index_name, field, value):
    """Search with a tag filter."""
    query = f'@{field}:{{{value}}}'
    result = redis_env.cmd('FT.SEARCH', index_name, query)
    request.node.search_result = result


@when(parsers.parse('I delete the document "{doc_id}"'))
def delete_document(redis_env, doc_id):
    """Delete a document."""
    redis_env.expect('DEL', doc_id).equal(1)


@when(parsers.parse('I update document "{doc_id}" with field "{field_name}" set to "{field_value}"'))
def update_document(redis_env, doc_id, field_name, field_value):
    """Update a document field."""
    redis_env.expect('HSET', doc_id, field_name, field_value).equal(0)


# Then steps
@then('the environment should be available')
def environment_available(redis_env):
    """Verify the environment is available."""
    assert redis_env is not None


@then(parsers.parse('I should get {count:d} result'))
@then(parsers.parse('I should get {count:d} results'))
def verify_result_count(request, count):
    """Verify the number of search results."""
    result = request.node.search_result
    assert result is not None
    assert result[0] == count, f"Expected {count} results, got {result[0]}, result: {result}"


@then(parsers.parse('the only result should be "{doc_id}"'))
def verify_first_result(request, doc_id):
    """Verify the first search result document ID."""
    result = request.node.search_result
    assert result is not None
    # we only expect one result
    assert result[0] == 1
    # Handle both bytes and strings
    result_id = result[1].decode('utf-8') if isinstance(result[1], bytes) else result[1]
    assert result_id == doc_id, f"Expected first result to be '{doc_id}', got '{result_id}'"


def _decode(val):
    """Decode bytes to string if needed."""
    return val.decode('utf-8') if isinstance(val, bytes) else val


def _parse_search_results(result):
    """Parse FT.SEARCH results into a dict.

    Usage:
        result = [2, b'doc1', [b'title', b'hello'], b'doc2', [b'title', b'world']]
        _parse_search_results(result)
        # => {'doc1': {'title': 'hello'}, 'doc2': {'title': 'world'}}
    """
    docs = {}
    for i in range(1, len(result), 2):
        field_list = result[i + 1] if i + 1 < len(result) else []
        fields = {_decode(field_list[j]): _decode(field_list[j + 1]) for j in range(0, len(field_list), 2)}
        docs[_decode(result[i])] = fields
    return docs


def _verify_docs_in_results(actual, datatable, check_exact=False):
    """Verify expected docs are in results.

    Usage:
        actual = {'doc1': {'title': 'hello'}, 'doc2': {'title': 'world'}}
        datatable = [['doc_id', 'title'], ['doc1', 'hello']]
        _verify_docs_in_results(actual, datatable)  # passes, doc2 ignored
        _verify_docs_in_results(actual, datatable, check_exact=True)  # fails, doc2 unexpected
    """
    headers = datatable[0]
    expected = {row[headers.index('doc_id')]: dict(zip(headers, row)) for row in datatable[1:]}

    if check_exact:
        assert set(actual.keys()) == set(expected.keys()), \
            f"Expected exactly {list(expected.keys())}, got {list(actual.keys())}"

    for doc_id, expected_fields in expected.items():
        assert doc_id in actual, f"Expected '{doc_id}' in results, got {list(actual.keys())}"
        for field, value in expected_fields.items():
            if field != 'doc_id':
                assert actual[doc_id].get(field) == value, \
                    f"Expected '{field}'='{value}' for '{doc_id}', got '{actual[doc_id].get(field)}'"


@then('the results should contain:')
def verify_results_contain_docs(request, datatable):
    """Verify the search results contain specific documents (others may exist).

    Usage:
        Then the results should contain:
          | doc_id |
          | doc1   |

        Then the results should contain:
          | doc_id | title |
          | doc1   | hello |
    """
    result = request.node.search_result
    assert result is not None
    actual = _parse_search_results(result)
    _verify_docs_in_results(actual, datatable, check_exact=False)


@then('the results should only contain:')
def verify_results_only_contain_docs(request, datatable):
    """Verify the search results contain exactly the specified documents (no extras).

    Usage:
        Then the results should only contain:
          | doc_id |
          | doc1   |
          | doc2   |

        Then the results should only contain:
          | doc_id | title |
          | doc1   | hello |
          | doc2   | world |
    """
    result = request.node.search_result
    assert result is not None
    actual = _parse_search_results(result)
    _verify_docs_in_results(actual, datatable, check_exact=True)


@then('the results should be empty')
def verify_results_empty(request):
    """Verify the search returned no results."""
    result = request.node.search_result
    assert result is not None
    assert result[0] == 0, f"Expected 0 results, got {result[0]}, result: {result}"


@then(parsers.parse('the index "{index_name}" should exist'))
def verify_index_exists(redis_env, index_name):
    """Verify that an index exists after RDB load."""
    # Use FT.INFO to check if index exists
    # If index doesn't exist, this will return an error
    result = redis_env.cmd('FT.INFO', index_name)
    assert result is not None, f"Index {index_name} should exist after RDB load"
