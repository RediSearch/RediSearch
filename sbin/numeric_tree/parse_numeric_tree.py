#!/usr/bin/python3
import json
import sys

# Global variables for optimized parsing
_lines = []
_line_index = 0
_total_lines = 0
_progress_counter = 0
_enable_assertions = True

def set_assertion_mode(enabled):
    """Enable or disable assertions for performance"""
    global _enable_assertions
    _enable_assertions = enabled

def report_progress(msg):
    """Report progress less frequently to reduce I/O overhead"""
    global _progress_counter
    _progress_counter += 1
    if _progress_counter % 1000 == 0:  # Report every 1000 lines instead of every line
        remaining = _total_lines - _line_index
        print(f"\r{msg} (remaining: {remaining})", end='', flush=True)

def next_line():
    """Optimized line reading using index instead of pop(0)"""
    global _line_index, _lines
    if _line_index >= len(_lines):
        raise IndexError("No more lines to read")

    line = _lines[_line_index].rstrip()  # Use rstrip() instead of strip() for better performance
    _line_index += 1

    if _line_index % 1000 == 0:  # Report progress less frequently
        report_progress(f"Processing line {_line_index}")

    return line

def assert_line_equals(expected):
    """Optimized assertion with optional disabling"""
    if _enable_assertions:
        line = next_line()
        if line != expected:
            raise AssertionError(f"Expected '{expected}', got '{line}' at line {_line_index}")
    else:
        next_line()  # Just consume the line without checking

def assert_line_starts_with(expected):
    """Optimized assertion for lines starting with a specific string"""
    if _enable_assertions:
        line = next_line()
        if not line.startswith(expected):
            raise AssertionError(f"Expected line to start with '{expected}', got '{line}' at line {_line_index}")
    else:
        next_line()  # Just consume the line without checking

def next_int():
    """Optimized integer parsing"""
    return int(next_line())

def next_float():
    """Optimized float parsing"""
    return float(next_line())

def parse_leaf(node):
    """Optimized leaf parsing with reduced function calls"""
    node['leaf'] = True
    # Parse leaf data with optimized assertions
    node['min_val'] = next_float()
    assert_line_equals('maxVal')
    node['max_val'] = next_float()
    assert_line_equals('unique_sum')
    node['unique_sum'] = next_float()
    assert_line_starts_with('invertedIndexSize')
    node['inverted_index_size'] = next_int()
    assert_line_equals('card')
    node['card'] = next_int()
    assert_line_equals('cardCheck')
    node['card_check'] = next_int()
    assert_line_equals('splitCard')
    node['split_cardinality'] = next_int()
    assert_line_equals('entries')
    assert_line_equals('numDocs')
    node['num_docs'] = next_int()
    line = next_line()
    old_node = False
    if line.startswith('numEntries'):
        node['num_entries'] = next_int()
        old_node = True
    assert_line_equals('lastId')
    node['last_id'] = next_int()
    assert_line_equals('size')
    node['size'] = next_int()
    line = next_line()
    if line.startswith('blocks_efficiency'):
        assert old_node
        node['blocks_efficiency'] = next_float()
        line = next_line()

    assert line == 'values'
    # Optimized values parsing
    node['values'] = []
    last_doc_id = node['last_id']
    while _line_index < _total_lines:
        assert_line_equals('value')
        number = next_int()
        assert_line_equals('docId')
        doc_id = next_int()
        node['values'].append((number, doc_id))
        if doc_id == last_doc_id:
            break

    node['doc_count'] = len(node['values'])
    
    if not old_node:
        assert_line_equals('left')
        assert_line_equals('')
        assert_line_equals('right')
        assert_line_equals('')

def parse_old_node(parent_id=None, node_id=0):
    """Optimized node parsing without passing lines around"""
    node = {}
    node['id'] = node_id
    if parent_id is not None:
        node['parent_id'] = parent_id

    value_or_range = next_line()
    if value_or_range == 'range':
        assert_line_equals('minVal')
        parse_leaf(node)
        return node, node_id + 1

    # Parse node header
    assert value_or_range == 'value'
    node['value'] = next_float()
    assert_line_equals('maxDepth')
    node['max_depth'] = next_int()
    next_node_id = node_id + 1
    assert_line_equals('left')
    left, next_node_id = parse_old_node(node_id, next_node_id)
    assert_line_equals('right')
    right, next_node_id = parse_old_node(node_id, next_node_id)
    node['left'] = left
    node['right'] = right
    node['doc_count'] = left['doc_count'] + right['doc_count']
    return node, next_node_id + 1


def parse_node(parent_id=None, node_id=0):
    """Optimized node parsing without passing lines around"""
    node = {}

    # Parse node header
    assert_line_equals('value')
    node['value'] = next_float()
    node['id'] = node_id
    if parent_id is not None:
        node['parent_id'] = parent_id

    assert_line_equals('maxDepth')
    node['max_depth'] = next_int()
    assert_line_equals('range')

    empty_or_minVal_line = next_line()
    next_node_id = node_id + 1

    if empty_or_minVal_line == 'minVal':
        # This is a leaf node
        if _enable_assertions and node['max_depth'] != 0:
            raise AssertionError(f"Leaf node should have max_depth=0, got {node['max_depth']}")
        parse_leaf(node)
    else:
        # This is an internal node with children
        if _enable_assertions and len(empty_or_minVal_line) != 0:
            raise AssertionError(f"Expected empty line, got '{empty_or_minVal_line}'")

        assert_line_equals('left')
        left, next_node_id = parse_node(node_id, next_node_id)
        assert_line_equals('right')
        right, next_node_id = parse_node(node_id, next_node_id)
        node['left'] = left
        node['right'] = right
        node['doc_count'] = left['doc_count'] + right['doc_count']
    return node, next_node_id


def parse_tree_file(file_path):
    """Optimized tree file parsing with global line management"""
    global _lines, _line_index, _total_lines, _progress_counter

    # Read and preprocess all lines at once
    with open(file_path, 'r') as f:
        _lines = [line.rstrip() for line in f.readlines()]  # Pre-strip all lines

    _line_index = 0
    _total_lines = len(_lines)
    _progress_counter = 0

    print(f"Parsing file with {_total_lines} lines...")

    # Parse tree metadata
    tree = {}
    while _line_index < _total_lines and _lines[_line_index] != 'root':
        line = next_line()
        if line == 'root':
            break
        tree[line] = next_int()
    print("Tree metadata:", tree)

    # Skip the 'root' line
    next_line()

    found_range = False
    cur_line = _line_index
    while cur_line < _total_lines and _lines[cur_line] != 'left':
        if _lines[cur_line] == 'range':
            found_range = True
            break
        cur_line += 1

    # Parse the tree structure
    if found_range:
        root, _ = parse_node(None, 0)
    else:
        root, _ = parse_old_node(None, 0)

    print(f"\nParsing completed. Processed {_line_index} lines.")
    return root


if __name__ == "__main__":
    import sys
    import time

    # Parse command line arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'dump_numidxtree.txt'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'tree.json'

    # Check for performance mode flag
    if '--fast' in sys.argv:
        set_assertion_mode(False)
        print("Running in fast mode (assertions disabled)")

    try:
        start_time = time.time()
        tree = parse_tree_file(input_file)
        parse_time = time.time() - start_time

        print(f"Parsing took {parse_time:.2f} seconds")

        # Write output with better error handling
        start_time = time.time()
        with open(output_file, 'w') as f:
            json.dump(tree, f, indent=2)
        write_time = time.time() - start_time

        print(f"Writing JSON took {write_time:.2f} seconds")
        print(f"Output written to {output_file}")

    except Exception as e:
        import traceback
        print(f"\nError: {e}")
        traceback.print_exc()
        sys.exit(1)