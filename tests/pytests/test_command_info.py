from common import *
import json
import os

def _load_command_expectations():
    commands_json_path = os.path.join(os.path.dirname(__file__), '..', '..', 'commands.json')
    """Load and parse the command info expectations file."""
    try:
        with open(commands_json_path, 'r') as f:
            commands_data = json.load(f)
    except FileNotFoundError:
        raise Exception(f"Missing commands.json.")
    except Exception as e:
        raise Exception(f"Failed to load commands.json: {e}.")

    return commands_data

def _lists_equal_insensitive(a, b):
    """
    Compare two lists of strings case-insensitively and order-insensitively.
    Returns True if they contain the same items (ignoring case and order).
    """
    return set(map(str.lower, a)) == set(map(str.lower, b))

def _convert_flags_to_boolean_fields(flags_list):
    """
    Convert a flags array to boolean fields in a dictionary.

    Args:
        flags_list: List of flag strings (e.g., ['optional', 'multiple'])

    Returns:
        dict: Dictionary with boolean fields (e.g., {'optional': True, 'multiple': True})
    """
    if not isinstance(flags_list, list):
        return {}

    boolean_fields = {}
    for flag in flags_list:
        if flag in ['optional', 'multiple']:
            boolean_fields[flag] = True
    return boolean_fields

def _normalize_single_argument(arg):
    """
    Normalize a single argument dictionary.
    - Convert flags array to boolean fields
    - Recursively normalize nested arguments

    Args:
        arg: Single argument dictionary or non-dict value

    Returns:
        Normalized argument structure
    """
    if not isinstance(arg, dict):
        return arg

    normalized = {}

    for key, value in arg.items():
        if key == 'flags':
            # Convert flags array to boolean fields
            boolean_fields = _convert_flags_to_boolean_fields(value)
            normalized.update(boolean_fields)
        elif key == 'arguments' and isinstance(value, list):
            # Recursively normalize nested arguments
            normalized[key] = _normalize_arguments_structure(value)
        else:
            normalized[key] = value

    return normalized

def _normalize_arguments_structure(args):
    """
    Normalize arguments structure by converting flags and processing nested arguments.

    Args:
        args: List of arguments or non-list value

    Returns:
        Normalized arguments structure
    """
    if not isinstance(args, list):
        return args

    return [_normalize_single_argument(arg) for arg in args]

def _strip_fields(data, fields_to_strip):
    """
    Recursively remove specified fields from dictionary/list structure.

    Args:
        data: Dictionary or list to strip fields from
        fields_to_strip: List of field names to remove

    Returns:
        Cleaned data structure with specified fields removed
    """
    if isinstance(data, dict):
        stripped = {}
        for key, value in data.items():
            if key not in fields_to_strip:
                stripped[key] = _strip_fields(value, fields_to_strip)
        return stripped
    elif isinstance(data, list):
        return [_strip_fields(item, fields_to_strip) for item in data]
    else:
        return data

def _is_function_to_block_transformation(actual, expected):
    """
    Check if this is a function->block transformation case.

    When type is 'block' in actual but 'function' in expected, some fields may be omitted.
    Functions with arguments are transformed to blocks, and the function token is omitted.

    Args:
        actual: The actual data structure
        expected: The expected data structure

    Returns:
        bool: True if this is a function->block transformation
    """
    if not isinstance(actual, dict) or not isinstance(expected, dict):
        return False

    return (actual.get('type') == 'block' and
            expected.get('type') == 'function')

def _should_skip_field_in_comparison(key, actual, is_function_to_block):
    """
    Determine if a field should be skipped during comparison.

    IMPORTANT: Token field is ONLY skipped for function->block transformations.
    For all other cases, token field must be present and match.

    Args:
        key: The field name to check
        actual: The actual data structure
        is_function_to_block: Whether this is a function->block transformation

    Returns:
        bool: True if field should be skipped
    """
    # Only skip fields in function->block transformation cases
    if not is_function_to_block:
        return False

    # Token field is omitted ONLY in function->block transformation
    # For all other argument types, token must be present
    if key == 'token' and key not in actual:
        return True

    # Arguments may be empty or missing in function->block transformation
    if key == 'arguments':
        if key not in actual:
            return True
        if isinstance(actual[key], list) and len(actual[key]) == 0:
            return True

    return False

def _compare_type_field(actual_type, expected_type):
    """
    Compare type fields with support for function->block transformation.

    Args:
        actual_type: The actual type value
        expected_type: The expected type value

    Returns:
        bool: True if types match (including transformation cases)
    """
    if actual_type == expected_type:
        return True

    # Allow 'block' in actual to match 'function' in expected
    return actual_type == 'block' and expected_type == 'function'

def _dict_contains(actual, expected):
    """
    Check if actual dictionary contains all keys/values from expected.

    Lenient means: actual must contain all keys/values from expected,
    but can have extra fields.

    Note: Token field is only skipped for function->block transformations.
    For all other argument types, token must be present and match.

    Args:
        actual: The actual dictionary
        expected: The expected dictionary

    Returns:
        bool: True if actual contains all of expected
    """
    is_function_to_block = _is_function_to_block_transformation(actual, expected)

    # Check that all expected keys exist in actual
    for key, expected_value in expected.items():
        # Skip fields that are expected to be missing in transformations
        # (Only applies to function->block: token and empty arguments)
        if _should_skip_field_in_comparison(key, actual, is_function_to_block):
            continue

        if key not in actual:
            return False

        # Special handling for type field
        if key == 'type':
            if not _compare_type_field(actual[key], expected_value):
                return False
        else:
            # Recursively check nested structures
            if not _is_dict_included(actual[key], expected_value):
                return False

    return True

def _compare_lists(actual, expected):
    """
    Compare two lists element by element in order.

    Args:
        actual: The actual list
        expected: The expected list

    Returns:
        bool: True if lists match element by element
    """
    if len(actual) != len(expected):
        return False

    for i in range(len(expected)):
        if not _is_dict_included(actual[i], expected[i]):
            return False

    return True

def _is_dict_included(actual, expected):
    """
    Check if expected structure is included in actual (lenient comparison).
    Recursively compares nested dictionaries and lists.

    Lenient means: actual must contain all keys/values from expected,
    but can have extra fields.

    Args:
        actual: The actual data structure
        expected: The expected data structure to check for

    Returns:
        bool: True if expected is included in actual, False otherwise
    """
    # Type mismatch - not included
    if type(actual) != type(expected):
        return False

    # Route to appropriate comparison function
    if isinstance(expected, dict):
        return _dict_contains(actual, expected)
    elif isinstance(expected, list):
        return _compare_lists(actual, expected)
    else:
        # Primitive types - direct comparison
        return actual == expected

def compare_arguments(actual, expected, fields_to_strip=None):
    """
    Compare actual and expected command arguments with normalization and field stripping.

    Args:
        actual: The actual arguments from Redis COMMAND DOCS (may have flags array, display_text, etc.)
        expected: The expected arguments from commands.json (has boolean flags, no display_text)
        fields_to_strip: List of field names to remove from both actual and expected before comparison
                        (default: ['display_text'])

    Returns:
        bool: True if arguments match after normalization and stripping, False otherwise
    """
    if fields_to_strip is None:
        fields_to_strip = ['display_text']

    # Step 1: Normalize actual
    normalized_actual = _normalize_arguments_structure(actual)

    # Step 2: Strip fields from both
    stripped_actual = _strip_fields(normalized_actual, fields_to_strip)
    stripped_expected = _strip_fields(expected, fields_to_strip)

    # Step 3: Compare using recursive inclusion
    return _is_dict_included(stripped_actual, stripped_expected)

"""Test that command info is available for all RediSearch commands using generated expectations."""
def test_command_info_availability():
    env = Env(protocol=3)

    # Load expectations
    try:
        expectations = _load_command_expectations()
    except Exception as e:
        env.fail(str(e))
    env.assertEqual(len(expectations) > 0, True, message="Should have command expectations")

    # Get Redis connection
    conn = env.getConnection()

    success_count = 0
    failed_commands = []

    for cmd_name, expected in expectations.items():

        if env.isCluster() and cmd_name.startswith('FT.CONFIG'):
            #we only register this command if we are not in a cluster mode
            continue

        cmd_upper = cmd_name.upper().replace(' ', '|')

        try:
            # Get command info
            info = conn.execute_command(f"COMMAND DOCS {cmd_upper}")
            if not info or not isinstance(info, dict) or cmd_upper not in info:
                failed_commands.append(f"{cmd_name}: No command info returned")
                continue
            info = info[cmd_upper]

            # Track failures for this specific command
            initial_failure_count = len(failed_commands)

            shared_fields = expected.keys() & info.keys()
            if 'group' in shared_fields:
                shared_fields.remove('group')
            for field in shared_fields:
                # Use compare_arguments for arguments field, direct comparison for others
                if field == 'arguments':
                    if not compare_arguments(info[field], expected[field], ['display_text', 'expression', 'group', 'function']):
                        failed_commands.append(f"{cmd_name}: {field} mismatch")
                else:
                    if info[field] != expected[field]:
                        failed_commands.append(f"{cmd_name}: {field} mismatch - expected '{expected[field]}', got '{info[field]}'")

            # Only count as success if no failures were added for this command
            if len(failed_commands) == initial_failure_count:
                success_count += 1

        except Exception as e:
            failed_commands.append(f"{cmd_name}: Error getting command info: {e}")

    # Report results
    env.debugPrint(f"Successfully validated {success_count}/{len(expectations)} commands")

    if failed_commands:
        env.debugPrint("Failed commands:")
        for failure in failed_commands[:10]:  # Show first 10 failures
            env.debugPrint(f"  - {failure}")
        if len(failed_commands) > 10:
            env.debugPrint(f"  ... and {len(failed_commands) - 10} more failures")

    # Strict assertion - all commands should pass
    env.assertEqual(len(failed_commands), 0,
                   message=f"All commands should have correct info. {len(failed_commands)} failed: {failed_commands[:3]}")



"""Test that commands with tips have the correct tips field in command info."""
def test_command_info_tips_field():
    env = Env(protocol=3)
    # Load expectations to find commands with tips
    try:
        commands_json = _load_command_expectations()
    except Exception as e:
        env.fail(str(e))

    # Find commands that have tips defined
    commands_with_tips = {cmd: data for cmd, data in commands_json.items() if 'command_tips' in data}

    env.assertEqual(len(commands_with_tips) > 0, True, message="Should have at least some commands with tips")

    conn = env.getConnection()

    failed_tips = []

    for cmd_name, expected_data in commands_with_tips.items():
        cmd_upper = cmd_name.upper().replace(' ', '|')
        expected_tips = expected_data['command_tips']

        try:
            # Get command info
            info = conn.execute_command("COMMAND", "INFO", cmd_upper)
            if not info or not isinstance(info, dict) or cmd_upper not in info:
                failed_tips.append(f"{cmd_name}: No command info returned")
                continue

            info = info[cmd_upper]

            # Check if tips field exists and matches
            if 'tips' not in info:
                failed_tips.append(f"{cmd_name}: Missing tips field")
            elif not _lists_equal_insensitive(info['tips'], expected_tips):
                failed_tips.append(f"{cmd_name}: Tips mismatch - expected '{expected_tips}', got '{info['tips']}'")
            else:
                env.debugPrint(f"✓ {cmd_name}: tips = '{info['tips']}'")

        except Exception as e:
            failed_tips.append(f"{cmd_name}: Error testing tips: {e}")

    # Report results
    if failed_tips:
        env.debugPrint("Failed tips validation:")
        for failure in failed_tips:
            env.debugPrint(f"  - {failure}")

    # Strict assertion - all commands with tips should pass
    env.assertEqual(len(failed_tips), 0,
                   message=f"All commands with tips should have correct tips field. Failed: {failed_tips}")

"""Test the structure of command info for specific well-known commands."""
def test_specific_command_docs_structure():
    env = Env(protocol=3)
    conn = env.getConnection()

    # Test FT.CREATE command info
    docs = conn.execute_command("COMMAND DOCS FT.CREATE")
    env.assertEqual(docs is not None, True, message="FT.CREATE should have command docs")
    env.assertEqual(len(docs) > 0, True, message="FT.CREATE should have non-empty docs")

    cmd_docs = docs["FT.CREATE"]
    env.assertEqual(cmd_docs is not None, True, message="FT.CREATE should have command docs structure")

    # Check required fields
    env.assertEqual('summary' in cmd_docs, True, message="FT.CREATE should have summary")
    env.assertEqual('complexity' in cmd_docs, True, message="FT.CREATE should have complexity")
    env.assertEqual('since' in cmd_docs, True, message="FT.CREATE should have since field")


    # Check that summary is meaningful
    summary = cmd_docs['summary']
    env.assertEqual(isinstance(summary, str), True, message="Summary should be a string")
    env.assertEqual(len(summary) > 10, True, message="Summary should be descriptive")
    env.assertEqual('index' in summary.lower(), True, message="FT.CREATE summary should mention index")

    env.debugPrint(f"FT.CREATE summary: {summary}")
    env.debugPrint(f"FT.CREATE complexity: {cmd_docs.get('complexity', 'N/A')}")
    env.debugPrint(f"FT.CREATE since: {cmd_docs.get('since', 'N/A')}")
