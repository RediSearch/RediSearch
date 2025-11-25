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

def _normalize_arguments_structure(args):
    """
    Normalize actual arguments structure:
    - Convert flags array to boolean fields
    - Recursively process nested arguments
    """
    if not isinstance(args, list):
        return args

    normalized = []
    for arg in args:
        if not isinstance(arg, dict):
            normalized.append(arg)
            continue

        # Create a copy of the argument
        normalized_arg = {}

        for key, value in arg.items():
            # Handle flags array - convert to boolean fields
            if key == 'flags':
                if isinstance(value, list):
                    for flag in value:
                        # Convert flags like 'optional', 'multiple' to boolean fields
                        if flag in ['optional', 'multiple']:
                            normalized_arg[flag] = True
                # If flags is not a list or empty, skip it
                continue

            # Recursively normalize nested arguments
            if key == 'arguments' and isinstance(value, list):
                normalized_arg[key] = _normalize_arguments_structure(value)
            else:
                normalized_arg[key] = value

        normalized.append(normalized_arg)

    return normalized

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


def _is_dict_included(actual, expected):
    """
    Check if expected dictionary structure is included in actual (lenient comparison).
    Recursively compares nested dictionaries and lists.

    Lenient means: actual must contain all keys/values from expected, but can have extra fields.

    Args:
        actual: The actual data structure
        expected: The expected data structure to check for

    Returns:
        bool: True if expected is included in actual, False otherwise
    """
    # Type mismatch - not included
    if type(actual) != type(expected):
        return False

    # Handle dictionaries
    if isinstance(expected, dict):
        # Check if this is a function->block transformation case
        # When type is 'block' in actual but 'function' in expected, some fields may be omitted
        # (Functions with arguments are transformed to blocks, and the function token is omitted)
        is_function_to_block = False
        if 'type' in expected and 'type' in actual:
            if actual['type'] == 'block' and expected['type'] == 'function':
                is_function_to_block = True

        # Check that all expected keys exist in actual
        for key, expected_value in expected.items():
            # Special handling for function->block transformation
            if is_function_to_block:
                # When transforming function to block, the token field is omitted
                if key == 'token' and key not in actual:
                    continue  # Skip token field check - it's expected to be missing
                # When transforming function to block, arguments may be empty or missing
                if key == 'arguments':
                    # If actual has empty arguments or arguments is missing, accept it
                    if key not in actual:
                        continue  # Skip arguments check - missing is acceptable for block type
                    if isinstance(actual[key], list) and len(actual[key]) == 0:
                        continue  # Skip arguments check - empty is acceptable for block type

            if key not in actual:
                return False

            # Special case: type field - allow 'block' in actual to match 'function' in expected
            if key == 'type':
                if actual[key] == expected_value or (actual[key] == 'block' and expected_value == 'function'):
                    continue  # Type matches (with function->block transformation)
                else:
                    return False
            else:
                # Recursively check nested structures
                if not _is_dict_included(actual[key], expected_value):
                    return False
        return True

    # Handle lists - compare element by element in order
    elif isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        for i in range(len(expected)):
            if not _is_dict_included(actual[i], expected[i]):
                return False
        return True

    # Handle primitive types - direct comparison
    else:
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

def test_command_info_availability():
    env = Env(protocol=3)
    """Test that command info is available for all RediSearch commands using generated expectations."""

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

            # Validate expected fields (dynamically extract field names from expectations)
            expected_fields =  list(expected.keys())

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



def test_command_info_tips_field():
    """Test that commands with tips have the correct tips field in command info."""
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

def test_specific_command_docs_structure():
    """Test the structure of command info for specific well-known commands."""
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
