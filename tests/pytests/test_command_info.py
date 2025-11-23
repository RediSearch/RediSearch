from common import *
import json
import os

def load_command_expectations():
    """Load and parse the command info expectations file."""
    expectations_path = os.path.join(os.path.dirname(__file__), 'command_info_expectations.json')
    try:
        with open(expectations_path, 'r') as f:
            expectations_data = json.load(f)
    except FileNotFoundError:
        raise Exception(f"Missing command_info_expectations.json. Run 'make build' to generate test expectations.")
    except Exception as e:
        raise Exception(f"Failed to load command_info_expectations.json: {e}. Run 'make build' to regenerate.")

    # Extract commands from the generated structure
    if '_commands' not in expectations_data:
        raise Exception(f"Invalid expectations file format. Run 'make build' to regenerate.")

    return expectations_data['_commands']

def test_command_info_availability(env):
    """Test that command info is available for all RediSearch commands using generated expectations."""

    # Load expectations
    try:
        expectations = load_command_expectations()
    except Exception as e:
        env.fail(str(e))
    env.assertEqual(len(expectations) > 0, True, message="Should have command expectations")

    # Get Redis connection
    conn = env.getConnection()

    success_count = 0
    failed_commands = []

    for cmd_name, expected in expectations.items():
        cmd_upper = cmd_name.upper()

        try:
            # Get command info
            info = conn.execute_command("COMMAND", "INFO", cmd_upper)
            if not info or not isinstance(info, dict) or cmd_upper not in info:
                failed_commands.append(f"{cmd_name}: No command info returned")
                continue


            # Track failures for this specific command
            initial_failure_count = len(failed_commands)

            # Validate expected fields (dynamically extract field names from expectations)
            expected_fields = [field for field in expected.keys()
                             if field not in ['name', 'has_info', 'tips']]  # Exclude special fields

            if 'complexity' in expected_fields:
                expected_fields.remove('complexity')


            for field in expected_fields:
                if field not in info:
                    failed_commands.append(f"{cmd_name}: Missing {field} field")
                elif info[field] != expected[field]:
                    failed_commands.append(f"{cmd_name}: {field} mismatch - expected '{expected[field]}', got '{info[field]}'")

            # Check tips if expected
            if 'tips' in expected:
                if 'tips' not in info:
                    failed_commands.append(f"{cmd_name}: Missing tips field")
                elif info['tips'] != expected['tips']:
                    failed_commands.append(f"{cmd_name}: Tips mismatch - expected '{expected['tips']}', got '{info['tips']}'")

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



def test_command_info_tips_field(env):
    """Test that commands with tips have the correct tips field in command info."""

    # Load expectations to find commands with tips
    try:
        expectations = load_command_expectations()
    except Exception as e:
        env.fail(str(e))

    # Find commands that have tips defined
    commands_with_tips = {cmd: data for cmd, data in expectations.items() if 'tips' in data}

    env.assertEqual(len(commands_with_tips) > 0, True, message="Should have at least some commands with tips")

    conn = env.getConnection()

    failed_tips = []

    for cmd_name, expected_data in commands_with_tips.items():
        cmd_upper = cmd_name.upper()
        expected_tips = expected_data['tips']

        try:
            # Get command info
            info = conn.execute_command("COMMAND", "INFO", cmd_upper)
            if not info or len(info) == 0 or info[0] is None:
                failed_tips.append(f"{cmd_name}: No command info returned")
                continue

            cmd_info = info[0]

            # Command info is returned as a list of key-value pairs
            # Convert to dict for easier access
            info_dict = to_dict(cmd_info)

            # Check if tips field exists and matches
            if 'tips' not in info_dict:
                failed_tips.append(f"{cmd_name}: Missing tips field")
            elif info_dict['tips'] != expected_tips:
                failed_tips.append(f"{cmd_name}: Tips mismatch - expected '{expected_tips}', got '{info_dict['tips']}'")
            else:
                env.debugPrint(f"✓ {cmd_name}: tips = '{info_dict['tips']}'")

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

def test_specific_command_info_structure(env):
    """Test the structure of command info for specific well-known commands."""

    conn = env.getConnection()

    # Test FT.CREATE command info
    info = conn.execute_command("COMMAND", "INFO", "FT.CREATE")
    env.assertEqual(info is not None, True, message="FT.CREATE should have command info")
    env.assertEqual(len(info) > 0, True, message="FT.CREATE should have non-empty info")

    cmd_info = info[0]
    env.assertEqual(cmd_info is not None, True, message="FT.CREATE should have command info structure")

    # Convert to dict
    info_dict = to_dict(cmd_info)

    # Check required fields
    env.assertEqual('summary' in info_dict, True, message="FT.CREATE should have summary")
    env.assertEqual('complexity' in info_dict, True, message="FT.CREATE should have complexity")
    env.assertEqual('since' in info_dict, True, message="FT.CREATE should have since field")
    env.assertEqual('arity' in info_dict, True, message="FT.CREATE should have arity")

    # Check that summary is meaningful
    summary = info_dict['summary']
    env.assertEqual(isinstance(summary, str), True, message="Summary should be a string")
    env.assertEqual(len(summary) > 10, True, message="Summary should be descriptive")
    env.assertEqual('index' in summary.lower(), True, message="FT.CREATE summary should mention index")

    env.debugPrint(f"FT.CREATE summary: {summary}")
    env.debugPrint(f"FT.CREATE complexity: {info_dict.get('complexity', 'N/A')}")
    env.debugPrint(f"FT.CREATE since: {info_dict.get('since', 'N/A')}")
