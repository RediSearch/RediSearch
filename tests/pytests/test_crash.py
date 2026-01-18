import RLTest
from common import *


class CrashingEnv(RLTest.Env):
    def getEnvByName(self):
        env = super().getEnvByName()
        env._stopProcess = self.passStopProcess
        return env

    # stopping the process checks if the process crashed and output the crash message
    # since we are testing the crash itself, the process already crashed and we want to avoid the crash error message
    def passStopProcess(self, *args, **kwargs):
        pass


def extract_query_crash_output(env, expected_fragments, crash_in_rust=False):
    """
    Extract values for each fragment from the crash log, checking they appear in order.

    Args:
        env: Test environment
        expected_fragments: List of field names/fragments to extract in order (e.g., "search_num_docs:")
        crash_in_rust: Whether to crash in Rust code

    Returns:
        Dictionary mapping fragment to extracted value (or None if not found)
        Fragments must appear in the order specified in expected_fragments
    """
    logDir = env.cmd("config", "get", "dir")[1]
    logFileName = env.cmd("CONFIG", "GET", "logfile")[1]
    logFilePath = os.path.join(logDir, logFileName)
    env.cmd("FT.CREATE", "idx", "SCHEMA", "text", "TEXT")
    runDebugQueryCommandAndCrash(
        env, ["FT.SEARCH", "idx", "*"], crash_in_rust=crash_in_rust
    )

    # Initialize result dictionary with None for all fragments
    results = {fragment: None for fragment in expected_fragments}
    pos = 0  # Track position in expected_fragments to enforce ordering

    with open(logFilePath) as logFile:
        for line in logFile:
            # Only look for the next expected fragment (enforces ordering)
            if pos < len(expected_fragments):
                fragment = expected_fragments[pos]
                if fragment in line:
                    # Extract the value after the fragment
                    idx = line.find(fragment)
                    if idx != -1:
                        # Get the part after the fragment
                        value_part = line[idx + len(fragment):].strip()
                        # If there's a value after the colon, extract it
                        if value_part:
                            # Remove trailing comments or newlines
                            value_part = value_part.split('#')[0].strip()
                            results[fragment] = value_part if value_part else line.strip()
                        else:
                            # Just mark that we found the fragment (e.g., section headers)
                            results[fragment] = line.strip()
                    # Move to next fragment
                    pos += 1

    return results


# we expect to see out index information about the crash in the log file
@skip(cluster=True)
def test_query_thread_crash():
    env = CrashingEnv(testName="test_query_thread_crash", freshEnv=True)

    results = extract_query_crash_output(env, [
        "search_current_thread",
        # Index name is now a section header, not a field
        "search_idx",
        # Fields are now in nested dictionaries
        "search_number_of_docs:",
        "search_index_properties",
        "search_max_doc_id:",
        "search_num_terms:",
        "search_index_properties_in_mb",
        "search_inverted_size:",
        "search_total_inverted_index_blocks:",
        "search_index_failures",
        "search_indexing:",
        "search_run_time_ns:",
    ])

    # Verify all fragments were found
    for fragment, value in results.items():
        env.assertIsNotNone(value, f"Fragment '{fragment}' not found in crash log")

    # Verify specific values
    # Empty index should have 0 documents
    env.assertEqual(results["search_number_of_docs:"], "0")
    env.assertEqual(results["search_max_doc_id:"], "0")
    env.assertEqual(results["search_num_terms:"], "0")

    # Inverted size should be 0.0 for empty index
    env.assertEqual(results["search_inverted_size:"], "0.000000")

    # Total inverted index blocks should be >= 0
    blocks = int(results["search_total_inverted_index_blocks:"])
    env.assertGreaterEqual(blocks, 0)

    # Indexing should be 0 (not currently indexing)
    env.assertEqual(results["search_indexing:"], "0")

    # Run time should be > 0 (some time elapsed)
    run_time = int(results["search_run_time_ns:"])
    env.assertGreater(run_time, 0)
