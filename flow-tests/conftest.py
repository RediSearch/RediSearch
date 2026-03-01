"""
Pytest configuration for RediSearchDisk tests.

This module configures the RLTest environment for BDD tests:
1. Sets RLTest Defaults from environment variables (module path, redis server, env type)
2. Provides redis_env fixture that creates a Redis cluster with the module loaded
3. Imports step definitions from the steps package
"""
import os
import pytest
from pathlib import Path
from RLTest import Defaults, Env
from common import configure_search_cluster_single_shard

# Automatically discover and import all step definition modules
# This finds all *_steps.py files in the steps directory
steps_dir = Path(__file__).parent / 'steps'
pytest_plugins = [
    f'steps.{step_file.stem}'
    for step_file in steps_dir.glob('*_steps.py')
]


def pytest_configure(config):
    """
    Configure RLTest Defaults from environment variables.

    This is called once at the start of the test session.
    The runtests.py script sets these environment variables:
    - RLTEST_MODULE: Path to the redisearch.so module
    - RLTEST_REDIS_SERVER: Path to redis-server binary
    - RLTEST_REDIS_PORT: Base port for Redis instances (default: 6379)
    - RLTEST_ENV: Environment type (oss-cluster, oss, etc.)
    - RLTEST_LOG_DIR: Directory for test logs (default: ./logs)

    These defaults are used by Env() when creating Redis test environments.
    """
    # Register custom markers
    config.addinivalue_line(
        "markers", "module_args(args): pass custom module arguments to Redis"
    )
    # Set RLTest module path
    module_path = os.getenv('RLTEST_MODULE')
    if module_path:
        Defaults.module = module_path

    # Set Redis server path
    redis_server = os.getenv('RLTEST_REDIS_SERVER', 'redis-server')
    Defaults.redis_server = redis_server

    # Set Redis port (used as base port in cluster mode)
    redis_port = int(os.getenv('RLTEST_REDIS_PORT', '6379'))
    Defaults.port = redis_port

    # Set environment type (oss-cluster for Redis OSS cluster)
    env_type = os.getenv('RLTEST_ENV', 'oss-cluster')
    Defaults.env = env_type

    # Set log directory
    log_dir = os.getenv('RLTEST_LOG_DIR', './logs')
    Defaults.logdir = log_dir

    # Enable randomized ports to avoid port conflicts in cluster mode
    Defaults.randomize_ports = True

    # Make RLTest raise exceptions on assertion failures so pytest can detect them
    # Without this, RLTest only prints failures to stdout and pytest thinks tests passed
    Defaults.exit_on_failure = True


@pytest.fixture(scope='function')
def redis_env(request):
    """
    Create and manage RLTest environment for each test.

    This fixture:
    - Creates an isolated working directory for each test in logs/<test_name>/
    - Creates a Redis cluster using RLTest with the configured module
    - Verifies the environment started successfully
    - Yields the environment for use in tests
    - Verifies Redis is still running and healthy after the test completes
    - Cleans up (flushes) the environment after each test

    Each test gets its own subdirectory in the logs folder, ensuring complete
    isolation even when running tests in parallel.

    Custom module arguments can be passed using the @pytest.mark.module_args marker:

        @pytest.mark.module_args('WORKERS 2 TIERED_HNSW_BUFFER_LIMIT 10')
        def test_with_custom_args(redis_env):
            ...

    Raises:
        Exception: If the Redis environment fails to start, is not healthy,
                   or crashes during the test execution
    """
    import tempfile
    import shutil
    from pathlib import Path

    # Get test name for better log file naming
    # Format: test_file::test_name or feature_name::scenario_name
    test_name = request.node.name

    # Sanitize test name for use as directory name (replace invalid chars)
    safe_test_name = test_name.replace('::', '_').replace('/', '_').replace('[', '_').replace(']', '_').replace(' ', '_')

    # Create test-specific directory inside the logs folder
    base_log_dir = os.getenv('RLTEST_LOG_DIR', './logs')
    test_log_dir = Path(base_log_dir) / safe_test_name

    # Clean up any existing directory from a previous run
    if test_log_dir.exists():
        shutil.rmtree(test_log_dir)

    # Create the test-specific log directory
    test_log_dir.mkdir(parents=True, exist_ok=True)

    # Create a temporary Redis config file with bigredis-enabled
    # Use a unique bigredis directory for each test to avoid RocksDB lock conflicts
    # when running tests in parallel. The directory is inside the test's log directory.
    bigredis_path = test_log_dir / "redis.big"
    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write('bigredis-enabled yes\n')
        f.write('bigredis-driver speedb\n')
        f.write(f'bigredis-path {bigredis_path}\n')
        f.write('loglevel debug\n')
        f.write('bigredis-use-async no\n') # Temporary, until async API is added to Speedb Rust crate.
        f.write('enable-debug-command yes\n') # Required for _FT.DEBUG commands
        f.write('enable-module-command yes\n') # Required for `MODULE ..` commands
        redis_config_file = f.name

    # Check for custom module arguments via @pytest.mark.module_args marker
    module_args_marker = request.node.get_closest_marker('module_args')
    module_args = module_args_marker.args[0] if module_args_marker else None

    env = None
    try:
        # Create RLTest environment with defaults set in pytest_configure
        # The Env() will use Defaults.module, Defaults.redis_server, and Defaults.env
        # Pass logDir=test_log_dir so Redis runs from the isolated test directory
        # This ensures each test has its own redisearch database directory
        # and prevents conflicts when running tests in parallel
        # RLTest will use test_log_dir as the cwd when spawning Redis processes
        env = Env(
            testName=test_name,
            redisConfigFile=redis_config_file,
            logDir=str(test_log_dir),
            decodeResponses=True,
            moduleArgs=module_args
        )

        # Verify the environment is up and healthy
        if not env.isUp():
            raise Exception("Redis environment failed to start. Check logs for port binding or other errors.")

        if not env.isHealthy():
            raise Exception("Redis environment is not healthy. Check logs for errors.")

        # Configure search cluster settings
        # Get the connection to execute commands
        conn = env.getConnection()

        configure_search_cluster_single_shard(conn, env.port)

        yield env

        # Verify Redis is still running and healthy after the test
        if not env.isUp():
            raise Exception(f"Redis crashed during test '{test_name}'. Check logs at {test_log_dir}")

        if not env.isHealthy():
            raise Exception(f"Redis became unhealthy during test '{test_name}'. Check logs at {test_log_dir}")

        # Verify we can still execute commands (additional sanity check)
        try:
            conn = env.getConnection()
            conn.execute_command('PING')
        except Exception as e:
            raise Exception(f"Redis is not responding after test '{test_name}': {e}. Check logs at {test_log_dir}")

        # Cleanup: flush all data after each test to ensure test isolation
        env.flush()
    finally:
        # Stop the environment to avoid orphaned Redis processes
        if env is not None:
            env.stop()

        # Clean up the temporary config file
        if os.path.exists(redis_config_file):
            os.unlink(redis_config_file)

        # Note: The test_log_dir (logs/<test_name>/) is left intact for debugging.
        # It contains Redis logs and the redisearch database directory.
        # The entire logs/ directory can be cleaned by running with --no-clear-logs=false
        # or by manually deleting it between test runs.


def pytest_bdd_step_error(request, feature, scenario, step, step_func, step_func_args, exception):
    """Hook to provide better error messages for BDD steps."""
    print(f"\nStep failed: {step.keyword} {step.name}")
    print(f"Feature: {feature.name}")
    print(f"Scenario: {scenario.name}")


@pytest.fixture(scope='function')
def conn(redis_env):
    """Get Redis connection from the environment."""
    return redis_env.getConnection()


@pytest.fixture(scope='function')
def new_hnsw_disk_index(conn):
    """
    Create a standard HNSW disk index for testing.

    Creates index 'idx' with field 'vec' using common test parameters:
    - DIM=4, M=16, EF_CONSTRUCTION=200, EF_RUNTIME=10, RERANK (flag)
    - DISTANCE_METRIC=L2

    Returns the connection for further commands.
    """
    result = conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )
    assert result == 'OK', f"FT.CREATE failed: {result}"
    return conn
