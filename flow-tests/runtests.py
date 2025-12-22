#!/usr/bin/env python3
"""
RediSearchDisk Test Runner

This script runs integration tests for RediSearchDisk using RLTest.
It brings up a Redis Flex cluster with the module loaded and runs pytest tests.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def find_module(module_path: str | None) -> Path:
    """Find the RediSearchDisk module .so file and return its absolute path."""
    if module_path:
        module = Path(module_path)
        if not module.exists():
            print(f"Error: Module not found at: {module}", file=sys.stderr)
            sys.exit(1)
        # Resolve to absolute path before returning
        return module.resolve()

    # Try to find the module in the build directory
    script_dir = Path(__file__).parent
    root_dir = script_dir.parent
    default_module = root_dir / "build" / "redisearch.so"

    if default_module.exists():
        # Resolve to absolute path before returning
        return default_module.resolve()

    print("Error: Module not found. Please specify --module or build the module first.", file=sys.stderr)
    sys.exit(1)


def check_redis_server(redis_server: str) -> None:
    """Check if redis-server is available."""
    if not shutil.which(redis_server):
        print(f"Error: redis-server not found at: {redis_server}", file=sys.stderr)
        print("Please install Redis or set --redis-server", file=sys.stderr)
        sys.exit(1)


def check_python_deps(parallel: bool = False) -> None:
    """Check if required Python dependencies are available.

    Args:
        parallel: If True, also check for pytest-xdist (required for parallel testing)
    """
    missing_deps = []

    try:
        import RLTest  # noqa: F401
    except ImportError:
        missing_deps.append("RLTest")

    try:
        from pytest_bdd import given  # noqa: F401
    except ImportError:
        missing_deps.append("pytest-bdd")

    if parallel:
        try:
            from xdist import plugin  # noqa: F401
        except ImportError:
            missing_deps.append("pytest-xdist")

    if missing_deps:
        script_dir = Path(__file__).parent
        print(f"Error: Missing required dependencies: {', '.join(missing_deps)}", file=sys.stderr)
        print("\nPlease install dependencies first:", file=sys.stderr)
        print(f"  cd {script_dir.parent} && pip install RLTest pytest-bdd", file=sys.stderr)
        if parallel:
            print(f"  cd {script_dir.parent} && pip install pytest-xdist", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Run RediSearchDisk integration tests using RLTest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  ./runtests.py
  ./runtests.py --module ../build/redisearch.so
  ./runtests.py --test test_basic.py::test_create_index
  ./runtests.py --parallel
        """
    )
    
    parser.add_argument(
        "--module",
        default=os.environ.get("MODULE"),
        help="Path to RediSearchDisk module .so (default: build/redisearch.so or MODULE env var)"
    )
    parser.add_argument(
        "--redis-server",
        default=os.environ.get("REDIS_SERVER", "redis-server"),
        help="Location of redis-server (default: redis-server or REDIS_SERVER env var)"
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.environ.get("REDIS_PORT", "6379")),
        help="Redis server base port (default: 6379 or REDIS_PORT env var). In cluster mode, additional ports will be allocated automatically."
    )
    parser.add_argument(
        "--redis-lib-path",
        default=os.environ.get("REDIS_LIB_PATH"),
        help="Path to directory containing bs_speedb.so (e.g., flex-server/src). Will be added to LD_LIBRARY_PATH. If not specified, will try to auto-detect from redis-server location."
    )
    parser.add_argument(
        "--test", "-t",
        default=os.environ.get("TEST"),
        help="Run specific test (e.g. test_basic.py::test_module_loads)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=os.environ.get("VERBOSE", "0") == "1",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        default=os.environ.get("PARALLEL", "0") == "1",
        help="Run tests in parallel"
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        default=os.environ.get("LOG", "1") == "0",
        help="Disable logging"
    )
    parser.add_argument(
        "--no-clear-logs",
        action="store_true",
        default=os.environ.get("CLEAR_LOGS", "1") == "0",
        help="Do not remove logs prior to running tests"
    )
    parser.add_argument(
        "extra_args",
        nargs="*",
        help="Extra arguments to pass to RLTest"
    )
    
    args = parser.parse_args()
    
    # Find and validate module
    module = find_module(args.module)
    print(f"Using module: {module}")
    print(f"Using Redis server: {args.redis_server}")
    print(f"Using Redis base port: {args.redis_port}")

    # Check dependencies
    check_redis_server(args.redis_server)
    check_python_deps(parallel=args.parallel)

    # Determine library path for bs_speedb.so
    redis_server_lib_path = None

    if args.redis_lib_path:
        # Use explicitly provided path
        redis_server_lib_path = Path(args.redis_lib_path)
        if not redis_server_lib_path.exists():
            print(f"Error: Specified redis library path does not exist: {redis_server_lib_path}", file=sys.stderr)
            sys.exit(1)
        print(f"Using redis library path: {redis_server_lib_path}")
    else:
        # Try to auto-detect from redis-server location
        # The flex server (redis-server) has bs_speedb.so in its src directory
        redis_server_path = shutil.which(args.redis_server)
        if redis_server_path:
            # Get the directory containing redis-server binary
            redis_bin_dir = Path(redis_server_path).parent
            # bs_speedb.so is in the src directory relative to the repository root
            # Assuming redis-server is in <repo>/src/redis-server, go up one level and into src
            redis_repo_root = redis_bin_dir.parent if redis_bin_dir.name == "src" else redis_bin_dir
            redis_server_lib_path = redis_repo_root / "src"

            if redis_server_lib_path.exists():
                print(f"Auto-detected redis library path: {redis_server_lib_path}")
            else:
                print(f"Warning: Could not auto-detect redis library path. bs_speedb.so may not be found.", file=sys.stderr)
                print(f"  Tried: {redis_server_lib_path}", file=sys.stderr)
                print(f"  Use --redis-lib-path to specify the directory containing bs_speedb.so", file=sys.stderr)
                redis_server_lib_path = None
    
    # Setup logs directory
    script_dir = Path(__file__).parent
    log_dir = script_dir / "logs"

    # Clear logs if requested
    if not args.no_clear_logs and log_dir.exists():
        shutil.rmtree(log_dir)

    # Create logs directory
    log_dir.mkdir(exist_ok=True)

    # Build pytest command for BDD tests
    pytest_cmd = [
        sys.executable, "-m", "pytest",
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
    ]

    # Set environment variables for RLTest
    env_vars = os.environ.copy()
    env_vars["RLTEST_MODULE"] = str(module)
    env_vars["RLTEST_REDIS_SERVER"] = args.redis_server
    env_vars["RLTEST_REDIS_PORT"] = str(args.redis_port)
    env_vars["RLTEST_ENV"] = "oss"
    env_vars["RLTEST_LOG_DIR"] = str(log_dir)

    # Enable Rust backtraces with full debug information
    env_vars["RUST_BACKTRACE"] = "full"
    env_vars["RUST_LIB_BACKTRACE"] = "1"

    # Set LD_LIBRARY_PATH to include redis server library path for bs_speedb.so
    if redis_server_lib_path:
        current_ld_path = env_vars.get("LD_LIBRARY_PATH", "")
        if current_ld_path:
            env_vars["LD_LIBRARY_PATH"] = f"{redis_server_lib_path}:{current_ld_path}"
        else:
            env_vars["LD_LIBRARY_PATH"] = str(redis_server_lib_path)

    if args.verbose:
        pytest_cmd.append("-vv")

    if args.parallel:
        import multiprocessing
        pytest_cmd.extend(["-n", str(multiprocessing.cpu_count())])

    # Add specific test path if provided (must come before extra_args)
    if args.test:
        pytest_cmd.append(args.test)

    # Add extra arguments
    pytest_cmd.extend(args.extra_args)

    # Run the tests
    print("Running BDD tests with pytest...")
    os.chdir(script_dir)

    try:
        subprocess.run(pytest_cmd, env=env_vars, check=True)
        print("Tests completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Tests failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()

