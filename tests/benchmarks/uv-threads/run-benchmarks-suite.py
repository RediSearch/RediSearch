#!/usr/bin/env python3
import os
import subprocess
import yaml
import json
from pathlib import Path
import tempfile
import shutil
import time
from typing import Optional
from dataclasses import dataclass
import copy

@dataclass
class BenchmarkConfig:
    setup: str
    branch: str
    # from here down this is not split
    workers: int
    search_threads: int
    search_io_threads: Optional[int]

CONFIGS = [
    # New 16-primaries matrix - master branch (no SEARCH_IO_THREADS)
    BenchmarkConfig("oss-cluster-16-primaries", "master", 10, 10, None),
    BenchmarkConfig("oss-cluster-16-primaries", "master", 10, 20, None),

    # New 16-primaries matrix - joan-uv-threads branch (with SEARCH_IO_THREADS)
    BenchmarkConfig("oss-cluster-16-primaries", "joan-uv-threads", 10, 10, 10),
    BenchmarkConfig("oss-cluster-16-primaries", "joan-uv-threads", 10, 20, 10),
    BenchmarkConfig("oss-cluster-16-primaries", "joan-uv-threads", 10, 10, 20),
    BenchmarkConfig("oss-cluster-16-primaries", "joan-uv-threads", 10, 20, 20),
]

# Path to the original YAML file
YAML_FILE = "search-numeric-with-results.yml"
MODULE_PATH = "redisearch-master.so"

def check_required_env_vars():
    required_vars = [
        'SERVER_PRIVATE_IP',
        'SERVER_PUBLIC_IP',
        'CLIENT_PUBLIC_IP',
        'REDISTIMESERIES_HOST',
        'REDISTIMESERIES_PORT',
        'REDISTIMESERIES_PASS'
    ]

    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print("Error: Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease export the missing environment variables and re-run.")
        exit(1)

def update_default_yamls():
    """Update defaults.yml with new setups based on configurations"""
    # Path to the defaults.yml file
    defaults_path = "defaults.yml"

    # Load the defaults.yml file
    with open(defaults_path, 'r') as f:
        defaults_config = yaml.safe_load(f)

    # For each benchmark configuration
    for config in CONFIGS:
        setup = config.setup
        branch = config.branch
        workers = config.workers
        search_threads = config.search_threads
        search_io_threads = config.search_io_threads

        # Create a unique name for the new setup
        sio_part = f"_sio{search_io_threads}" if search_io_threads is not None else ""
        new_setup_name = f"{setup}_{branch}_w{workers}_st{search_threads}{sio_part}"

        # Check if this setup already exists
        setup_exists = False
        for setup_config in defaults_config['spec']['setups']:
            if setup_config['name'] == new_setup_name:
                setup_exists = True
                break

        if not setup_exists:
            # Find the original setup to copy from
            original_setup = None
            for setup_config in defaults_config['spec']['setups']:
                if setup_config['name'] == setup:
                    original_setup = copy.deepcopy(setup_config)
                    break

            if original_setup:
                # Modify the name of the copied setup
                original_setup['name'] = new_setup_name
                # Add the new setup to the defaults.yml
                defaults_config['spec']['setups'].append(original_setup)

    # Write the updated defaults.yml back to file
    with open(defaults_path, 'w') as f:
        yaml.dump(defaults_config, f, default_flow_style=False)

    return defaults_path


def update_yaml(yaml_path, workers, search_threads, search_io_threads):
    """Update the YAML file with new configuration parameters"""
    # Create a temporary file
    temp_fd, temp_path = tempfile.mkstemp(suffix='.yml')
    os.close(temp_fd)

    # Copy the original YAML file
    shutil.copy2(yaml_path, temp_path)

    # Load the YAML file
    with open(temp_path, 'r') as f:
        config = yaml.safe_load(f)

    # Update the module configuration parameters
    for element in config['dbconfig']:
      if isinstance(element, dict):
        for k, v in element.items():
          if k == 'module-configuration-parameters':
              v['redisearch']['WORKERS'] = workers
              v['redisearch']['SEARCH_THREADS'] = search_threads
              if search_io_threads is not None:
                v['redisearch']['SEARCH_IO_THREADS'] = search_io_threads
              else:
                if 'SEARCH_IO_THREADS' in v['redisearch']:
                    del v['redisearch']['SEARCH_IO_THREADS']
              v['redisearch']['CONN_PER_SHARD'] = workers + 1

    # Write the updated YAML back to the temporary file
    with open(temp_path, 'w') as f:
        yaml.dump(config, f)

    return temp_path

def run_benchmark(coord, env, setup, branch, yaml_path):
    """Run the benchmark with the given configuration"""
    env_vars = os.environ.copy()
    env_vars.update({
        'COORD': coord,
        'ENV': env,
        'SETUP': setup,
        'GITHUB_BRANCH': branch
    })

    # Get server inventory parameters from environment variables
    server_private_ip = os.environ.get('SERVER_PRIVATE_IP', '127.0.0.1')
    server_public_ip = os.environ.get('SERVER_PUBLIC_IP', '127.0.0.1')
    client_public_ip = os.environ.get('CLIENT_PUBLIC_IP', '127.0.0.1')

    # Instead of creating a single string, prepare the inventory as separate arguments
    inventory_value = f"server_private_ip={server_private_ip},server_public_ip={server_public_ip},client_public_ip={client_public_ip}"

    # Get TimeSeries parameters from environment variables
    ts_host = os.environ.get('REDISTIMESERIES_HOST')
    ts_port = os.environ.get('REDISTIMESERIES_PORT')
    ts_pass = os.environ.get('REDISTIMESERIES_PASS')

    module_path = 'redisearch-master.so' if branch == 'master' else 'redisearch-multiple-uv-threads.so'
    cmd = [
        'redisbench-admin', 'run-remote',
        '--module_path', module_path,
        '--test', yaml_path,
    ]

    # Add TimeSeries parameters if they are set in environment variables
    if ts_host:
        cmd.extend(['--redistimeseries_host', ts_host])
    if ts_port:
        cmd.extend(['--redistimeseries_port', ts_port])
    if ts_pass:
        cmd.extend(['--redistimeseries_pass', ts_pass])

    # Enable pushing results to TimeSeries if host is provided
    if ts_host:
        cmd.append('--push_results_redistimeseries')

    # Add inventory as separate arguments
    cmd.extend(['--inventory', inventory_value])

    try:
        print(f"Running command: {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd,
            env=env_vars,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Redirect stderr to stdout
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )

        # Stream output in real-time
        for line in process.stdout:
            print(line, end='')  # Print each line as it comes

        # Wait for process to complete and get return code
        return_code = process.wait()
        if return_code != 0:
            print(f"Process exited with code {return_code}")
            return False
        return True
    except subprocess.CalledProcessError as e:
        print(f"Benchmark failed with error: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False


def main():
  # Run benchmarks for each configuration
  check_required_env_vars()
  update_default_yamls()
  for config in CONFIGS:
      setup = config.setup
      branch = config.branch
      workers = config.workers
      search_threads = config.search_threads
      search_io_threads = config.search_io_threads
      sio_part = f"_sio{search_io_threads}" if search_io_threads is not None else ""
      new_setup_name = f"{setup}_{branch}_w{workers}_st{search_threads}{sio_part}"

      print("\n" + "="*50)
      print(f"Running benchmark with configuration:")
      print(f"COORD=1 ENV=oss-cluster SETUP={new_setup_name}")
      print(f"BRANCH={branch}")
      print(f"WORKERS={workers} SEARCH_THREADS={search_threads} SEARCH_IO_THREADS={search_io_threads} CONN_PER_SHARD={workers + 1}")
      print("="*50)

      # Update YAML file
      temp_yaml = update_yaml(YAML_FILE, workers, search_threads, search_io_threads)

      # Create a unique identifier for this configuration
      config_id = f"{branch}_{setup}_w{workers}_st{search_threads}_sio{search_io_threads}"

      # Run the benchmark
      success = run_benchmark("1", "oss-cluster", new_setup_name, branch, temp_yaml)

      if success:
          print(f"Benchmark completed!!!!")
      else:
          print(f"Benchmark failed for configuration: {config_id}")

      # Clean up temporary YAML file
      os.unlink(temp_yaml)

      # Add a small delay between runs to avoid potential conflicts
      time.sleep(1)

  print("\nAll benchmarks completed")


if __name__ == "__main__":
  main()
