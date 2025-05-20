#!/usr/bin/env python3

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

def execute_command(args):
    """Execute queries and save results to the specified output path."""
    output_path = Path(args.output)

    # Create parent directories if they don't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Create file path for output.txt in the output directory
    output_file = output_path / "output.txt"

    print(f"Executing queries and saving results to {output_file}")

    # Get current date and time
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Write directly to the output file
    output_file.write_text(f"Query executed on: {current_date}\n")

    print(f"Successfully wrote current date to {output_file}")

def compare_command(args):
    """Compare baseline results with changeset results using system diff command."""
    baseline = args.baseline
    changeset = args.changeset

    # Validate that both paths exist
    if not Path(baseline).exists():
        print(f"Error: Baseline not found: {baseline}", file=sys.stderr)
        sys.exit(1)

    if not Path(changeset).exists():
        print(f"Error: Changeset not found: {changeset}", file=sys.stderr)
        sys.exit(1)

    print(f"Comparing {baseline} with {changeset}")

    try:
        # Use -r for recursive directory comparison and -u for unified diff format
        result = subprocess.run(
            ["diff", "-ru", "--color=auto", baseline, changeset],
            text=True
        )

        if result.returncode == 0:
            print("No differences found.")
            return 0
        elif result.returncode == 1:  # Return code 1 from diff means differences were found
            print("Differences found.")
            return 1
        else:  # Return code 2 means trouble
            print(f"Diff command failed with return code {result.returncode}")
            return 2

    except FileNotFoundError:
        print("Error: 'diff' command not found. Make sure it's installed on your system.", file=sys.stderr)
        sys.exit(1)

def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Tool for executing and comparing query results",
        prog="query_diff"
    )

    # Create subparsers for commands
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    subparsers.required = True  # Make subcommand required

    # Execute command
    execute_parser = subparsers.add_parser("execute", help="Execute queries and save results")
    execute_parser.add_argument(
        "--output",
        required=True,
        help="Path to save execution results"
    )
    execute_parser.set_defaults(func=execute_command)

    # Compare command
    compare_parser = subparsers.add_parser("compare", help="Compare baseline with changeset")
    compare_parser.add_argument(
        "--baseline",
        required=True,
        help="Path to baseline results"
    )
    compare_parser.add_argument(
        "--changeset",
        required=True,
        help="Path to changeset results"
    )
    compare_parser.set_defaults(func=compare_command)

    # Parse arguments and call the appropriate function
    args = parser.parse_args()
    result = args.func(args)

    if result is not None:
        sys.exit(result)

if __name__ == "__main__":
    main()
