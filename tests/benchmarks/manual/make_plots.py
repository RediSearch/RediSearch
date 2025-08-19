#!/usr/bin/env python3
"""
Benchmark Results Visualization Script

This script reads benchmark results from results/loop/ directory and creates
visualizations showing RAM and DISK performance for READ and SKIP-TO benchmarks.
"""

import os
import re
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, List, Tuple


def parse_benchmark_file(filepath: str) -> Tuple[int, Dict[str, Dict[str, float]]]:
    """
    Parse a single benchmark results file.

    Args:
        filepath: Path to the benchmark results file

    Returns:
        Tuple of (num_documents, results_dict) where results_dict contains:
        {
            'read': {'ram_avg': float, 'disk_avg': float},
            'skip_to': {'ram_avg': float, 'disk_avg': float}
        }
    """
    filename = os.path.basename(filepath)

    # Extract number of documents from filename
    num_docs_match = re.search(r'_(\d+)\.txt$', filename)
    if not num_docs_match:
        raise ValueError(f"Could not extract number of documents from filename: {filename}")

    num_documents = int(num_docs_match.group(1))

    results = {
        'read': {'ram_avg': None, 'disk_avg': None, 'ratio': None},
        'skip_to': {'ram_avg': None, 'disk_avg': None, 'ratio': None}
    }

    missing_fields = []

    try:
        with open(filepath, 'r') as f:
            content = f.read()
    except Exception as e:
        raise ValueError(f"Could not read file {filename}: {e}")

    # Parse READ benchmark results
    read_section = re.search(r'READ BENCHMARK RESULTS.*?(?=SKIP-TO BENCHMARK RESULTS)', content, re.DOTALL)
    if read_section:
        print(f"  Found READ BENCHMARK RESULTS section in {filename}")
        ram_match = re.search(r'RAM avg time: ([\d.]+) seconds', read_section.group(0))
        disk_match = re.search(r'DISK avg time: ([\d.]+) seconds', read_section.group(0))
        ratio_match = re.search(r'SUMMARY: Disk_time / Ram_time \(averages ratio\): ([\d.]+)', read_section.group(0))

        if ram_match:
            results['read']['ram_avg'] = float(ram_match.group(1))
            print(f"    READ RAM avg time: {results['read']['ram_avg']} seconds")
        else:
            missing_fields.append("READ RAM avg time")

        if disk_match:
            results['read']['disk_avg'] = float(disk_match.group(1))
            print(f"    READ DISK avg time: {results['read']['disk_avg']} seconds")
        else:
            missing_fields.append("READ DISK avg time")

        if ratio_match:
            results['read']['ratio'] = float(ratio_match.group(1))
            print(f"    READ Disk/RAM ratio: {results['read']['ratio']}")
        else:
            missing_fields.append("READ Disk/RAM ratio")
    else:
        print(f"  WARNING: No READ BENCHMARK RESULTS section found in {filename}")
        missing_fields.extend(["READ RAM avg time", "READ DISK avg time", "READ Disk/RAM ratio"])

    # Parse SKIP-TO benchmark results
    skip_to_section = re.search(r'SKIP-TO BENCHMARK RESULTS.*', content, re.DOTALL)
    if skip_to_section:
        print(f"  Found SKIP-TO BENCHMARK RESULTS section in {filename}")
        ram_match = re.search(r'RAM avg time: ([\d.]+) seconds', skip_to_section.group(0))
        disk_match = re.search(r'DISK avg time: ([\d.]+) seconds', skip_to_section.group(0))
        ratio_match = re.search(r'SUMMARY: Disk_time / Ram_time \(averages ratio\): ([\d.]+)', skip_to_section.group(0))

        if ram_match:
            results['skip_to']['ram_avg'] = float(ram_match.group(1))
            print(f"    SKIP-TO RAM avg time: {results['skip_to']['ram_avg']} seconds")
        else:
            missing_fields.append("SKIP-TO RAM avg time")

        if disk_match:
            results['skip_to']['disk_avg'] = float(disk_match.group(1))
            print(f"    SKIP-TO DISK avg time: {results['skip_to']['disk_avg']} seconds")
        else:
            missing_fields.append("SKIP-TO DISK avg time")

        if ratio_match:
            results['skip_to']['ratio'] = float(ratio_match.group(1))
            print(f"    SKIP-TO Disk/RAM ratio: {results['skip_to']['ratio']}")
        else:
            missing_fields.append("SKIP-TO Disk/RAM ratio")
    else:
        print(f"  WARNING: No SKIP-TO BENCHMARK RESULTS section found in {filename}")
        missing_fields.extend(["SKIP-TO RAM avg time", "SKIP-TO DISK avg time", "SKIP-TO Disk/RAM ratio"])

    if missing_fields:
        print(f"  WARNING: Missing fields in {filename}: {', '.join(missing_fields)}")

    return num_documents, results


def load_all_benchmark_data(results_dir: str) -> Dict[int, Dict[str, Dict[str, float]]]:
    """
    Load all benchmark data from the results directory.

    Args:
        results_dir: Path to the directory containing benchmark results

    Returns:
        Dictionary mapping num_documents to results data
    """
    all_data = {}

    if not os.path.exists(results_dir):
        raise FileNotFoundError(f"Results directory not found: {results_dir}")

    for filename in os.listdir(results_dir):
        if filename.endswith('.txt') and filename != 'log.txt':
            filepath = os.path.join(results_dir, filename)
            try:
                num_docs, results = parse_benchmark_file(filepath)
                all_data[num_docs] = results
                print(f"Loaded data for {num_docs:,} documents")
            except Exception as e:
                print(f"Warning: Could not parse {filename}: {e}")

    return all_data


def create_benchmark_plots(data: Dict[int, Dict[str, Dict[str, float]]]):
    """
    Create visualization plots for the benchmark data.

    Args:
        data: Dictionary mapping num_documents to results data
    """
    # Sort data by number of documents
    sorted_docs = sorted(data.keys())

    # Extract data for plotting, filtering out None values
    def extract_valid_data(docs_list, benchmark_type, metric_type):
        """Extract valid (non-None) data points for plotting."""
        valid_docs = []
        valid_times = []
        missing_count = 0

        for docs in docs_list:
            value = data[docs][benchmark_type][metric_type]
            if value is not None:
                valid_docs.append(docs)
                valid_times.append(value)
            else:
                missing_count += 1

        if missing_count > 0:
            print(f"  WARNING: {missing_count} data points missing for {benchmark_type} {metric_type}")

        return valid_docs, valid_times

    # Extract data for each plot
    read_ram_docs, read_ram_times = extract_valid_data(sorted_docs, 'read', 'ram_avg')
    read_disk_docs, read_disk_times = extract_valid_data(sorted_docs, 'read', 'disk_avg')
    skip_to_ram_docs, skip_to_ram_times = extract_valid_data(sorted_docs, 'skip_to', 'ram_avg')
    skip_to_disk_docs, skip_to_disk_times = extract_valid_data(sorted_docs, 'skip_to', 'disk_avg')

    # Extract ratio data
    read_ratio_docs, read_ratios = extract_valid_data(sorted_docs, 'read', 'ratio')
    skip_to_ratio_docs, skip_to_ratios = extract_valid_data(sorted_docs, 'skip_to', 'ratio')

    # Check if we have any data to plot
    if not any([read_ram_times, read_disk_times, skip_to_ram_times, skip_to_disk_times, read_ratios, skip_to_ratios]):
        print("ERROR: No valid data found for plotting!")
        return

    # Create the figure with 2x2 subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('RediSearch Benchmark Results (CA)', fontsize=16, fontweight='bold')

    # READ benchmark - both RAM and DISK on same plot
    ax1.set_title('READ Benchmark - Average Times', fontweight='bold')
    if read_ram_times:
        ax1.plot(read_ram_docs, read_ram_times, 'b-o', linewidth=2, markersize=6, label=f'RAM ({len(read_ram_times)} points)')
    if read_disk_times:
        ax1.plot(read_disk_docs, read_disk_times, 'r-o', linewidth=2, markersize=6, label=f'DISK ({len(read_disk_times)} points)')

    if not read_ram_times and not read_disk_times:
        ax1.text(0.5, 0.5, 'No READ benchmark data available', ha='center', va='center', transform=ax1.transAxes)
    else:
        ax1.legend()

    ax1.set_xlabel('Number of Keys')
    ax1.set_ylabel('Time (seconds)')
    ax1.grid(True, alpha=0.3)
    ax1.ticklabel_format(style='plain', axis='x')

    # SKIP-TO benchmark - both RAM and DISK on same plot
    ax2.set_title('SKIP-TO Benchmark - Average Times', fontweight='bold')
    if skip_to_ram_times:
        ax2.plot(skip_to_ram_docs, skip_to_ram_times, 'g-o', linewidth=2, markersize=6, label=f'RAM ({len(skip_to_ram_times)} points)')
    if skip_to_disk_times:
        ax2.plot(skip_to_disk_docs, skip_to_disk_times, 'm-o', linewidth=2, markersize=6, label=f'DISK ({len(skip_to_disk_times)} points)')

    if not skip_to_ram_times and not skip_to_disk_times:
        ax2.text(0.5, 0.5, 'No SKIP-TO benchmark data available', ha='center', va='center', transform=ax2.transAxes)
    else:
        ax2.legend()

    ax2.set_xlabel('Number of Keys')
    ax2.set_ylabel('Time (seconds)')
    ax2.grid(True, alpha=0.3)
    ax2.ticklabel_format(style='plain', axis='x')

    # READ benchmark - Disk/RAM ratio
    ax3.set_title('READ Benchmark - Disk/RAM Ratio', fontweight='bold')
    if read_ratios:
        ax3.plot(read_ratio_docs, read_ratios, 'orange', marker='o', linewidth=2, markersize=6, label=f'Disk/RAM Ratio ({len(read_ratios)} points)')
        ax3.legend()
    else:
        ax3.text(0.5, 0.5, 'No READ ratio data available', ha='center', va='center', transform=ax3.transAxes)

    ax3.set_xlabel('Number of Keys')
    ax3.set_ylabel('Disk Time / RAM Time')
    ax3.grid(True, alpha=0.3)
    ax3.ticklabel_format(style='plain', axis='x')

    # SKIP-TO benchmark - Disk/RAM ratio
    ax4.set_title('SKIP-TO Benchmark - Disk/RAM Ratio', fontweight='bold')
    if skip_to_ratios:
        ax4.plot(skip_to_ratio_docs, skip_to_ratios, 'purple', marker='o', linewidth=2, markersize=6, label=f'Disk/RAM Ratio ({len(skip_to_ratios)} points)')
        ax4.legend()
    else:
        ax4.text(0.5, 0.5, 'No SKIP-TO ratio data available', ha='center', va='center', transform=ax4.transAxes)

    ax4.set_xlabel('Number of Keys')
    ax4.set_ylabel('Disk Time / RAM Time')
    ax4.grid(True, alpha=0.3)
    ax4.ticklabel_format(style='plain', axis='x')

    # Adjust layout to prevent overlap
    plt.tight_layout()

    # Save the plot as an image file
    output_filename = "tests/benchmarks/manual/loop/benchmark_results.png"
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved as: {output_filename}")

    # Try to show the plot (will work if display is available)
    try:
        plt.show()
        print("Plot displayed successfully.")
    except Exception as e:
        print(f"Could not display plot interactively (this is normal in headless environments): {e}")
        print(f"Please view the saved image file: {output_filename}")

    # Print summary statistics
    print("\n" + "="*50)
    print("SUMMARY STATISTICS")
    print("="*50)
    print(f"Total files processed: {len(sorted_docs)}")
    print(f"Document range: {min(sorted_docs):,} to {max(sorted_docs):,}")

    print(f"\nREAD Benchmark:")
    if read_ram_times:
        print(f"  RAM data points: {len(read_ram_times)}")
        print(f"  RAM time range: {min(read_ram_times):.6f}s to {max(read_ram_times):.6f}s")
    else:
        print(f"  RAM data points: 0 (no data available)")

    if read_disk_times:
        print(f"  DISK data points: {len(read_disk_times)}")
        print(f"  DISK time range: {min(read_disk_times):.6f}s to {max(read_disk_times):.6f}s")
    else:
        print(f"  DISK data points: 0 (no data available)")

    if read_ratios:
        print(f"  Ratio data points: {len(read_ratios)}")
        print(f"  Disk/RAM ratio range: {min(read_ratios):.2f} to {max(read_ratios):.2f}")
    else:
        print(f"  Ratio data points: 0 (no data available)")

    print(f"\nSKIP-TO Benchmark:")
    if skip_to_ram_times:
        print(f"  RAM data points: {len(skip_to_ram_times)}")
        print(f"  RAM time range: {min(skip_to_ram_times):.6f}s to {max(skip_to_ram_times):.6f}s")
    else:
        print(f"  RAM data points: 0 (no data available)")

    if skip_to_disk_times:
        print(f"  DISK data points: {len(skip_to_disk_times)}")
        print(f"  DISK time range: {min(skip_to_disk_times):.6f}s to {max(skip_to_disk_times):.6f}s")
    else:
        print(f"  DISK data points: 0 (no data available)")

    if skip_to_ratios:
        print(f"  Ratio data points: {len(skip_to_ratios)}")
        print(f"  Disk/RAM ratio range: {min(skip_to_ratios):.2f} to {max(skip_to_ratios):.2f}")
    else:
        print(f"  Ratio data points: 0 (no data available)")


def main():
    """Main function to run the benchmark visualization."""
    results_dir = "/home/ubuntu/rsdisk/RediSearchDisk/tests/benchmarks/manual/loop"

    print("Loading benchmark data...")
    try:
        data = load_all_benchmark_data(results_dir)

        if not data:
            print("No benchmark data found!")
            return

        print(f"\nLoaded data for {len(data)} different document counts")
        print("Creating plots...")

        create_benchmark_plots(data)

    except Exception as e:
        print(f"Error: {e}")
        return


if __name__ == "__main__":
    main()