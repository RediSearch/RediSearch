#!/usr/bin/env python3
"""
RediSearch Query Profile Test Script with JSON Output

This script:
- Detects the RediSearch module version at startup
- Executes queries using FT.PROFILE for detailed performance analysis
- Outputs profile results in JSON format for analysis
- Monitors Redis slowlog for slow query detection
- Measures query execution time

Usage:
    # Execute a query with profile analysis
    ./test_redisearch_profile.py --index-name my_index --query "hello world"

    # Output profile results in JSON format
    ./test_redisearch_profile.py --index-name my_index --query "@field:[1 100]" --json

    # Save profile results to JSON file
    ./test_redisearch_profile.py --index-name my_index --query "test" --json-file profile_results.json
"""

import redis
import time
import sys
import datetime
import argparse
import json
from typing import Optional


class RediSearchTester:
    """Test runner with RediSearch module version detection and VTune profiling"""

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        """Initialize Redis connection and detect RediSearch module version"""
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True
            )
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"✗ Failed to connect to Redis at {redis_host}:{redis_port}")
            sys.exit(1)

        # Detect and print RediSearch module version
        self.redisearch_version = self.detect_redisearch_version()
        if self.redisearch_version:
            print(f"✓ RediSearch module version: {self.redisearch_version}")
        else:
            print("✗ RediSearch module not found or not loaded")
            sys.exit(1)

    def detect_redisearch_version(self) -> Optional[str]:
        """Detect RediSearch module version using MODULE LIST command"""
        try:
            modules = self.redis_client.execute_command('MODULE', 'LIST')
            for module in modules:
                # Module info format: [name, version, ...]
                if len(module) >= 4 and module[1].lower() == 'search':
                    version = module[3]  # Version is at index 3
                    return str(version)
            return None
        except Exception as e:
            print(f"Warning: Could not detect RediSearch version: {e}")
            return None



    def get_slowlog_before_query(self) -> int:
        """Get current slowlog length to establish baseline"""
        try:
            slowlog = self.redis_client.execute_command('SLOWLOG', 'LEN')
            return int(slowlog)
        except Exception as e:
            print(f"Warning: Could not get slowlog length: {e}")
            return 0

    def check_slowlog_after_query(self, baseline_length: int) -> None:
        """Check if new entries were added to slowlog and display them"""
        try:
            current_length = self.redis_client.execute_command('SLOWLOG', 'LEN')
            current_length = int(current_length)

            if current_length > baseline_length:
                new_entries = current_length - baseline_length
                print(f"⚠ {new_entries} new slow query(ies) detected!")

                # Get the new slow log entries
                slowlog_entries = self.redis_client.execute_command('SLOWLOG', 'GET', str(new_entries))

                for i, entry in enumerate(slowlog_entries):
                    timestamp = entry[1]
                    duration_microseconds = entry[2]
                    duration_ms = duration_microseconds / 1000.0
                    command = ' '.join(str(arg) for arg in entry[3])

                    print(f"  Slow Query #{i+1}:")
                    print(f"    Duration: {duration_ms:.2f} ms ({duration_microseconds} μs)")
                    print(f"    Timestamp: {timestamp}")
                    print(f"    Command: {command}")
                    print()
            else:
                print("✓ No slow queries detected")

        except Exception as e:
            print(f"Warning: Could not check slowlog: {e}")

    def run_query_test(self, index_name: str, query: str, output_json: bool = False, json_file: str = None, html_tree_file: str = None):
        """Run a specific query test with FT.PROFILE"""
        # Get baseline slowlog length
        slowlog_baseline = self.get_slowlog_before_query()

        # Run the query with profiling
        print(f"🚀 Executing profiled query on index '{index_name}':")
        print(f"   Query: {query}")

        start_time = time.time()
        try:
            # Execute the FT.PROFILE command to get profiling information
            profile_result = self.redis_client.execute_command('FT.PROFILE', index_name, 'AGGREGATE',
                                                             'QUERY', query, 'LIMIT', '0', '60', 'TIMEOUT', '0')
            execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds

            print(f"✓ Profiled query executed successfully in {execution_time:.2f} ms")

            # Parse the profile result
            if isinstance(profile_result, list) and len(profile_result) >= 2:
                query_results = profile_result[0]  # Actual query results
                profile_data = profile_result[1]   # Profile information

                print(f"   Results returned: {len(query_results) if isinstance(query_results, list) else 'N/A'}")

                # Output profile information
                if output_json:
                    self.output_profile_json(profile_data, json_file, index_name, query, execution_time)
                elif html_tree_file:
                    self.generate_html_tree(profile_data, html_tree_file, index_name, query, execution_time)
                else:
                    self.display_profile_summary(profile_data)

                # Generate HTML tree if requested
                if html_tree_file and not output_json:
                    self.generate_html_tree(profile_data, html_tree_file, index_name, query, execution_time)

                # Check slowlog for any slow queries
                self.check_slowlog_after_query(slowlog_baseline)

                return True, (query_results, profile_data)
            else:
                print("⚠ Unexpected profile result format")
                return False, profile_result

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            print(f"✗ Query failed after {execution_time:.2f} ms: {e}")

            # Still check slowlog in case of failure
            self.check_slowlog_after_query(slowlog_baseline)

            return False, None

    def run_comparison_test(self, index1: str, index2: str, query: str, html_tree_file: str = None):
        """Run the same query on two indexes and compare results"""
        print(f"🔄 Running comparison test between '{index1}' and '{index2}'")

        # Run query on first index
        print(f"\n📊 Testing index: {index1}")
        success1, result1 = self.run_query_test(index1, query, output_json=False, json_file=None, html_tree_file=None)

        if not success1:
            print(f"✗ Failed to run query on index '{index1}'")
            return False, None

        # Run query on second index
        print(f"\n📊 Testing index: {index2}")
        success2, result2 = self.run_query_test(index2, query, output_json=False, json_file=None, html_tree_file=None)

        if not success2:
            print(f"✗ Failed to run query on index '{index2}'")
            return False, None

        # Extract profile data from results
        if isinstance(result1, tuple) and len(result1) == 2:
            query_results1, profile_data1 = result1
        else:
            print(f"✗ Unexpected result format from index '{index1}'")
            return False, None

        if isinstance(result2, tuple) and len(result2) == 2:
            query_results2, profile_data2 = result2
        else:
            print(f"✗ Unexpected result format from index '{index2}'")
            return False, None

        # Generate comparison HTML if requested
        if html_tree_file:
            self.generate_comparison_html(
                profile_data1, profile_data2,
                html_tree_file, index1, index2, query,
                len(query_results1) if isinstance(query_results1, list) else 0,
                len(query_results2) if isinstance(query_results2, list) else 0
            )

        return True, {
            'index1': {'name': index1, 'results': query_results1, 'profile': profile_data1},
            'index2': {'name': index2, 'results': query_results2, 'profile': profile_data2}
        }

    def parse_profile_list(self, profile_list):
        """Parse RediSearch profile list format into structured dictionary"""
        if not isinstance(profile_list, list) or len(profile_list) == 0:
            return profile_list

        result = {}
        i = 0
        while i < len(profile_list):
            if i + 1 < len(profile_list):
                key = profile_list[i]
                value = profile_list[i + 1]

                # Handle nested structures
                if isinstance(value, list) and key in ['Child iterators', 'Child iterator']:
                    if key == 'Child iterators':
                        # Multiple child iterators
                        result[key] = [self.parse_profile_list(child) for child in value]
                    else:
                        # Single child iterator
                        result[key] = self.parse_profile_list(value)
                elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], list):
                    # List of profile structures (like Result processors profile)
                    result[key] = [self.parse_profile_list(item) for item in value]
                else:
                    result[key] = value
                i += 2
            else:
                # Odd number of elements, treat as single value
                result[f"item_{i}"] = profile_list[i]
                i += 1

        return result

    def parse_profile_data(self, raw_profile_data):
        """Parse the complete profile data structure"""
        try:
            if isinstance(raw_profile_data, dict):
                # Already a dictionary, parse nested structures
                parsed = {}
                for key, value in raw_profile_data.items():
                    if isinstance(value, list) and key in ['Iterators profile', 'Result processors profile']:
                        if key == 'Iterators profile':
                            parsed[key] = self.parse_profile_list(value)
                        else:  # Result processors profile
                            parsed[key] = [self.parse_profile_list(item) for item in value]
                    else:
                        parsed[key] = value
                return parsed
            elif isinstance(raw_profile_data, list):
                # List format, convert to dictionary
                return self.parse_profile_list(raw_profile_data)
            else:
                return raw_profile_data
        except Exception as e:
            print(f"Warning: Error parsing profile data: {e}")
            return raw_profile_data

    def display_profile_summary(self, profile_data):
        """Display a summary of the profile data"""
        print(f"\n📊 Query Profile Summary:")
        try:
            # Parse the profile data first
            parsed_data = self.parse_profile_data(profile_data)

            if isinstance(parsed_data, dict):
                # Extract key metrics from profile data
                timing_fields = ['Total profile time', 'Parsing time', 'Pipeline creation time']
                for field in timing_fields:
                    if field in parsed_data:
                        print(f"   {field}: {parsed_data[field]} ms")

                # Display warning if present
                if 'Warning' in parsed_data:
                    warning = parsed_data['Warning']
                    if warning != 'None':
                        print(f"   ⚠ Warning: {warning}")

                # Display iterators information if available
                if 'Iterators profile' in parsed_data:
                    iterators = parsed_data['Iterators profile']
                    if isinstance(iterators, dict):
                        iter_type = iterators.get('Type', 'Unknown')
                        counter = iterators.get('Counter', 'N/A')
                        print(f"   Iterator: {iter_type} (Counter: {counter})")
                    else:
                        print(f"   Iterator information: {type(iterators)}")

                # Display result processors if available
                if 'Result processors profile' in parsed_data:
                    processors = parsed_data['Result processors profile']
                    if isinstance(processors, list):
                        print(f"   Result processors: {len(processors)} stages")
                        for i, proc in enumerate(processors):
                            if isinstance(proc, dict) and 'Type' in proc:
                                proc_type = proc['Type']
                                counter = proc.get('Counter', 'N/A')
                                print(f"     {i+1}. {proc_type} (Counter: {counter})")
                    else:
                        print(f"   Result processors: {type(processors)}")
            else:
                print(f"   Profile data type: {type(parsed_data)}")
                print(f"   Profile data: {str(parsed_data)[:200]}...")

        except Exception as e:
            print(f"   Error displaying profile summary: {e}")

    def output_profile_json(self, profile_data, json_file: str, index_name: str, query: str, execution_time: float):
        """Output profile data in JSON format"""
        try:
            # Parse the profile data into structured format
            parsed_profile = self.parse_profile_data(profile_data)

            # Create comprehensive profile output
            profile_output = {
                "metadata": {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "redisearch_version": self.redisearch_version,
                    "index_name": index_name,
                    "query": query,
                    "execution_time_ms": round(execution_time, 3)
                },
                "profile": parsed_profile,
                "raw_profile_data": profile_data  # Keep original for reference
            }

            if json_file:
                # Write to specified file
                with open(json_file, 'w') as f:
                    json.dump(profile_output, f, indent=2, default=str, ensure_ascii=False)
                print(f"✓ Profile data saved to: {json_file}")

                # Also show a summary of what was saved
                print(f"   Metadata: timestamp, version, index, query, execution time")
                if isinstance(parsed_profile, dict):
                    print(f"   Profile sections: {', '.join(parsed_profile.keys())}")
            else:
                # Output to stdout
                print(f"\n📄 Profile JSON Output:")
                print(json.dumps(profile_output, indent=2, default=str, ensure_ascii=False))

        except Exception as e:
            print(f"✗ Error outputting JSON: {e}")
            print(f"   Error details: {str(e)}")
            # Fallback to simple display
            try:
                fallback_output = {
                    "metadata": {
                        "timestamp": datetime.datetime.now().isoformat(),
                        "index_name": index_name,
                        "query": query,
                        "execution_time_ms": execution_time,
                        "error": "Failed to parse profile data"
                    },
                    "raw_profile_data": str(profile_data)
                }
                if json_file:
                    with open(json_file, 'w') as f:
                        json.dump(fallback_output, f, indent=2, default=str)
                    print(f"✓ Fallback profile data saved to: {json_file}")
                else:
                    print(json.dumps(fallback_output, indent=2, default=str))
            except Exception as fallback_error:
                print(f"✗ Fallback JSON output also failed: {fallback_error}")
                print(f"Raw profile data: {profile_data}")

    def generate_html_tree(self, profile_data, html_file: str, index_name: str, query: str, execution_time: float):
        """Generate interactive HTML tree visualization of profile data"""
        try:
            print("🔄 Attempting to parse profile data for HTML tree...")

            # Parse the profile data into structured format
            parsed_profile = self.parse_profile_data(profile_data)

            # Generate HTML content
            html_content = self.create_html_tree_content(parsed_profile, index_name, query, execution_time)

            # Write to file
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"✓ Interactive HTML tree saved to: {html_file}")
            print(f"   Open in browser to explore the profile data interactively")

        except Exception as e:
            print(f"✗ HTML tree generation aborted due to parsing failure")
            print(f"   Parsing error: {str(e)}")
            print(f"\n⚠ HTML tree was NOT generated due to parsing errors.")
            print(f"   Please provide the parsing error details above to fix the script.")

    def generate_comparison_html(self, profile_data1, profile_data2, html_file: str,
                               index1: str, index2: str, query: str,
                               result_count1: int, result_count2: int):
        """Generate comparison HTML with diff between two profile results"""
        try:
            print("🔄 Attempting to parse profile data for comparison HTML...")

            # Parse both profile datasets
            parsed_profile1 = self.parse_profile_data(profile_data1)
            parsed_profile2 = self.parse_profile_data(profile_data2)

            # Generate comparison HTML content
            html_content = self.create_comparison_html_content(
                parsed_profile1, parsed_profile2,
                index1, index2, query,
                result_count1, result_count2
            )

            # Write to file
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"✓ Comparison HTML report saved to: {html_file}")
            print(f"   Open in browser to explore the profile comparison interactively")

        except Exception as e:
            print(f"✗ Comparison HTML generation aborted due to parsing failure")
            print(f"   Parsing error: {str(e)}")
            print(f"\n⚠ Comparison HTML was NOT generated due to parsing errors.")
            print(f"   Please provide the parsing error details above to fix the script.")

    def create_html_tree_content(self, parsed_data, index_name: str, query: str, execution_time: float):
        """Create the HTML content for the interactive flow diagram tree"""
        # Generate the flow diagram HTML
        flow_diagram = self.generate_flow_diagram(parsed_data)

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RediSearch Profile Tree - {index_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 24px;
        }}
        .header .meta {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .tree-container {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .tree {{
            font-family: 'Courier New', monospace;
            font-size: 14px;
            line-height: 1.6;
        }}
        .tree-node {{
            margin: 2px 0;
            position: relative;
        }}
        .tree-toggle {{
            cursor: pointer;
            user-select: none;
            display: inline-block;
            width: 20px;
            text-align: center;
            font-weight: bold;
            color: #666;
        }}
        .tree-toggle:hover {{
            color: #333;
            background-color: #f0f0f0;
            border-radius: 3px;
        }}
        .tree-content {{
            display: inline-block;
            margin-left: 5px;
        }}
        .tree-children {{
            margin-left: 25px;
            border-left: 2px solid #e0e0e0;
            padding-left: 15px;
        }}
        .tree-children.collapsed {{
            display: none;
        }}
        .tree-key {{
            color: #0066cc;
            font-weight: bold;
        }}
        .tree-value {{
            color: #d73a49;
        }}
        .tree-type {{
            color: #6f42c1;
            font-style: italic;
            font-size: 12px;
        }}
        .tree-number {{
            color: #005cc5;
        }}
        .tree-string {{
            color: #032f62;
        }}
        .controls {{
            margin-bottom: 15px;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 5px;
        }}
        .btn {{
            background-color: #007bff;
            color: white;
            border: none;
            padding: 8px 16px;
            margin-right: 10px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }}
        .btn:hover {{
            background-color: #0056b3;
        }}
        .search-box {{
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            margin-right: 10px;
            font-size: 14px;
            width: 200px;
        }}
        .highlight {{
            background-color: yellow;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>RediSearch Profile Tree</h1>
        <div class="meta">
            <strong>Index:</strong> {index_name} |
            <strong>Query:</strong> {query} |
            <strong>Execution Time:</strong> {execution_time:.2f} ms |
            <strong>Generated:</strong> {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        </div>
    </div>

    <div class="tree-container">
        <div class="controls">
            <button class="btn" onclick="expandAll()">Expand All</button>
            <button class="btn" onclick="collapseAll()">Collapse All</button>
            <input type="text" class="search-box" id="searchBox" placeholder="Search in tree..." onkeyup="searchTree()">
            <button class="btn" onclick="clearSearch()">Clear</button>
        </div>

        <div class="tree" id="tree">
            {tree_html}
        </div>
    </div>

    <script>
        function toggleNode(element) {{
            const children = element.parentNode.querySelector('.tree-children');
            const toggle = element;

            if (children) {{
                if (children.classList.contains('collapsed')) {{
                    children.classList.remove('collapsed');
                    toggle.textContent = '▼';
                }} else {{
                    children.classList.add('collapsed');
                    toggle.textContent = '▶';
                }}
            }}
        }}

        function expandAll() {{
            const children = document.querySelectorAll('.tree-children');
            const toggles = document.querySelectorAll('.tree-toggle');

            children.forEach(child => child.classList.remove('collapsed'));
            toggles.forEach(toggle => {{
                if (toggle.nextElementSibling && toggle.nextElementSibling.querySelector('.tree-children')) {{
                    toggle.textContent = '▼';
                }}
            }});
        }}

        function collapseAll() {{
            const children = document.querySelectorAll('.tree-children');
            const toggles = document.querySelectorAll('.tree-toggle');

            children.forEach(child => child.classList.add('collapsed'));
            toggles.forEach(toggle => {{
                if (toggle.nextElementSibling && toggle.nextElementSibling.querySelector('.tree-children')) {{
                    toggle.textContent = '▶';
                }}
            }});
        }}

        function searchTree() {{
            const searchTerm = document.getElementById('searchBox').value.toLowerCase();
            const nodes = document.querySelectorAll('.tree-content');

            // Clear previous highlights
            nodes.forEach(node => {{
                node.innerHTML = node.innerHTML.replace(/<mark class="highlight">(.*?)<\\/mark>/g, '$1');
            }});

            if (searchTerm) {{
                nodes.forEach(node => {{
                    const text = node.textContent.toLowerCase();
                    if (text.includes(searchTerm)) {{
                        // Highlight matching text
                        const regex = new RegExp(`(${{searchTerm}})`, 'gi');
                        node.innerHTML = node.innerHTML.replace(regex, '<mark class="highlight">$1</mark>');

                        // Expand parent nodes to show match
                        let parent = node.closest('.tree-children');
                        while (parent) {{
                            parent.classList.remove('collapsed');
                            const toggle = parent.parentNode.querySelector('.tree-toggle');
                            if (toggle) toggle.textContent = '▼';
                            parent = parent.parentNode.closest('.tree-children');
                        }}
                    }}
                }});
            }}
        }}

        function clearSearch() {{
            document.getElementById('searchBox').value = '';
            searchTree();
        }}

        // Initialize with some nodes collapsed for better overview
        document.addEventListener('DOMContentLoaded', function() {{
            const deepChildren = document.querySelectorAll('.tree-children .tree-children');
            deepChildren.forEach(child => {{
                child.classList.add('collapsed');
                const toggle = child.parentNode.querySelector('.tree-toggle');
                if (toggle) toggle.textContent = '▶';
            }});
        }});
    </script>
</body>
</html>"""

        return html_template

    def create_comparison_html_content(self, parsed_data1, parsed_data2,
                                     index1: str, index2: str, query: str,
                                     result_count1: int, result_count2: int):
        """Create HTML content for comparison view"""
        # Generate tree HTML for both datasets
        tree_html1 = self.generate_tree_html(parsed_data1)
        tree_html2 = self.generate_tree_html(parsed_data2)

        # Extract key metrics for comparison
        metrics1 = self.extract_key_metrics(parsed_data1)
        metrics2 = self.extract_key_metrics(parsed_data2)

        # Generate comparison table
        comparison_table = self.generate_comparison_table(metrics1, metrics2, index1, index2, result_count1, result_count2)

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RediSearch Profile Comparison - {index1} vs {index2}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 24px;
        }}
        .header .meta {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .comparison-container {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .index-panel {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .index-panel h2 {{
            margin: 0 0 15px 0;
            padding: 10px;
            border-radius: 5px;
            font-size: 18px;
        }}
        .index1 h2 {{
            background-color: #e3f2fd;
            color: #1976d2;
        }}
        .index2 h2 {{
            background-color: #f3e5f5;
            color: #7b1fa2;
        }}
        .comparison-table {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .comparison-table h2 {{
            margin: 0 0 15px 0;
            color: #333;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: bold;
        }}
        .metric-name {{
            font-weight: bold;
            color: #495057;
        }}
        .value-better {{
            color: #28a745;
            font-weight: bold;
        }}
        .value-worse {{
            color: #dc3545;
            font-weight: bold;
        }}
        .value-same {{
            color: #6c757d;
        }}
        .diff-positive {{
            background-color: #d4edda;
            color: #155724;
        }}
        .diff-negative {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .tree {{
            font-family: 'Courier New', monospace;
            font-size: 12px;
            line-height: 1.4;
            max-height: 600px;
            overflow-y: auto;
        }}
        .tree-node {{
            margin: 1px 0;
            position: relative;
        }}
        .tree-toggle {{
            cursor: pointer;
            user-select: none;
            display: inline-block;
            width: 15px;
            text-align: center;
            font-weight: bold;
            color: #666;
            font-size: 10px;
        }}
        .tree-toggle:hover {{
            color: #333;
            background-color: #f0f0f0;
            border-radius: 3px;
        }}
        .tree-content {{
            display: inline-block;
            margin-left: 3px;
        }}
        .tree-children {{
            margin-left: 20px;
            border-left: 1px solid #e0e0e0;
            padding-left: 10px;
        }}
        .tree-children.collapsed {{
            display: none;
        }}
        .tree-key {{
            color: #0066cc;
            font-weight: bold;
        }}
        .tree-value {{
            color: #d73a49;
        }}
        .tree-type {{
            color: #6f42c1;
            font-style: italic;
            font-size: 10px;
        }}
        .tree-number {{
            color: #005cc5;
        }}
        .tree-string {{
            color: #032f62;
        }}
        .controls {{
            margin-bottom: 15px;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 5px;
        }}
        .btn {{
            background-color: #007bff;
            color: white;
            border: none;
            padding: 6px 12px;
            margin-right: 8px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
        }}
        .btn:hover {{
            background-color: #0056b3;
        }}
        .tabs {{
            display: flex;
            margin-bottom: 10px;
        }}
        .tab {{
            padding: 10px 20px;
            background-color: #e9ecef;
            border: none;
            cursor: pointer;
            border-radius: 5px 5px 0 0;
            margin-right: 5px;
        }}
        .tab.active {{
            background-color: #007bff;
            color: white;
        }}
        .tab-content {{
            display: none;
        }}
        .tab-content.active {{
            display: block;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>RediSearch Profile Comparison</h1>
        <div class="meta">
            <strong>Query:</strong> {query} |
            <strong>Indexes:</strong> {index1} vs {index2} |
            <strong>Generated:</strong> {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        </div>
    </div>

    <div class="comparison-table">
        <h2>📊 Performance Comparison</h2>
        {comparison_table}
    </div>

    <div class="tabs">
        <button class="tab active" onclick="showTab('comparison')">Side-by-Side</button>
        <button class="tab" onclick="showTab('index1')">Index 1 Only</button>
        <button class="tab" onclick="showTab('index2')">Index 2 Only</button>
    </div>

    <div id="comparison" class="tab-content active">
        <div class="comparison-container">
            <div class="index-panel index1">
                <h2>{index1}</h2>
                <div class="controls">
                    <button class="btn" onclick="expandAll('tree1')">Expand All</button>
                    <button class="btn" onclick="collapseAll('tree1')">Collapse All</button>
                </div>
                <div class="tree" id="tree1">
                    {tree_html1}
                </div>
            </div>

            <div class="index-panel index2">
                <h2>{index2}</h2>
                <div class="controls">
                    <button class="btn" onclick="expandAll('tree2')">Expand All</button>
                    <button class="btn" onclick="collapseAll('tree2')">Collapse All</button>
                </div>
                <div class="tree" id="tree2">
                    {tree_html2}
                </div>
            </div>
        </div>
    </div>

    <div id="index1" class="tab-content">
        <div class="index-panel">
            <h2>{index1} - Full Profile</h2>
            <div class="controls">
                <button class="btn" onclick="expandAll('tree1-full')">Expand All</button>
                <button class="btn" onclick="collapseAll('tree1-full')">Collapse All</button>
            </div>
            <div class="tree" id="tree1-full">
                {tree_html1}
            </div>
        </div>
    </div>

    <div id="index2" class="tab-content">
        <div class="index-panel">
            <h2>{index2} - Full Profile</h2>
            <div class="controls">
                <button class="btn" onclick="expandAll('tree2-full')">Expand All</button>
                <button class="btn" onclick="collapseAll('tree2-full')">Collapse All</button>
            </div>
            <div class="tree" id="tree2-full">
                {tree_html2}
            </div>
        </div>
    </div>

    <script>
        function toggleNode(element) {{
            const children = element.parentNode.querySelector('.tree-children');
            const toggle = element;

            if (children) {{
                if (children.classList.contains('collapsed')) {{
                    children.classList.remove('collapsed');
                    toggle.textContent = '▼';
                }} else {{
                    children.classList.add('collapsed');
                    toggle.textContent = '▶';
                }}
            }}
        }}

        function expandAll(treeId) {{
            const tree = document.getElementById(treeId);
            if (tree) {{
                const children = tree.querySelectorAll('.tree-children');
                const toggles = tree.querySelectorAll('.tree-toggle');

                children.forEach(child => child.classList.remove('collapsed'));
                toggles.forEach(toggle => {{
                    if (toggle.nextElementSibling && toggle.nextElementSibling.querySelector('.tree-children')) {{
                        toggle.textContent = '▼';
                    }}
                }});
            }}
        }}

        function collapseAll(treeId) {{
            const tree = document.getElementById(treeId);
            if (tree) {{
                const children = tree.querySelectorAll('.tree-children');
                const toggles = tree.querySelectorAll('.tree-toggle');

                children.forEach(child => child.classList.add('collapsed'));
                toggles.forEach(toggle => {{
                    if (toggle.nextElementSibling && toggle.nextElementSibling.querySelector('.tree-children')) {{
                        toggle.textContent = '▶';
                    }}
                }});
            }}
        }}

        function showTab(tabName) {{
            // Hide all tab contents
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));

            // Remove active class from all tabs
            const tabs = document.querySelectorAll('.tab');
            tabs.forEach(tab => tab.classList.remove('active'));

            // Show selected tab content
            document.getElementById(tabName).classList.add('active');

            // Add active class to clicked tab
            event.target.classList.add('active');
        }}

        // Initialize with some nodes collapsed for better overview
        document.addEventListener('DOMContentLoaded', function() {{
            const deepChildren = document.querySelectorAll('.tree-children .tree-children');
            deepChildren.forEach(child => {{
                child.classList.add('collapsed');
                const toggle = child.parentNode.querySelector('.tree-toggle');
                if (toggle) toggle.textContent = '▶';
            }});
        }});
    </script>
</body>
</html>"""

        return html_template

    def create_comparison_html_content(self, parsed_data1, parsed_data2,
                                     index1: str, index2: str, query: str,
                                     result_count1: int, result_count2: int):
        """Create HTML content with separate interactive graphs for each index"""
        # Generate separate interactive graphs for each index
        graph1_data = self.extract_graph_data(parsed_data1, index1)
        graph2_data = self.extract_graph_data(parsed_data2, index2)

        # Extract key metrics for comparison
        metrics1 = self.extract_key_metrics(parsed_data1)
        metrics2 = self.extract_key_metrics(parsed_data2)
        comparison_table = self.generate_comparison_table(metrics1, metrics2, index1, index2, result_count1, result_count2)

        # Generate leaf comparison table
        leaf_comparison_table = self.generate_leaf_comparison_table(graph1_data, graph2_data, index1, index2)

        html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RediSearch Profile Comparison - {index1} vs {index2}</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
            color: #333;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 24px;
        }}
        .header .meta {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .comparison-table {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .graphs-container {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 20px;
        }}
        .graph-panel {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .graph-panel h3 {{
            margin: 0 0 15px 0;
            padding: 10px;
            border-radius: 5px;
            text-align: center;
        }}
        .graph-panel.index1 h3 {{
            background-color: #e3f2fd;
            color: #1976d2;
        }}
        .graph-panel.index2 h3 {{
            background-color: #f3e5f5;
            color: #7b1fa2;
        }}
        .tree-graph {{
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
        .flow-node, .tree-node, .tree-node-small {{
            position: absolute;
            background: white;
            border: 2px solid #ddd;
            border-radius: 8px;
            padding: 12px;
            min-width: 120px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            cursor: pointer;
            transition: all 0.3s ease;
        }}
        .tree-node-small {{
            padding: 8px;
            min-width: 80px;
            font-size: 12px;
        }}
        .flow-node:hover, .tree-node:hover, .tree-node-small:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }}
        .tree-title {{
            position: absolute;
            font-weight: bold;
            font-size: 18px;
            color: #333;
        }}
        .tree-title.index1 {{
            color: #1976d2;
        }}
        .tree-title.index2 {{
            color: #7b1fa2;
        }}
        .index1 {{
            border-color: #1976d2;
            background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        }}
        .index2 {{
            border-color: #7b1fa2;
            background: linear-gradient(135deg, #f3e5f5 0%, #e1bee7 100%);
        }}
        .diff-better {{
            border-color: #4caf50;
            background: linear-gradient(135deg, #e8f5e8 0%, #c8e6c9 100%);
        }}
        .diff-worse {{
            border-color: #f44336;
            background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%);
        }}
        .diff-same {{
            border-color: #9e9e9e;
            background: linear-gradient(135deg, #f5f5f5 0%, #eeeeee 100%);
        }}
        .tree-connection {{
            position: absolute;
            background-color: #666;
        }}
        .tree-connection.index1 {{
            background-color: #1976d2;
        }}
        .tree-connection.index2 {{
            background-color: #7b1fa2;
        }}
        .tree-connection-svg {{
            position: absolute;
            color: #666;
        }}
        .tree-connection-svg.index1 {{
            color: #1976d2;
        }}
        .tree-connection-svg.index2 {{
            color: #7b1fa2;
        }}
        .tree-arrow {{
            position: absolute;
            width: 0;
            height: 0;
            border-left: 6px solid transparent;
            border-right: 6px solid transparent;
            border-top: 8px solid #666;
        }}
        .tree-arrow.index1 {{
            border-top-color: #1976d2;
        }}
        .tree-arrow.index2 {{
            border-top-color: #7b1fa2;
        }}
        .node-title {{
            font-weight: bold;
            font-size: 14px;
            margin-bottom: 8px;
            color: #333;
        }}
        .node-details {{
            font-size: 12px;
            color: #666;
        }}
        .node-comparison {{
            font-size: 11px;
            margin-top: 6px;
            padding-top: 6px;
            border-top: 1px solid #ddd;
        }}
        .value-better {{
            color: #4caf50;
            font-weight: bold;
        }}
        .value-worse {{
            color: #f44336;
            font-weight: bold;
        }}
        .value-same {{
            color: #666;
        }}
        .flow-connection {{
            position: absolute;
            border-left: 2px solid #666;
            border-bottom: 2px solid #666;
        }}
        .flow-connection.better {{
            border-color: #4caf50;
        }}
        .flow-connection.worse {{
            border-color: #f44336;
        }}
        .flow-arrow {{
            position: absolute;
            width: 0;
            height: 0;
            border-left: 6px solid transparent;
            border-right: 6px solid transparent;
            border-top: 8px solid #666;
        }}
        .flow-arrow.better {{
            border-top-color: #4caf50;
        }}
        .flow-arrow.worse {{
            border-top-color: #f44336;
        }}
        .legend {{
            margin-top: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 5px;
        }}
        .legend-item {{
            display: inline-block;
            margin-right: 20px;
            margin-bottom: 5px;
        }}
        .legend-color {{
            display: inline-block;
            width: 20px;
            height: 15px;
            border-radius: 3px;
            margin-right: 8px;
            vertical-align: middle;
        }}
        .controls {{
            margin-bottom: 15px;
            padding: 10px;
            background-color: #f8f9fa;
            border-radius: 5px;
        }}
        .btn {{
            background-color: #007bff;
            color: white;
            border: none;
            padding: 8px 16px;
            margin-right: 10px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }}
        .btn:hover {{
            background-color: #0056b3;
        }}
        .controls label {{
            display: inline-flex;
            align-items: center;
            font-size: 14px;
            color: #333;
            cursor: pointer;
        }}
        .controls input[type="checkbox"] {{
            margin-right: 5px;
            transform: scale(1.1);
        }}
        .leaf-comparison-table {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .sortable-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
        }}
        .sortable-table th {{
            background-color: #f8f9fa;
            padding: 12px 8px;
            text-align: left;
            border-bottom: 2px solid #dee2e6;
            cursor: pointer;
            user-select: none;
            position: relative;
            font-weight: bold;
            font-size: 13px;
        }}
        .sortable-table th:hover {{
            background-color: #e9ecef;
        }}
        .sortable-table td {{
            padding: 10px 8px;
            border-bottom: 1px solid #dee2e6;
            font-size: 13px;
        }}
        .sortable-table tr:hover {{
            background-color: #f8f9fa;
        }}
        .sort-arrow {{
            font-size: 12px;
            color: #6c757d;
            margin-left: 5px;
        }}
        .number-cell {{
            text-align: right;
            font-family: 'Courier New', monospace;
        }}
        .term-cell {{
            font-weight: bold;
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        .positive {{
            color: #dc3545;
            font-weight: bold;
        }}
        .negative {{
            color: #28a745;
            font-weight: bold;
        }}
        .neutral {{
            color: #6c757d;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: bold;
        }}
        .metric-name {{
            font-weight: bold;
            color: #495057;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>RediSearch Profile Comparison Flow</h1>
        <div class="meta">
            <strong>Query:</strong> {query} |
            <strong>Indexes:</strong> {index1} vs {index2} |
            <strong>Generated:</strong> {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        </div>
    </div>

    <div class="comparison-table">
        <h2>📊 Performance Metrics Comparison</h2>
        {comparison_table}
    </div>

    <div class="comparison-table">
        {leaf_comparison_table}
    </div>

    <div class="graphs-container">
        <div class="graph-panel index1">
            <h3>{index1}</h3>
            <div class="controls">
                <button class="btn" onclick="expandAll('graph1')">Expand All</button>
                <button class="btn" onclick="collapseAll('graph1')">Collapse All</button>
                <button class="btn" onclick="resetZoom('graph1')">Reset Zoom</button>
                <label style="margin-left: 15px;">
                    <input type="checkbox" id="showLabels1" checked onchange="toggleLabels('graph1')">
                    Show Labels
                </label>
                <label style="margin-left: 15px;">
                    Hide nodes with time ≤
                    <input type="number" id="timeThreshold1" value="0" min="0" step="0.1"
                           style="width: 80px; margin: 0 5px;" onchange="filterByTime('graph1')">
                    ms
                </label>
            </div>
            <svg class="tree-graph" id="graph1"></svg>
        </div>

        <div class="graph-panel index2">
            <h3>{index2}</h3>
            <div class="controls">
                <button class="btn" onclick="expandAll('graph2')">Expand All</button>
                <button class="btn" onclick="collapseAll('graph2')">Collapse All</button>
                <button class="btn" onclick="resetZoom('graph2')">Reset Zoom</button>
                <label style="margin-left: 15px;">
                    <input type="checkbox" id="showLabels2" checked onchange="toggleLabels('graph2')">
                    Show Labels
                </label>
                <label style="margin-left: 15px;">
                    Hide nodes with time ≤
                    <input type="number" id="timeThreshold2" value="0" min="0" step="0.1"
                           style="width: 80px; margin: 0 5px;" onchange="filterByTime('graph2')">
                    ms
                </label>
            </div>
            <svg class="tree-graph" id="graph2"></svg>
        </div>
    </div>

    <script>
        // Graph data for both indexes
        const graph1Data = {self.format_graph_data_for_js(graph1_data)};
        const graph2Data = {self.format_graph_data_for_js(graph2_data)};

        // Color schemes for different node types
        const colorSchemes = {{
            iterator: {{
                'UNION': '#ff6b6b',
                'INTERSECT': '#4ecdc4',
                'TEXT': '#45b7d1',
                'NUMERIC': '#96ceb4',
                'TAG': '#feca57',
                'GEO': '#ff9ff3',
                'default': '#95a5a6'
            }},
            processor: {{
                'Index': '#e17055',
                'Loader': '#74b9ff',
                'Pager': '#a29bfe',
                'Limiter': '#fd79a8',
                'Sorter': '#fdcb6e',
                'Scorer': '#6c5ce7',
                'default': '#636e72'
            }}
        }};

        // Create interactive tree graphs
        document.addEventListener('DOMContentLoaded', function() {{
            createTreeGraph('graph1', graph1Data, '#1976d2');
            createTreeGraph('graph2', graph2Data, '#7b1fa2');
            createLegend();
        }});

        function createTreeGraph(containerId, data, color) {{
            const container = d3.select(`#${{containerId}}`);
            const width = container.node().clientWidth;
            const height = container.node().clientHeight;

            // Clear any existing content
            container.selectAll("*").remove();

            // Create SVG with zoom behavior
            const svg = container
                .attr("viewBox", [0, 0, width, height])
                .style("font", "12px sans-serif");

            const g = svg.append("g");

            // Create zoom behavior
            const zoom = d3.zoom()
                .scaleExtent([0.1, 3])
                .on("zoom", (event) => {{
                    g.attr("transform", event.transform);
                }});

            svg.call(zoom);

            // Create tree layout
            const tree = d3.tree().size([height - 100, width - 200]);
            const root = d3.hierarchy(data);

            // Collapse nodes initially (except root and first level)
            root.descendants().forEach(d => {{
                if (d.depth > 1) {{
                    d._children = d.children;
                    d.children = null;
                }}
            }});

            update(root);

            function update(source) {{
                const treeData = tree(root);
                const nodes = treeData.descendants();
                const links = treeData.descendants().slice(1);

                // Update nodes
                const node = g.selectAll('.node')
                    .data(nodes, d => d.id || (d.id = ++i));

                const nodeEnter = node.enter().append('g')
                    .attr('class', 'node')
                    .attr('transform', d => `translate(${{source.y0 || 0}},${{source.x0 || 0}})`)
                    .on('click', click);

                // Add circles for nodes with type-based colors
                nodeEnter.append('circle')
                    .attr('r', 1e-6)
                    .style('fill', d => {{
                        if (d.data.type === 'iterator') {{
                            return colorSchemes.iterator[d.data.subtype] || colorSchemes.iterator.default;
                        }} else if (d.data.type === 'processor') {{
                            return colorSchemes.processor[d.data.subtype] || colorSchemes.processor.default;
                        }}
                        return d._children ? color : '#fff';
                    }})
                    .style('stroke', d => {{
                        if (d.data.type === 'iterator') {{
                            return colorSchemes.iterator[d.data.subtype] || colorSchemes.iterator.default;
                        }} else if (d.data.type === 'processor') {{
                            return colorSchemes.processor[d.data.subtype] || colorSchemes.processor.default;
                        }}
                        return color;
                    }})
                    .style('stroke-width', '2px');

                // Add labels
                nodeEnter.append('text')
                    .attr('dy', '.35em')
                    .attr('x', d => d.children || d._children ? -13 : 13)
                    .style('text-anchor', d => d.children || d._children ? 'end' : 'start')
                    .style('font-size', '11px')
                    .style('fill', '#333')
                    .text(d => d.data.name);

                // Add details on hover
                nodeEnter.append('title')
                    .text(d => d.data.details || d.data.name);

                // Transition nodes to their new position
                const nodeUpdate = nodeEnter.merge(node);

                nodeUpdate.transition()
                    .duration(750)
                    .attr('transform', d => `translate(${{d.y}},${{d.x}})`);

                nodeUpdate.select('circle')
                    .transition()
                    .duration(750)
                    .attr('r', 8)
                    .style('fill', d => {{
                        if (d.data.type === 'iterator') {{
                            return colorSchemes.iterator[d.data.subtype] || colorSchemes.iterator.default;
                        }} else if (d.data.type === 'processor') {{
                            return colorSchemes.processor[d.data.subtype] || colorSchemes.processor.default;
                        }}
                        return d._children ? color : '#fff';
                    }});

                // Remove exiting nodes
                const nodeExit = node.exit().transition()
                    .duration(750)
                    .attr('transform', d => `translate(${{source.y}},${{source.x}})`)
                    .remove();

                nodeExit.select('circle')
                    .attr('r', 1e-6);

                nodeExit.select('text')
                    .style('fill-opacity', 1e-6);

                // Update links
                const link = g.selectAll('.link')
                    .data(links, d => d.id);

                const linkEnter = link.enter().insert('path', 'g')
                    .attr('class', 'link')
                    .style('fill', 'none')
                    .style('stroke', color)
                    .style('stroke-width', '2px')
                    .attr('d', d => {{
                        const o = {{x: source.x0 || 0, y: source.y0 || 0}};
                        return diagonal(o, o);
                    }});

                const linkUpdate = linkEnter.merge(link);

                linkUpdate.transition()
                    .duration(750)
                    .attr('d', d => diagonal(d, d.parent));

                link.exit().transition()
                    .duration(750)
                    .attr('d', d => {{
                        const o = {{x: source.x, y: source.y}};
                        return diagonal(o, o);
                    }})
                    .remove();

                // Store old positions for transition
                nodes.forEach(d => {{
                    d.x0 = d.x;
                    d.y0 = d.y;
                }});
            }}

            function click(event, d) {{
                if (d.children) {{
                    d._children = d.children;
                    d.children = null;
                }} else {{
                    d.children = d._children;
                    d._children = null;
                }}
                update(d);
            }}

            function diagonal(s, d) {{
                return `M ${{s.y}} ${{s.x}}
                        C ${{(s.y + d.y) / 2}} ${{s.x}},
                          ${{(s.y + d.y) / 2}} ${{d.x}},
                          ${{d.y}} ${{d.x}}`;
            }}

            // Store references for control functions
            window[`${{containerId}}_root`] = root;
            window[`${{containerId}}_update`] = update;
        }}

        let i = 0; // Counter for node IDs

        function expandAll(graphId) {{
            const root = window[`${{graphId}}_root`];
            const update = window[`${{graphId}}_update`];

            root.descendants().forEach(d => {{
                if (d._children) {{
                    d.children = d._children;
                    d._children = null;
                }}
            }});
            update(root);
        }}

        function collapseAll(graphId) {{
            const root = window[`${{graphId}}_root`];
            const update = window[`${{graphId}}_update`];

            root.descendants().forEach(d => {{
                if (d.children && d.depth > 0) {{
                    d._children = d.children;
                    d.children = null;
                }}
            }});
            update(root);
        }}

        function resetZoom(graphId) {{
            d3.select(`#${{graphId}}`).transition().duration(750).call(
                d3.zoom().transform,
                d3.zoomIdentity
            );
        }}

        function toggleLabels(graphId) {{
            const checkbox = document.getElementById(`showLabels${{graphId.slice(-1)}}`);
            const isVisible = checkbox.checked;

            d3.select(`#${{graphId}}`)
                .selectAll('text')
                .transition()
                .duration(300)
                .style('opacity', isVisible ? 1 : 0);
        }}

        function filterByTime(graphId) {{
            const thresholdInput = document.getElementById(`timeThreshold${{graphId.slice(-1)}}`);
            const threshold = parseFloat(thresholdInput.value) || 0;

            console.log(`Filtering ${{graphId}} with threshold: ${{threshold}}`);

            d3.select(`#${{graphId}}`)
                .selectAll('g.node')
                .each(function(d) {{
                    const node = d3.select(this);
                    const nodeData = d.data;

                    // Extract time from node details - try multiple patterns
                    let nodeTime = 0;
                    if (nodeData && nodeData.details) {{
                        console.log(`Node details: ${{nodeData.details}}`);

                        // Try different time patterns
                        const timePatterns = [
                            /Time:\\s*([0-9.]+)/,           // Time: 123.45
                            /Time:\\s*([0-9.]+)\\s*ms/,     // Time: 123.45 ms
                            /Time:\\s*([0-9.]+)\\s*$/m,     // Time: 123.45 (end of line)
                            /([0-9.]+)\\s*ms/,              // 123.45 ms
                            /Time[^0-9]*([0-9.]+)/          // Time (any chars) 123.45
                        ];

                        for (const pattern of timePatterns) {{
                            const timeMatch = nodeData.details.match(pattern);
                            if (timeMatch) {{
                                nodeTime = parseFloat(timeMatch[1]);
                                console.log(`Found time ${{nodeTime}} for node ${{nodeData.name}}`);
                                break;
                            }}
                        }}

                        if (nodeTime === 0) {{
                            console.log(`No time found for node ${{nodeData.name}}`);
                        }}
                    }}

                    // Hide/show node based on time threshold
                    const shouldHide = nodeTime > 0 && nodeTime <= threshold;
                    console.log(`Node ${{nodeData.name}}: time=${{nodeTime}}, threshold=${{threshold}}, shouldHide=${{shouldHide}}`);

                    node.transition()
                        .duration(300)
                        .style('opacity', shouldHide ? 0.1 : 1)
                        .style('pointer-events', shouldHide ? 'none' : 'all');
                }});
        }}

        function createLegend() {{
            const legendContainer = d3.select('body').append('div')
                .attr('class', 'legend-container')
                .style('position', 'fixed')
                .style('top', '20px')
                .style('right', '20px')
                .style('background', 'white')
                .style('border', '1px solid #ddd')
                .style('border-radius', '5px')
                .style('padding', '15px')
                .style('box-shadow', '0 2px 4px rgba(0,0,0,0.1)')
                .style('font-size', '12px')
                .style('z-index', '1000');

            legendContainer.append('h4')
                .text('Node Types')
                .style('margin', '0 0 10px 0')
                .style('font-size', '14px');

            // Iterator types
            const iteratorSection = legendContainer.append('div')
                .style('margin-bottom', '10px');

            iteratorSection.append('div')
                .text('Iterators:')
                .style('font-weight', 'bold')
                .style('margin-bottom', '5px');

            Object.entries(colorSchemes.iterator).forEach(([type, color]) => {{
                if (type !== 'default') {{
                    const item = iteratorSection.append('div')
                        .style('display', 'flex')
                        .style('align-items', 'center')
                        .style('margin-bottom', '3px');

                    item.append('div')
                        .style('width', '12px')
                        .style('height', '12px')
                        .style('background-color', color)
                        .style('border-radius', '50%')
                        .style('margin-right', '8px');

                    item.append('span').text(type);
                }}
            }});

            // Processor types
            const processorSection = legendContainer.append('div');

            processorSection.append('div')
                .text('Processors:')
                .style('font-weight', 'bold')
                .style('margin-bottom', '5px');

            Object.entries(colorSchemes.processor).forEach(([type, color]) => {{
                if (type !== 'default') {{
                    const item = processorSection.append('div')
                        .style('display', 'flex')
                        .style('align-items', 'center')
                        .style('margin-bottom', '3px');

                    item.append('div')
                        .style('width', '12px')
                        .style('height', '12px')
                        .style('background-color', color)
                        .style('border-radius', '50%')
                        .style('margin-right', '8px');

                    item.append('span').text(type);
                }}
            }});
        }}

        function sortTable(columnIndex, dataType) {{
            const table = document.getElementById('leafComparisonTable');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));

            // Determine sort direction
            const currentDirection = table.getAttribute('data-sort-direction') || 'asc';
            const newDirection = currentDirection === 'asc' ? 'desc' : 'asc';
            table.setAttribute('data-sort-direction', newDirection);

            // Update sort arrows
            const headers = table.querySelectorAll('th .sort-arrow');
            headers.forEach(arrow => arrow.textContent = '↕');
            const currentHeader = table.querySelectorAll('th')[columnIndex].querySelector('.sort-arrow');
            currentHeader.textContent = newDirection === 'asc' ? '↑' : '↓';

            // Sort rows
            rows.sort((a, b) => {{
                const cellA = a.cells[columnIndex].textContent.trim();
                const cellB = b.cells[columnIndex].textContent.trim();

                let valueA, valueB;

                if (dataType === 'number') {{
                    valueA = parseFloat(cellA) || 0;
                    valueB = parseFloat(cellB) || 0;
                }} else {{
                    valueA = cellA.toLowerCase();
                    valueB = cellB.toLowerCase();
                }}

                if (valueA < valueB) return newDirection === 'asc' ? -1 : 1;
                if (valueA > valueB) return newDirection === 'asc' ? 1 : -1;
                return 0;
            }});

            // Reorder rows in table
            rows.forEach(row => tbody.appendChild(row));
        }}
    </script>
</body>
</html>"""

        return html_template

    def extract_graph_data(self, parsed_data, index_name):
        """Extract hierarchical graph data from parsed profile data"""
        print(f"🔍 Extracting graph data for {index_name}")
        print(f"   Parsed data type: {type(parsed_data)}")

        if not isinstance(parsed_data, dict):
            print(f"   ⚠ Data is not a dictionary, returning empty graph")
            return {"name": "No Data", "children": []}

        print(f"   Available keys: {list(parsed_data.keys())}")

        # Check if this is a sharded/distributed setup
        if 'Shards' in parsed_data or 'Coordinator' in parsed_data:
            print(f"   🔍 Detected sharded/distributed setup")
            shards_data = str(parsed_data.get('Shards', 'None'))[:100]
            coordinator_data = str(parsed_data.get('Coordinator', 'None'))[:100]
            print(f"   Shards data: {shards_data}...")
            print(f"   Coordinator data: {coordinator_data}...")

            # Try to extract profile from coordinator or first shard
            profile_data = None
            if 'Coordinator' in parsed_data and parsed_data['Coordinator']:
                print(f"   Using Coordinator data")
                profile_data = parsed_data['Coordinator']
            elif 'Shards' in parsed_data and parsed_data['Shards']:
                print(f"   Using first Shard data")
                shards = parsed_data['Shards']
                if isinstance(shards, list) and len(shards) > 0:
                    profile_data = shards[0]
                elif isinstance(shards, dict):
                    profile_data = shards

            if profile_data:
                print(f"   Profile data type: {type(profile_data)}")
                if isinstance(profile_data, dict):
                    #print(f"   Profile data keys: {list(profile_data.keys())}")
                    content_str = str(profile_data)[:200]
                    print(f"   Profile data content: {content_str}...")
                else:
                    print(f"   Profile data: {str(profile_data)[:100]}...")
                return self.extract_graph_data(profile_data, f"{index_name} (from shard/coordinator)")
            else:
                print(f"   ✗ Could not find profile data in sharded structure")
                return {"name": f"No Profile Data ({index_name})", "children": []}

        # Root node
        total_time = parsed_data.get('Total profile time')
        root = {
            "name": f"Query Profile ({index_name})",
            "details": f"Total Time: {total_time}ms",
            "children": []
        }
        print(f"   Created root node with total time: {total_time}ms")

        # Add timing information - check if keys exist
        parsing_time = parsed_data.get('Parsing time')
        pipeline_time = parsed_data.get('Pipeline creation time')

        print(f"   Timing data - Parsing: {parsing_time}ms, Pipeline: {pipeline_time}ms")

        if parsing_time is None:
            print(f"   ✗ MISSING 'Parsing time' in parsed data")
        if pipeline_time is None:
            print(f"   ✗ MISSING 'Pipeline creation time' in parsed data")

        timing_node = {
            "name": "Timing",
            "details": "Query execution timing breakdown",
            "children": [
                {
                    "name": f"Parsing: {parsing_time}ms",
                    "details": f"Time spent parsing the query: {parsing_time}ms"
                },
                {
                    "name": f"Pipeline: {pipeline_time}ms",
                    "details": f"Time spent creating execution pipeline: {pipeline_time}ms"
                }
            ]
        }
        root["children"].append(timing_node)
        print(f"   Added timing node with {len(timing_node['children'])} children")

        # Add iterator tree
        iterator_profile = parsed_data.get('Iterators profile', {})
        print(f"   Iterator profile type: {type(iterator_profile)}")

        if isinstance(iterator_profile, dict):
            print(f"   Iterator profile keys: {list(iterator_profile.keys())}")
            print(f"   Building iterator tree from dict...")
            iterator_node = self.build_iterator_tree(iterator_profile)
            if iterator_node:
                root["children"].append(iterator_node)
                print(f"   Added iterator node: {iterator_node['name']}")
            else:
                print(f"   ⚠ Iterator tree building returned None")
        elif isinstance(iterator_profile, list):
            print(f"   Iterator profile is a list with {len(iterator_profile)} items")
            iter_str = str(iterator_profile)[:200]
            print(f"   Iterator profile content: {iter_str}...")
            print(f"   Building iterator tree from list...")
            iterator_node = self.build_iterator_tree_from_list(iterator_profile)
            if iterator_node:
                root["children"].append(iterator_node)
                print(f"   Added iterator node: {iterator_node['name']}")
            else:
                print(f"   ⚠ Iterator tree building from list returned None")
        else:
            print(f"   ⚠ Iterator profile is neither dict nor list: {type(iterator_profile)}")

        # Add result processors
        processors_profile = parsed_data.get('Result processors profile', [])
        print(f"   Result processors profile type: {type(processors_profile)}")
        proc_str = str(processors_profile)[:150]
        print(f"   Result processors content: {proc_str}...")

        if isinstance(processors_profile, list) and processors_profile:
            print(f"   Processing {len(processors_profile)} result processors")
            processors_node = {
                "name": "Result Processors",
                "details": f"Processing pipeline with {len(processors_profile)} stages",
                "children": []
            }

            for i, proc in enumerate(processors_profile):
                proc_str = str(proc)[:80]
                print(f"     Processor {i}: {proc_str}...")
                if isinstance(proc, dict):
                    # Check for required fields without fallbacks
                    if 'Type' not in proc:
                        print(f"       ✗ MISSING 'Type' in processor {i}: {proc}")
                        continue

                    proc_type = proc['Type']
                    proc_counter = proc.get('Counter')  # May be None
                    print(f"       Type: {proc_type}, Counter: {proc_counter}")

                    proc_node = {
                        "name": f"{proc_type}",
                        "type": "processor",
                        "subtype": proc_type,
                        "details": f"Type: {proc_type}\\nCounter: {proc_counter} documents"
                    }
                    processors_node["children"].append(proc_node)
                    print(f"       Added processor node: {proc_node['name']}")
                else:
                    print(f"       ✗ Processor {i} is not a dict: {type(proc)}")

            root["children"].append(processors_node)
            print(f"   Added processors node with {len(processors_node['children'])} children")
        else:
            print(f"   ⚠ No result processors or not a list")

        # Final summary
        print(f"   📊 Final graph structure for {index_name}:")
        print(f"     Root: {root['name']}")
        print(f"     Children: {len(root['children'])}")
        for i, child in enumerate(root['children']):
            child_count = len(child.get('children', []))
            print(f"       {i+1}. {child['name']} ({child_count} children)")

        return root

    def build_iterator_tree(self, iterator_data):
        """Build iterator tree structure recursively"""
        data_str = str(iterator_data)[:100]
        print(f"     Building iterator tree from: {data_str}...")

        if not isinstance(iterator_data, dict):
            print(f"     ⚠ Iterator data is not a dict: {type(iterator_data)}")
            return None

        print(f"     Iterator data keys: {list(iterator_data.keys())}")

        # Extract data without fallbacks - fail if missing
        if 'Type' not in iterator_data:
            print(f"     ✗ MISSING 'Type' in iterator data: {iterator_data}")
            return None

        iter_type = iterator_data['Type']
        iter_counter = iterator_data.get('Counter')  # Legacy fallback
        iter_term = iterator_data.get('Term')  # May be None
        iter_size = iterator_data.get('Size')  # May be None

        print(f"     Extracted - Type: {iter_type}, Counter: {iter_counter}, Term: {iter_term}, Size: {iter_size}")

        # Check for missing critical fields
        if iter_counter is None:
            print(f"     ⚠ No Counter found for {iter_type}")
        if iter_size is None:
            print(f"     ⚠ No Size found for {iter_type}")

        # Build details string with all available fields
        details_parts = [
            f"Type: {iter_type}",
            f"Term: {iter_term}",
            f"Counter: {iter_counter}",
            f"Size: {iter_size}"
        ]

        # Create main iterator node with type for coloring
        iterator_node = {
            "name": f"{iter_type}",
            "type": "iterator",
            "subtype": iter_type,
            "details": "\n".join(details_parts),
            "children": []
        }
        print(f"     Created iterator node: {iterator_node['name']}")

        # Add child iterators if they exist
        child_iterators = iterator_data.get('Child iterators', [])
        child_str = str(child_iterators)[:100]
        print(f"     Child iterators: {child_str}...")

        if isinstance(child_iterators, list):
            print(f"     Processing {len(child_iterators)} child iterators")
            for i, child in enumerate(child_iterators):
                child_str = str(child)[:80]
                print(f"       Child {i}: {child_str}...")
                if isinstance(child, dict):
                    child_node = self.build_iterator_tree(child)
                    if child_node:
                        iterator_node["children"].append(child_node)
                        print(f"       Added child node: {child_node['name']}")
                else:
                    print(f"       ⚠ Child {i} is not a dict: {type(child)}")
        else:
            print(f"     ⚠ Child iterators is not a list: {type(child_iterators)}")

        # Handle single child iterator
        child_iterator = iterator_data.get('Child iterator', {})
        if isinstance(child_iterator, dict) and child_iterator:
            child_node = self.build_iterator_tree(child_iterator)
            if child_node:
                iterator_node["children"].append(child_node)

        # Sort children by Counter (higher count = higher position)
        iterator_node = self.sort_children_by_counter(iterator_node)

        # Calculate missing Size for UNION/INTERSECT iterators
        iterator_node = self.calculate_missing_iterator_size(iterator_node)

        return iterator_node

    def sort_children_by_counter(self, iterator_node):
        """Sort child iterators by Counter (higher count = higher position)"""
        if not iterator_node.get('children'):
            return iterator_node

        def get_counter(child):
            """Extract Counter from child iterator"""
            details = child.get('details', '')
            parsed_details = self.parse_details_string(details)
            read_counter = parsed_details.get('Counter')
            return read_counter if read_counter is not None else 0

        # Sort children by Counter in descending order (highest first)
        sorted_children = sorted(iterator_node['children'], key=get_counter, reverse=True)
        iterator_node['children'] = sorted_children

        if len(sorted_children) > 1:
            print(f"     Sorted {len(sorted_children)} children by Counter:")
            for i, child in enumerate(sorted_children):
                read_count = get_counter(child)
                print(f"       {i+1}. {child['name']} (Counter: {read_count})")

        return iterator_node



    def calculate_missing_iterator_size(self, iterator_node):
        """Calculate missing Size for UNION/INTERSECT iterators (recursive)"""
        # First, recursively process all children
        for child in iterator_node.get('children', []):
            self.calculate_missing_iterator_size(child)

        # Check if this node needs Size calculation
        iterator_type = iterator_node.get('subtype')
        if iterator_type not in ['UNION', 'INTERSECT'] or not iterator_node.get('children'):
            return iterator_node

        # Check if Size is already present and valid
        current_details = iterator_node.get('details', '')
        parsed_details = self.parse_details_string(current_details)
        current_size = parsed_details.get('Size')

        if current_size is not None and current_size > 0:
            print(f"     {iterator_type} already has Size: {current_size}")
            return iterator_node

        # Calculate size based on children
        calculated_size = None
        valid_children = 0

        if iterator_type == 'UNION':
            # For UNION: sum of all children sizes
            total_size = 0
            for child in iterator_node['children']:
                child_details = child.get('details', '')
                child_parsed = self.parse_details_string(child_details)
                child_size = child_parsed.get('Size')
                if child_size is not None and child_size > 0:
                    total_size += child_size
                    valid_children += 1

            if valid_children > 0:
                calculated_size = total_size
                print(f"     Calculated UNION size: {calculated_size} (sum of {valid_children} children)")

        elif iterator_type == 'INTERSECT':
            # For INTERSECT: we could use minimum of children sizes or leave as None
            # Since INTERSECT result size depends on actual intersection, not just sum
            print(f"     INTERSECT size calculation not implemented (depends on actual intersection)")
            return iterator_node

        # Update the node's details with calculated size
        if calculated_size is not None:
            lines = current_details.split('\\n')
            updated_lines = []
            size_updated = False

            for line in lines:
                if line.startswith('Size:'):
                    if iterator_type == 'UNION':
                        updated_lines.append(f"Size: {calculated_size} (sum of {valid_children} children)")
                    else:
                        updated_lines.append(f"Size: {calculated_size} (calculated)")
                    size_updated = True
                else:
                    updated_lines.append(line)

            # If no Size line existed, add it
            if not size_updated:
                if iterator_type == 'UNION':
                    updated_lines.append(f"Size: {calculated_size} (sum of {valid_children} children)")
                else:
                    updated_lines.append(f"Size: {calculated_size} (calculated)")

            iterator_node['details'] = '\\n'.join(updated_lines)

        return iterator_node

    def parse_details_string(self, details):
        """Parse details string into key-value dictionary"""
        parsed = {}
        if not details:
            return parsed

        lines = details.split('\n')
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip()
                value = value.strip()

                # Handle special cases and type conversion
                if key in ['Time']:
                    if value == 'N/A':
                        parsed[key] = None
                    else:
                        try:
                            parsed[key] = float(value)
                            print(f"       Parsed {key}: {parsed[key]}")
                        except ValueError as e:
                            print(f"       ✗ Failed to parse {key} '{value}': {e}")
                            parsed[key] = None
                elif key in ['Counter', 'Size']:
                    if value == 'N/A':
                        parsed[key] = None
                    else:
                        # Handle special Size formats like "2856 (sum of 3 children)"
                        if key == 'Size' and '(' in value:
                            value = value.split('(')[0].strip()
                        try:
                            parsed[key] = int(value)
                            print(f"       Parsed {key}: {parsed[key]}")
                        except ValueError as e:
                            print(f"       ✗ Failed to parse {key} '{value}': {e}")
                            parsed[key] = None
                else:
                    # String fields like Term, Type, Query type
                    parsed[key] = value if value != 'N/A' else None
                    if parsed[key] is not None:
                        print(f"       Parsed {key}: '{parsed[key]}'")
                    else:
                        print(f"       {key} is N/A")

        return parsed

    def build_iterator_tree_from_list(self, iterator_list):
        """Build iterator tree from list format like ['Type', 'UNION', 'Query type', 'UNION', ...]"""
        print(f"     Building iterator tree from list with {len(iterator_list)} items")
        list_str = str(iterator_list)[:150]
        print(f"     List content: {list_str}...")

        if not isinstance(iterator_list, list) or len(iterator_list) < 2:
            short_list = str(iterator_list)[:50]
            print(f"     ✗ Invalid iterator list: {short_list}...")
            return None

        # Parse the entire list to understand its structure
        parsed_data = {}
        child_iterators = []
        i = 0

        while i < len(iterator_list):
            item = iterator_list[i]
            item_str = str(item)[:30]
            print(f"       Item {i}: {item_str}... ({type(item).__name__})")

            # If it's a string followed by a value, treat as key-value pair
            if isinstance(item, str) and i + 1 < len(iterator_list):
                next_item = iterator_list[i + 1]

                # Check if next item is a simple value (not a list/dict)
                if not isinstance(next_item, (list, dict)):
                    parsed_data[item] = next_item
                    print(f"         Parsed: {item} = {next_item}")
                    i += 2
                    continue
                # If next item is a list/dict, it might be child iterator data or special field
                elif isinstance(next_item, list):
                    next_str = str(next_item)[:60]
                    print(f"         Found list after '{item}': {next_str}...")
                    if 'child' in item.lower() or 'iterator' in item.lower():
                        child_iterators.append(next_item)
                        print(f"         Added as child iterator")
                    else:
                        print(f"         Skipping list field '{item}' (not child iterator or history)")
                    i += 2
                    continue

            # If it's a list by itself, might be a child iterator or nested iterator structure
            elif isinstance(item, list):
                item_list_str = str(item)[:60]
                print(f"         Found standalone list: {item_list_str}...")

                # Check if this list contains iterator data (starts with 'Type' or contains nested lists)
                if len(item) > 0 and (
                    (isinstance(item[0], str) and item[0] == 'Type') or  # Direct iterator
                    any(isinstance(x, list) for x in item)  # Contains nested iterators
                ):
                    child_iterators.append(item)
                    print(f"         Added as child iterator")
                else:
                    print(f"         Skipping non-iterator list")
                i += 1
                continue

            # Skip unrecognized items
            i += 1

        print(f"     Parsed main data: {parsed_data}")
        print(f"     Found {len(child_iterators)} potential child iterators")

        # Extract main iterator info
        iter_type = parsed_data.get('Type')
        if not iter_type:
            print(f"     ✗ No 'Type' found in parsed iterator data")
            return None

        query_type = parsed_data.get('Query type')
        time_val = parsed_data.get('Time')
        counter = parsed_data.get('Counter')  # Legacy fallback
        term = parsed_data.get('Term') or parsed_data.get('term')
        size = parsed_data.get('Size')  # May be None

        # Check for missing fields
        if query_type is None:
            print(f"     ⚠ No Query type found for {iter_type}")
        if time_val is None:
            print(f"     ⚠ No Time found for {iter_type}")
        if term is None:
            print(f"     ⚠ No Term found for {iter_type}")

        # Build details string with all available fields
        details_parts = [
            f"Type: {iter_type}",
            f"Term: {term}",
            f"Query type: {query_type}",
            f"Time: {time_val}",
            f"Counter: {counter}",
        ]

        if size is not None:
            details_parts.append(f"Size: {size}")

        # Create iterator node with type for coloring
        iterator_node = {
            "name": f"{iter_type}",
            "type": "iterator",
            "subtype": iter_type,
            "details": "\n".join(details_parts),
            "children": []
        }

        print(f"     Created iterator node: {iterator_node['name']}")

        # Process child iterators recursively
        for idx, child_data in enumerate(child_iterators):
            child_str = str(child_data)[:80]
            print(f"       Processing child {idx}: {child_str}...")

            self._process_child_iterator_data(child_data, iterator_node, idx)

        print(f"     Iterator {iter_type} has {len(iterator_node['children'])} children")

        # Sort children by Counter (higher count = higher position)
        iterator_node = self.sort_children_by_counter(iterator_node)

        # Calculate missing Size for UNION/INTERSECT iterators
        iterator_node = self.calculate_missing_iterator_size(iterator_node)

        return iterator_node

    def _process_child_iterator_data(self, child_data, parent_node, idx):
        """Recursively process child iterator data"""
        if not isinstance(child_data, list):
            print(f"         ⚠ Child {idx} is not a list")
            return

        # Check if this is a list of iterator lists
        if len(child_data) > 0 and isinstance(child_data[0], list):
            print(f"         Child {idx} contains {len(child_data)} sub-iterators")
            for sub_idx, sub_list in enumerate(child_data):
                sub_str = str(sub_list)[:60]
                print(f"           Sub-iterator {sub_idx}: {sub_str}...")
                self._process_child_iterator_data(sub_list, parent_node, f"{idx}.{sub_idx}")

        # Check if this is a direct iterator (starts with 'Type')
        elif len(child_data) >= 2 and child_data[0] == 'Type':
            print(f"         Building iterator from child {idx}")
            child_node = self.build_iterator_tree_from_list(child_data)
            if child_node:
                parent_node["children"].append(child_node)
                print(f"         Added child: {child_node['name']}")
            else:
                print(f"         ⚠ Failed to build iterator from child {idx}")

        # Check if this contains mixed data (key-value pairs + nested lists)
        else:
            print(f"         Parsing mixed data in child {idx}")
            # Look for nested iterator structures within this data
            for i, item in enumerate(child_data):
                if isinstance(item, list):
                    item_str = str(item)[:50]
                    print(f"           Found nested list at {i}: {item_str}...")
                    self._process_child_iterator_data(item, parent_node, f"{idx}.{i}")
                elif isinstance(item, str) and item == 'Type' and i + 1 < len(child_data):
                    # Found start of an iterator definition
                    print(f"           Found iterator definition starting at {i}")
                    # Extract this iterator's data
                    iterator_data = child_data[i:]
                    child_node = self.build_iterator_tree_from_list(iterator_data)
                    if child_node:
                        parent_node["children"].append(child_node)
                        print(f"           Added iterator: {child_node['name']}")
                    break

    def format_graph_data_for_js(self, graph_data):
        """Format graph data as JavaScript object string"""
        import json

        print(f"🔧 Formatting graph data for JavaScript:")
        print(f"   Data type: {type(graph_data)}")
        print(f"   Data keys: {list(graph_data.keys()) if isinstance(graph_data, dict) else 'N/A'}")

        json_str = json.dumps(graph_data, indent=2)
        print(f"   JSON length: {len(json_str)} characters")
        print(f"   JSON preview: {json_str[:200]}...")

        return json_str

    def extract_leaf_nodes(self, graph_data, path=""):
        """Extract all leaf nodes (terminal iterators) from graph data"""
        leaves = []

        def traverse(node, current_path):
            if not isinstance(node, dict):
                return

            node_name = node.get('name', 'Unknown')
            node_type = node.get('type', '')
            node_subtype = node.get('subtype', '')

            # Build current path
            full_path = f"{current_path}/{node_name}" if current_path else node_name

            children = node.get('children', [])

            # If this is an iterator with no children, it's a leaf
            if node_type == 'iterator' and len(children) == 0:
                # Extract details for comparison
                details = node.get('details', '')
                print(f"       Leaf node {node_name} details: {details}")
                parsed_details = self.parse_details_string(details)
                print(f"       Parsed details: {parsed_details}")
                term = parsed_details.get('Term')
                time = parsed_details.get('Time')
                counter = parsed_details.get('Counter')  # Fallback counter
                size = parsed_details.get('Size')

                # Check for parsing failures
                parsing_issues = []
                if term is None:
                    parsing_issues.append("term")
                if time is None:
                    parsing_issues.append("time")
                if counter is None:
                    parsing_issues.append("counter")
                if size is None:
                    parsing_issues.append("size")

                if parsing_issues:
                    print(f"       ⚠ Leaf node {node_name} missing: {', '.join(parsing_issues)}")

                print(f"       Added leaf node: {node_name} (term: {term}, type: {node_subtype})")

                leaves.append({
                    'name': node_name,
                    'type': node_subtype,
                    'term': term,
                    'time': time,
                    'counter': counter,
                    'read_counter': read_counter,
                    'skip_counter': skip_counter,
                    'size': size,
                    'path': full_path,
                    'details': details,
                    'parsing_issues': parsing_issues
                })

            # Recursively process children
            for child in children:
                traverse(child, full_path)

        traverse(graph_data, path)
        return leaves

    def extract_term_from_details(self, details):
        """Extract term from details string"""
        if 'Term:' in details:
            lines = details.split('\\n')
            for line in lines:
                if line.startswith('Term:'):
                    term = line.split(':', 1)[1].strip()
                    print(f"       Extracted term: '{term}'")
                    return term
        print(f"       ✗ No 'Term:' found in details: {details[:100]}...")
        return None

    def extract_time_from_details(self, details):
        """Extract time from details string"""
        if 'Time:' in details:
            lines = details.split('\\n')
            for line in lines:
                if line.startswith('Time:'):
                    time_str = line.split(':', 1)[1].strip()
                    if time_str == 'N/A':
                        print(f"       Time is N/A")
                        return None
                    try:
                        time_val = float(time_str)
                        print(f"       Extracted time: {time_val}")
                        return time_val
                    except ValueError as e:
                        print(f"       ✗ Failed to parse time '{time_str}': {e}")
                        return None
        print(f"       ✗ No 'Time:' found in details: {details[:100]}...")
        return None

    def extract_counter_from_details(self, details):
        """Extract counter from details string"""
        if 'Counter:' in details:
            lines = details.split('\\n')
            for line in lines:
                if line.startswith('Counter:'):
                    counter_str = line.split(':', 1)[1].strip()
                    try:
                        return int(counter_str) if counter_str != 'N/A' else 0
                    except:
                        return 0
        return 0


    def extract_skip_counter_from_details(self, details):
        """Extract Skip Counter from details string"""
        if 'Skip Counter:' in details:
            lines = details.split('\\n')
            for line in lines:
                if line.startswith('Skip Counter:'):
                    counter_str = line.split(':', 1)[1].strip()
                    if counter_str == 'N/A':
                        print(f"       Skip Counter is N/A")
                        return None
                    try:
                        counter_val = int(counter_str)
                        print(f"       Extracted Skip Counter: {counter_val}")
                        return counter_val
                    except ValueError as e:
                        print(f"       ✗ Failed to parse Skip Counter '{counter_str}': {e}")
                        return None
        print(f"       ✗ No 'Skip Counter:' found in details: {details[:100]}...")
        return None

    def extract_size_from_details(self, details):
        """Extract size from details string"""
        if 'Size:' in details:
            lines = details.split('\\n')
            for line in lines:
                if line.startswith('Size:'):
                    size_str = line.split(':', 1)[1].strip()
                    # Handle special case for UNION sum format
                    if '(sum of' in size_str:
                        size_str = size_str.split('(')[0].strip()

                    if size_str == 'N/A':
                        print(f"       Size is N/A")
                        return None
                    try:
                        size_val = int(size_str)
                        print(f"       Extracted size: {size_val}")
                        return size_val
                    except ValueError as e:
                        print(f"       ✗ Failed to parse size '{size_str}': {e}")
                        return None
        print(f"       ✗ No 'Size:' found in details: {details[:100]}...")
        return None

    def generate_leaf_comparison_table(self, graph1_data, graph2_data, index1, index2):
        """Generate HTML table comparing leaf nodes between two indexes"""
        leaves1 = self.extract_leaf_nodes(graph1_data)
        leaves2 = self.extract_leaf_nodes(graph2_data)

        print(f"   Found {len(leaves1)} leaf nodes in {index1}")
        print(f"   Found {len(leaves2)} leaf nodes in {index2}")

        # Create comparison data
        comparison_data = []

        # Match leaves by term for comparison
        terms1 = {leaf['term']: leaf for leaf in leaves1}
        terms2 = {leaf['term']: leaf for leaf in leaves2}

        print(f"   Terms in {index1}: {list(terms1.keys())}")
        print(f"   Terms in {index2}: {list(terms2.keys())}")

        all_terms = set(terms1.keys()) | set(terms2.keys())
        print(f"   All unique terms: {list(all_terms)}")

        for term in sorted(all_terms):
            leaf1 = terms1.get(term, {})
            leaf2 = terms2.get(term, {})

            # Handle None values explicitly
            def safe_get(leaf, key, default='MISSING'):
                value = leaf.get(key)
                return value if value is not None else default

            comparison_data.append({
                'term': term,
                'type1': safe_get(leaf1, 'type'),
                'type2': safe_get(leaf2, 'type'),
                'time1': safe_get(leaf1, 'time', 0),
                'time2': safe_get(leaf2, 'time', 0),
                'counter1': safe_get(leaf1, 'counter', 0),
                'counter2': safe_get(leaf2, 'counter', 0),
                'size1': safe_get(leaf1, 'size', 0),
                'size2': safe_get(leaf2, 'size', 0),
                'path1': safe_get(leaf1, 'path'),
                'path2': safe_get(leaf2, 'path'),
                'issues1': leaf1.get('parsing_issues', []),
                'issues2': leaf2.get('parsing_issues', [])
            })

        # Generate HTML table
        table_html = f"""
        <div class="leaf-comparison-table">
            <h3>🍃 Leaf Iterator Comparison</h3>
            <p>Comparing terminal iterators between {index1} and {index2}</p>
            <table id="leafComparisonTable" class="sortable-table">
                <thead>
                    <tr>
                        <th onclick="sortTable(0, 'string')">Term <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(1, 'string')">{index1} Type <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(2, 'number')">{index1} Time (ms) <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(3, 'number')">{index1} Counter <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(5, 'number')">{index1} Size <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(6, 'string')">{index2} Type <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(7, 'number')">{index2} Time (ms) <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(8, 'number')">{index2} Counter <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(10, 'number')">{index2} Size <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(11, 'number')">Time Diff <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(12, 'number')">Counter Diff <span class="sort-arrow">↕</span></th>
                        <th onclick="sortTable(14, 'number')">Size Diff <span class="sort-arrow">↕</span></th>
                    </tr>
                </thead>
                <tbody>
        """

        for row in comparison_data:
            # Handle missing values for calculations
            def safe_calc(val1, val2):
                if val1 == 'MISSING' or val2 == 'MISSING':
                    return 'N/A'
                try:
                    return val1 - val2
                except (TypeError, ValueError):
                    return 'N/A'

            time_diff = safe_calc(row['time1'], row['time2'])
            read_counter_diff = safe_calc(row['read_counter1'], row['read_counter2'])
            skip_counter_diff = safe_calc(row['skip_counter1'], row['skip_counter2'])
            size_diff = safe_calc(row['size1'], row['size2'])

            # Color coding for differences
            def get_diff_class(diff):
                if diff == 'N/A':
                    return 'neutral'
                return 'positive' if diff > 0 else 'negative' if diff < 0 else 'neutral'

            time_diff_class = get_diff_class(time_diff)
            read_counter_diff_class = get_diff_class(read_counter_diff)
            skip_counter_diff_class = get_diff_class(skip_counter_diff)
            size_diff_class = get_diff_class(size_diff)

            # Format difference values
            def format_diff(diff):
                if diff == 'N/A':
                    return 'N/A'
                if isinstance(diff, float):
                    return f"{diff:+.1f}"
                return f"{diff:+d}"

            # Add warning indicators for parsing issues
            issues1_str = f" ⚠({','.join(row['issues1'])})" if row['issues1'] else ""
            issues2_str = f" ⚠({','.join(row['issues2'])})" if row['issues2'] else ""

            table_html += f"""
                    <tr>
                        <td class="term-cell" title="{row['path1']} | {row['path2']}">{row['term']}</td>
                        <td>{row['type1']}{issues1_str}</td>
                        <td class="number-cell">{row['time1']}</td>
                        <td class="number-cell">{row['read_counter1']}</td>
                        <td class="number-cell">{row['skip_counter1']}</td>
                        <td class="number-cell">{row['size1']}</td>
                        <td>{row['type2']}{issues2_str}</td>
                        <td class="number-cell">{row['time2']}</td>
                        <td class="number-cell">{row['read_counter2']}</td>
                        <td class="number-cell">{row['skip_counter2']}</td>
                        <td class="number-cell">{row['size2']}</td>
                        <td class="number-cell {time_diff_class}">{format_diff(time_diff)}</td>
                        <td class="number-cell {read_counter_diff_class}">{format_diff(read_counter_diff)}</td>
                        <td class="number-cell {skip_counter_diff_class}">{format_diff(skip_counter_diff)}</td>
                        <td class="number-cell {size_diff_class}">{format_diff(size_diff)}</td>
                    </tr>
            """

        table_html += """
                </tbody>
            </table>
        </div>
        """

        return table_html

    def generate_tree_html(self, data, level=0):
        """Generate HTML for tree structure"""
        if data is None:
            return '<div class="tree-node"><span class="tree-content"><span class="tree-value">null</span></span></div>'

        if isinstance(data, dict):
            if not data:
                return '<div class="tree-node"><span class="tree-content"><span class="tree-value">{}</span></span></div>'

            html = []
            for key, value in data.items():
                if isinstance(value, (dict, list)) and value:
                    # Expandable node
                    html.append(f'''
                    <div class="tree-node">
                        <span class="tree-toggle" onclick="toggleNode(this)">▼</span>
                        <span class="tree-content">
                            <span class="tree-key">"{key}"</span>:
                            <span class="tree-type">({type(value).__name__})</span>
                        </span>
                        <div class="tree-children">
                            {self.generate_tree_html(value, level + 1)}
                        </div>
                    </div>''')
                else:
                    # Leaf node
                    value_html = self.format_value_html(value)
                    html.append(f'''
                    <div class="tree-node">
                        <span class="tree-toggle"></span>
                        <span class="tree-content">
                            <span class="tree-key">"{key}"</span>: {value_html}
                        </span>
                    </div>''')
            return ''.join(html)

        elif isinstance(data, list):
            if not data:
                return '<div class="tree-node"><span class="tree-content"><span class="tree-value">[]</span></span></div>'

            html = []
            for i, item in enumerate(data):
                if isinstance(item, (dict, list)) and item:
                    # Expandable array item
                    html.append(f'''
                    <div class="tree-node">
                        <span class="tree-toggle" onclick="toggleNode(this)">▼</span>
                        <span class="tree-content">
                            <span class="tree-key">[{i}]</span>:
                            <span class="tree-type">({type(item).__name__})</span>
                        </span>
                        <div class="tree-children">
                            {self.generate_tree_html(item, level + 1)}
                        </div>
                    </div>''')
                else:
                    # Leaf array item
                    value_html = self.format_value_html(item)
                    html.append(f'''
                    <div class="tree-node">
                        <span class="tree-toggle"></span>
                        <span class="tree-content">
                            <span class="tree-key">[{i}]</span>: {value_html}
                        </span>
                    </div>''')
            return ''.join(html)

        else:
            # Simple value
            return f'<div class="tree-node"><span class="tree-content">{self.format_value_html(data)}</span></div>'

    def format_value_html(self, value):
        """Format a value for HTML display"""
        if isinstance(value, str):
            return f'<span class="tree-string">"{value}"</span>'
        elif isinstance(value, (int, float)):
            return f'<span class="tree-number">{value}</span>'
        elif isinstance(value, bool):
            return f'<span class="tree-value">{str(value).lower()}</span>'
        elif value is None:
            return '<span class="tree-value">null</span>'
        else:
            return f'<span class="tree-value">{str(value)}</span>'

    def generate_comparison_flow_diagram(self, parsed_data1, parsed_data2, index1: str, index2: str):
        """Generate flow diagram HTML with separate trees for each index"""
        # Extract flow components from both datasets
        flow1 = self.extract_flow_components(parsed_data1, index1)
        flow2 = self.extract_flow_components(parsed_data2, index2)

        # Generate two separate trees side by side
        tree1_html = self.generate_iterator_tree(flow1, index1, "index1", x_offset=50)
        tree2_html = self.generate_iterator_tree(flow2, index2, "index2", x_offset=600)

        # Combine both trees
        all_html = tree1_html + "\\n" + tree2_html
        return all_html

    def generate_iterator_tree(self, flow_data, index_name, tree_class, x_offset=0):
        """Generate iterator and processor tree for a single index"""
        nodes_html = []
        connections_html = []

        # Start positions
        y_pos = 50
        x_pos = x_offset

        # Query parsing node
        parsing_time = flow_data.get('parsing_time', 0)
        nodes_html.append(self.create_tree_node(
            "Query Parsing",
            f"{parsing_time}ms",
            tree_class,
            x_pos, y_pos, f"parsing_{tree_class}"
        ))

        # Pipeline creation
        y_pos += 100
        pipeline_time = flow_data.get('pipeline_time', 0)
        nodes_html.append(self.create_tree_node(
            "Pipeline Creation",
            f"{pipeline_time}ms",
            tree_class,
            x_pos, y_pos, f"pipeline_{tree_class}"
        ))

        # Connection from parsing to pipeline
        connections_html.append(self.create_tree_connection(
            x_pos + 75, y_pos - 100 + 40, x_pos + 75, y_pos, tree_class
        ))

        # Iterator tree
        y_pos += 100
        iterator_data = flow_data.get('iterator', {})
        if iterator_data:
            iterator_html, iterator_height = self.generate_iterator_subtree(
                iterator_data, tree_class, x_pos, y_pos
            )
            nodes_html.extend(iterator_html)

            # Connection from pipeline to iterator
            connections_html.append(self.create_tree_connection(
                x_pos + 75, y_pos - 100 + 40, x_pos + 75, y_pos, tree_class
            ))

            y_pos += iterator_height + 50

        # Result processors tree
        processors = flow_data.get('processors', [])
        if processors:
            processor_html, processor_height = self.generate_processor_subtree(
                processors, tree_class, x_pos, y_pos
            )
            nodes_html.extend(processor_html)

            # Connection from iterator to processors
            if iterator_data:
                connections_html.append(self.create_tree_connection(
                    x_pos + 75, y_pos - 50, x_pos + 75, y_pos, tree_class
                ))

        # Add index title
        title_html = f'''
        <div class="tree-title {tree_class}" style="left: {x_pos}px; top: 10px;">
            <h3>{index_name}</h3>
        </div>'''

        return title_html + "\\n".join(nodes_html + connections_html)

    def generate_iterator_subtree(self, iterator_data, tree_class, x_pos, y_pos):
        """Generate iterator subtree with child iterators"""
        nodes_html = []
        connections_html = []

        # Main iterator node
        iter_type = iterator_data.get('type', 'Unknown')
        iter_counter = iterator_data.get('counter', 0)
        iter_term = iterator_data.get('term', '')

        main_node_html = self.create_tree_node(
            f"Iterator: {iter_type}",
            f"Counter: {iter_counter}\\nTerm: {iter_term}",
            tree_class,
            x_pos, y_pos, f"iterator_main_{tree_class}"
        )
        nodes_html.append(main_node_html)

        current_height = 80

        # Child iterators if they exist
        child_iterators = iterator_data.get('child_iterators', [])
        if child_iterators:
            child_y = y_pos + 100
            child_x_start = x_pos - 50
            child_spacing = 120

            for i, child in enumerate(child_iterators):
                child_x = child_x_start + (i * child_spacing)
                child_type = child.get('type', 'Unknown')
                child_counter = child.get('counter', 0)
                child_term = child.get('term', '')

                child_node_html = self.create_tree_node(
                    f"Child: {child_type}",
                    f"Counter: {child_counter}\\nTerm: {child_term}",
                    tree_class,
                    child_x, child_y, f"iterator_child_{i}_{tree_class}",
                    size="small"
                )
                nodes_html.append(child_node_html)

                # Connection from main iterator to child
                connections_html.append(self.create_tree_connection(
                    x_pos + 75, y_pos + 40, child_x + 50, child_y, tree_class
                ))

            current_height += 100

        return nodes_html + connections_html, current_height

    def generate_processor_subtree(self, processors, tree_class, x_pos, y_pos):
        """Generate result processor subtree"""
        nodes_html = []
        connections_html = []

        current_y = y_pos

        for i, processor in enumerate(processors):
            proc_type = processor.get('type', 'Unknown')
            proc_counter = processor.get('counter', 0)

            node_html = self.create_tree_node(
                f"Processor: {proc_type}",
                f"Counter: {proc_counter}",
                tree_class,
                x_pos, current_y, f"processor_{i}_{tree_class}"
            )
            nodes_html.append(node_html)

            # Connection to next processor
            if i < len(processors) - 1:
                connections_html.append(self.create_tree_connection(
                    x_pos + 75, current_y + 40, x_pos + 75, current_y + 100, tree_class
                ))

            current_y += 100

        total_height = len(processors) * 100
        return nodes_html + connections_html, total_height

    def extract_flow_components(self, parsed_data, index_name):
        """Extract flow components from parsed profile data"""
        components = {}

        if isinstance(parsed_data, dict):
            # Extract timing information
            components['parsing_time'] = parsed_data.get('Parsing time', 0)
            components['pipeline_time'] = parsed_data.get('Pipeline creation time', 0)
            components['total_time'] = parsed_data.get('Total profile time', 0)

            # Extract iterator information
            iterator_profile = parsed_data.get('Iterators profile', {})
            if isinstance(iterator_profile, dict):
                components['iterator'] = {
                    'type': iterator_profile.get('Type', 'Unknown'),
                    'counter': iterator_profile.get('Counter', 0),
                    'term': iterator_profile.get('Term', ''),
                    'size': iterator_profile.get('Size', 0),
                    'child_iterators': []
                }

                # Extract child iterators if they exist
                child_iterators = iterator_profile.get('Child iterators', [])
                if isinstance(child_iterators, list):
                    for child in child_iterators:
                        if isinstance(child, dict):
                            components['iterator']['child_iterators'].append({
                                'type': child.get('Type', 'Unknown'),
                                'counter': child.get('Counter', 0),
                                'term': child.get('Term', ''),
                                'size': child.get('Size', 0)
                            })

            # Extract result processors
            processors_profile = parsed_data.get('Result processors profile', [])
            if isinstance(processors_profile, list):
                components['processors'] = []
                for proc in processors_profile:
                    if isinstance(proc, dict):
                        components['processors'].append({
                            'type': proc.get('Type', 'Unknown'),
                            'counter': proc.get('Counter', 0)
                        })

        return components

    def calculate_performance_diff(self, value1, value2, lower_is_better=True):
        """Calculate performance difference and return classification"""
        if value1 == 0 and value2 == 0:
            return 'same'

        if value1 == 0 or value2 == 0:
            return 'different'

        # Calculate percentage difference
        diff_percent = abs(value1 - value2) / max(value1, value2) * 100

        if diff_percent < 5:  # Less than 5% difference
            return 'same'

        if lower_is_better:
            return 'better' if value1 < value2 else 'worse'
        else:
            return 'better' if value1 > value2 else 'worse'

    def create_flow_node(self, title, details, diff_class, x, y, node_id):
        """Create a flow diagram node"""
        # Convert actual newlines to <br> for HTML line breaks
        html_details = details.replace('\n', '<br>')
        return f'''
        <div class="flow-node diff-{diff_class}"
             style="left: {x}px; top: {y}px;"
             id="node-{node_id}">
            <div class="node-title">{title}</div>
            <div class="node-details">{html_details}</div>
        </div>'''

    def create_tree_node(self, title, details, tree_class, x, y, node_id, size="normal"):
        """Create a tree diagram node with color gradients"""
        size_class = "tree-node-small" if size == "small" else "tree-node"
        # Convert actual newlines to <br> for HTML line breaks
        html_details = details.replace('\n', '<br>')

        return f'''
        <div class="{size_class} {tree_class}"
             style="left: {x}px; top: {y}px;"
             id="node-{node_id}">
            <div class="node-title">{title}</div>
            <div class="node-details">{html_details}</div>
        </div>'''

    def create_tree_connection(self, x1, y1, x2, y2, tree_class):
        """Create a connection line between tree nodes"""
        if x1 == x2:
            # Vertical connection
            height = abs(y2 - y1)
            top = min(y1, y2)
            return f'''
            <div class="tree-connection {tree_class}"
                 style="left: {x1}px; top: {top}px; height: {height}px; width: 2px;">
            </div>
            <div class="tree-arrow {tree_class}"
                 style="left: {x1-6}px; top: {y2-8}px;">
            </div>'''
        else:
            # Diagonal connection
            width = abs(x2 - x1)
            height = abs(y2 - y1)
            left = min(x1, x2)
            top = min(y1, y2)

            return f'''
            <svg class="tree-connection-svg {tree_class}"
                 style="left: {left}px; top: {top}px; width: {width}px; height: {height}px; position: absolute;">
                <line x1="{x1-left}" y1="{y1-top}" x2="{x2-left}" y2="{y2-top}"
                      stroke="currentColor" stroke-width="2"/>
            </svg>
            <div class="tree-arrow {tree_class}"
                 style="left: {x2-6}px; top: {y2-8}px;">
            </div>'''

    def create_connection(self, x1, y1, x2, y2, diff_class):
        """Create a connection line between nodes"""
        # Simple vertical connection
        height = y2 - y1
        return f'''
        <div class="flow-connection {diff_class}"
             style="left: {x1}px; top: {y1}px; height: {height}px; width: 2px;">
        </div>
        <div class="flow-arrow {diff_class}"
             style="left: {x1-6}px; top: {y2-8}px;">
        </div>'''

    def extract_key_metrics(self, parsed_data):
        """Extract key metrics for comparison table"""
        metrics = {}

        if isinstance(parsed_data, dict):
            metrics['Total Time'] = parsed_data.get('Total profile time', 0)
            metrics['Parsing Time'] = parsed_data.get('Parsing time', 0)
            metrics['Pipeline Time'] = parsed_data.get('Pipeline creation time', 0)

            # Iterator metrics
            iterator_profile = parsed_data.get('Iterators profile', {})
            if isinstance(iterator_profile, dict):
                metrics['Iterator Type'] = iterator_profile.get('Type', 'Unknown')
                metrics['Iterator Counter'] = iterator_profile.get('Counter', 0)

            # Result processor count
            processors_profile = parsed_data.get('Result processors profile', [])
            if isinstance(processors_profile, list):
                metrics['Processor Stages'] = len(processors_profile)

        return metrics

    def generate_comparison_table(self, metrics1, metrics2, index1, index2, result_count1, result_count2):
        """Generate HTML comparison table"""
        table_rows = []

        # Add result count comparison
        table_rows.append(f'''
        <tr>
            <td class="metric-name">Result Count</td>
            <td class="{'value-better' if result_count1 > result_count2 else 'value-worse' if result_count1 < result_count2 else 'value-same'}">{result_count1}</td>
            <td class="{'value-better' if result_count2 > result_count1 else 'value-worse' if result_count2 < result_count1 else 'value-same'}">{result_count2}</td>
            <td>{result_count1 - result_count2:+d}</td>
        </tr>''')

        # Add metrics comparison
        all_metrics = set(metrics1.keys()) | set(metrics2.keys())

        for metric in sorted(all_metrics):
            val1 = metrics1.get(metric, 'N/A')
            val2 = metrics2.get(metric, 'N/A')

            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                diff = val1 - val2
                diff_str = f"{diff:+.2f}" if isinstance(diff, float) else f"{diff:+d}"

                # For timing metrics, lower is better
                is_timing = 'time' in metric.lower()
                if is_timing:
                    val1_class = 'value-better' if val1 < val2 else 'value-worse' if val1 > val2 else 'value-same'
                    val2_class = 'value-better' if val2 < val1 else 'value-worse' if val2 > val1 else 'value-same'
                else:
                    val1_class = 'value-better' if val1 > val2 else 'value-worse' if val1 < val2 else 'value-same'
                    val2_class = 'value-better' if val2 > val1 else 'value-worse' if val2 < val1 else 'value-same'
            else:
                val1_class = val2_class = 'value-same'
                diff_str = 'N/A'

            table_rows.append(f'''
            <tr>
                <td class="metric-name">{metric}</td>
                <td class="{val1_class}">{val1}</td>
                <td class="{val2_class}">{val2}</td>
                <td>{diff_str}</td>
            </tr>''')

        return f'''
        <table>
            <thead>
                <tr>
                    <th>Metric</th>
                    <th>{index1}</th>
                    <th>{index2}</th>
                    <th>Difference</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>'''


def main():
    parser = argparse.ArgumentParser(description='RediSearch Query Profile Test with JSON Output')
    parser.add_argument('--redis-host', default='localhost', help='Redis host (default: localhost)')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port (default: 6379)')

    # Query-specific arguments
    parser.add_argument('--index-name', required=True, help='RediSearch index name to query')
    parser.add_argument('--query', required=True, help='Query string to execute (will be used with FT.PROFILE)')
    parser.add_argument('--compare-index', help='Second index name for comparison (generates diff in HTML report)')

    # Output format arguments
    parser.add_argument('--json', action='store_true', help='Output profile results in JSON format')
    parser.add_argument('--json-file', help='Save JSON profile results to specified file')
    parser.add_argument('--html-tree', help='Generate interactive HTML tree visualization of profile data')

    args = parser.parse_args()

    print("=" * 60)
    print("RediSearch Query Profile Test with JSON Output")
    print("=" * 60)

    # Initialize tester (this will detect RediSearch version)
    tester = RediSearchTester(args.redis_host, args.redis_port)

    if args.compare_index:
        # Run comparison test
        print(f"\n🔍 Running comparison test...")
        print(f"Primary Index: {args.index_name}")
        print(f"Comparison Index: {args.compare_index}")
        print(f"Query: {args.query}")

        success, result = tester.run_comparison_test(
            index1=args.index_name,
            index2=args.compare_index,
            query=args.query,
            html_tree_file=args.html_tree
        )
    else:
        # Run single index test
        print(f"\n🔍 Running query test...")
        print(f"Index: {args.index_name}")
        print(f"Query: {args.query}")

        success, result = tester.run_query_test(
            index_name=args.index_name,
            query=args.query,
            output_json=args.json or args.json_file is not None,
            json_file=args.json_file,
            html_tree_file=args.html_tree
        )

    # Display basic result information if successful
    if success and result:
        if args.compare_index:
            # Comparison mode results
            if isinstance(result, dict) and 'index1' in result and 'index2' in result:
                print(f"\n📊 Comparison Results Summary:")

                index1_data = result['index1']
                index2_data = result['index2']

                results1 = index1_data['results']
                results2 = index2_data['results']

                count1 = len(results1) if isinstance(results1, list) else 0
                count2 = len(results2) if isinstance(results2, list) else 0

                print(f"   {args.index_name}: {count1} results")
                print(f"   {args.compare_index}: {count2} results")
                print(f"   Difference: {count1 - count2:+d} results")

                if args.html_tree:
                    print(f"\n✓ Comparison flow diagram generated: {args.html_tree}")
            else:
                print(f"\n📊 Comparison Result Summary:")
                print(f"   Result type: {type(result)}")
        else:
            # Single index mode results
            if isinstance(result, tuple) and len(result) == 2:
                query_results, profile_data = result
                print(f"\n📊 Query Result Summary:")
                if isinstance(query_results, list):
                    print(f"   Total results: {len(query_results)}")
                    if len(query_results) > 0:
                        print(f"   First few results: {query_results[:3] if len(query_results) > 3 else query_results}")
                else:
                    print(f"   Query results type: {type(query_results)}")
            else:
                print(f"\n📊 Result Summary:")
                print(f"   Result type: {type(result)}")
                print(f"   Result: {str(result)[:200]}...")

    print("=" * 60)
    if success:
        print("✓ Test completed successfully")
    else:
        print("✗ Test failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
