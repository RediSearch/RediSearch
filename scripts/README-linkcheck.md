# Link Checker for Markdown Files

This directory contains a comprehensive link checker that validates all links in Markdown files, including anchor verification.

## Features

- ✅ **Comprehensive Link Detection**: Finds links in `[text](url)`, `<url>`, and bare URL formats
- 📁 **Relative Link Checking**: Validates relative file paths (e.g., `docs/images/logo.svg`)
- 🔗 **Anchor Verification**: Validates that anchor links (`#section`) actually exist in the target page
- 🚀 **Concurrent Processing**: Multi-threaded checking for faster execution
- ⚙️ **Configurable**: Supports exclusion lists and custom settings
- 🤖 **CI/CD Integration**: GitHub Action for automated weekly checks
- 📊 **Detailed Reporting**: Clear output with line numbers and failure reasons

## Files

- `check_links.py` - Main link checker script
- `link-check-config.json` - Configuration file
- `requirements-linkcheck.txt` - Python dependencies
- `test_link_checker.py` - Test script
- `README-linkcheck.md` - This documentation

## Installation

```bash
# Install dependencies
uv pip install -r scripts/requirements-linkcheck.txt

# Make script executable (Linux/Mac)
chmod +x scripts/check_links.py
```

## Usage

### Basic Usage

```bash
# Check all markdown files in current directory (failures only)
python scripts/check_links.py

# Check with verbose output (show all links)
python scripts/check_links.py --verbose

# Check specific directory
python scripts/check_links.py docs/

# Use custom configuration
python scripts/check_links.py --config my-config.json
```

### Command Line Options

```bash
python scripts/check_links.py [directory] [options]

Options:
  --config FILE         Configuration file (default: scripts/link-check-config.json)
  --timeout SECONDS     Request timeout (overrides config)
  --max-workers N       Concurrent workers (overrides config)
  --delay SECONDS       Delay between requests (overrides config)
  --verbose, -v         Show all links (including successful ones)
  --help               Show help message
```

### Configuration File

The `link-check-config.json` file allows you to customize the checker behavior:

```json
{
  "exclude_urls": [
    "https://example.com/placeholder",
    "https://localhost",
    "http://localhost"
  ],
  "exclude_link_patterns": [
    ".*\\.local.*",
    ".*127\\.0\\.0\\.1.*",
    ".*\\$\\{.*\\}.*"
  ],
  "exclude_directories": [
    "bin", "deps", "tests", ".github"
  ],
  "timeout": 15,
  "max_workers": 5,
  "delay": 0.2,
  "user_agent": "Mozilla/5.0 (compatible; RediSearch-LinkChecker/1.0)"
}
```

**Configuration Options:**
- `exclude_urls`: List of exact URLs to skip
- `exclude_link_patterns`: List of regex patterns for resolved link URLs/paths to skip
  - **Note**: Patterns starting with `^/` match absolute file system paths (for relative links)
  - **Note**: Other patterns match any part of URLs (for absolute links)
- `exclude_directories`: List of directory names to skip when scanning for markdown files
- `timeout`: Request timeout in seconds
- `max_workers`: Number of concurrent threads
- `delay`: Delay between requests (be respectful to servers)
- `user_agent`: User agent string for requests

## GitHub Action

The link checker runs automatically:

- **Weekly**: Every Sunday at 20:20 UTC (with benchmarks)
- **On PRs**: When markdown files, link checker script, dependencies, or workflow are modified
- **On-demand**: Add the `check-links` label to any PR to trigger validation
- **Manual**: Can be triggered manually from GitHub Actions tab

### Workflow Features

- 🔄 **Automatic Issue Creation**: Creates GitHub issues for broken links found in weekly runs
- 💬 **PR Comments**: Comments on PRs when link check fails
- 📁 **Artifact Upload**: Saves detailed logs for failed runs
- ⚡ **Smart Throttling**: Uses conservative settings to avoid overwhelming servers
- 🏷️ **Label Trigger**: Add `check-links` label to any PR to run validation on-demand

### Using the Label Trigger

To run link checking on any PR (even if it doesn't modify markdown files):

1. **Add the label**: Go to the PR and add the `check-links` label
2. **Workflow runs**: The link checker will automatically run
3. **Results**: Check the Actions tab for results and any PR comments

## Testing

Run the test script to verify functionality:

```bash
python scripts/test_link_checker.py
```

This creates temporary markdown files with various link types and tests the checker.

## How It Works

1. **Discovery**: Recursively finds all `.md` files in the specified directory
2. **Extraction**: Uses regex patterns to extract links from markdown content
3. **Classification**: Determines if links are absolute URLs or relative file paths
4. **Validation**:
   - **Absolute URLs**: Makes HTTP requests to verify accessibility
   - **Relative Paths**: Checks file system existence relative to the markdown file
5. **Anchor Check**: For links with anchors, parses HTML to verify anchor exists
6. **Reporting**: Provides detailed results with line numbers, link types, and error messages

### Anchor Verification

The checker validates anchors by:
- Looking for elements with matching `id` attributes
- Checking for `<a name="anchor">` tags
- Searching for GitHub-style header anchors (`<h1 id="anchor">`)
- Parsing HTML content to find anchor targets

### Smart Request Handling

The link checker uses a **hybrid approach** for maximum reliability and efficiency:

**Primary Method - HTTP Session:**
- Uses `requests.Session()` for connection pooling and faster performance
- Maintains cookies and headers across requests
- Supports full anchor verification by parsing HTML content
- Efficient for checking multiple links from the same domain

**Fallback Method - cURL:**
- Automatically falls back to `curl` when sites block automated requests
- Uses browser-like headers to bypass bot detection
- Handles sites that specifically block Python `requests` library
- Examples: Package registries (crates.io, npm), some corporate sites

**Example scenarios:**
- ✅ `github.com` links → Fast session-based checking with anchor verification
- ✅ `crates.io` links → Fallback to curl when requests are blocked
- ✅ `docs.rs` links → Session works, full anchor checking available

### Exclusion Pattern Logic

The `exclude_link_patterns` work differently for different link types:

**For Relative Links** (resolved to absolute file paths):
- `^/.*/node_modules/.*` - Excludes `/path/to/project/node_modules/package.json`
- `^/.*/build/.*` - Excludes `/path/to/project/build/output.js`
- Won't affect URLs like `https://redis.io/docs/build/guide`

**For Absolute URLs**:
- `.*\\.local.*` - Excludes `https://myapp.local/api`
- `.*127\\.0\\.0\\.1.*` - Excludes `http://127.0.0.1:8080`
- Won't affect file paths like `/home/user/build/file.txt`

## Best Practices

### For Documentation Authors

1. **Use Descriptive Link Text**: Avoid "click here" or generic text
2. **Test Locally**: Run the checker before committing changes
3. **Keep Links Current**: Regularly review and update external links
4. **Use Relative Links**: For internal documentation, prefer relative paths
5. **Check File Paths**: Ensure relative links point to existing files
6. **Verify Images**: Make sure image links point to actual image files

### For Maintainers

1. **Review Weekly Reports**: Check automated issues for broken links
2. **Update Exclusions**: Add problematic but valid URLs to exclusion list
3. **Monitor Performance**: Adjust `delay` and `max_workers` if needed
4. **Keep Dependencies Updated**: Regularly update Python packages

## Troubleshooting

### Common Issues

**False Positives**: Some sites block automated requests
- Solution: The checker automatically tries `curl` as fallback, but you can also add persistent blockers to `exclude_urls`

**Timeouts**: Slow or unreliable sites
- Solution: Increase `timeout` value or exclude the URL

**Rate Limiting**: Too many requests too quickly
- Solution: Increase `delay` or reduce `max_workers`

**Anchor Not Found**: Valid anchor reported as missing
- Solution: Check if site uses JavaScript to generate anchors (may need exclusion)

### Debug Mode

For detailed debugging, modify the script to add verbose logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

When modifying the link checker:

1. Test changes with `test_link_checker.py`
2. Update configuration examples if adding new options
3. Update this README for new features
4. Test the GitHub Action in a fork before merging

## Dependencies

- **requests**: HTTP client library
- **beautifulsoup4**: HTML parsing for anchor verification
- **lxml**: Fast XML/HTML parser (optional but recommended)

All dependencies are pinned in `requirements-linkcheck.txt` for reproducible builds.
