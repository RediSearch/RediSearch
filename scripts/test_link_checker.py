#!/usr/bin/env python3
"""
Test script for the link checker.
Creates sample markdown files and tests the link checker functionality.
"""

import tempfile
from pathlib import Path
from check_links import LinkChecker


def create_test_files():
    """Create test markdown files with various link types."""
    test_dir = tempfile.mkdtemp()
    
    # Test file with good links
    good_md = Path(test_dir) / "good_links.md"
    good_md.write_text("""
# Test Document with Good Links

This document contains valid links:

- [GitHub](https://github.com)
- [Google](https://www.google.com)
- [Python.org](https://www.python.org)
- Direct link: https://docs.python.org/3/

## With Anchors
- [Python Tutorial](https://docs.python.org/3/tutorial/index.html#tutorial-index)
""")
    
    # Test file with bad links
    bad_md = Path(test_dir) / "bad_links.md"
    bad_md.write_text("""
# Test Document with Bad Links

This document contains invalid links:

- [Broken Link](https://this-domain-does-not-exist-12345.com)
- [Bad Anchor](https://github.com#non-existent-anchor)
- Direct bad link: https://httpstat.us/404

## Mixed Links
- [Good Link](https://github.com)
- [Another Bad Link](https://httpstat.us/500)
""")
    
    # Test file with excluded links
    excluded_md = Path(test_dir) / "excluded_links.md"
    excluded_md.write_text("""
# Test Document with Excluded Links

These should be excluded:

- [Localhost](http://localhost:8080)
- [Example](https://example.com/placeholder)
- [Variable](https://api.${ENVIRONMENT}.example.com)
""")

    # Test file with relative links
    relative_md = Path(test_dir) / "relative_links.md"
    relative_md.write_text("""
# Test Document with Relative Links

These are relative links:

- [Good relative link](good_links.md)
- [Bad relative link](non_existent_file.md)
- [Image link](../images/logo.png)
""")

    # Create a test image file
    images_dir = Path(test_dir).parent / "images"
    images_dir.mkdir(exist_ok=True)
    (images_dir / "logo.png").write_bytes(b"fake png content")
    
    return test_dir


def test_link_checker():
    """Test the link checker functionality."""
    print("🧪 Testing Link Checker...")
    
    test_dir = create_test_files()
    print(f"Created test files in: {test_dir}")
    
    # Test with default config
    print("\n1. Testing with default configuration...")
    checker = LinkChecker()
    
    # Test good links
    print("   Testing good links...")
    good_file = Path(test_dir) / "good_links.md"
    results = checker.check_links_in_file(good_file)
    good_count = sum(1 for _, _, is_valid, _, _ in results if is_valid)
    print(f"   ✅ {good_count}/{len(results)} links valid in good_links.md")
    
    # Test with configuration
    print("\n2. Testing with configuration...")
    config = {
        'timeout': 5,
        'max_workers': 2,
        'delay': 0.1,
        'exclude_urls': ['https://example.com/placeholder'],
        'exclude_link_patterns': [r'.*localhost.*', r'.*\$\{.*\}.*']
    }
    
    checker_with_config = LinkChecker(config)
    
    # Test excluded links
    print("   Testing excluded links...")
    excluded_file = Path(test_dir) / "excluded_links.md"
    excluded_results = checker_with_config.check_links_in_file(excluded_file)
    print(f"   ℹ️  Found {len(excluded_results)} links after exclusions")

    # Test relative links
    print("   Testing relative links...")
    relative_file = Path(test_dir) / "relative_links.md"
    relative_results = checker_with_config.check_links_in_file(relative_file)
    relative_valid = sum(1 for _, _, is_valid, _, _ in relative_results if is_valid)
    print(f"   📁 {relative_valid}/{len(relative_results)} relative links valid")

    print(f"\n🧹 Cleaning up test directory: {test_dir}")
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)
    # Also clean up the images directory we created
    images_dir = Path(test_dir).parent / "images"
    if images_dir.exists():
        shutil.rmtree(images_dir, ignore_errors=True)
    
    print("✅ Link checker test completed!")


if __name__ == '__main__':
    test_link_checker()
