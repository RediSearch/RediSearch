#!/usr/bin/env python3
"""
Rust Code Review Script

This script generates automated code reviews for Rust files in a PR using OpenAI's GPT-4.
It analyzes git diffs and posts comprehensive review comments to the PR.

Usage:
- Basic review: `/rust_review`
- With specific topics: `/rust_review focus on: performance, memory safety`
- Alternative syntax: `/rust_review consider: error handling, testing`
- Or: `/rust_review check: async, concurrency`

Supported topics include: performance, memory, safety, concurrency, threading, async,
error handling, testing, documentation, api design, architecture, algorithms,
data structures, optimization, readability, maintainability, security, validation,
logging, debugging, refactoring, patterns, best practices, conventions, style,
formatting, naming, types, lifetimes, borrowing, ownership, traits, generics,
macros, modules, crates, dependencies, cargo, clippy, rustfmt, ffi, c integration,
bindings, extern, unsafe, interop.
"""

import os
import json
import requests
import subprocess
import sys
import re
from openai import OpenAI


def extract_user_topics(comment_body):
    """
    Safely extract user-specified review topics from the comment body.

    Expected format: /rust_review focus on: topic1, topic2, topic3
    or: /rust_review consider: topic1, topic2
    or: /rust_review check: topic1, topic2
    """
    if not comment_body:
        return []

    # Define allowed topic keywords to prevent prompt injection
    allowed_keywords = {
        'performance', 'memory', 'safety', 'concurrency', 'threading', 'async',
        'error handling', 'testing', 'documentation', 'api design', 'architecture',
        'algorithms', 'data structures', 'optimization', 'readability', 'maintainability',
        'security', 'validation', 'logging', 'debugging', 'refactoring', 'patterns',
        'best practices', 'conventions', 'style', 'formatting', 'naming', 'types',
        'lifetimes', 'borrowing', 'ownership', 'traits', 'generics', 'macros',
        'modules', 'crates', 'dependencies', 'cargo', 'clippy', 'rustfmt',
        'ffi', 'c integration', 'bindings', 'extern', 'unsafe', 'interop'
    }

    # Extract topics using regex patterns
    patterns = [
        r'/rust_review\s+(?:focus\s+on|consider|check|review):\s*(.+?)(?:\n|$)',
        r'/rust_review\s+(.+?)(?:\n|$)'
    ]

    topics = []
    for pattern in patterns:
        match = re.search(pattern, comment_body, re.IGNORECASE | re.MULTILINE)
        if match:
            topic_text = match.group(1).strip()
            # Split by common delimiters and clean up
            raw_topics = re.split(r'[,;]\s*', topic_text)

            for topic in raw_topics:
                # Check for potential injection attempts before cleaning
                original_topic = topic.strip()

                # Detect potential injection patterns
                injection_patterns = [
                    r'system\s*:', r'ignore\s+previous', r'new\s+instructions',
                    r'<script', r'javascript:', r'prompt\s*\(', r'alert\s*\(',
                    r'eval\s*\(', r'function\s*\(', r'return\s+', r'console\.',
                    r'document\.', r'window\.', r'process\.', r'require\s*\(',
                    r'import\s+', r'from\s+.*import', r'exec\s*\(', r'__.*__'
                ]

                # If any injection pattern is detected, skip this entire comment
                if any(re.search(pattern, original_topic, re.IGNORECASE) for pattern in injection_patterns):
                    print(f"Potential injection attempt detected, blocking entire request")
                    return []

                # Clean and validate topic
                clean_topic = re.sub(r'[^\w\s-]', '', original_topic.lower())
                if clean_topic and len(clean_topic) <= 50:  # Limit length
                    # Check if topic contains allowed keywords
                    if any(keyword in clean_topic for keyword in allowed_keywords):
                        topics.append(clean_topic)
            break  # Use first matching pattern

    # Limit number of topics to prevent prompt bloat
    return topics[:5]


def get_file_diff(file_path):
    """Get the git diff for a specific file with context lines."""
    try:
        # Get the diff for this file with more context lines
        result = subprocess.run(
            ["git", "diff", "-U10", "--staged", "origin/main", "--", file_path],
            capture_output=True,
            text=True,
            check=True
        )
        if not result.stdout:
            # If no staged changes, get the regular diff
            result = subprocess.run(
                ["git", "diff", "-U10", "origin/main", "--", file_path],
                capture_output=True,
                text=True,
                check=True
            )
        return result.stdout
    except subprocess.CalledProcessError:
        return f"Error getting diff for {file_path}"


def generate_review_prompt(all_diffs, user_topics=None):
    """Generate the prompt for OpenAI based on the file diffs and user topics."""
    prompt = """
You are a senior Rust developer performing a code review. Review the following code changes with focus on Rust code for:
1. Code quality and best practices
2. Potential bugs or edge cases
3. Performance considerations
4. Readability and maintainability
5. Proper error handling
6. Memory safety concerns
7. FFI safety (for C/Rust interop code)
8. Unsafe code correctness
"""

    # Add user-specified topics if provided
    if user_topics:
        prompt += "\nAdditionally, pay special attention to the following areas:\n"
        for i, topic in enumerate(user_topics, 7):
            prompt += f"{i}. {topic.title()}\n"

    prompt += """
Format your review as a markdown document with sections for each file, highlighting both positive aspects and areas for improvement.
Be specific and provide actionable feedback. Include code examples where appropriate.

Focus primarily on Rust code, but also review C/C++ files for:
- FFI safety and correctness
- Memory management
- Integration points with Rust code
- Potential issues that could affect Rust code safety

Here are the changes to review:

"""

    for file, diff in all_diffs.items():
        prompt += f"\n## File: {file}\n```diff\n{diff}\n```\n"

    return prompt


def call_openai_api(client, prompt):
    """Call OpenAI API to generate the code review."""
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a senior Rust developer performing a thorough code review."
                },
                {"role": "user", "content": prompt}
            ],
            max_tokens=4000,
            temperature=0.7
        )
        return response.choices[0].message.content
    except (ConnectionError, TimeoutError) as e:
        print(f"Network error calling OpenAI API: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error calling OpenAI API: {str(e)}")
        sys.exit(1)


def post_review_comment(repo, pr_number, review, github_token, user_topics=None):
    """Post the review as a comment on the PR."""
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Add footer with information about user topics if provided
    footer = "_This review was automatically generated by the Rust Code Reviewer Bot._"
    if user_topics:
        topics_text = ", ".join(user_topics)
        footer = (f"_This review was automatically generated by the Rust Code Reviewer Bot "
                 f"with focus on: {topics_text}._")

    comment_data = {
        "body": f"# Rust Code Review\n\n{review}\n\n{footer}"
    }

    comment_url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    response = requests.post(comment_url, headers=headers, json=comment_data)

    if response.status_code >= 400:
        print(f"Error posting comment: {response.status_code} - {response.text}")
        sys.exit(1)
    else:
        print(f"Successfully posted code review comment to PR #{pr_number}")


def main():
    """Main function to orchestrate the code review process."""
    # Configuration from environment variables
    pr_number = os.environ.get("PR_NUMBER")
    repo = os.environ.get("REPO")
    github_token = os.environ.get("GITHUB_TOKEN")
    openai_api_key = os.environ.get("OPENAI_API_KEY")
    comment_body = os.environ.get("COMMENT_BODY", "")

    if not all([pr_number, repo, github_token, openai_api_key]):
        print("Missing required environment variables")
        sys.exit(1)

    # Initialize OpenAI client
    client = OpenAI(api_key=openai_api_key)

    # Extract user-specified topics from the comment
    user_topics = extract_user_topics(comment_body)
    if user_topics:
        print(f"User requested focus on: {', '.join(user_topics)}")
    else:
        print("No specific topics requested, using default review criteria")

    # Get the list of changed Rust files
    files_str = os.environ.get("CHANGED_FILES", "")
    if not files_str:
        print("No Rust files to review")
        sys.exit(0)

    files = files_str.split()

    # Collect diffs for all changed files
    all_diffs = {}
    for file in files:
        diff = get_file_diff(file)
        if diff and not diff.startswith("Error"):
            all_diffs[file] = diff

    if not all_diffs:
        print("No meaningful diffs found in Rust files")
        sys.exit(0)

    # Generate and post the review with user topics
    prompt = generate_review_prompt(all_diffs, user_topics)
    review = call_openai_api(client, prompt)
    post_review_comment(repo, pr_number, review, github_token, user_topics)


def test_topic_extraction():
    """Test function for topic extraction (for development/debugging)."""
    test_cases = [
        "/rust_review focus on: performance, memory safety",
        "/rust_review consider: error handling, testing, documentation",
        "/rust_review check: async, concurrency, lifetimes",
        "/rust_review performance and memory optimization",
        "/rust_review",  # No topics
        "/rust_review focus on: invalid_topic, performance",  # Mixed valid/invalid
    ]

    for test_case in test_cases:
        topics = extract_user_topics(test_case)
        print(f"Input: {test_case}")
        print(f"Extracted topics: {topics}")
        print("---")


if __name__ == "__main__":
    # Uncomment the line below to test topic extraction
    # test_topic_extraction()
    main()
