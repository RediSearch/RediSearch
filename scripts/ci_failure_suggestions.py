#!/usr/bin/env python3
"""
CI Failure Suggestions - AI-powered analysis of CI failures.

This module uses OpenAI to analyze CI failures and provide specific,
actionable fix suggestions for the RediSearch project.
"""

import os
import json
import hashlib
from pathlib import Path
from typing import Optional

# Optional import - will be None if openai is not installed
try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    openai = None

# Cache directory for AI responses (to avoid redundant API calls)
CACHE_DIR = Path(__file__).parent / ".ai_cache"

# System prompt for the AI
SYSTEM_PROMPT = """You are a CI/CD expert analyzing failures in the RediSearch project.

RediSearch is a Redis module providing full-text search, secondary indexing, and vector similarity search.
The codebase consists of:
- C code in src/ (being ported to Rust)
- Rust code in src/redisearch_rs/
- Python tests in tests/pytests/
- Build system using CMake and Cargo

Build commands:
- ./build.sh                    # Full build
- ./build.sh DEBUG=1            # Debug build
- ./build.sh RUN_PYTEST TEST=x  # Run specific Python test
- cd src/redisearch_rs && cargo test  # Rust tests

Your task: Analyze the CI failure and provide a SPECIFIC, ACTIONABLE fix suggestion.
Be concise (2-3 sentences max). Focus on the root cause and exact steps to fix it.
If you see specific file paths, function names, or error codes, reference them.
Do not provide generic advice - be as specific as possible based on the error details."""


def _get_cache_key(error_message: str, job_name: str, log_excerpt: str) -> str:
    """Generate a cache key for the AI response."""
    content = f"{error_message}|{job_name}|{log_excerpt}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def _get_cached_suggestion(cache_key: str) -> Optional[str]:
    """Retrieve a cached AI suggestion if available."""
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                return data.get('suggestion')
        except (json.JSONDecodeError, IOError):
            pass
    return None


def _cache_suggestion(cache_key: str, suggestion: str):
    """Cache an AI suggestion for future use."""
    CACHE_DIR.mkdir(exist_ok=True)
    cache_file = CACHE_DIR / f"{cache_key}.json"
    try:
        with open(cache_file, 'w') as f:
            json.dump({'suggestion': suggestion}, f)
    except IOError:
        pass  # Caching is best-effort


def analyze_failure_with_ai(
    error_message: Optional[str],
    job_name: str,
    log_excerpt: Optional[str] = None,
    failure_type: str = "unknown"
) -> Optional[str]:
    """
    Use OpenAI to analyze a CI failure and provide specific fix suggestions.

    Args:
        error_message: The error message from the failure
        job_name: The name of the failed job
        log_excerpt: Additional log context (last ~50 lines before failure)
        failure_type: The categorized failure type

    Returns:
        A specific, actionable suggestion from the AI, or None if unavailable
    """
    # Try project key first (no org ID needed), fall back to regular API key
    api_key = os.getenv("OPENAI_PROJECT_KEY") or os.getenv("OPENAI_API_KEY")

    if not api_key:
        print("      ⚠️  OPENAI_PROJECT_KEY or OPENAI_API_KEY not set, skipping AI analysis")
        return None

    if not OPENAI_AVAILABLE:
        print("      ⚠️  openai package not installed, skipping AI analysis")
        return None

    # Build the content to analyze
    content_parts = [f"Job: {job_name}"]

    if failure_type and failure_type != "unknown":
        content_parts.append(f"Failure type: {failure_type}")

    if error_message:
        content_parts.append(f"Error message:\n{error_message}")

    if log_excerpt:
        # Limit log excerpt to avoid token limits
        lines = log_excerpt.split('\n')
        if len(lines) > 50:
            lines = lines[-50:]
        content_parts.append(f"Log excerpt (last lines before failure):\n" + '\n'.join(lines))

    user_content = '\n\n'.join(content_parts)

    # Check cache first
    cache_key = _get_cache_key(error_message or "", job_name, log_excerpt or "")
    cached = _get_cached_suggestion(cache_key)
    if cached:
        return cached

    try:
        # Support organization ID if the API key is org-tied
        org_id = os.getenv("OPENAI_ORG_ID")
        client = openai.OpenAI(
            api_key=api_key,
            organization=org_id  # None is fine if not set
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Fast and cost-effective
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_content}
            ],
            max_tokens=200,
            temperature=0.3  # Lower temperature for more focused responses
        )

        suggestion = response.choices[0].message.content.strip()

        # Cache the result
        _cache_suggestion(cache_key, suggestion)

        return suggestion

    except Exception as e:
        error_str = str(e)
        if "invalid_organization" in error_str:
            print(f"      ⚠️  AI analysis failed: Invalid OpenAI organization.")
            print(f"         Either use a project API key (sk-proj-...) which doesn't need org ID,")
            print(f"         or set OPENAI_ORG_ID to your org ID (org-xxx) from platform.openai.com/settings/organization")
        else:
            print(f"      ⚠️  AI analysis failed: {e}")
        return None


def get_suggestion_for_failure(
    error_message: Optional[str],
    job_name: str,
    failure_type: str,
    log_excerpt: Optional[str] = None
) -> Optional[str]:
    """
    Get a suggested fix for a CI failure using AI analysis.

    Args:
        error_message: The error message from the failure (may be None)
        job_name: The name of the failed job
        failure_type: The categorized failure type
        log_excerpt: Additional log context for better analysis

    Returns:
        A suggestion string from AI, or None if unavailable
    """
    return analyze_failure_with_ai(
        error_message=error_message,
        job_name=job_name,
        log_excerpt=log_excerpt,
        failure_type=failure_type
    )

