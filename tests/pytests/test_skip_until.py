"""
Tests for the skip_until decorator and skipTestUntil function.
"""

from common import skip_until, skipTestUntil
from unittest import SkipTest
from datetime import datetime, timedelta
import pytest


class TestSkipUntilDecorator:
    """Tests for the @skip_until decorator."""

    def test_skips_when_date_is_in_future(self):
        """Test that a test is skipped when the date is in the future."""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        @skip_until(future_date, reason="Testing future skip")
        def dummy_test():
            return "executed"

        with pytest.raises(SkipTest) as exc_info:
            dummy_test()

        assert future_date in str(exc_info.value)
        assert "Testing future skip" in str(exc_info.value)

    def test_runs_when_date_is_in_past(self):
        """Test that a test runs when the date has passed."""
        past_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        @skip_until(past_date, reason="This should not skip")
        def dummy_test():
            return "executed"

        result = dummy_test()
        assert result == "executed"

    def test_runs_when_date_is_today(self):
        """Test that a test runs when the date is today."""
        today = datetime.now().strftime("%Y-%m-%d")

        @skip_until(today, reason="Today's date")
        def dummy_test():
            return "executed"

        result = dummy_test()
        assert result == "executed"

    def test_skip_without_reason(self):
        """Test that skip works without a reason."""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        @skip_until(future_date)
        def dummy_test():
            return "executed"

        with pytest.raises(SkipTest) as exc_info:
            dummy_test()

        assert future_date in str(exc_info.value)

    def test_preserves_function_arguments(self):
        """Test that the decorator preserves function arguments."""
        past_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        @skip_until(past_date)
        def dummy_test(a, b, c=None):
            return (a, b, c)

        result = dummy_test(1, 2, c=3)
        assert result == (1, 2, 3)


class TestSkipTestUntilFunction:
    """Tests for the skipTestUntil function."""

    def test_skips_when_date_is_in_future(self):
        """Test that skipTestUntil raises SkipTest for future dates."""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

        with pytest.raises(SkipTest) as exc_info:
            skipTestUntil(future_date, reason="Future skip from function")

        assert future_date in str(exc_info.value)
        assert "Future skip from function" in str(exc_info.value)

    def test_does_not_skip_when_date_is_in_past(self):
        """Test that skipTestUntil does not raise for past dates."""
        past_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        # This should not raise
        skipTestUntil(past_date, reason="Past date")
        # If we get here, the test passed
        assert True
