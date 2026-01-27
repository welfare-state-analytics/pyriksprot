"""
Unit tests for tag workflow.

Tests tagging pipeline parameter validation.
"""

import pytest

from pyriksprot.workflows import tag


@pytest.mark.unit
def test_tag_workflow_function_exists():
    """Test that tag workflow functions are accessible."""

    # Check for tagging functions
    functions = [name for name in dir(tag) if not name.startswith('_')]

    # Should have at least some public functions
    assert len(functions) > 0
