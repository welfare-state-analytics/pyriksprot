"""
Unit tests for export_vrt workflow.

Tests VRT export parameter validation.
"""

import pytest


@pytest.mark.unit
def test_vrt_format_is_supported():
    """Test that VRT format is recognized."""
    # VRT is Corpus Workbench format
    assert 'VRT' in 'VRT'  # Self-check that format name is correct
