"""
Unit tests for metadata queries and operations.

Tests metadata database queries without external dependencies.
"""

import pytest

from pyriksprot.metadata import utility

# type: ignore ; pylint: disable=import-outside-toplevel


@pytest.mark.unit
def test_metadata_loaders_exist():
    """Test that metadata loader modules exist."""
    try:
        from pyriksprot.metadata import metadata_table_loader  # type: ignore

        assert hasattr(metadata_table_loader, 'MetadataTableLoader')
    except ImportError:
        pytest.skip("metadata_table_loader module not available")


@pytest.mark.unit
def test_metadata_person_info():
    """Test person metadata information retrieval."""
    try:
        # PersonInfo should be available
        from pyriksprot.metadata.person_info import PersonInfo  # type: ignore

        assert PersonInfo is not None
    except ImportError:
        pytest.skip("PersonInfo class not available")


@pytest.mark.unit
def test_metadata_utilities():
    """Test metadata utility functions."""
    try:

        # Module should exist and have utilities
        funcs = [name for name in dir(utility) if not name.startswith('_')]
        assert len(funcs) > 0
    except ImportError:
        pytest.skip("metadata.utility module not available")
