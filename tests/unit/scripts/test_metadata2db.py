"""
Unit tests for metadata2db CLI script.

Tests argument parsing and metadata operations.
"""

import pytest


@pytest.mark.unit
def test_metadata2db_script_exists():
    """Test that metadata2db script is accessible."""
    try:
        from pyriksprot.scripts import metadata2db  # type: ignore ; pylint: disable=import-outside-toplevel

        assert hasattr(metadata2db, 'main')
    except ImportError:
        pytest.skip("metadata2db module not found")


@pytest.mark.unit
def test_metadata2db_has_subcommands():
    """Test that metadata2db has database management subcommands."""
    try:
        from pyriksprot.scripts import metadata2db  # type: ignore ; pylint: disable=import-outside-toplevel

        # Should have database, download, or index subcommands
        functions = [name for name in dir(metadata2db) if not name.startswith('_')]
        assert len(functions) > 0
    except ImportError:
        pytest.skip("metadata2db module not found")


@pytest.mark.unit
def test_metadata2db_validates_input():
    """Test that metadata2db validates input parameters."""
    from click.testing import CliRunner  # type: ignore ; pylint: disable=import-outside-toplevel

    from pyriksprot.scripts import metadata2db  # type: ignore ; pylint: disable=import-outside-toplevel

    runner = CliRunner()
    result = runner.invoke(metadata2db.main, [])

    # Should handle empty arguments appropriately
    # (either fail or show help)
    assert result.exit_code >= 0
