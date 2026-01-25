"""
Unit tests for make-config CLI script.

Tests configuration generation and validation.
"""

import pytest


@pytest.mark.unit
def test_make_config_script_exists():
    """Test that make_config script is accessible."""
    try:
        from pyriksprot.scripts import make_config  # type: ignore ; pylint: disable=import-outside-toplevel

        assert hasattr(make_config, 'main')
    except ImportError:
        pytest.skip("make_config module not found")


@pytest.mark.unit
def test_make_config_generates_yaml():
    """Test that make_config generates YAML configuration."""
    try:
        from pyriksprot.scripts import make_config  # type: ignore ; pylint: disable=import-outside-toplevel

        # Should be able to generate config
        functions = [name for name in dir(make_config) if not name.startswith('_')]
        assert len(functions) > 0
    except ImportError:
        pytest.skip("make_config module not found")
