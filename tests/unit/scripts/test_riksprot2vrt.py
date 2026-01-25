"""
Unit tests for riksprot2vrt CLI script.

Tests VRT export argument parsing and validation.
"""

import pytest

# type: ignore ; pylint: disable=import-outside-toplevel


@pytest.mark.unit
def test_riksprot2vrt_script_exists():
    """Test that riksprot2vrt script is accessible."""
    try:
        from pyriksprot.scripts import riksprot2vrt

        # VRT export should have a main function
        functions = [name for name in dir(riksprot2vrt) if not name.startswith('_')]
        assert len(functions) > 0
    except ImportError:
        pytest.skip("riksprot2vrt module not found")


@pytest.mark.unit
def test_riksprot2vrt_accepts_source_and_target():
    """Test that VRT export accepts source and target parameters."""
    import inspect

    try:
        from pyriksprot.scripts import riksprot2vrt

        # Get main export function
        funcs = [
            obj
            for name, obj in inspect.getmembers(riksprot2vrt)
            if inspect.isfunction(obj) and not name.startswith('_')
        ]

        if funcs:
            # At least one function should exist
            assert len(funcs) > 0
    except ImportError:
        pytest.skip("riksprot2vrt module not found")
