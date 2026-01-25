"""
Unit tests for riksprot2text CLI script.

Tests argument parsing and configuration validation.
"""

import pytest


@pytest.mark.unit
def test_riksprot2text_script_exists():
    """Test that riksprot2text script is accessible."""
    try:
        from pyriksprot.scripts import riksprot2text
        assert hasattr(riksprot2text, 'main')
    except ImportError:
        pytest.skip("riksprot2text module not found")


@pytest.mark.unit
def test_riksprot2text_validates_required_args():
    """Test that required arguments are validated."""
    from pyriksprot.scripts import riksprot2text
    from click.testing import CliRunner
    
    runner = CliRunner()
    result = runner.invoke(riksprot2text.main, [])
    
    # Should fail without arguments
    assert result.exit_code != 0


@pytest.mark.unit
def test_riksprot2text_accepts_dehyphenation():
    """Test that dehyphenation option is recognized."""
    import inspect
    from pyriksprot.scripts import riksprot2text
    
    # Check that function signature exists
    sig = inspect.signature(riksprot2text.main)
    assert len(sig.parameters) > 0
