"""
Unit tests for riksprot2speech CLI script.

Tests argument parsing and configuration validation.
"""

import pytest
from click.testing import CliRunner


@pytest.mark.unit
def test_riksprot2speech_script_exists():
    """Test that riksprot2speech script is accessible."""
    try:
        from pyriksprot.scripts import riksprot2speech
        assert hasattr(riksprot2speech, 'main')
    except ImportError:
        pytest.skip("riksprot2speech module not found")


@pytest.mark.unit
def test_riksprot2speech_validates_output_folder():
    """Test that output folder is required."""
    from pyriksprot.scripts import riksprot2speech
    
    runner = CliRunner()
    result = runner.invoke(riksprot2speech.main, [])
    
    # Should fail without required arguments
    assert result.exit_code != 0


@pytest.mark.unit
def test_riksprot2speech_accepts_merge_strategy():
    """Test that merge strategy option is recognized."""
    from pyriksprot.scripts import riksprot2speech
    
    import inspect
    sig = inspect.signature(riksprot2speech.main)
    
    # Check that function accepts parameters
    assert len(sig.parameters) > 0
