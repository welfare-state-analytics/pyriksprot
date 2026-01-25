"""
Integration tests for speech corpus generation CLI scripts.

Tests riksprot2speech and riksprot2speech_index with real data.
"""

import pytest
from pyriksprot.scripts import riksprot2speech
from pyriksprot.scripts import riksprot2speech_index
from click.testing import CliRunner

@pytest.fixture
def run_slow_tests(request):
    return request.config.getoption("--with-slow", default=False)

# pylint: disable=unused-argument, redefined-outer-name

@pytest.mark.integration
def test_riksprot2speech_help(run_slow_tests):
    """Test that riksprot2speech --help works."""

    if not run_slow_tests:
        pytest.skip("Integration test (slow)")

    runner = CliRunner()
    result = runner.invoke(riksprot2speech.main, ['--help'])
    
    assert result.exit_code == 0
    assert 'Usage:' in result.output or 'help' in result.output.lower()


@pytest.mark.integration
def test_riksprot2speech_index_help(run_slow_tests):  # pylint: disable=unused-argument
    """Test that riksprot2speech_index --help works."""

    if not run_slow_tests:
        pytest.skip("Integration test (slow)")
    
    try:

        
        runner = CliRunner()
        result = runner.invoke(riksprot2speech_index.main, ['--help'])
        
        assert result.exit_code == 0
    except ImportError:
        pytest.skip("riksprot2speech_index module not found")
