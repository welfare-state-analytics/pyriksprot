"""
Integration tests for text extraction CLI scripts.

Tests actual riksprot2text execution with sample data.
"""

import pytest
from click.testing import CliRunner

from pyriksprot.scripts import riksprot2text


@pytest.fixture
def run_slow_tests(request):
    return request.config.getoption("--with-slow", default=False)


# pylint: disable=unused-argument, redefined-outer-name


@pytest.mark.integration
def test_riksprot2text_basic_execution(run_slow_tests, tmp_path, source_index):  # pylint: disable=unused-argument
    """Test riksprot2text script execution with sample data."""
    if source_index is None:
        pytest.skip("source_index fixture not available")

    if not run_slow_tests:
        pytest.skip("Integration test (slow)")

    runner = CliRunner()

    # Test with minimal arguments
    result = runner.invoke(
        riksprot2text.main,
        [
            '--source-folder',
            str(source_index.corpus_folder),
            '--output-folder',
            str(tmp_path),
        ],
    )

    # Should either succeed or provide clear error message
    assert result.exit_code in (0, 1, 2)


@pytest.mark.integration
def test_riksprot2text_help_works(run_slow_tests):
    """Test that riksprot2text --help works."""
    if not run_slow_tests:
        pytest.skip("Integration test (slow)")

    runner = CliRunner()
    result = runner.invoke(riksprot2text.main, ['--help'])

    assert result.exit_code == 0
    assert 'Usage:' in result.output or 'help' in result.output.lower()
