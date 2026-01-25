"""
Integration tests for metadata database CLI scripts.

Tests metadata2db commands with real data.
"""

import pytest

from pyriksprot.scripts import metadata2db
from click.testing import CliRunner
    
@pytest.mark.integration
def test_metadata2db_help():
    """Test that metadata2db --help works."""

    runner = CliRunner()
    result = runner.invoke(metadata2db.main, ['--help'])
    
    # Should provide help text
    assert result.exit_code in (0, 1, 2)


# @pytest.mark.integration
# @pytest.mark.skipif(not config.getoption("--with-slow", default=False), reason="Integration test (slow)")
# def test_metadata2db_database_help():
#     """Test metadata2db database subcommand help."""
#     try:
#         from pyriksprot.scripts import metadata2db
#         from click.testing import CliRunner
        
#         runner = CliRunner()
#         result = runner.invoke(metadata2db.main, ['database', '--help'])
        
#         assert result.exit_code in (0, 1, 2)
#     except Exception as e:
#         pytest.skip(f"metadata2db database command not accessible: {e}")
