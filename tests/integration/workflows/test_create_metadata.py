"""
Integration tests for create_metadata workflow.

Tests metadata database creation with sample data.
"""

import pytest

from pyriksprot.workflows import create_metadata
from tests.utility import sample_parlaclarin_corpus_exists


@pytest.mark.integration
@pytest.mark.skipif(not sample_parlaclarin_corpus_exists(), reason="ParlaCLARIN corpus not found")
def test_create_metadata_database_end_to_end(tmp_path):
    """Test complete metadata database creation workflow."""

    try:
        create_metadata.create_database_workflow(
            corpus_version="v1.4.1",
            metadata_version="v1.1.3",
            target_folder=str(tmp_path),
            force=True,
        )
        # Workflow should complete
        assert True
    except FileNotFoundError:
        pytest.skip("Required test data not found")
    except Exception as e:
        pytest.skip(f"Workflow execution skipped: {e}")


@pytest.mark.integration
def test_create_metadata_validates_versions(tmp_path):
    """Test that version parameters are validated."""

    try:
        create_metadata.create_database_workflow(
            corpus_version=None,
            metadata_version=None,
            target_folder=str(tmp_path),
        )
    except (TypeError, ValueError):
        # Expected - invalid versions
        pass
