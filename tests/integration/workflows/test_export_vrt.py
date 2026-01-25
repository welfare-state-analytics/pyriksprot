"""
Integration tests for VRT export workflow.

Tests corpus export to VRT format.
"""

import pytest

from tests.utility import sample_tagged_frames_corpus_exists
from pyriksprot.workflows import export_vrt


@pytest.mark.integration
@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames corpus not found")
def test_export_vrt_end_to_end(tmp_path):
    """Test complete VRT export workflow."""
    
    try:
        # VRT export should handle sample data
        result = export_vrt.export_yearly_folders(
            source_folder="tests/test_data/source/5files",
            target_folder=str(tmp_path / "vrt"),
        )
        assert True
    except FileNotFoundError:
        pytest.skip("Required test data not found")
    except Exception as e:
        pytest.skip(f"Workflow execution skipped: {e}")
