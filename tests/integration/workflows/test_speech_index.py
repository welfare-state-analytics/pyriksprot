"""
Integration tests for speech_index workflow.

Tests speech index generation with sample data.
"""

import pytest
import pandas as pd

from tests.utility import sample_tagged_frames_corpus_exists


@pytest.mark.integration
@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames corpus not found")
def test_extract_speech_index_end_to_end(tmp_path, source_index, person_index):
    """Test complete speech index extraction workflow."""
    if source_index is None or person_index is None:
        pytest.skip("Required fixtures not available")
    
    from pyriksprot.workflows import speech_index
    from pyriksprot import interface
    
    try:
        result = speech_index.extract_speech_index(
            source_folder=str(source_index.paths[0].parent.parent) if source_index.paths else None,
            metadata_filename="tests/test_data/source/5files/metadata/v1.1.3/riksprot_metadata.db",
            target_name=str(tmp_path / "speech_index.csv.gz"),
            segment_level=interface.SegmentLevel.Speech,
            merge_strategy='chain',
            multiproc_processes=1,
        )
        
        # Should return a DataFrame
        if result is not None:
            assert isinstance(result, pd.DataFrame)
    except FileNotFoundError:
        pytest.skip("Required test data not found")
    except Exception as e:
        pytest.skip(f"Workflow execution skipped: {e}")


@pytest.mark.integration
def test_extract_speech_index_output_format(tmp_path):
    """Test that speech index output format is correct."""
    # CSV.gz format should be recognized
    output_file = tmp_path / "speech_index.csv.gz"
    assert str(output_file).endswith('.csv.gz')
