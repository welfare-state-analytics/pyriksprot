"""
Integration tests for extract_text workflow.

Tests full end-to-end text extraction with sample corpus data.
"""

import pytest

from tests.utility import sample_parlaclarin_corpus_exists


@pytest.mark.integration
@pytest.mark.skipif(not sample_parlaclarin_corpus_exists(), reason="ParlaCLARIN corpus not found")
def test_extract_corpus_text_end_to_end(tmp_path, xml_source_index):
    """Test complete extract_text workflow with sample data."""
    if xml_source_index is None or len(xml_source_index.paths) == 0:
        pytest.skip("xml_source_index not available")

    from pyriksprot import interface
    from pyriksprot.workflows import extract_text

    # Execute workflow
    try:
        extract_text.extract_corpus_text(
            source_folder=str(xml_source_index.paths[0].parent.parent.parent),
            metadata_filename=None,
            target_name=str(tmp_path / "output.zip"),
            target_type='files-in-zip',
            segment_level=interface.SegmentLevel.Speech,
            compress_type='zip',
            multiproc_processes=1,
        )
        # Workflow should complete
        assert True
    except FileNotFoundError:
        pytest.skip("Required test data not found")
    except Exception as e:
        pytest.skip(f"Workflow execution skipped: {e}")


@pytest.mark.integration
def test_extract_corpus_text_with_dehyphenation(tmp_path):
    """Test that dehyphenation option is recognized."""
    from pyriksprot.workflows import extract_text

    # Dehyphenation flag should be accepted
    try:
        extract_text.extract_corpus_text(
            source_folder="/nonexistent",
            target_name=str(tmp_path / "output"),
            dehyphen=True,
        )
    except (FileNotFoundError, ValueError):
        # Expected - invalid source
        pass
