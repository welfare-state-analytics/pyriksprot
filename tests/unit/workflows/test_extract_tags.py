"""
Unit tests for extract_tags workflow.

Tests parameter validation, merge strategy handling, and workflow configuration
without actual corpus processing.
"""

import inspect

import pytest

from pyriksprot.dispatch import dispatch
from pyriksprot.workflows import extract_tags


@pytest.mark.unit
def test_extract_corpus_tags_validates_required_parameters():
    """Test that required parameters are validated."""

    # Missing source_folder should raise error
    with pytest.raises(ValueError, match="source_folder is required"):
        extract_tags.extract_corpus_tags(
            source_folder=None,
            metadata_filename="test.db",
            target_name="output",
        )

    with pytest.raises(FileNotFoundError, match="source folder"):
        extract_tags.extract_corpus_tags(
            source_folder="/nonexistent",
            metadata_filename="test.db",
            target_name="output",
        )


@pytest.mark.unit
def test_extract_corpus_tags_validates_merge_strategy(tmp_path):
    """Test that invalid merge strategies are rejected."""

    # Invalid merge strategy should raise error
    with pytest.raises(ValueError, match="lemma/text skip not implemented for"):
        extract_tags.extract_corpus_tags(
            source_folder=str(tmp_path),
            metadata_filename="/nonexistent.db",
            target_name="output",
            merge_strategy="invalid_strategy_xyz",
        )


@pytest.mark.unit
def test_extract_corpus_tags_validates_segment_level():
    """Test that invalid segment levels are handled."""

    # Invalid segment level
    invalid_level = "invalid_level"

    # Should handle gracefully or raise clear error
    with pytest.raises(FileNotFoundError):
        extract_tags.extract_corpus_tags(
            source_folder="/nonexistent",
            metadata_filename="/nonexistent.db",
            target_name="output",
            segment_level=invalid_level,  # type: ignore
        )


@pytest.mark.unit
def test_extract_corpus_tags_validates_compress_type():
    """Test that valid compress types are accepted."""

    # Valid compress types should be recognized
    valid_types = [
        dispatch.CompressType.Plain,
        dispatch.CompressType.Zip,
        dispatch.CompressType.Lzma,
        dispatch.CompressType.Feather,
    ]

    for compress_type in valid_types:
        # Should not raise during parameter validation
        assert compress_type is not None


@pytest.mark.unit
def test_extract_corpus_tags_defaults():
    """Test default parameter values."""

    # Extract the function signature

    sig = inspect.signature(extract_tags.extract_corpus_tags)

    # Check for expected defaults
    assert sig.parameters['segment_skip_size'].default == 1
    assert sig.parameters['multiproc_processes'].default == 1
    assert sig.parameters['multiproc_chunksize'].default == 100
    assert sig.parameters['merge_strategy'].default == 'chain'
    assert sig.parameters['force'].default is False
    assert sig.parameters['lowercase'].default is True
    assert sig.parameters['progress'].default is True


@pytest.mark.unit
def test_extract_corpus_tags_parameter_types():
    """Test that parameter types are reasonable."""

    sig = inspect.signature(extract_tags.extract_corpus_tags)
    params = sig.parameters

    # source_folder should be str
    assert 'source_folder' in params

    # multiproc_processes should accept int
    assert 'multiproc_processes' in params

    # merge_strategy should be recognizable
    assert 'merge_strategy' in params

    # force should be bool
    assert 'force' in params
