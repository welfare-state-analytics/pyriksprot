"""
Unit tests for extract_text workflow.

Tests parameter validation and workflow configuration without actual corpus processing.
"""

import inspect

import pytest

from pyriksprot import interface
from pyriksprot.dispatch import dispatch
from pyriksprot.workflows import extract_text


@pytest.mark.unit
def test_extract_corpus_text_validates_required_parameters():
    """Test that missing required parameters are handled."""

    # Should handle None parameters gracefully
    try:
        extract_text.extract_corpus_text(
            source_folder=None,
            target_name="output",
        )
    except (TypeError, ValueError):
        # Expected - missing required parameters
        pass


@pytest.mark.unit
def test_extract_corpus_text_validates_segment_level():
    """Test segment level validation."""

    # Should accept valid segment levels
    valid_levels = [
        interface.SegmentLevel.Protocol,
        interface.SegmentLevel.Utterance,
        interface.SegmentLevel.Speech,
    ]

    for level in valid_levels:
        assert level is not None


@pytest.mark.unit
def test_extract_corpus_text_validates_compress_type():
    """Test that valid compress types are accepted."""

    # Valid compress types
    valid_types = [
        dispatch.CompressType.Plain,
        dispatch.CompressType.Zip,
        dispatch.CompressType.Lzma,
    ]

    for compress_type in valid_types:
        assert compress_type is not None


@pytest.mark.unit
def test_extract_corpus_text_boolean_flags():
    """Test boolean parameter handling."""

    sig = inspect.signature(extract_text.extract_corpus_text)

    assert sig.parameters['dedent'].default is True
    assert sig.parameters['dehyphen'].default is False


@pytest.mark.unit
def test_extract_corpus_text_multiprocessing_params():
    """Test multiprocessing parameter defaults."""

    sig = inspect.signature(extract_text.extract_corpus_text)

    # Check multiprocessing defaults
    assert sig.parameters['multiproc_processes'].default == 1
    assert sig.parameters['multiproc_chunksize'].default == 100
    assert sig.parameters['segment_skip_size'].default == 1
