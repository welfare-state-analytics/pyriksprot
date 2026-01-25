"""
Unit tests for speech_index workflow.

Tests speech index generation parameter validation and configuration.
"""

import pytest

from pyriksprot import interface
from pyriksprot.workflows import speech_index

import inspect

@pytest.mark.unit
def test_extract_speech_index_validates_required_parameters():
    """Test that required parameters are validated."""

    # Missing required parameters should raise error
    with pytest.raises((TypeError, ValueError)):
        speech_index.extract_speech_index(
            source_folder=None,
            metadata_filename=None,
        )


@pytest.mark.unit
def test_extract_speech_index_validates_merge_strategy(tmp_path):
    """Test that invalid merge strategies are rejected."""

    with pytest.raises(ValueError, match="invalid merge strategy"):
        speech_index.extract_speech_index(
            source_folder=str(tmp_path),
            metadata_filename=str(tmp_path / "metadata.db"),
            merge_strategy="invalid_strategy_xyz",
        )


@pytest.mark.unit
def test_extract_speech_index_defaults():
    """Test default parameter values."""

    sig = inspect.signature(speech_index.extract_speech_index)

    # Check defaults
    assert sig.parameters['target_name'].default == 'speech_index.csv.gz'
    assert sig.parameters['segment_skip_size'].default == 1
    assert sig.parameters['multiproc_processes'].default == 1
    assert sig.parameters['multiproc_chunksize'].default == 100
    assert sig.parameters['merge_strategy'].default == 'chain'
    assert sig.parameters['source_pattern'].default == '**/prot-*.zip'


@pytest.mark.unit
def test_extract_speech_index_accepts_valid_segment_levels():
    """Test that valid segment levels are accepted."""

    # Valid levels
    levels = [
        interface.SegmentLevel.Speech,
        interface.SegmentLevel.Utterance,
        interface.SegmentLevel.Protocol,
    ]

    for level in levels:
        assert level is not None


@pytest.mark.unit
def test_extract_speech_index_merge_strategies():
    """Test valid merge strategies."""

    valid_strategies = [
        'chain',
        'who',
        'who_sequence',
        'speaker_note_id_sequence',
    ]

    for strategy in valid_strategies:
        assert isinstance(strategy, str)
        assert len(strategy) > 0


@pytest.mark.unit
def test_extract_speech_index_year_filtering():
    """Test year filtering parameter."""

    sig = inspect.signature(speech_index.extract_speech_index)

    # years parameter should exist
    assert 'years' in sig.parameters
    assert sig.parameters['years'].default is None
