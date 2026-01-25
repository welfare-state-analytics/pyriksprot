"""
Unit tests for corpus iteration functionality.

Tests iteration patterns without file system access.
"""

import pytest

from tests.fixtures.synthetic import minimal_protocol, minimal_utterance  # type: ignore

# pylint: disable=unused-argument


@pytest.mark.unit
def test_protocol_utterance_iteration(minimal_protocol):  # type: ignore
    """Test iterating over protocol utterances."""
    utterances = list(minimal_protocol.utterances)

    assert len(utterances) > 0
    assert all(hasattr(u, 'who') for u in utterances)
    assert all(hasattr(u, 'text') for u in utterances)


@pytest.mark.unit
def test_utterance_speaker_reference(minimal_utterance):  # type: ignore
    """Test that utterance has speaker reference."""
    assert hasattr(minimal_utterance, 'who')
    assert minimal_utterance.who is not None


@pytest.mark.unit
def test_utterance_page_reference(minimal_utterance):  # type: ignore
    """Test utterance page reference."""
    assert hasattr(minimal_utterance, 'page_number')
    # Page number can be None or an integer
    if minimal_utterance.page_number is not None:
        assert isinstance(minimal_utterance.page_number, int)
        assert minimal_utterance.page_number > 0
