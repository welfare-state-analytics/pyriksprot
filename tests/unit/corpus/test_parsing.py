"""
Unit tests for corpus parsing functionality.

Tests ParlaCLARIN XML parsing without file I/O.
"""

import pytest

from tests.fixtures.synthetic import minimal_protocol, minimal_utterance  # type: ignore ; pylint: disable=unused-import

# pylint: disable=redefined-outer-name


@pytest.mark.unit
def test_utterance_to_dict(minimal_utterance):
    """Test that utterances can be converted to dict."""
    result = minimal_utterance.to_dict()

    assert isinstance(result, dict)
    assert 'who' in result
    assert 'u_id' in result


@pytest.mark.unit
def test_protocol_structure(minimal_protocol):
    """Test minimal protocol structure."""
    assert minimal_protocol.name is not None
    assert minimal_protocol.date is not None
    assert len(minimal_protocol.utterances) > 0


@pytest.mark.unit
def test_utterance_text_extraction(minimal_utterance):
    """Test extracting text from utterance."""
    text = minimal_utterance.text

    assert isinstance(text, str)
    assert len(text) >= 0


@pytest.mark.unit
@pytest.mark.skip(reason="Date parsing not implemented yet")
def test_protocol_year_extraction(minimal_protocol):
    """Test extracting year from protocol."""
    year = minimal_protocol.date.year if minimal_protocol.date else None

    assert year is not None
    assert 1800 <= year <= 2100
