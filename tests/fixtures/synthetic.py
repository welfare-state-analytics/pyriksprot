"""
Synthetic test fixtures for unit testing.

These fixtures provide minimal, in-memory test data that don't require
external files, databases, or network access. They're designed to be fast
and isolated for unit testing.
"""

from typing import Optional

import pandas as pd
import pytest

from pyriksprot import interface, to_speech

# pylint: disable=redefined-outer-name, too-many-positional-arguments
# ============================================================================
# Utterance Fixtures
# ============================================================================


@pytest.fixture
def minimal_utterance() -> interface.Utterance:
    """Single utterance with minimal valid data."""
    return interface.Utterance(
        u_id="u1",
        who="speaker_a",
        speaker_note_id="note_1",
        paragraphs=["This is the first utterance."],
        page_number=1,
    )


@pytest.fixture
def utterance_sequence_same_speaker() -> list[interface.Utterance]:
    """Three consecutive utterances from same speaker."""
    return [
        interface.Utterance(
            u_id="u1",
            prev_id=None,
            next_id="u2",
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["First utterance."],
        ),
        interface.Utterance(
            u_id="u2",
            prev_id="u1",
            next_id="u3",
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["Second utterance."],
        ),
        interface.Utterance(
            u_id="u3",
            prev_id="u2",
            next_id=None,
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["Third utterance."],
        ),
    ]


@pytest.fixture
def utterance_sequence_different_speakers() -> list[interface.Utterance]:
    """Three utterances with different speakers."""
    return [
        interface.Utterance(
            u_id="u1",
            prev_id=None,
            next_id="u2",
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["First utterance."],
        ),
        interface.Utterance(
            u_id="u2",
            prev_id="u1",
            next_id="u3",
            who="speaker_b",
            speaker_note_id="note_2",
            paragraphs=["Second utterance."],
        ),
        interface.Utterance(
            u_id="u3",
            prev_id="u2",
            next_id=None,
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["Third utterance."],
        ),
    ]


@pytest.fixture
def utterance_sequence_with_unknowns() -> list[interface.Utterance]:
    """Utterances with unknown speakers mixed in."""
    return [
        interface.Utterance(
            u_id="u1",
            prev_id=None,
            next_id="u2",
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["First utterance."],
        ),
        interface.Utterance(
            u_id="u2",
            prev_id="u1",
            next_id="u3",
            who="unknown",
            speaker_note_id="missing",
            paragraphs=["Unknown speaker."],
        ),
        interface.Utterance(
            u_id="u3",
            prev_id="u2",
            next_id=None,
            who="speaker_a",
            speaker_note_id="note_1",
            paragraphs=["Back to known speaker."],
        ),
    ]


# ============================================================================
# Protocol Fixtures
# ============================================================================


@pytest.fixture
def minimal_protocol() -> interface.Protocol:
    """Minimal valid protocol with basic structure."""
    return interface.Protocol(
        date="2000",
        name="prot-2000-minimal",
        utterances=[
            interface.Utterance(
                u_id="u1",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["Utterance one."],
            ),
            interface.Utterance(
                u_id="u2",
                who="speaker_b",
                speaker_note_id="note_2",
                paragraphs=["Utterance two."],
            ),
        ],
        speaker_notes={
            "note_1": "Speaker A is the first speaker",
            "note_2": "Speaker B is the second speaker",
        },
        page_references=[],
    )


@pytest.fixture
def protocol_single_speaker() -> interface.Protocol:
    """Protocol with only one speaker (all utterances)."""
    return interface.Protocol(
        date="1995",
        name="prot-1995-single-speaker",
        utterances=[
            interface.Utterance(
                u_id="u1",
                prev_id=None,
                next_id="u2",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["First statement."],
            ),
            interface.Utterance(
                u_id="u2",
                prev_id="u1",
                next_id="u3",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["Second statement."],
            ),
            interface.Utterance(
                u_id="u3",
                prev_id="u2",
                next_id=None,
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["Third statement."],
            ),
        ],
        speaker_notes={"note_1": "Speaker A"},
        page_references=[],
    )


@pytest.fixture
def protocol_multiple_speakers() -> interface.Protocol:
    """Protocol with multiple speakers."""
    return interface.Protocol(
        date="1990",
        name="prot-1990-multiple-speakers",
        utterances=[
            interface.Utterance(
                u_id="u1",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["A speaks."],
            ),
            interface.Utterance(
                u_id="u2",
                who="speaker_b",
                speaker_note_id="note_2",
                paragraphs=["B responds."],
            ),
            interface.Utterance(
                u_id="u3",
                who="speaker_c",
                speaker_note_id="note_3",
                paragraphs=["C intervenes."],
            ),
            interface.Utterance(
                u_id="u4",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["A speaks again."],
            ),
        ],
        speaker_notes={
            "note_1": "Speaker A",
            "note_2": "Speaker B",
            "note_3": "Speaker C",
        },
        page_references=[],
    )


@pytest.fixture
def protocol_with_unknown_speakers() -> interface.Protocol:
    """Protocol containing utterances with unknown speakers."""
    return interface.Protocol(
        date="1988",
        name="prot-1988-with-unknowns",
        utterances=[
            interface.Utterance(
                u_id="u1",
                who="speaker_a",
                speaker_note_id="note_1",
                paragraphs=["Known speaker."],
            ),
            interface.Utterance(
                u_id="u2",
                who="unknown",
                speaker_note_id="missing",
                paragraphs=["Unknown intervention."],
            ),
            interface.Utterance(
                u_id="u3",
                who="speaker_b",
                speaker_note_id="note_2",
                paragraphs=["Another known speaker."],
            ),
        ],
        speaker_notes={
            "note_1": "Speaker A",
            "note_2": "Speaker B",
        },
        page_references=[],
    )


@pytest.fixture
def protocol_empty() -> interface.Protocol:
    """Empty protocol with no utterances."""
    return interface.Protocol(
        date="1985",
        name="prot-1985-empty",
        utterances=[],
        speaker_notes={},
        page_references=[],
    )


# ============================================================================
# Speech Fixtures (from Protocols)
# ============================================================================


@pytest.fixture
def speeches_from_minimal_protocol(minimal_protocol: interface.Protocol) -> list[interface.Speech]:
    """Generate speeches from minimal protocol using default merge strategy."""

    return to_speech.to_speeches(protocol=minimal_protocol, merge_strategy="chain")


@pytest.fixture
def speeches_from_single_speaker_protocol(
    protocol_single_speaker: interface.Protocol,
) -> list[interface.Speech]:
    """Generate speeches from single speaker protocol."""

    return to_speech.to_speeches(protocol=protocol_single_speaker, merge_strategy="chain")


# ============================================================================
# Tagged Frame Fixtures
# ============================================================================


@pytest.fixture
def minimal_tagged_frame() -> pd.DataFrame:
    """Minimal valid tagged frame (POS-tagged text)."""
    return pd.DataFrame(
        {
            "token": ["This", "is", "text", "."],
            "pos": ["DT", "VB", "NN", "MAD"],
            "lemma": ["this", "be", "text", "."],
            "msd": ["", "", "", ""],
        }
    )


@pytest.fixture
def tagged_frame_with_multiple_tokens() -> pd.DataFrame:
    """Larger tagged frame with more diverse tokens."""
    return pd.DataFrame(
        {
            "token": [
                "The",
                "Swedish",
                "Parliament",
                "convenes",
                "today",
                "to",
                "discuss",
                "important",
                "matters",
                ".",
            ],
            "pos": ["DT", "JJ", "NN", "VB", "NN", "TO", "VB", "JJ", "NN", "MAD"],
            "lemma": [
                "the",
                "Swedish",
                "parliament",
                "convene",
                "today",
                "to",
                "discuss",
                "important",
                "matter",
                ".",
            ],
            "msd": ["", "", "", "", "", "", "", "", "", ""],
        }
    )


@pytest.fixture
def tagged_frame_with_repeated_tokens() -> pd.DataFrame:
    """Tagged frame with repeated tokens for testing aggregation."""
    return pd.DataFrame(
        {
            "token": ["word", "word", "word", "text", "text", "other"],
            "pos": ["NN", "NN", "NN", "NN", "NN", "JJ"],
            "lemma": ["word", "word", "word", "text", "text", "other"],
            "msd": ["", "", "", "", "", ""],
        }
    )


# ============================================================================
# Domain Model Fixtures
# ============================================================================


@pytest.fixture
def minimal_speaker_note() -> dict[str, str]:
    """Minimal speaker note data."""
    return {"speaker_id": "123", "name": "Speaker A", "party": "Party A"}


@pytest.fixture
def minimal_page_reference() -> interface.PageReference:
    """Minimal page reference."""
    return interface.PageReference(source_id=0, page_number=1, reference="Page 1")


# ============================================================================
# Test Data Builders
# ============================================================================


def build_utterance(
    u_id: str = "u1",
    who: str = "speaker",
    speaker_note_id: str = "note_1",
    paragraphs: Optional[list[str]] = None,
    prev_id: Optional[str] = None,
    next_id: Optional[str] = None,
    page_number: int = 0,
) -> interface.Utterance:
    """
    Build a custom utterance for testing.

    Useful for creating specific utterance configurations without using fixtures.
    """
    return interface.Utterance(
        u_id=u_id,
        who=who,
        speaker_note_id=speaker_note_id,
        paragraphs=paragraphs or ["Sample paragraph."],
        prev_id=prev_id,
        next_id=next_id,
        page_number=page_number,
    )


def build_protocol(
    name: str = "prot-test",
    date: str = "2000",
    utterances: Optional[list[interface.Utterance]] = None,
    speaker_notes: Optional[dict[str, str]] = None,
) -> interface.Protocol:
    """
    Build a custom protocol for testing.

    Useful for creating specific protocol configurations without using fixtures.
    """
    if utterances is None:
        utterances = [build_utterance()]

    if speaker_notes is None:
        speaker_notes = {"note_1": "Default Speaker"}

    return interface.Protocol(
        name=name,
        date=date,
        utterances=utterances,
        speaker_notes=speaker_notes,
        page_references=[],
    )


def build_tagged_frame(
    tokens: Optional[list[str]] = None,
    pos_tags: Optional[list[str]] = None,
    lemmas: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Build a custom tagged frame for testing.

    Useful for creating specific tagged text configurations without using fixtures.
    """
    if tokens is None:
        tokens = ["word"]
    if pos_tags is None:
        pos_tags = ["NN"] * len(tokens)
    if lemmas is None:
        lemmas = tokens.copy()

    return pd.DataFrame(
        {
            "token": tokens,
            "pos": pos_tags,
            "lemma": lemmas,
            "msd": [""] * len(tokens),
        }
    )
