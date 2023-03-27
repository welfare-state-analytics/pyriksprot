from __future__ import annotations

import glob
import itertools
import os
import uuid
from typing import Callable

import pandas as pd
import pytest

from pyriksprot import interface, to_speech
from pyriksprot.corpus import tagged as tagged_corpus

from .utility import TAGGED_SOURCE_PATTERN, UTTERANCES_DICTS, create_utterances

# pylint: disable=redefined-outer-name

jj = os.path.join


@pytest.fixture(scope='module')
def utterances() -> list[interface.Utterance]:
    return create_utterances()


def test_utterance_text():
    u: interface.Utterance = interface.Utterance(u_id="A", speaker_note_id="x", who="x", paragraphs=["X", "Y", "C"])
    assert u.text == '\n'.join(["X", "Y", "C"])


def test_utterance_checksumtext():
    u: interface.Utterance = interface.Utterance(u_id="A", speaker_note_id="x", who="x", paragraphs=["X", "Y", "C"])
    assert u.checksum() == '6060d006e0494206'


def test_utterances_to_dict():

    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster(None)
    assert who_sequences == []

    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster([])
    assert who_sequences == []

    utterances: list[interface.Utterance] = [
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa1", who='A'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa1", who='A'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xb1", who='B'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xb1", who='B'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa2", who='A'),
    ]

    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster(utterances)

    assert len(who_sequences) == 3
    assert len(who_sequences[0]) == 2
    assert len(who_sequences[1]) == 2
    assert len(who_sequences[2]) == 1
    assert set(x.who for x in who_sequences[0]) == {'A'}
    assert set(x.who for x in who_sequences[1]) == {'B'}
    assert set(x.who for x in who_sequences[2]) == {'A'}


def test_utterances_who_sequences(utterances: list[interface.Utterance]):

    data = interface.UtteranceHelper.to_dict(utterances)
    assert data == UTTERANCES_DICTS


def test_utterances_to_csv(utterances: list[interface.Utterance]):

    data: str = interface.UtteranceHelper.to_csv(utterances)
    loaded_utterances = interface.UtteranceHelper.from_csv(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_json(utterances: list[interface.Utterance]):

    data: str = interface.UtteranceHelper.to_json(utterances)
    loaded_utterances = interface.UtteranceHelper.from_json(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_pandas(utterances: list[interface.Utterance]):

    data: pd.DataFrame = interface.UtteranceHelper.to_dataframe(utterances)
    assert data.reset_index().to_dict(orient='records') == UTTERANCES_DICTS


def test_protocol_create(utterances: list[interface.Utterance]):

    protocol: interface.Protocol = interface.Protocol(
        date="1958", name="prot-1958-fake", utterances=utterances, speaker_notes={}
    )

    assert protocol is not None
    assert len(protocol.utterances) == 5
    assert len(protocol) == 5
    assert protocol.name == "prot-1958-fake"
    assert protocol.date == "1958"

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text, 'has text'
    assert protocol.checksum() == '7e5112f9db8c8462d89fac08714ce15b432d7733', 'checksum'

    assert protocol.text == '\n'.join(text.text for text in utterances)


def test_protocol_preprocess():
    """Modifies utterances:"""
    utterances: list[interface.Utterance] = create_utterances()

    protocol: interface.Protocol = interface.Protocol(
        date="1950", name="prot-1958-fake", utterances=utterances, speaker_notes={}
    )

    preprocess: Callable[[str], str] = lambda t: 'APA'

    protocol.preprocess(preprocess=preprocess)

    assert protocol.text == 'APA\nAPA\nAPA\nAPA\nAPA\nAPA'


def test_protocols_to_items():
    filenames: list[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    _ = itertools.chain(
        p.to_segments(content_type=interface.ContentType.Text, segment_level=interface.SegmentLevel.Who)
        for p in protocols
    )
