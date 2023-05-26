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

from . import fakes
from .utility import RIKSPROT_PARLACLARIN_FAKE_FOLDER, TAGGED_SOURCE_PATTERN

# pylint: disable=redefined-outer-name

jj = os.path.join


@pytest.fixture(scope='module')
def utterances() -> list[interface.Utterance]:
    return fakes.load_sample_utterances(f'{RIKSPROT_PARLACLARIN_FAKE_FOLDER}/parlaclarin/prot-1958-fake.xml')


def test_utterance_text():
    u: interface.Utterance = interface.Utterance(u_id="u", speaker_note_id="x", who="x", paragraphs=["X", "Y", "C"])
    assert u.text == '\n'.join(["X", "Y", "C"])


def test_utterance_checksumtext():
    u: interface.Utterance = interface.Utterance(u_id="u", speaker_note_id="x", who="x", paragraphs=["X", "Y", "C"])
    assert u.checksum() == '6060d006e0494206'


def test_utterances_to_dict():
    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster(None)
    assert who_sequences == []

    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster([])
    assert who_sequences == []

    utterances: list[interface.Utterance] = [
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa1", who='otto'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa1", who='otto'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xb1", who='ove'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xb1", who='ove'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', speaker_note_id="xa2", who='otto'),
    ]

    who_sequences: list[list[interface.Utterance]] = to_speech.MergeByWhoSequence().cluster(utterances)

    assert len(who_sequences) == 3
    assert len(who_sequences[0]) == 2
    assert len(who_sequences[1]) == 2
    assert len(who_sequences[2]) == 1
    assert set(x.who for x in who_sequences[0]) == {'otto'}
    assert set(x.who for x in who_sequences[1]) == {'ove'}
    assert set(x.who for x in who_sequences[2]) == {'otto'}


def test_utterances_who_sequences(utterances: list[interface.Utterance]):
    assert [u.who for u in utterances] == ['olle', 'olle', 'kalle', 'unknown', 'unknown', 'olle']


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
    assert data.reset_index().to_dict(orient='records') == [u.to_dict() for u in utterances]


def test_protocol_create(utterances: list[interface.Utterance]):
    protocol: interface.Protocol = interface.Protocol(
        date="1958", name="prot-1958-fake", utterances=utterances, speaker_notes={}
    )

    assert protocol is not None
    assert len(protocol.utterances) == 6
    assert len(protocol) == 6
    assert protocol.name == "prot-1958-fake"
    assert protocol.date == "1958"

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text, 'has text'
    assert protocol.checksum() == '3cf7f69e2dcf54586f9fedeb2e50ea69ea1d6179', 'checksum'

    assert protocol.text == '\n'.join(text.text for text in utterances)


def test_protocol_preprocess():
    """Modifies utterances:"""
    utterances: list[interface.Utterance] = fakes.load_sample_utterances(
        f'{RIKSPROT_PARLACLARIN_FAKE_FOLDER}/prot-1958-fake.xml'
    )

    protocol: interface.Protocol = interface.Protocol(
        date="1950", name="prot-1958-fake", utterances=utterances, speaker_notes={}
    )

    preprocess: Callable[[str], str] = lambda t: 'APA'

    protocol.preprocess(preprocess=preprocess)

    assert protocol.text == 'APA\nAPA\nAPA\nAPA\nAPA\nAPA\nAPA'


def test_protocols_to_items():
    filenames: list[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    _ = itertools.chain(
        p.to_segments(content_type=interface.ContentType.Text, segment_level=interface.SegmentLevel.Who)
        for p in protocols
    )
