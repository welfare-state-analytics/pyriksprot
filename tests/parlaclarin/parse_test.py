import os
from collections import defaultdict

import pytest

from pyriksprot import interface
from pyriksprot.corpus import parlaclarin

from ..utility import RIKSPROT_PARLACLARIN_FAKE_FOLDER, RIKSPROT_PARLACLARIN_FOLDER
from .utility import count_speaker_notes, count_utterances

jj = os.path.join


def test_to_protocol_in_depth_validation_of_correct_parlaclarin_xml():

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(
        jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, "prot-1958-fake.xml")
    )

    assert protocol is not None
    assert len(protocol.utterances) == 4
    assert len(protocol) == 4

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text
    assert protocol.checksum()
    assert len(protocol.utterances) == 4


@pytest.mark.parametrize(
    'filename',
    [
        "prot-1933--fk--5.xml",
        "prot-1955--ak--22.xml",
        "prot-197879--14.xml",
        'prot-199192--127.xml',
        'prot-199192--21.xml',
        "prot-199596--35.xml",
    ],
)
def test_parlaclarin_n_utterances(filename: str):

    path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(path, segment_skip_size=0)

    assert len(protocol.utterances) == count_utterances(path)
    assert not any(not bool(u.speaker_note_id) for u in protocol.utterances)


@pytest.mark.parametrize(
    'filename',
    [
        "prot-1933--fk--5.xml",
        "prot-1955--ak--22.xml",
        "prot-197879--14.xml",
        'prot-199192--21.xml',
        'prot-199192--127.xml',
        "prot-199596--35.xml",
    ],
)
def test_parlaclarin_n_speaker_notes(filename: str):

    path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    counter: dict[str, int] = count_speaker_notes(path)
    n_speaker_notes_with_utterances: int = len([k for k in counter if counter[k] > 0])

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(path, segment_skip_size=0)

    assert len(set(u.speaker_note_id for u in protocol.utterances)) == n_speaker_notes_with_utterances
