import os

import pytest

from pyriksprot import interface
from pyriksprot.configuration import ConfigValue
from pyriksprot.corpus import parlaclarin

from .. import fakes
from .utility import count_utterances

jj = os.path.join


@pytest.mark.parametrize(
    'protocol_name',
    [
        "prot-1958-fake",
        "prot-1960-fake",
        "prot-1980-empty",
    ],
)
def test_to_protocol_in_depth_validation_of_correct_parlaclarin_xml(protocol_name: str):
    fakes_folder: ConfigValue = ConfigValue("fakes:folder").resolve()
    filename: str = jj(fakes_folder, f"{protocol_name}.xml")
    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(filename)

    """Load truth"""
    utterances: list[interface.Utterance] = fakes.load_sample_utterances(filename)

    assert protocol is not None
    assert len(protocol.utterances) == len(utterances)
    assert len(protocol) == len(utterances)

    assert protocol.name == protocol_name
    assert protocol.date == protocol_name.split('-')[1]
    assert 'empty' in protocol_name or protocol.has_text
    assert protocol.checksum()
    assert len(protocol.utterances) == len(utterances)


@pytest.mark.parametrize(
    'filename',
    [
        "prot-1933--fk--005.xml",
        "prot-1955--ak--022.xml",
        "prot-197879--014.xml",
        'prot-199192--127.xml',
        'prot-199192--021.xml',
        "prot-199596--035.xml",
    ],
)
def test_parlaclarin_n_utterances(filename: str):
    corpus_folder: ConfigValue = ConfigValue("corpus:folder").resolve()
    path: str = jj(corpus_folder, filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(path)

    assert len(protocol.utterances) == count_utterances(path)
    assert not any(not bool(u.speaker_note_id) for u in protocol.utterances)


# FIXME: Check counts
@pytest.mark.parametrize(
    'filename, u_count, intro_count',
    [
        ("prot-1933--fk--005.xml", 0, 1),
        ("prot-1955--ak--022.xml", 428, 165),
        ("prot-197879--014.xml", 1, 0),
        ('prot-199192--021.xml', 113, 21),
        ('prot-199192--127.xml', 2568, 250),
        ("prot-199596--035.xml", 393, 54),
    ],
)
def test_parlaclarin_n_speaker_notes(filename: str, u_count: int, intro_count: int):
    corpus_folder: ConfigValue = ConfigValue("corpus:folder").resolve()

    path: str = jj(corpus_folder, filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(path)

    assert len(protocol.utterances) == u_count
    assert len(protocol.speaker_notes) == intro_count
