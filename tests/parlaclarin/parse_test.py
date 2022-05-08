import os

import pytest

from pyriksprot import interface
from pyriksprot.corpus import parlaclarin

from ..utility import RIKSPROT_PARLACLARIN_FAKE_FOLDER, RIKSPROT_PARLACLARIN_FOLDER

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
    assert protocol.has_text, 'has text'
    assert protocol.checksum(), 'checksum'
    # FIXME: More checks


@pytest.mark.parametrize(
    'filename,n_utterances',
    [
        ("prot-1933--fk--5.xml", 2),
        ("prot-1955--ak--22.xml", 414),
        ("prot-197879--14.xml", 0),
        ('prot-199192--127.xml', 2631),
        ('prot-199192--21.xml', 136),
        ("prot-199596--35.xml", 386),
    ],
)
def test_parlaclarin_xml_with_no_utterances(filename: str, n_utterances: int):

    path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(path, segment_skip_size=0)

    assert len(protocol.utterances) == n_utterances
    assert not any(not bool(u.speaker_note_id) for u in protocol.utterances) == n_utterances

    # assert not protocol.has_text
    # FIXME: More checks


def test_to_protocol_by_untangle():
    filename = jj(RIKSPROT_PARLACLARIN_FAKE_FOLDER, "prot-1958-fake.xml")

    protocol: parlaclarin.XmlUntangleProtocol = parlaclarin.XmlUntangleProtocol(filename)

    assert protocol is not None
    assert len(protocol.utterances) == 4
    assert len(protocol) == 4

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text, 'has text'
