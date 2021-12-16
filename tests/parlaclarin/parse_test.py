import os

import pytest

from pyriksprot import interface, parlaclarin
from ..utility import PARLACLARIN_FAKE_FOLDER, PARLACLARIN_SOURCE_FOLDER

jj = os.path.join


def test_to_protocol_in_depth_validation_of_correct_parlaclarin_xml():

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(
        jj(PARLACLARIN_FAKE_FOLDER, "prot-1958-fake.xml")
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
    'filename',
    [
        ("prot-197879--14.xml"),
    ],
)
def test_parlaclarin_xml_with_no_utterances(filename):

    path: str = jj(PARLACLARIN_SOURCE_FOLDER, filename.split('-')[1], filename)

    protocol = parlaclarin.ProtocolMapper.to_protocol(path, segment_skip_size=0)

    assert len(protocol.utterances) == 0, "utterances empty"
    assert not protocol.has_text
    # FIXME: More checks


def test_to_protocol_by_untangle():
    filename = jj(PARLACLARIN_FAKE_FOLDER, "prot-1958-fake.xml")

    protocol: parlaclarin.XmlUntangleProtocol = parlaclarin.XmlUntangleProtocol(filename)

    assert protocol is not None
    assert len(protocol.utterances) == 4
    assert len(protocol) == 4

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text, 'has text'
