import os

import pytest

from pyriksprot import model, parse

jj = os.path.join


def test_to_protocol_in_depth_validation_of_correct_parlaclarin_xml():

    protocol: model.Protocol = parse.ProtocolMapper.to_protocol(jj("tests", "test_data", "fake", "prot-1958-fake.xml"))

    assert protocol is not None
    assert len(protocol.utterances) == 4
    assert len(protocol) == 4

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text(), 'has text'
    assert protocol.checksum(), 'checksum'
    # FIXME: More checks


@pytest.mark.parametrize(
    'filename',
    [
        ("prot-197879--14.xml"),
        ("prot-199596--35.xml"),
    ],
)
def test_parse_xml_with_no_utterances(filename):

    protocol = parse.ProtocolMapper.to_protocol(jj("tests", "test_data", "source", filename), skip_size=0)

    assert len(protocol.utterances) == 0, "utterances empty"
    assert not protocol.has_text()
    # FIXME: More checks
