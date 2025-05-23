import glob
import os
import shutil
import uuid

import pytest

from pyriksprot import interface
from pyriksprot.configuration import ConfigValue
from pyriksprot.corpus import parlaclarin
from pyriksprot.corpus.parlaclarin.parse import ProtocolMapper
from pyriksprot.corpus.utility import create_tei_corpus_xml, load_chamber_indexes
from pyriksprot.metadata.corpus_index_factory import CorpusScanner

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
    fakes_folder: str = ConfigValue("fakes:folder").resolve()
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
    corpus_folder: str = ConfigValue("corpus:folder").resolve()
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
        ('prot-199192--127.xml', 2568, 249),
        ("prot-199596--035.xml", 393, 41),
    ],
)
def test_parlaclarin_n_speaker_notes(filename: str, u_count: int, intro_count: int):
    corpus_folder: str = ConfigValue("corpus:folder").resolve()

    path: str = jj(corpus_folder, filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(path)

    assert len(protocol.utterances) == u_count
    assert len(protocol.speaker_notes) == intro_count


def test_load_chambers():
    chambers: dict[str, set[str]] = load_chamber_indexes(folder=ConfigValue("corpus:folder").resolve())

    assert set(chambers.keys()) == {'ak', 'fk', 'ek'}
    assert all(len(chambers[chamber]) > 0 for chamber in chambers)

    p2c: dict[str, str] = {v: k for k, p in chambers.items() for v in p}

    assert len(p2c) == sum(len(p) for _, p in chambers.items())


def test_scan_folder():
    corpus_folder: str = ConfigValue("corpus:folder").resolve()

    scanner = CorpusScanner(parser=ProtocolMapper)

    scan_result: CorpusScanner.ScanResult = scanner.scan(
        filenames=glob.glob(jj(corpus_folder, '**/prot-*-*.xml'), recursive=True),
        chambers=load_chamber_indexes(folder=corpus_folder),
    )

    assert isinstance(scan_result, CorpusScanner.ScanResult)
    assert isinstance(scan_result.protocols, list)
    assert len(scan_result.protocols) > 0
    assert isinstance(scan_result.protocols[0], tuple)
    assert len(scan_result.protocols[0]) == 6
    assert len(scan_result.speeches) > 0


def test_create_tei_corpus_xml():
    source_folder: str = ConfigValue("corpus.folder").resolve()
    target_folder: str = f'tests/output/{str(uuid.uuid4())[8]}'

    os.makedirs(target_folder, exist_ok=True)

    create_tei_corpus_xml(source_folder=source_folder, target_folder=target_folder)

    assert os.path.isfile(jj(target_folder, 'prot-ak.xml'))
    assert os.path.isfile(jj(target_folder, 'prot-fk.xml'))
    assert os.path.isfile(jj(target_folder, 'prot-ek.xml'))

    assert os.path.getsize(jj(target_folder, 'prot-ak.xml')) > 0
    assert os.path.getsize(jj(target_folder, 'prot-fk.xml')) > 0
    assert os.path.getsize(jj(target_folder, 'prot-ek.xml')) > 0

    shutil.rmtree(target_folder)
