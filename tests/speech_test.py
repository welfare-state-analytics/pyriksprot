from __future__ import annotations

import glob
import os
import uuid
from typing import Type

import pytest

from pyriksprot import interface
from pyriksprot import to_speech as ts
from pyriksprot import utility
from pyriksprot.corpus import iterate, parlaclarin
from pyriksprot.corpus import tagged as tagged_corpus
from pyriksprot.foss import untangle
from pyriksprot.workflows._extract_speech_ids import extract_speech_ids_by_strategy
from tests import fakes
from tests.parlaclarin.utility import count_utterances

from .utility import (
    RIKSPROT_PARLACLARIN_FAKE_FOLDER,
    RIKSPROT_PARLACLARIN_FOLDER,
    TAGGED_SOURCE_FOLDER,
    TAGGED_SOURCE_PATTERN,
    sample_tagged_frames_corpus_exists,
)

# pylint: disable=redefined-outer-name

jj = os.path.join


@pytest.fixture(scope='module')
def utterances() -> list[interface.Utterance]:
    return fakes.load_sample_utterances(f'{RIKSPROT_PARLACLARIN_FAKE_FOLDER}/prot-1958-fake.xml')


@pytest.mark.parametrize(
    'strategy, strategy_name',
    [
        (ts.MergeByWho, 'who'),
        (ts.MergeStrategyType.chain, 'chain'),
        (ts.MergeStrategyType.chain_consecutive_unknowns, 'chain_consecutive_unknowns'),
        (ts.MergeByWhoSequence, 'who_sequence'),
        (ts.MergeBySpeakerNoteIdSequence, 'speaker_note_id_sequence'),
        (ts.MergeByWhoSpeakerNoteIdSequence, 'who_speaker_note_id_sequence'),
    ],
)
def test_merge_speech_by_strategy(utterances: list[interface.Utterance], strategy: Type, strategy_name: str):
    document_name: str = "prot-1958-fake"
    year: int = int(document_name.split("-")[1])

    utterances: list[interface.Utterance] = fakes.load_sample_utterances(
        f'{RIKSPROT_PARLACLARIN_FAKE_FOLDER}/{document_name}.xml'
    )

    protocol: interface.Protocol = interface.Protocol(
        date=f"{year}", name=document_name, utterances=utterances, speaker_notes={}
    )
    speeches = ts.to_speeches(protocol=protocol, merge_strategy=strategy, skip_size=0)

    expected_speeches: list[iterate.ProtocolSegment] = list(fakes.load_expected_speeches(strategy_name, document_name))

    assert len(speeches) == len(expected_speeches)

    for i, speech in enumerate(speeches):
        assert speech.speech_index == i + 1
        assert speech.speech_date == protocol.date
        assert speech.document_name == f'{protocol.name}_{speech.speech_index:03}'
        assert speech.speech_name.startswith(protocol.name)
        assert speech.delimiter == '\n'
        for interface.utterance in speech.utterances:
            assert interface.utterance.who == speech.who

    assert [s.who for s in speeches] == [s.who for s in expected_speeches]
    assert [s.text for s in speeches] == [s.text for s in expected_speeches]
    assert [s.speech_id for s in speeches] == [s.speech_id for s in expected_speeches]


def test_speech_annotation():
    utterances: list[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", speaker_note_id="a1", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", speaker_note_id="a1", annotation='header\nC\nD'),
        interface.Utterance(u_id='i-3', who="apa", speaker_note_id="a1", annotation='header\nE\nF'),
    ]
    speech = interface.Speech(
        protocol_name="prot-01",
        document_name="prot-01-001",
        speech_id="s-1",
        who="apa",
        speech_date="1999",
        speech_index=1,
        page_number=0,
        utterances=utterances,
    )

    assert speech.tagged_text == 'header\nA\nB\nC\nD\nE\nF'

    utterances: list[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", speaker_note_id="a1", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", speaker_note_id="a1", annotation='header'),
        interface.Utterance(u_id='i-3', who="apa", speaker_note_id="a1", annotation='header\nE\nF'),
    ]
    speech = interface.Speech(
        protocol_name="prot-01",
        document_name="prot-01-001",
        speech_id="s-1",
        who="apa",
        speech_date="1999",
        speech_index=1,
        page_number=0,
        utterances=utterances,
    )

    assert speech.tagged_text == 'header\nA\nB\nE\nF'

    utterances: list[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", speaker_note_id="a1", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", speaker_note_id="a1", annotation='header\n'),
        interface.Utterance(u_id='i-3', who="apa", speaker_note_id="a1", annotation='header\nE\nF'),
    ]
    speech = interface.Speech(
        protocol_name="prot-01",
        document_name="prot-01-001",
        speech_id="s-1",
        who="apa",
        speech_date="1999",
        speech_index=1,
        page_number=0,
        utterances=utterances,
    )

    assert speech.tagged_text == 'header\nA\nB\nE\nF'

    # Test file ending with NL


"""
Note: Grouping by just speaker-note-id is not enough due to utterances that lack speaker-note
Hence it is (for now) commented out.
"""


# FIXME: Verify expected counts!
@pytest.mark.parametrize(
    'filename, speech_count, strategy',
    [
        ("prot-1933--fk--5.xml", 0, ts.MergeStrategyType.chain),
        ("prot-1933--fk--5.xml", 0, ts.MergeStrategyType.who_speaker_note_id_sequence),
        ('prot-1933--fk--5.xml', 0, ts.MergeStrategyType.who_sequence),
        ("prot-1955--ak--22.xml", 186, ts.MergeStrategyType.chain),
        ("prot-1955--ak--22.xml", 167, ts.MergeStrategyType.chain_consecutive_unknowns),
        ("prot-1955--ak--22.xml", 167, ts.MergeStrategyType.who_speaker_note_id_sequence),
        ("prot-1955--ak--22.xml", 160, ts.MergeStrategyType.who_sequence),
        ('prot-199192--127.xml', 291, ts.MergeStrategyType.chain),
        ('prot-199192--127.xml', 251, ts.MergeStrategyType.chain_consecutive_unknowns),
        ('prot-199192--127.xml', 53, ts.MergeStrategyType.who),
        ('prot-199192--127.xml', 248, ts.MergeStrategyType.who_sequence),
        ('prot-199192--127.xml', 251, ts.MergeStrategyType.who_speaker_note_id_sequence),
    ],
)
def test_protocol_to_speeches_with_different_strategies(filename: str, speech_count: int, strategy: str):
    document_name: str = utility.strip_path_and_extension(filename)

    xml_path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.parse(xml_path)

    speeches = ts.to_speeches(protocol=protocol, merge_strategy=strategy, skip_size=0)
    assert len(speeches) == speech_count

    assert all(x.text != "" for x in speeches)
    assert document_name == protocol.name
    assert protocol.date is not None
    assert protocol.has_text == any(x.text != "" for x in speeches)

    speeches = ts.to_speeches(protocol=protocol, merge_strategy=strategy, skip_size=3)
    assert all(len(x.text) >= 3 for x in speeches)

    speeches = ts.to_speeches(protocol=protocol, merge_strategy=strategy, skip_size=100)
    assert all(len(x.text) >= 100 for x in speeches)


@pytest.mark.parametrize(
    'filename, expected_speech_count',
    [
        ('prot-199192--21.xml', 0),
    ],
)
def test_to_speeches_with_faulty_attribute(filename, expected_speech_count):
    path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    data = untangle.parse(path)

    protocol = parlaclarin.ProtocolMapper.parse(data)
    speeches = ts.to_speeches(protocol=protocol, merge_strategy=ts.MergeStrategyType.chain)
    assert len(speeches) != expected_speech_count, "speech length"


@pytest.mark.parametrize('storage_format', [interface.StorageFormat.JSON, interface.StorageFormat.CSV])
def test_store_protocols(storage_format: interface.StorageFormat):
    protocol: interface.Protocol = interface.Protocol(
        name='prot-1958-fake',
        date='1958',
        utterances=[
            interface.Utterance(
                u_id='i-1',
                who='alma',
                speaker_note_id='a1',
                prev_id=None,
                next_id='i-2',
                paragraphs=['Hej! Detta är en mening.'],
                tagged_text="token\tpos\tlemma\nA\ta\tNN",
                delimiter='\n',
                page_number=0,
            )
        ],
        speaker_notes={},
    )

    output_filename: str = jj("tests", "output", f"{str(uuid.uuid4())}.zip")

    tagged_corpus.store_protocol(
        output_filename,
        protocol=protocol,
        storage_format=storage_format,
        checksum='apa',
    )

    assert os.path.isfile(output_filename)

    metadata: dict = tagged_corpus.load_metadata(output_filename)

    assert metadata is not None

    loaded_protocol: interface.Protocol = tagged_corpus.load_protocol(output_filename)

    assert loaded_protocol is not None
    assert protocol.name == loaded_protocol.name
    assert protocol.date == loaded_protocol.date
    assert [u.__dict__ for u in protocol.utterances] == [u.__dict__ for u in loaded_protocol.utterances]

    # os.unlink(output_filename)


def test_load_protocols_with_non_existing_file():
    filename: str = 'this/is/a/non-existing/path/**/prot-1973--21.zip'
    protocol: interface.Protocol | None = tagged_corpus.load_protocol(filename=filename)
    assert protocol is None


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_load_protocol_with_empty_existing_file():
    protocol: interface.Protocol | None = tagged_corpus.load_protocol(
        filename=jj(TAGGED_SOURCE_FOLDER, 'prot-1973--21.zip')
    )
    assert protocol is None


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_load_protocols_from_filenames():
    filenames: list[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    assert len(protocols) == 5


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
def test_load_protocols_from_folder():
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=TAGGED_SOURCE_FOLDER)]
    assert len(protocols) == 5


@pytest.mark.skipif(not sample_tagged_frames_corpus_exists(), reason="Tagged frames not found")
@pytest.mark.parametrize(
    'protocol_name,merge_strategy,expected_speech_count',
    [
        ('prot-1955--ak--22', 'who_sequence', 160),
        ('prot-1955--ak--22', 'who_speaker_note_id_sequence', 167),
        ('prot-1955--ak--22', 'speaker_note_id_sequence', 167),
        ('prot-1955--ak--22', 'chain', 186),
        ('prot-199192--127', 'who_sequence', 248),
        ('prot-199192--127', 'who_speaker_note_id_sequence', 251),
        ('prot-199192--127', 'speaker_note_id_sequence', 251),
        ('prot-199192--127', 'chain', 291),
    ],
)
def test_protocol_to_items(protocol_name: str, merge_strategy: str, expected_speech_count: int):

    subfolder: str = protocol_name.split('-')[1]
    filename: str = jj(TAGGED_SOURCE_FOLDER, subfolder, f'{protocol_name}.zip')

    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)

    assert protocol is not None

    xml_filename = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", protocol_name.split("-")[1], f"{protocol_name}.xml")
    expected_utterance_count = count_utterances(xml_filename)

    assert len(protocol.utterances) == expected_utterance_count

    items = iterate.to_segments(
        protocol=protocol,
        content_type=interface.ContentType.Text,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=merge_strategy,
        which_year="filename",
    )
    assert len(items) == expected_speech_count


@pytest.mark.skip(reason="Infrastructure test")
@pytest.mark.parametrize(
    'protocol_name', ['prot-199192--21', 'prot-199192--127', 'prot-1933--fk--5', 'prot-1955--ak--22', 'prot-199596--35']
)
def test_protocol_to_speeches(protocol_name: str):
    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')
    extract_speech_ids_by_strategy(filename=filename)
