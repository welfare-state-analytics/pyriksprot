from __future__ import annotations

import glob
import os
import uuid

import pandas as pd
import pytest

from pyriksprot import interface
from pyriksprot import to_speech as ts
from pyriksprot import utility
from pyriksprot.corpus import iterate, parlaclarin
from pyriksprot.corpus import tagged as tagged_corpus
from pyriksprot.foss import untangle
from tests.parlaclarin.utility import count_utterances

from .utility import RIKSPROT_PARLACLARIN_FOLDER, TAGGED_SOURCE_FOLDER, TAGGED_SOURCE_PATTERN, create_utterances

# pylint: disable=redefined-outer-name

jj = os.path.join


@pytest.fixture(scope='module')
def utterances() -> list[interface.Utterance]:
    return create_utterances()


@pytest.mark.parametrize(
    'strategy, expected_count, expected_whos, expected_ids, expected_texts',
    [
        (
            ts.MergeByWho,
            2,
            [{'A'}, {'B'}],
            ['i-1', 'i-3'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nAdam är dum.',
                'Jag heter Adam.\nOve är dum.',
            ],
        ),
        (
            ts.MergeStrategyType.chain,
            4,
            [{'A'}, {'B'}, {'B'}, {'A'}],
            ['i-1', 'i-3', 'i-4', 'i-5'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.',
                'Ove är dum.',
                'Adam är dum.',
            ],
        ),
        (
            ts.MergeByWhoSequence,
            3,
            [{'A'}, {'B'}, {'A'}],
            ['i-1', 'i-3', 'i-5'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.\nOve är dum.',
                'Adam är dum.',
            ],
        ),
        (
            ts.MergeBySpeakerNoteIdSequence,
            4,
            [{'A'}, {'B'}, {'B'}, {'A'}],
            ['i-1', 'i-3', 'i-4', 'i-5'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.',
                'Ove är dum.',
                'Adam är dum.',
            ],
        ),
        (
            ts.MergeByWhoSpeakerNoteIdSequence,
            4,
            [{'A'}, {'B'}, {'B'}, {'A'}],
            ['i-1', 'i-3', 'i-4', 'i-5'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.',
                'Ove är dum.',
                'Adam är dum.',
            ],
        ),
    ],
)
def test_merge_speech_by_strategy(
    utterances: list[interface.Utterance], strategy, expected_count, expected_whos, expected_ids, expected_texts
):

    protocol: interface.Protocol = interface.Protocol(
        date="1950", name="prot-1958-fake", utterances=utterances, speaker_notes={}
    )
    speeches = ts.to_speeches(protocol=protocol, merge_strategy=strategy, skip_size=0)

    assert len(speeches) == expected_count

    for i, speech in enumerate(speeches):
        assert speech.speech_index == i + 1
        assert speech.speech_date == protocol.date
        assert speech.document_name == f'{protocol.name}_{speech.speech_index:03}'
        assert speech.speech_name.startswith(protocol.name)
        assert speech.delimiter == '\n'
        for interface.utterance in speech.utterances:
            assert interface.utterance.who == speech.who

    assert [s.text for s in speeches] == expected_texts

    assert [{u.who for u in s.utterances} for s in speeches] == expected_whos
    assert [s.speech_id for s in speeches] == expected_ids


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
        page_number="0",
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
        page_number="0",
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
        page_number="0",
        utterances=utterances,
    )

    assert speech.tagged_text == 'header\nA\nB\nE\nF'

    # Test file ending with NL


"""
Note: Grouping by just speaker-note-id is not enough due to utterances that lack speaker-note
Hence it is (for now) commented out.
"""


@pytest.mark.parametrize(
    'filename, speech_count, strategy',
    [
        ("prot-1933--fk--5.xml", 1, ts.MergeStrategyType.chain),
        ("prot-1933--fk--5.xml", 1, ts.MergeStrategyType.who_speaker_note_id_sequence),
        # ("prot-1933--fk--5.xml", 1, ts.MergeStrategyType.speaker_note_id_sequence),
        ('prot-1933--fk--5.xml', 1, ts.MergeStrategyType.who_sequence),
        ("prot-1955--ak--22.xml", 151, ts.MergeStrategyType.chain),
        ("prot-1955--ak--22.xml", 151, ts.MergeStrategyType.who_speaker_note_id_sequence),
        # ("prot-1955--ak--22.xml", 149, ts.MergeStrategyType.speaker_note_id_sequence),
        ("prot-1955--ak--22.xml", 149, ts.MergeStrategyType.who_sequence),
        ('prot-199192--127.xml', 231, ts.MergeStrategyType.chain),
        ('prot-199192--127.xml', 52, ts.MergeStrategyType.who),
        ('prot-199192--127.xml', 220, ts.MergeStrategyType.who_sequence),
        ('prot-199192--127.xml', 231, ts.MergeStrategyType.who_speaker_note_id_sequence),
        # ('prot-199192--127.xml', 208, ts.MergeStrategyType.speaker_note_id_sequence),
    ],
)
def test_protocol_to_speeches_with_different_strategies(filename: str, speech_count: int, strategy: str):
    document_name: str = utility.strip_path_and_extension(filename)

    xml_path: str = jj(RIKSPROT_PARLACLARIN_FOLDER, "protocols", filename.split('-')[1], filename)

    # log_utterance_sequence(xml_path, f"tests/output/{document_name}_sequence.log")
    # counter: dict[str, int] = count_speaker_notes(xml_path)

    # n_speaker_note_without_utterance: int = len([k for k in counter if not counter[k]])

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(xml_path)

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

    protocol = parlaclarin.ProtocolMapper.to_protocol(data, segment_skip_size=0)
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
                n='c01',
                who='A',
                speaker_note_id='a1',
                prev_id=None,
                next_id='i-2',
                paragraphs=['Hej! Detta är en mening.'],
                tagged_text="token\tpos\tlemma\nA\ta\tNN",
                delimiter='\n',
                page_number='',
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


def test_load_protocol_with_empty_existing_file():
    protocol: interface.Protocol | None = tagged_corpus.load_protocol(
        filename=jj(TAGGED_SOURCE_FOLDER, 'prot-1973--21.zip')
    )
    assert protocol is None


def test_load_protocols_from_filenames():
    filenames: list[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    assert len(protocols) == 5


def test_load_protocols_from_folder():
    protocols: list[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=TAGGED_SOURCE_FOLDER)]
    assert len(protocols) == 5


@pytest.mark.parametrize(
    'protocol_name,merge_strategy,expected_speech_count',
    [
        ('prot-1955--ak--22', 'who_sequence', 149),
        ('prot-1955--ak--22', 'who_speaker_note_id_sequence', 151),
        ('prot-1955--ak--22', 'speaker_note_id_sequence', 151),
        ('prot-1955--ak--22', 'chain', 151),
        ('prot-199192--127', 'who_sequence', 220),
        ('prot-199192--127', 'who_speaker_note_id_sequence', 231),
        ('prot-199192--127', 'speaker_note_id_sequence', 231),
        ('prot-199192--127', 'chain', 231),
    ],
)
def test_protocol_to_items(protocol_name: str, merge_strategy: str, expected_speech_count: int):

    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')

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
    )
    assert len(items) == expected_speech_count


@pytest.mark.skip(reason="Infrastructure test")
@pytest.mark.parametrize(
    'protocol_name', ['prot-199192--21', 'prot-199192--127', 'prot-1933--fk--5', 'prot-1955--ak--22', 'prot-199596--35']
)
def test_protocol_to_speeches(protocol_name: str):

    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')

    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)
    utterances: pd.DataFrame = pd.DataFrame(
        data=[(x.u_id, x.who, x.next_id, x.prev_id, x.speaker_note_id) for x in protocol.utterances],
        columns=['u_id', 'who', 'next_id', 'prev_id', 'speaker_note_id'],
    )
    for merge_strategy in ['who_sequence', 'who_speaker_note_id_sequence', 'speaker_note_id_sequence', 'chain']:

        merger: ts.IMergeStrategy = ts.MergerFactory.get(merge_strategy)

        items: list[list[interface.Utterance]] = merger.group(protocol.utterances)

        speech_ids = []
        for i, item in enumerate(items):
            speech_ids.extend(len(item) * [i])

        utterances[merge_strategy] = speech_ids

    utterances.to_excel(f"utterances_{protocol_name}.xlsx")
