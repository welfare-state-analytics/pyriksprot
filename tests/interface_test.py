from __future__ import annotations

import glob
import os
import uuid
from typing import Callable, List

import pandas as pd
import pytest
from black import itertools

from pyriksprot import interface, parlaclarin, segment, tagged_corpus, utility
from pyriksprot.foss import untangle

from .utility import (
    PARLACLARIN_SOURCE_FOLDER,
    TAGGED_SOURCE_FOLDER,
    TAGGED_SOURCE_PATTERN,
    UTTERANCES_DICTS,
    create_utterances,
)

# pylint: disable=redefined-outer-name

jj = os.path.join


@pytest.fixture(scope='module')
def utterances() -> List[interface.Utterance]:
    return create_utterances()


def test_utterance_text():
    u: interface.Utterance = interface.Utterance(u_id="A", paragraphs=["X", "Y", "C"])
    assert u.text == '\n'.join(["X", "Y", "C"])


def test_utterance_checksumtext():
    u: interface.Utterance = interface.Utterance(u_id="A", paragraphs=["X", "Y", "C"])
    assert u.checksum() == '6060d006e0494206'


def test_utterances_to_dict():

    who_sequences: List[List[interface.Utterance]] = segment.MergeSpeechByWhoSequence().split(None)
    assert who_sequences == []

    who_sequences: List[List[interface.Utterance]] = segment.MergeSpeechByWhoSequence().split([])
    assert who_sequences == []

    utterances: List[interface.Utterance] = [
        interface.Utterance(u_id=f'{uuid.uuid4()}', who='A'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', who='A'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', who='B'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', who='B'),
        interface.Utterance(u_id=f'{uuid.uuid4()}', who='A'),
    ]

    who_sequences: List[List[interface.Utterance]] = segment.MergeSpeechByWhoSequence().split(utterances)

    assert len(who_sequences) == 3
    assert len(who_sequences[0]) == 2
    assert len(who_sequences[1]) == 2
    assert len(who_sequences[2]) == 1
    assert set(x.who for x in who_sequences[0]) == {'A'}
    assert set(x.who for x in who_sequences[1]) == {'B'}
    assert set(x.who for x in who_sequences[2]) == {'A'}


def test_utterances_who_sequences(utterances: List[interface.Utterance]):

    data = interface.UtteranceHelper.to_dict(utterances)
    assert data == UTTERANCES_DICTS


def test_utterances_to_csv(utterances: List[interface.Utterance]):

    data: str = interface.UtteranceHelper.to_csv(utterances)
    loaded_utterances = interface.UtteranceHelper.from_csv(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_json(utterances: List[interface.Utterance]):

    data: str = interface.UtteranceHelper.to_json(utterances)
    loaded_utterances = interface.UtteranceHelper.from_json(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_pandas(utterances: List[interface.Utterance]):

    data: pd.DataFrame = interface.UtteranceHelper.to_dataframe(utterances)
    assert data.reset_index().to_dict(orient='records') == UTTERANCES_DICTS


def test_protocol_create(utterances: List[interface.Utterance]):

    protocol: interface.Protocol = interface.Protocol(date="1958", name="prot-1958-fake", utterances=utterances)

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
    utterances: List[interface.Utterance] = create_utterances()

    protocol: interface.Protocol = interface.Protocol(date="1950", name="prot-1958-fake", utterances=utterances)

    preprocess: Callable[[str], str] = lambda t: 'APA'

    protocol.preprocess(preprocess=preprocess)

    assert protocol.text == 'APA\nAPA\nAPA\nAPA\nAPA\nAPA'


@pytest.mark.parametrize(
    'cls, strategy, expected_count, expected_whos, expected_ids, expected_texts',
    [
        (
            segment.MergeSpeechByWho,
            'who',
            2,
            [{'A'}, {'B'}],
            ['i-1', 'i-3'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nAdam är dum.',
                'Jag heter Adam.\nOve är dum.',
            ],
        ),
        (
            segment.MergeSpeechByChain,
            segment.MergeSpeechStrategyType.chain,
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
            segment.MergeSpeechByWhoSequence,
            segment.MergeSpeechStrategyType.who_sequence,
            3,
            [{'A'}, {'B'}, {'A'}],
            ['i-1', 'i-3', 'i-5'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.\nOve är dum.',
                'Adam är dum.',
            ],
        ),
    ],
)
def test_merge_speech_by_strategy(
    utterances: List[interface.Utterance], cls, strategy, expected_count, expected_whos, expected_ids, expected_texts
):

    protocol: interface.Protocol = interface.Protocol(date="1950", name="prot-1958-fake", utterances=utterances)
    for speeches in [
        cls().speeches(protocol=protocol, segment_skip_size=0),
        segment.to_speeches(protocol=protocol, merge_strategy=strategy, segment_skip_size=0),
    ]:

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

    utterances: List[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", annotation='header\nC\nD'),
        interface.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
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

    utterances: List[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", annotation='header'),
        interface.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
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

    utterances: List[interface.Utterance] = [
        interface.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        interface.Utterance(u_id='i-2', who="apa", annotation='header\n'),
        interface.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
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


@pytest.mark.parametrize(
    'filename, speech_count, non_empty_speech_count, strategy',
    [
        ("prot-1933--fk--5.xml", 1, 1, segment.MergeSpeechStrategyType.chain),
        # ("prot-1933--fk--5.xml", 1, 1, segment.MergeSpeechStrategyType.Who),
        # ("prot-1933--fk--5.xml", 1, 1, segment.MergeSpeechStrategyType.WhoSequence),
        ("prot-1955--ak--22.xml", 147, 147, segment.MergeSpeechStrategyType.chain),
        # ("prot-1955--ak--22.xml", 53, 53, segment.MergeSpeechStrategyType.Who),
        # ("prot-1955--ak--22.xml", 149, 149, segment.MergeSpeechStrategyType.WhoSequence),
        ('prot-199192--127.xml', 222, 222, segment.MergeSpeechStrategyType.chain),
        ('prot-199192--127.xml', 49, 49, segment.MergeSpeechStrategyType.who),
        ('prot-199192--127.xml', 208, 208, segment.MergeSpeechStrategyType.who_sequence),
        ('prot-199192--127.xml', 208, 208, segment.MergeSpeechStrategyType.who_speaker_hash_sequence),
        # ('prot-199192--127.xml', 208, 208, segment.MergeSpeechStrategyType.speaker_hash_sequence),
    ],
)
def test_protocol_to_speeches_with_different_strategies(
    filename: str, speech_count: int, non_empty_speech_count: int, strategy: str
):

    path: str = jj(PARLACLARIN_SOURCE_FOLDER, "protocols", filename.split('-')[1], filename)
    document_name: str = utility.strip_path_and_extension(filename)

    protocol: interface.Protocol = parlaclarin.ProtocolMapper.to_protocol(path)

    speeches = segment.to_speeches(protocol=protocol, merge_strategy=strategy, segment_skip_size=0)
    assert len(speeches) == speech_count, "speech count"

    speeches = segment.to_speeches(protocol=protocol, merge_strategy=strategy, segment_skip_size=1)
    assert len(speeches) == non_empty_speech_count

    assert all(x.text != "" for x in speeches)
    assert document_name == protocol.name
    assert protocol.date is not None
    assert protocol.has_text == any(x.text != "" for x in speeches)

    speeches = segment.to_speeches(protocol=protocol, merge_strategy=strategy, segment_skip_size=3)
    assert all(len(x.text) >= 3 for x in speeches)

    speeches = segment.to_speeches(protocol=protocol, merge_strategy=strategy, segment_skip_size=100)
    assert all(len(x.text) >= 100 for x in speeches)


@pytest.mark.parametrize(
    'filename, expected_speech_count',
    [
        ('prot-199192--21.xml', 0),
    ],
)
def test_to_speeches_with_faulty_attribute(filename, expected_speech_count):
    path: str = jj(PARLACLARIN_SOURCE_FOLDER, "protocols", filename.split('-')[1], filename)

    data = untangle.parse(path)

    protocol = parlaclarin.ProtocolMapper.to_protocol(data, segment_skip_size=0)
    speeches = segment.to_speeches(protocol=protocol, merge_strategy=segment.MergeSpeechStrategyType.chain)
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
                prev_id=None,
                next_id='i-2',
                paragraphs=['Hej! Detta är en mening.'],
                tagged_text="token\tpos\tlemma\nA\ta\tNN",
                delimiter='\n',
                page_number='',
            )
        ],
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
    filenames: List[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: List[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    assert len(protocols) == 5


def test_load_protocols_from_folder():
    protocols: List[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=TAGGED_SOURCE_FOLDER)]
    assert len(protocols) == 5


@pytest.mark.parametrize(
    'protocol_name,merge_strategy,expected_utterance_count,expected_speech_count',
    [
        ('prot-1955--ak--22', 'who_sequence', 414, 146),
        ('prot-1955--ak--22', 'who_speaker_hash_sequence', 414, 151),
        ('prot-1955--ak--22', 'speaker_hash_sequence', 414, 151),
        ('prot-1955--ak--22', 'chain', 414, 151),
        ('prot-199192--127', 'who_sequence', 274, 208),
        ('prot-199192--127', 'who_speaker_hash_sequence', 274, 222),
        ('prot-199192--127', 'speaker_hash_sequence', 274, 222),
        ('prot-199192--127', 'chain', 274, 222),
    ],
)
def test_protocol_to_items(
    protocol_name: str, merge_strategy: str, expected_utterance_count: int, expected_speech_count: int
):

    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')

    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)

    assert protocol is not None
    assert len(protocol.utterances) == expected_utterance_count

    items = segment.to_segments(
        protocol=protocol,
        content_type=interface.ContentType.Text,
        segment_level=interface.SegmentLevel.Speech,
        merge_strategy=merge_strategy,
    )
    assert len(items) == expected_speech_count


@pytest.mark.skip(reason="Infrastructure test")
@pytest.mark.parametrize('protocol_name', ['prot-199192--21', 'prot-199192--127', 'prot-1933--fk--5', 'prot-1955--ak--22', 'prot-199596--35'])
def test_protocol_to_speeches(protocol_name: str):

    filename: str = jj(TAGGED_SOURCE_FOLDER, f'{protocol_name}.zip')

    protocol: interface.Protocol = tagged_corpus.load_protocol(filename=filename)
    utterances: pd.DataFrame = pd.DataFrame(
        data=[(x.u_id, x.who, x.next_id, x.prev_id, x.speaker_hash) for x in protocol.utterances],
        columns=['u_id', 'who', 'next_id', 'prev_id', 'speaker_hash'],
    )
    for merge_strategy in ['who_sequence', 'who_speaker_hash_sequence', 'speaker_hash_sequence', 'chain']:

        merger: segment.IMergeSpeechStrategy = segment.SpeechMergerFactory.get(merge_strategy)

        items: list[list[interface.Utterance]] = merger.split(protocol.utterances)

        speech_ids = []
        for i, item in enumerate(items):
            speech_ids.extend(len(item) * [i])

        utterances[merge_strategy] = speech_ids

    utterances.to_excel(f"utterances_{protocol_name}.xlsx")


def test_protocols_to_items():
    filenames: List[str] = glob.glob(TAGGED_SOURCE_PATTERN, recursive=True)
    protocols: List[interface.Protocol] = [p for p in tagged_corpus.load_protocols(source=filenames)]
    _ = itertools.chain(
        p.to_segments(content_type=interface.ContentType.Text, segment_level=interface.SegmentLevel.Who)
        for p in protocols
    )
