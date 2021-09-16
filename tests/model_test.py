import os
import uuid
from typing import Callable, List

import pandas as pd
import pytest

from pyriksprot import model, parse, persist
from pyriksprot.foss import untangle

# pylint: disable=redefined-outer-name

jj = os.path.join

TAGGED_CSV_STR = (
    "token\tlemma\tpos\txpos\n"
    "Hej\thej\tIN\tIN\n"
    "!\t!\tMID\tMID\n"
    "Detta\tdetta\tPN\tPN.NEU.SIN.DEF.SUB+OBJ\n"
    "är\tvara\tVB\tVB.PRS.AKT\n"
    "ett\ten\tDT\tDT.NEU.SIN.IND\n"
    "test\ttest\tNN\tNN.NEU.SIN.IND.NOM\n"
    "!\t!\tMAD\tMAD\n"
    "'\t\tMAD\tMAD\n"
    '"\t\tMAD\tMAD'
)

UTTERANCES_DICTS = [
    {
        'u_id': 'i-1',
        'n': 'c01',
        'who': 'A',
        'prev_id': None,
        'next_id': 'i-2',
        'paragraphs': 'Hej! Detta är en mening.',
        'annotation': TAGGED_CSV_STR,
        'checksum': '107d28f2f90d3ccc',
    },
    {
        'u_id': 'i-2',
        'n': 'c02',
        'who': 'A',
        'prev_id': 'i-1',
        'next_id': None,
        'paragraphs': 'Jag heter Ove.@#@Vad heter du?',
        'annotation': TAGGED_CSV_STR,
        'checksum': '9c3ee2212f9db2eb',
    },
    {
        'u_id': 'i-3',
        'n': 'c03',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Jag heter Adam.',
        'annotation': TAGGED_CSV_STR,
        'checksum': '8a2880190e158a8a',
    },
    {
        'u_id': 'i-4',
        'n': 'c03',
        'who': 'B',
        'prev_id': None,
        'next_id': None,
        'paragraphs': 'Ove är dum.',
        'annotation': TAGGED_CSV_STR,
        'checksum': '13ed9d8bf4098390',
    },
]


def create_utterances() -> List[model.Utterance]:
    return [
        model.Utterance(
            u_id='i-1',
            n='c01',
            who='A',
            prev_id=None,
            next_id='i-2',
            paragraphs=['Hej! Detta är en mening.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        model.Utterance(
            u_id='i-2',
            n='c02',
            who='A',
            prev_id='i-1',
            next_id=None,
            paragraphs=['Jag heter Ove.', 'Vad heter du?'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        model.Utterance(
            u_id='i-3',
            n='c03',
            who='B',
            prev_id=None,
            next_id=None,
            paragraphs=['Jag heter Adam.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
        model.Utterance(
            u_id='i-4',
            n='c03',
            who='B',
            prev_id=None,
            next_id=None,
            paragraphs=['Ove är dum.'],
            delimiter='\n',
            annotation=TAGGED_CSV_STR,
        ),
    ]


@pytest.fixture(scope='module')
def utterances() -> List[model.Utterance]:
    return create_utterances()


def test_utterance_text():
    u: model.Utterance = model.Utterance(u_id="A", paragraphs=["X", "Y", "C"])
    assert u.text == '\n'.join(["X", "Y", "C"])


def test_utterance_checksumtext():
    u: model.Utterance = model.Utterance(u_id="A", paragraphs=["X", "Y", "C"])
    assert u.checksum() == '6060d006e0494206'


def test_utterances_to_dict(utterances: List[model.Utterance]):

    data = model.Utterances.to_dict(utterances)
    assert data == UTTERANCES_DICTS


def test_utterances_to_csv(utterances: List[model.Utterance]):

    data: str = model.Utterances.to_csv(utterances)
    loaded_utterances = model.Utterances.from_csv(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_json(utterances: List[model.Utterance]):

    data: str = model.Utterances.to_json(utterances)
    loaded_utterances = model.Utterances.from_json(data)
    assert [x.__dict__ for x in utterances] == [x.__dict__ for x in loaded_utterances]


def test_utterances_to_pandas(utterances: List[model.Utterance]):

    data: pd.DataFrame = model.Utterances.to_dataframe(utterances)
    assert data.reset_index().to_dict(orient='record') == UTTERANCES_DICTS


def test_protocol_create(utterances: List[model.Utterance]):

    protocol: model.Protocol = model.Protocol(date="1958", name="prot-1958-fake", utterances=utterances)

    assert protocol is not None
    assert len(protocol.utterances) == 4
    assert len(protocol) == 4
    assert protocol.name == "prot-1958-fake"
    assert protocol.date == "1958"

    assert protocol.name == 'prot-1958-fake'
    assert protocol.date == '1958'
    assert protocol.has_text(), 'has text'
    assert protocol.checksum() == 'c2e64f5dead6d180c1f05316811742e55601d625', 'checksum'

    assert protocol.text == '\n'.join(text.text for text in utterances)


def test_protocol_preprocess():
    """Modifies utterances:"""
    utterances: List[model.Utterance] = create_utterances()

    protocol: model.Protocol = model.Protocol(date="1950", name="prot-1958-fake", utterances=utterances)

    preprocess: Callable[[str], str] = lambda t: 'APA'

    protocol.preprocess(preprocess=preprocess)

    assert protocol.text == "APA\nAPA\nAPA\nAPA\nAPA"


@pytest.mark.parametrize(
    'cls, strategy, expected_count, expected_whos, expected_ids, expected_texts',
    [
        (
            model.MergeSpeechByWho,
            'who',
            2,
            [{'A'}, {'B'}],
            ['A', 'B'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.\nOve är dum.',
            ],
        ),
        (
            model.MergeSpeechById,
            'n',
            3,
            [{'A'}, {'A'}, {'B'}],
            ['c01', 'c02', 'c03'],
            [
                'Hej! Detta är en mening.',
                'Jag heter Ove.\nVad heter du?',
                'Jag heter Adam.\nOve är dum.',
            ],
        ),
        (
            model.MergeSpeechByChain,
            'chain',
            3,
            [{'A'}, {'B'}, {'B'}],
            ['i-1', 'i-3', 'i-4'],
            [
                'Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
                'Jag heter Adam.',
                'Ove är dum.',
            ],
        ),
    ],
)
def test_merge_speech_by_strategy(
    utterances: List[model.Utterance], cls, strategy, expected_count, expected_whos, expected_ids, expected_texts
):

    protocol: model.Protocol = model.Protocol(date="1950", name="prot-1958-fake", utterances=utterances)

    for speeches in [
        cls().speeches(protocol=protocol, skip_size=0),
        protocol.to_speeches(merge_strategy=strategy, skip_size=0),
    ]:

        assert len(speeches) == expected_count

        for i, speech in enumerate(speeches):
            assert speech.speech_date == protocol.date
            assert speech.document_name == protocol.name
            assert speech.speech_name.startswith(protocol.name)
            assert speech.delimiter == '\n'
            assert speech.speech_index == i + 1
            for model.utterance in speech.utterances:
                assert model.utterance.who == speech.speaker

        assert [s.text for s in speeches] == expected_texts

        assert [{u.who for u in s.utterances} for s in speeches] == expected_whos
        assert [s.speech_id for s in speeches] == expected_ids


def test_speech_annotation():

    utterances: List[model.Utterance] = [
        model.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        model.Utterance(u_id='i-2', who="apa", annotation='header\nC\nD'),
        model.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
    ]
    speech = model.Speech(
        document_name="prot-apa",
        speech_id="s-1",
        speaker="apa",
        speech_date="1999",
        speech_index=1,
        page_number="0",
        utterances=utterances,
    )

    assert speech.annotation == 'header\nA\nB\nC\nD\nE\nF'

    utterances: List[model.Utterance] = [
        model.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        model.Utterance(u_id='i-2', who="apa", annotation='header'),
        model.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
    ]
    speech = model.Speech(
        document_name="prot-apa",
        speech_id="s-1",
        speaker="apa",
        speech_date="1999",
        speech_index=1,
        page_number="0",
        utterances=utterances,
    )

    assert speech.annotation == 'header\nA\nB\nE\nF'

    utterances: List[model.Utterance] = [
        model.Utterance(u_id='i-1', who="apa", annotation='header\nA\nB'),
        model.Utterance(u_id='i-2', who="apa", annotation='header\n'),
        model.Utterance(u_id='i-3', who="apa", annotation='header\nE\nF'),
    ]
    speech = model.Speech(
        document_name="prot-apa",
        speech_id="s-1",
        speaker="apa",
        speech_date="1999",
        speech_index=1,
        page_number="0",
        utterances=utterances,
    )

    assert speech.annotation == 'header\nA\nB\nE\nF'

    # Test file ending with NL


@pytest.mark.parametrize(
    'filename, speech_count, non_empty_speech_count, strategy',
    [
        ("prot-1933--fk--5.xml", 1, 1, 'chain'),
        ("prot-1955--ak--22.xml", 82, 79, 'chain'),
        ('prot-199192--127.xml', 206, 206, 'chain'),
        ("prot-1933--fk--5.xml", 1, 1, 'n'),
        ("prot-1955--ak--22.xml", 33, 32, 'n'),
        ('prot-199192--127.xml', 51, 51, 'n'),
        ("prot-1933--fk--5.xml", 1, 1, 'who'),
        ("prot-1955--ak--22.xml", 33, 32, 'who'),
        ('prot-199192--127.xml', 51, 51, 'who'),
    ],
)
def test_protocol_to_speeches_with_different_strategies(
    filename: str, speech_count: int, non_empty_speech_count: int, strategy: str
):

    protocol: model.Protocol = parse.ProtocolMapper.to_protocol(jj("tests", "test_data", "xml", filename))

    speeches = protocol.to_speeches(merge_strategy=strategy, skip_size=0)
    assert len(speeches) == speech_count, "speech count"

    speeches = protocol.to_speeches(merge_strategy=strategy, skip_size=1)
    assert len(speeches) == non_empty_speech_count

    assert all(x.text != "" for x in speeches)
    assert os.path.splitext(filename)[0] == protocol.name
    assert protocol.date is not None
    assert protocol.has_text() == any(x.text != "" for x in speeches)

    speeches = protocol.to_speeches(merge_strategy=strategy, skip_size=3)
    assert all(len(x.text) > 3 for x in speeches)

    speeches = protocol.to_speeches(merge_strategy=strategy, skip_size=100)
    assert all(len(x.text) > 100 for x in speeches)


@pytest.mark.parametrize(
    'filename, expected_speech_count',
    [
        ('prot-199192--21.xml', 0),
    ],
)
def test_to_speeches_with_faulty_attribute(filename, expected_speech_count):

    data = untangle.parse(jj("tests", "test_data", "xml", filename))

    protocol = parse.ProtocolMapper.to_protocol(data, skip_size=0)
    speeches = protocol.to_speeches(merge_strategy='n')
    assert len(speeches) != expected_speech_count, "speech length"


@pytest.mark.parametrize('storage_format', ['json', 'csv'])
def test_store_protocols(storage_format: str):
    protocol: model.Protocol = model.Protocol(
        name='prot-1958-fake',
        date='1958',
        utterances=[
            model.Utterance(
                u_id='i-1',
                n='c01',
                who='A',
                prev_id=None,
                next_id='i-2',
                paragraphs=['Hej! Detta är en mening.'],
                annotation="token\tpos\tlemma\nA\ta\tNN",
                delimiter='\n',
            )
        ],
    )

    output_filename: str = jj("tests", "output", f"{str(uuid.uuid4())}.zip")

    persist.store_protocol(
        output_filename,
        protocol=protocol,
        storage_format=storage_format,
        checksum='apa',
    )

    assert os.path.isfile(output_filename)

    metadata: dict = persist.load_metadata(output_filename)

    assert metadata is not None

    loaded_protocol: model.Protocol = persist.load_protocol(output_filename)

    assert protocol.name == loaded_protocol.name
    assert protocol.date == loaded_protocol.date
    assert [u.__dict__ for u in protocol.utterances] == [u.__dict__ for u in loaded_protocol.utterances]

    # os.unlink(output_filename)
