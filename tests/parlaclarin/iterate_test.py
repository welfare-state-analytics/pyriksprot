import glob
import os
from typing import Iterable, List

import pytest

from pyriksprot import interface
from pyriksprot import parlaclarin, segment, utility

from ..utility import PARLACLARIN_FAKE_FOLDER, PARLACLARIN_SOURCE_PATTERN

jj = os.path.join


@pytest.mark.parametrize(
    'iterator_class',
    [
        parlaclarin.XmlProtocolSegmentIterator,
        parlaclarin.XmlUntangleSegmentIterator,
        parlaclarin.XmlSaxSegmentIterator,
    ],
)
def test_protocol_texts_iterator_metadata(iterator_class):

    filenames = glob.glob(PARLACLARIN_SOURCE_PATTERN, recursive=True)
    expected_document_names = sorted(utility.strip_path_and_extension(filenames))

    texts: Iterable[segment.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None)
    )

    assert len(texts) == 6
    assert [x.name for x in texts] == expected_document_names
    assert all(x.who is None for x in texts)
    assert [x.id for x in texts] == expected_document_names
    assert all(x.page_number == '0' for x in texts)


def test_xml_protocol_texts_iterator_texts():

    filenames: List[str] = sorted(glob.glob(PARLACLARIN_SOURCE_PATTERN, recursive=True))
    expected_document_names: List[str] = sorted(utility.strip_path_and_extension(filenames))

    texts: Iterable[segment.ProtocolSegment] = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None
        )
    )
    p_texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=2
        )
    )

    assert set(p.data for p in p_texts) == set(p.data for p in texts)

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level='protocol',
            segment_skip_size=0,
            multiproc_processes=None,
            multiproc_keep_order=True,
        )
    )
    assert {x.name for x in texts} == set(expected_document_names)

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=None, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 5

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Speech,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    assert len(texts) == 395

    texts = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert len(texts) == 111

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Utterance,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames,
            segment_level=interface.SegmentLevel.Utterance,
            segment_skip_size=1,
            multiproc_processes=None,
        )
    )
    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames, segment_level=interface.SegmentLevel.Who, segment_skip_size=1, multiproc_processes=None
        )
    )
    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))

    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)


EXPECTED_STREAM: list[segment.ProtocolSegment] = {
    'protocol': [
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Protocol,
            name='prot-1958-fake',
            who=None,
            id='prot-1958-fake',
            u_id=None,
            data='Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?\nJag heter Adam.\nOve är dum.',
            page_number='0',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Protocol,
            name='prot-1960-fake',
            who=None,
            id='prot-1960-fake',
            u_id=None,
            data='Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?\nJag håller med.\nTalmannen är snäll.\nJag håller också med.',
            page_number='0',
            year=1960,
        ),
    ],
    'speech': [
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Speech,
            name='prot-1958-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
            page_number='0',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Speech,
            name='prot-1958-fake_002',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag heter Adam.',
            page_number='1',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Speech,
            name='prot-1958-fake_003',
            who='B',
            id='i-4',
            u_id='i-4',
            data='Ove är dum.',
            page_number='1',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Speech,
            name='prot-1960-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?',
            page_number='0',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            segment_level=interface.SegmentLevel.Speech,
            content_type=interface.ContentType.Text,
            name='prot-1960-fake_002',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag håller med.\nTalmannen är snäll.',
            page_number='1',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Speech,
            name='prot-1960-fake_003',
            who='C',
            id='i-4',
            u_id='i-4',
            data='Jag håller också med.',
            page_number='1',
            year=1960,
        ),
    ],
    'who': [
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Who,
            name='prot-1958-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Hej! Detta är en mening.\nJag heter Ove.\nVad heter du?',
            page_number='0',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Who,
            name='prot-1958-fake_002',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag heter Adam.\nOve är dum.',
            page_number='1',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Who,
            name='prot-1960-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Herr Talman! Jag talar.\nDet regnar ute.\nVisste du det?',
            page_number='0',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Who,
            name='prot-1960-fake_002',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag håller med.\nTalmannen är snäll.',
            page_number='1',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Who,
            name='prot-1960-fake_003',
            who='C',
            id='i-4',
            u_id='i-4',
            data='Jag håller också med.',
            page_number='1',
            year=1960,
        ),
    ],
    'utterance': [
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1958-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Hej! Detta är en mening.',
            page_number='0',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1958-fake_002',
            who='A',
            id='i-2',
            u_id='i-2',
            data='Jag heter Ove.\nVad heter du?',
            page_number='0',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1958-fake_003',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag heter Adam.',
            page_number='1',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1958-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1958-fake_004',
            who='B',
            id='i-4',
            u_id='i-4',
            data='Ove är dum.',
            page_number='1',
            year=1958,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1960-fake_001',
            who='A',
            id='i-1',
            u_id='i-1',
            data='Herr Talman! Jag talar.',
            page_number='0',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1960-fake_002',
            who='A',
            id='i-2',
            u_id='i-2',
            data='Det regnar ute.\nVisste du det?',
            page_number='0',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1960-fake_003',
            who='B',
            id='i-3',
            u_id='i-3',
            data='Jag håller med.\nTalmannen är snäll.',
            page_number='1',
            year=1960,
        ),
        segment.ProtocolSegment(
            protocol_name='prot-1960-fake',
            content_type=interface.ContentType.Text,
            segment_level=interface.SegmentLevel.Utterance,
            name='prot-1960-fake_004',
            who='C',
            id='i-4',
            u_id='i-4',
            data='Jag håller också med.',
            page_number='1',
            year=1960,
        ),
    ],
}


@pytest.mark.parametrize(
    'iterator_class',
    [
        # DEPRECATED parlaclarin.XmlProtocolSegmentIterator,
        parlaclarin.XmlUntangleSegmentIterator,
        # DEPRECATED parlaclarin.XmlSaxSegmentIterator,
    ],
)
def test_protocol_texts_iterator(iterator_class):

    document_names: List[str] = ['prot-1958-fake', 'prot-1960-fake']
    filenames: List[str] = [jj(PARLACLARIN_FAKE_FOLDER, f"{name}.xml") for name in document_names]

    segments: List[segment.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=None)
    )
    assert segments == EXPECTED_STREAM['protocol']

    segments: List[segment.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level='protocol', segment_skip_size=0, multiproc_processes=2)
    )
    assert set(t.data for t in segments) == set(t.data for t in EXPECTED_STREAM['protocol'])


@pytest.mark.parametrize(
    'segment_level',
    [
        interface.SegmentLevel.Protocol,
        interface.SegmentLevel.Speech,
        interface.SegmentLevel.Who,
        interface.SegmentLevel.Utterance,
    ],
)
def test_protocol_texts_iterator_levels_compare(segment_level):

    filenames = [jj(PARLACLARIN_FAKE_FOLDER, f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]

    texts1 = list(
        parlaclarin.XmlUntangleSegmentIterator(
            filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None
        )
    )
    # with open('a.txt', 'w') as fp: fp.write(texts1[1].text)

    texts2 = list(
        parlaclarin.XmlSaxSegmentIterator(
            filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None
        )
    )
    # with open('b.txt', 'w') as fp: fp.write(texts2[1].text)

    assert all(''.join(texts1[i].data.split()) == ''.join(texts2[i].data.split()) for i in range(0, len(texts1)))


@pytest.mark.parametrize(
    'iterator_class, segment_level',
    [
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Protocol),
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Speech), # FIXME! Chain not working!!!!
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Who),
        # DEPRECATED (parlaclarin.XmlProtocolSegmentIterator, interface.SegmentLevel.Utterance),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Protocol),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Speech),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Who),
        (parlaclarin.XmlUntangleSegmentIterator, interface.SegmentLevel.Utterance),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Protocol),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Speech),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Who),
        # DEPRECATED (parlaclarin.XmlSaxSegmentIterator, interface.SegmentLevel.Utterance),
    ],
)
def test_protocol_texts_iterator_levels(iterator_class, segment_level: interface.SegmentLevel):

    filenames = [jj(PARLACLARIN_FAKE_FOLDER, f"{name}.xml") for name in ['prot-1958-fake', 'prot-1960-fake']]
    texts: list[segment.ProtocolSegment] = list(
        iterator_class(filenames=filenames, segment_level=segment_level, segment_skip_size=1, multiproc_processes=None)
    )
    assert len(texts) == len(EXPECTED_STREAM[segment_level])
    for t, x in zip(texts, EXPECTED_STREAM[segment_level]):
        assert t.protocol_name == x.protocol_name
        assert t.content_type == x.content_type
        assert t.segment_level == x.segment_level
        assert t.name == x.name
        assert t.who == x.who
        assert t.id == x.id
        assert t.u_id == x.u_id
        assert t.data == x.data
        assert t.page_number == x.page_number
        assert t.year == x.year
